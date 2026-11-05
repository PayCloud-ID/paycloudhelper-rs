//! Driven-reconnect pool: one single-service [`Channel`] per address slot.
//!
//! The `Channel` that [`GrpcClientFactory::connect`](crate::GrpcClientFactory::connect)
//! returns is a tower p2c balancer over tonic `Reconnect` services. A service
//! whose connection died re-dials *inside the polling of a request*; when
//! another service is ready, p2c serves the request from it and the re-dial is
//! left unpolled — TCP completes, the HTTP/2 preface is never written, and a
//! server with a short `ConnectionTimeout` closes the orphan. The next request
//! that lands on it is dropped unsent (`hyper::Error(Canceled, "connection
//! closed")`). That is PA-498.
//!
//! Here each slot is its own single-service channel, so a request that finds
//! its connection dead has no ready alternative inside that channel: it waits
//! for (and so drives) its own re-dial. Slots are picked round-robin, and an
//! address whose call just failed at transport level is avoided for a short
//! penalty.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tonic::body::Body;
use tonic::codegen::http::{Request, Response};
use tonic::transport::channel::Change;
use tonic::transport::{Channel, Endpoint};
use tower::{Service, ServiceExt};

use crate::discovery::EndpointKey;

/// How long an address is avoided after a transport-level failure.
pub(crate) const DEFAULT_PENALTY: Duration = Duration::from_secs(5);

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The peer a slot dials. Slots of one peer share their penalty: a pod that
/// went away is dead for every slot, so one failed call sidelines all of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Peer {
    /// The plain host-name seed (or the static, non-discovery pool).
    Seed,
    Addr(SocketAddr),
}

fn peer(key: EndpointKey) -> Peer {
    match key {
        EndpointKey::Seed(_) => Peer::Seed,
        EndpointKey::Addr(addr, _) => Peer::Addr(addr),
    }
}

#[derive(Debug)]
struct Slot<T> {
    key: EndpointKey,
    value: T,
    /// Milliseconds since [`SlotTable::base`] until which this peer is avoided.
    bad_until_ms: Arc<AtomicU64>,
}

/// Handle used to mark the peer a call went to as recently failed.
#[derive(Debug, Clone)]
pub(crate) struct Mark {
    bad_until_ms: Arc<AtomicU64>,
    base: Instant,
    penalty: Duration,
}

impl Mark {
    pub(crate) fn penalize(&self) {
        self.bad_until_ms.store(
            millis(self.base.elapsed() + self.penalty),
            Ordering::Relaxed,
        );
    }
}

fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

#[derive(Debug)]
pub(crate) struct SlotTable<T> {
    slots: RwLock<Vec<Slot<T>>>,
    cursor: AtomicUsize,
    base: Instant,
    penalty: Duration,
}

impl<T: Clone> SlotTable<T> {
    pub(crate) fn new(penalty: Duration) -> Self {
        Self {
            slots: RwLock::new(Vec::new()),
            cursor: AtomicUsize::new(0),
            base: Instant::now(),
            penalty,
        }
    }

    /// Add `value` under `key`, replacing a slot with the same key. A new slot
    /// joins its peer's penalty clock if the peer already has slots.
    pub(crate) fn insert(&self, key: EndpointKey, value: T) {
        let mut slots = self.slots.write().expect("slot table poisoned");
        slots.retain(|s| s.key != key);
        let bad_until_ms = slots
            .iter()
            .find(|s| peer(s.key) == peer(key))
            .map_or_else(|| Arc::new(AtomicU64::new(0)), |s| s.bad_until_ms.clone());
        slots.push(Slot {
            key,
            value,
            bad_until_ms,
        });
    }

    pub(crate) fn remove(&self, key: &EndpointKey) {
        self.slots
            .write()
            .expect("slot table poisoned")
            .retain(|s| s.key != *key);
    }

    pub(crate) fn len(&self) -> usize {
        self.slots.read().expect("slot table poisoned").len()
    }

    /// Next healthy slot in round-robin order; if every slot is penalised, the
    /// plain round-robin slot (a degraded pool still serves). `None` when empty.
    pub(crate) fn pick(&self) -> Option<(T, Mark)> {
        let slots = self.slots.read().expect("slot table poisoned");
        let n = slots.len();
        if n == 0 {
            return None;
        }
        let now = millis(self.base.elapsed());
        let start = self.cursor.fetch_add(1, Ordering::Relaxed);
        let chosen = (0..n)
            .map(|i| &slots[(start + i) % n])
            .find(|s| s.bad_until_ms.load(Ordering::Relaxed) <= now)
            .unwrap_or(&slots[start % n]);
        Some((
            chosen.value.clone(),
            Mark {
                bad_until_ms: chosen.bad_until_ms.clone(),
                base: self.base,
                penalty: self.penalty,
            },
        ))
    }
}

/// Errors produced by the pool itself (not by a slot's transport).
#[derive(Debug)]
pub enum PoolError {
    /// No slot exists yet (discovery has not inserted any address).
    NoEndpoints,
}

impl std::fmt::Display for PoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoEndpoints => f.write_str("pc-grpc: no gRPC endpoint is available yet"),
        }
    }
}

impl std::error::Error for PoolError {}

/// A client channel that owns one single-service [`Channel`] per address slot
/// and drives each reconnect itself (PA-498; see the module docs).
///
/// Implements `tower::Service<http::Request<tonic::body::Body>>`, so every
/// tonic-generated client accepts it in place of a `Channel`
/// (`HealthClient::new(pooled)`). Cheap to clone; the slots, the discovery
/// driver and its refresher end when the last clone is dropped.
#[derive(Clone)]
pub struct PooledChannel {
    shared: Arc<Shared>,
}

struct Shared {
    table: SlotTable<Channel>,
    /// Dropped with the last clone, which stops the driver.
    _shutdown: oneshot::Sender<()>,
}

impl std::fmt::Debug for PooledChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PooledChannel")
            .field("slots", &self.shared.table.len())
            .finish()
    }
}

impl PooledChannel {
    /// Build a pool over `initial` slots and spawn the driver that applies the
    /// discovery change stream `rx` to it. Must run inside a Tokio runtime.
    pub(crate) fn spawn(
        initial: Vec<(EndpointKey, Endpoint)>,
        rx: mpsc::Receiver<Change<EndpointKey, Endpoint>>,
    ) -> Self {
        let (shutdown, stop) = oneshot::channel();
        let shared = Arc::new(Shared {
            table: SlotTable::new(DEFAULT_PENALTY),
            _shutdown: shutdown,
        });
        for (key, endpoint) in initial {
            shared.table.insert(key, endpoint.connect_lazy());
        }
        tokio::spawn(drive(rx, Arc::downgrade(&shared), stop));
        Self { shared }
    }

    /// Number of slots currently in the pool.
    #[cfg(test)]
    pub(crate) fn slot_count(&self) -> usize {
        self.shared.table.len()
    }
}

impl Service<Request<Body>> for PooledChannel {
    type Response = Response<Body>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Readiness is per slot, awaited inside `call`.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let picked = self.shared.table.pick();
        Box::pin(async move {
            let (channel, mark) =
                picked.ok_or_else(|| Box::new(PoolError::NoEndpoints) as BoxError)?;
            // `oneshot` = poll_ready + call on the slot's own channel: a dead
            // connection is re-dialled *by this request*, which waits for it.
            match channel.oneshot(req).await {
                Ok(resp) => Ok(resp),
                Err(err) => {
                    mark.penalize();
                    Err(Box::new(err) as BoxError)
                }
            }
        })
    }
}

/// Applies the discovery change stream to the slot table until the last
/// [`PooledChannel`] clone is dropped. Unlike tower's balancer this drains
/// continuously, so discovery does not depend on a request polling it.
async fn drive(
    mut rx: mpsc::Receiver<Change<EndpointKey, Endpoint>>,
    shared: Weak<Shared>,
    mut stop: oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = &mut stop => return,
            change = rx.recv() => {
                let Some(change) = change else { return };
                let Some(shared) = shared.upgrade() else { return };
                match change {
                    Change::Insert(key, endpoint) => shared.table.insert(key, endpoint.connect_lazy()),
                    Change::Remove(key) => shared.table.remove(&key),
                }
            }
        }
    }
}

#[cfg(test)]
mod table_tests {
    use super::*;

    fn addr(i: usize) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, u8::try_from(i + 1).unwrap()], 9106))
    }

    /// `n` distinct peers, one slot each, holding the value `i`.
    fn table(n: usize, penalty: Duration) -> SlotTable<usize> {
        let t = SlotTable::new(penalty);
        for i in 0..n {
            t.insert(EndpointKey::Addr(addr(i), 0), i);
        }
        t
    }

    #[test]
    fn picks_round_robin() {
        let t = table(3, Duration::from_secs(5));
        let got: Vec<usize> = (0..6).map(|_| t.pick().unwrap().0).collect();
        assert_eq!(got, vec![0, 1, 2, 0, 1, 2]);
    }

    #[test]
    fn skips_a_penalised_slot_while_another_is_healthy() {
        let t = table(2, Duration::from_secs(5));
        let (v, mark) = t.pick().unwrap();
        assert_eq!(v, 0);
        mark.penalize();
        let got: Vec<usize> = (0..4).map(|_| t.pick().unwrap().0).collect();
        assert_eq!(got, vec![1, 1, 1, 1]);
    }

    #[test]
    fn a_penalty_covers_every_slot_of_the_same_peer() {
        let t = SlotTable::new(Duration::from_secs(5));
        t.insert(EndpointKey::Addr(addr(0), 0), 0);
        t.insert(EndpointKey::Addr(addr(0), 1), 1);
        t.insert(EndpointKey::Addr(addr(1), 0), 2);
        t.insert(EndpointKey::Addr(addr(1), 1), 3);
        let (v, mark) = t.pick().unwrap();
        assert_eq!(v, 0);
        mark.penalize();
        // Slot 1 is the same pod as slot 0: both are skipped.
        let got: Vec<usize> = (0..4).map(|_| t.pick().unwrap().0).collect();
        assert!(got.iter().all(|v| *v >= 2), "{got:?}");
    }

    #[test]
    fn never_refuses_while_a_slot_exists_even_if_all_are_penalised() {
        let t = table(2, Duration::from_secs(5));
        for _ in 0..2 {
            t.pick().unwrap().1.penalize();
        }
        assert!(t.pick().is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn a_penalty_expires() {
        let t = table(2, Duration::from_secs(5));
        t.pick().unwrap().1.penalize(); // slot 0 bad
        tokio::time::advance(Duration::from_secs(6)).await;
        let got: Vec<usize> = (0..2).map(|_| t.pick().unwrap().0).collect();
        assert!(
            got.contains(&0),
            "slot 0 must be eligible again after the penalty"
        );
    }

    #[test]
    fn empty_table_picks_nothing_and_remove_drops_the_slot() {
        let t = table(1, Duration::from_secs(5));
        t.remove(&EndpointKey::Addr(addr(0), 0));
        assert!(t.pick().is_none());
        assert_eq!(t.len(), 0);
    }

    #[test]
    fn insert_with_an_existing_key_replaces_it() {
        let t = table(1, Duration::from_secs(5));
        t.insert(EndpointKey::Addr(addr(0), 0), 7);
        assert_eq!(t.len(), 1);
        assert_eq!(t.pick().unwrap().0, 7);
    }
}
