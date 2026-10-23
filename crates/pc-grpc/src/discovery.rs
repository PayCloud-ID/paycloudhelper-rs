//! DNS-aware endpoint discovery for headless Services.
//!
//! A plain `vec![endpoint; pool]` hands tonic one DNS name; every subchannel
//! resolves it once, at connect time, and then stays on that pod for good. A
//! caller that connected while the upstream had one pod therefore never sees
//! the second one. Here the host is resolved to its A records instead: `pool`
//! balanced endpoints are kept **per address**, and a background task re-resolves
//! on an interval, inserting new addresses and removing gone ones.
//!
//! The address diff ([`diff_addrs`]) is pure; the refresher ([`run_refresher`])
//! is generic over a [`Resolver`] so its tests need no DNS.

use std::collections::BTreeSet;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::mpsc::Sender;
use tokio::time::{interval_at, Instant, MissedTickBehavior};
use tonic::transport::channel::Change;
use tonic::transport::Endpoint;

/// Key of one balanced endpoint: the pod address plus its slot within the
/// per-address pool of `pool` connections.
pub type EndpointKey = (SocketAddr, usize);

/// Upper bound on one DNS lookup. `getaddrinfo` runs on the blocking pool and
/// can hang; a stuck lookup must not wedge the refresher forever.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);

/// Boxed future returned by [`Resolver::lookup`].
pub type LookupFuture<'a> =
    Pin<Box<dyn Future<Output = io::Result<BTreeSet<SocketAddr>>> + Send + 'a>>;

/// Resolves a `host:port` target to the set of addresses behind it.
pub trait Resolver: Send + Sync + 'static {
    /// Look `host` up and return one [`SocketAddr`] (with `port`) per record.
    fn lookup<'a>(&'a self, host: &'a str, port: u16) -> LookupFuture<'a>;
}

/// [`Resolver`] backed by the system resolver (`tokio::net::lookup_host`).
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemResolver;

impl Resolver for SystemResolver {
    fn lookup<'a>(&'a self, host: &'a str, port: u16) -> LookupFuture<'a> {
        Box::pin(async move {
            let addrs = tokio::net::lookup_host((host, port)).await?;
            Ok(addrs.collect())
        })
    }
}

/// Compare the addresses currently balanced over (`old`) with a fresh lookup
/// (`new`), returning `(to_insert, to_remove)`.
///
/// An **empty** `new` yields no change. A lookup that returns nothing is far
/// more likely a DNS hiccup (or every pod mid-rollout) than "the service has no
/// backends", and dropping the last endpoints would turn a blip into an outage.
pub fn diff_addrs(
    old: &BTreeSet<SocketAddr>,
    new: &BTreeSet<SocketAddr>,
) -> (Vec<SocketAddr>, Vec<SocketAddr>) {
    if new.is_empty() {
        return (Vec::new(), Vec::new());
    }
    (
        new.difference(old).copied().collect(),
        old.difference(new).copied().collect(),
    )
}

/// Re-resolve `host:port` every `refresh` and feed the difference to the
/// balancer behind `tx`, until the channel is dropped.
///
/// * `known` is the set already inserted (empty when the channel was created
///   without an initial lookup); `immediate` runs the first lookup at once
///   instead of after one `refresh`.
/// * `build` turns an address into a configured [`Endpoint`]; each address gets
///   `pool` of them keyed `(addr, 0..pool)`.
/// * A failed, timed-out or empty lookup changes nothing (see [`diff_addrs`]).
///   Inserts are sent before removes, so the balancer never goes empty on a
///   full address swap.
/// * Returns when `tx` is closed — the [`tonic::transport::Channel`] and all its
///   clones were dropped — so the task never outlives the channel. A tick is
///   raced against [`Sender::closed`], so it ends promptly, not at the next tick.
///
/// `refresh` must be non-zero.
#[allow(clippy::too_many_arguments)]
pub async fn run_refresher<F>(
    resolver: Arc<dyn Resolver>,
    host: String,
    port: u16,
    refresh: Duration,
    immediate: bool,
    pool: usize,
    mut known: BTreeSet<SocketAddr>,
    build: F,
    tx: Sender<Change<EndpointKey, Endpoint>>,
) where
    F: Fn(SocketAddr) -> Result<Endpoint>,
{
    let start = if immediate {
        Instant::now()
    } else {
        Instant::now() + refresh
    };
    let mut ticker = interval_at(start, refresh);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            () = tx.closed() => return,
            _ = ticker.tick() => {}
        }

        let found = match tokio::time::timeout(LOOKUP_TIMEOUT, resolver.lookup(&host, port)).await {
            Ok(Ok(found)) => found,
            Ok(Err(err)) => {
                tracing::warn!(host = %host, error = %err, "pc-grpc: discovery lookup failed; keeping current endpoints");
                continue;
            }
            Err(_) => {
                tracing::warn!(host = %host, "pc-grpc: discovery lookup timed out; keeping current endpoints");
                continue;
            }
        };

        let (added, gone) = diff_addrs(&known, &found);
        for addr in added {
            let endpoint = match build(addr) {
                Ok(endpoint) => endpoint,
                Err(err) => {
                    // Not recorded in `known`, so the next tick retries it.
                    tracing::warn!(host = %host, %addr, error = %err, "pc-grpc: discovery cannot build endpoint");
                    continue;
                }
            };
            for slot in 0..pool {
                if tx
                    .send(Change::Insert((addr, slot), endpoint.clone()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            known.insert(addr);
            tracing::info!(host = %host, "pc-grpc: discovery target={host} +{}", addr.ip());
        }
        for addr in gone {
            for slot in 0..pool {
                if tx.send(Change::Remove((addr, slot))).await.is_err() {
                    return;
                }
            }
            known.remove(&addr);
            tracing::info!(host = %host, "pc-grpc: discovery target={host} -{}", addr.ip());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(addrs: &[&str]) -> BTreeSet<SocketAddr> {
        addrs.iter().map(|a| a.parse().unwrap()).collect()
    }

    fn a(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    #[test]
    fn diff_inserts_new_and_removes_gone() {
        let old = set(&["10.0.0.1:9106"]);
        let new = set(&["10.0.0.1:9106", "10.0.0.2:9106"]);
        assert_eq!(diff_addrs(&old, &new), (vec![a("10.0.0.2:9106")], vec![]));
    }

    #[test]
    fn diff_removes_a_pod_that_left() {
        let old = set(&["10.0.0.1:9106", "10.0.0.2:9106"]);
        let new = set(&["10.0.0.2:9106"]);
        assert_eq!(diff_addrs(&old, &new), (vec![], vec![a("10.0.0.1:9106")]));
    }

    #[test]
    fn an_empty_lookup_keeps_everything() {
        let old = set(&["10.0.0.1:9106"]);
        assert_eq!(diff_addrs(&old, &BTreeSet::new()), (vec![], vec![]));
    }

    use std::collections::VecDeque;
    use std::sync::Mutex;
    use tokio::sync::mpsc;

    /// Replays scripted lookup results, repeating the last one forever.
    struct FakeResolver {
        script: Mutex<VecDeque<io::Result<Vec<SocketAddr>>>>,
        last: Mutex<Option<Vec<SocketAddr>>>,
    }

    impl FakeResolver {
        fn new(script: Vec<io::Result<Vec<SocketAddr>>>) -> Arc<Self> {
            Arc::new(Self {
                script: Mutex::new(script.into()),
                last: Mutex::new(None),
            })
        }
    }

    impl Resolver for FakeResolver {
        fn lookup<'a>(&'a self, _host: &'a str, _port: u16) -> LookupFuture<'a> {
            let next = self.script.lock().unwrap().pop_front();
            let out = match next {
                Some(Ok(v)) => {
                    *self.last.lock().unwrap() = Some(v.clone());
                    Ok(v.into_iter().collect())
                }
                Some(Err(e)) => Err(e),
                None => Ok(self
                    .last
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap_or_default()
                    .into_iter()
                    .collect()),
            };
            Box::pin(async move { out })
        }
    }

    fn build(addr: SocketAddr) -> Result<Endpoint> {
        Ok(Endpoint::from_shared(format!("http://{addr}"))?)
    }

    fn spawn_refresher(
        resolver: Arc<dyn Resolver>,
        pool: usize,
        known: &[&str],
    ) -> (
        tokio::task::JoinHandle<()>,
        mpsc::Receiver<Change<EndpointKey, Endpoint>>,
    ) {
        let (tx, rx) = mpsc::channel(64);
        let known = set(known);
        let handle = tokio::spawn(run_refresher(
            resolver,
            "svc-headless".into(),
            9106,
            Duration::from_secs(10),
            false,
            pool,
            known,
            build,
            tx,
        ));
        (handle, rx)
    }

    #[tokio::test(start_paused = true)]
    async fn refresher_inserts_a_pod_that_joined_after_connect() {
        let (a1, b) = (a("10.0.0.1:9106"), a("10.0.0.2:9106"));
        // Tick 1 still sees only the original pod; tick 2 sees the new one.
        let fake = FakeResolver::new(vec![Ok(vec![a1]), Ok(vec![a1, b])]);
        let (handle, mut rx) = spawn_refresher(fake, 2, &["10.0.0.1:9106"]);

        // Nothing for a: it is already known. First change is b's pool.
        let mut keys = Vec::new();
        for _ in 0..2 {
            match rx.recv().await.expect("insert for b") {
                Change::Insert(k, _) => keys.push(k),
                Change::Remove(k) => panic!("unexpected remove {k:?}"),
            }
        }
        assert_eq!(keys, vec![(b, 0), (b, 1)]);
        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn refresher_removes_a_pod_that_left_but_not_on_empty_or_error() {
        let (a1, b) = (a("10.0.0.1:9106"), a("10.0.0.2:9106"));
        let fake = FakeResolver::new(vec![
            Ok(vec![]),                        // empty: keep all
            Err(io::Error::other("servfail")), // error: keep all
            Ok(vec![b]),                       // a1 left
        ]);
        let (handle, mut rx) = spawn_refresher(fake, 1, &["10.0.0.1:9106", "10.0.0.2:9106"]);

        match rx.recv().await.expect("remove a1") {
            Change::Remove(k) => assert_eq!(k, (a1, 0)),
            Change::Insert(k, _) => panic!("unexpected insert {k:?}"),
        }
        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn refresher_ends_when_the_channel_is_dropped() {
        let fake = FakeResolver::new(vec![Ok(vec![a("10.0.0.1:9106")])]);
        let (handle, rx) = spawn_refresher(fake, 1, &["10.0.0.1:9106"]);
        drop(rx);
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("refresher must stop once the balancer is gone")
            .unwrap();
    }
}
