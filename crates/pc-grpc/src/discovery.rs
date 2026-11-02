//! DNS-aware endpoint discovery for headless Services.
//!
//! A plain `vec![endpoint; pool]` hands tonic one DNS name; every subchannel
//! resolves it once, at connect time, and then stays on that pod for good. A
//! caller that connected while the upstream had one pod therefore never sees
//! the second one. Here the host is resolved to its A records instead: `pool`
//! balanced endpoints are kept **per address**, and a background task re-resolves
//! on an interval, inserting new addresses and removing gone ones.
//!
//! Crate-private: the public entry points are
//! [`GrpcClientFactory::connect_discovered`](crate::GrpcClientFactory::connect_discovered)
//! and its lazy sibling. The address diff ([`diff_addrs`]) is pure; the
//! refresher ([`Refresher`]) is generic over a [`Resolver`] so its tests need
//! no DNS.

use std::collections::BTreeSet;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::Sender;
use tonic::transport::channel::Change;
use tonic::transport::Endpoint;

/// Key of one balanced endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum EndpointKey {
    /// Slot `n` of the plain host-name endpoint a lazy channel starts with, so
    /// a call never waits on an empty balancer. Removed once real addresses
    /// have been inserted.
    Seed(usize),
    /// Slot `n` (of `pool`) of one resolved pod address.
    Addr(SocketAddr, usize),
}

type Changes = Vec<Change<EndpointKey, Endpoint>>;

/// First retry delay while no lookup has succeeded yet; doubles up to `refresh`.
const FIRST_RETRY: Duration = Duration::from_secs(1);

/// Upper bound on one DNS lookup. `getaddrinfo` runs on the blocking pool and
/// can hang; a stuck lookup must not wedge the refresher forever.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);

/// Boxed future returned by [`Resolver::lookup`].
pub(crate) type LookupFuture<'a> =
    Pin<Box<dyn Future<Output = io::Result<BTreeSet<SocketAddr>>> + Send + 'a>>;

/// Resolves a `host:port` target to the set of addresses behind it.
pub(crate) trait Resolver: Send + Sync + 'static {
    /// Look `host` up and return one [`SocketAddr`] (with `port`) per record.
    fn lookup<'a>(&'a self, host: &'a str, port: u16) -> LookupFuture<'a>;
}

/// [`Resolver`] backed by the system resolver (`tokio::net::lookup_host`).
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SystemResolver;

impl Resolver for SystemResolver {
    fn lookup<'a>(&'a self, host: &'a str, port: u16) -> LookupFuture<'a> {
        Box::pin(async move {
            let addrs = tokio::net::lookup_host((host, port)).await?;
            Ok(prefer_ipv4(addrs.collect()))
        })
    }
}

/// Keep only the IPv4 addresses when there is any, otherwise all of them.
///
/// `lookup_host` returns A and AAAA records; on a dual-stack Service one pod
/// would be balanced twice (and the cluster here is single-stack IPv4).
pub(crate) fn prefer_ipv4(addrs: BTreeSet<SocketAddr>) -> BTreeSet<SocketAddr> {
    if addrs.iter().any(SocketAddr::is_ipv4) {
        addrs.into_iter().filter(SocketAddr::is_ipv4).collect()
    } else {
        addrs
    }
}

/// Compare the addresses currently balanced over (`old`) with a fresh lookup
/// (`new`), returning `(to_insert, to_remove)`.
///
/// An **empty** `new` yields no change. A lookup that returns nothing is far
/// more likely a DNS hiccup (or every pod mid-rollout) than "the service has no
/// backends", and dropping the last endpoints would turn a blip into an outage.
pub(crate) fn diff_addrs(
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

/// Outcome of handing one batch of changes to the balancer.
enum Sent {
    Ok,
    /// The change queue is full: stop for this tick, the next one recomputes.
    Full,
    /// The channel is gone: the refresher must exit.
    Closed,
}

/// Queue `changes` all-or-nothing, without ever waiting.
///
/// The balancer drains its change queue only while it is being polled, which on
/// an idle channel may be never. An awaiting `send` could therefore park the
/// refresher forever; `try_reserve_many` either takes every slot or none, so a
/// per-address pool is never half-applied.
fn send_batch(tx: &Sender<Change<EndpointKey, Endpoint>>, changes: Changes) -> Sent {
    match tx.try_reserve_many(changes.len()) {
        Ok(permits) => {
            for (permit, change) in permits.zip(changes) {
                permit.send(change);
            }
            Sent::Ok
        }
        Err(TrySendError::Full(())) => Sent::Full,
        Err(TrySendError::Closed(())) => Sent::Closed,
    }
}

/// Background re-resolution of one `host:port` into a balanced channel.
pub(crate) struct Refresher {
    pub(crate) resolver: Arc<dyn Resolver>,
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) refresh: Duration,
    /// Endpoints kept per address. Must not exceed the change-queue capacity.
    pub(crate) pool: usize,
    /// `EndpointKey::Seed(0..seed_slots)` placeholders currently in the
    /// balancer; removed after the first real insert (0 = none).
    pub(crate) seed_slots: usize,
    /// Addresses already inserted (empty when the channel started without an
    /// initial lookup).
    pub(crate) known: BTreeSet<SocketAddr>,
    pub(crate) tx: Sender<Change<EndpointKey, Endpoint>>,
}

impl Refresher {
    /// Re-resolve every `refresh` and feed the difference to the balancer until
    /// the channel is dropped. `refresh` must be non-zero.
    ///
    /// * `immediate` runs the first lookup at once instead of after one `refresh`.
    /// * While no lookup has succeeded (`known` empty) a failure is retried after
    ///   1s, 2s, 4s, … capped at `refresh`; once an address is known the normal
    ///   interval applies.
    /// * `build` turns an address into a configured [`Endpoint`].
    /// * A failed, timed-out or empty lookup changes nothing (see [`diff_addrs`]).
    ///   Inserts go first, removals only for addresses that leave at least one
    ///   endpoint behind, so the balancer is never emptied (a full swap where
    ///   every new address fails to build keeps the old ones).
    /// * Nothing here waits on the balancer: if its change queue is full the tick
    ///   stops and `known` reflects only what was actually queued, so the next
    ///   tick recomputes the diff.
    /// * Returns when the [`tonic::transport::Channel`] and all its clones are
    ///   dropped; a pending sleep is raced against [`Sender::closed`], so that is
    ///   prompt, not at the next tick.
    pub(crate) async fn run<F>(mut self, immediate: bool, build: F)
    where
        F: Fn(SocketAddr) -> Result<Endpoint>,
    {
        let mut first = immediate;
        let mut retry = FIRST_RETRY.min(self.refresh);

        loop {
            let delay = if first {
                first = false;
                Duration::ZERO
            } else if self.known.is_empty() {
                let d = retry;
                retry = (retry * 2).min(self.refresh);
                d
            } else {
                self.refresh
            };
            tokio::select! {
                () = self.tx.closed() => return,
                () = tokio::time::sleep(delay) => {}
            }

            let found = match tokio::time::timeout(
                LOOKUP_TIMEOUT,
                self.resolver.lookup(&self.host, self.port),
            )
            .await
            {
                Ok(Ok(found)) => found,
                Ok(Err(err)) => {
                    tracing::warn!(host = %self.host, error = %err, "pc-grpc: discovery lookup failed; keeping current endpoints");
                    continue;
                }
                Err(_) => {
                    tracing::warn!(host = %self.host, "pc-grpc: discovery lookup timed out; keeping current endpoints");
                    continue;
                }
            };

            if matches!(self.apply(&found, &build), Sent::Closed) {
                return;
            }
        }
    }

    fn apply<F>(&mut self, found: &BTreeSet<SocketAddr>, build: &F) -> Sent
    where
        F: Fn(SocketAddr) -> Result<Endpoint>,
    {
        let (added, gone) = diff_addrs(&self.known, found);

        for addr in added {
            let endpoint = match build(addr) {
                Ok(endpoint) => endpoint,
                Err(err) => {
                    // Not recorded in `known`, so the next tick retries it.
                    tracing::warn!(host = %self.host, %addr, error = %err, "pc-grpc: discovery cannot build endpoint");
                    continue;
                }
            };
            let batch = (0..self.pool)
                .map(|slot| Change::Insert(EndpointKey::Addr(addr, slot), endpoint.clone()))
                .collect();
            match send_batch(&self.tx, batch) {
                Sent::Ok => {
                    self.known.insert(addr);
                    tracing::info!(host = %self.host, added = %addr.ip(), "pc-grpc: discovery added endpoint");
                }
                other => return other,
            }
        }

        // `gone` is a subset of `known`, so this is what would remain.
        if self.known.len() > gone.len() {
            for addr in gone {
                let batch = (0..self.pool)
                    .map(|slot| Change::Remove(EndpointKey::Addr(addr, slot)))
                    .collect();
                match send_batch(&self.tx, batch) {
                    Sent::Ok => {
                        self.known.remove(&addr);
                        tracing::info!(host = %self.host, removed = %addr.ip(), "pc-grpc: discovery removed endpoint");
                    }
                    other => return other,
                }
            }
        } else if !gone.is_empty() {
            tracing::warn!(host = %self.host, "pc-grpc: discovery would remove every endpoint; keeping them");
        }

        if self.seed_slots > 0 && !self.known.is_empty() {
            let batch = (0..self.seed_slots)
                .map(|slot| Change::Remove(EndpointKey::Seed(slot)))
                .collect();
            match send_batch(&self.tx, batch) {
                Sent::Ok => self.seed_slots = 0,
                other => return other,
            }
        }
        Sent::Ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::net::TcpListener;
    use std::process::{Child, Command, Stdio};
    use std::time::Instant;
    use tonic_health::pb::health_client::HealthClient;
    use tonic_health::pb::HealthCheckRequest;

    static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

    type Rx = mpsc::Receiver<Change<EndpointKey, Endpoint>>;

    fn refresher(
        resolver: Arc<dyn Resolver>,
        pool: usize,
        known: &[&str],
        capacity: usize,
    ) -> (Refresher, Rx) {
        let (tx, rx) = mpsc::channel(capacity);
        let r = Refresher {
            resolver,
            host: "svc-headless".into(),
            port: 9106,
            refresh: Duration::from_secs(10),
            pool,
            seed_slots: 0,
            known: set(known),
            tx,
        };
        (r, rx)
    }

    fn spawn_refresher(
        resolver: Arc<dyn Resolver>,
        pool: usize,
        known: &[&str],
    ) -> (tokio::task::JoinHandle<()>, Rx) {
        let (r, rx) = refresher(resolver, pool, known, 64);
        (tokio::spawn(r.run(false, build)), rx)
    }

    /// Let the refresher task run until it is next idle.
    async fn settle() {
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
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
        assert_eq!(keys, vec![EndpointKey::Addr(b, 0), EndpointKey::Addr(b, 1)]);
        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn refresher_removes_a_pod_that_left_but_not_on_empty_or_error() {
        let (a1, b) = (a("10.0.0.1:9106"), a("10.0.0.2:9106"));
        let fake = FakeResolver::new(vec![
            Ok(vec![]),                        // t=10s: empty: keep all
            Err(io::Error::other("servfail")), // t=20s: error: keep all
            Ok(vec![b]),                       // t=30s: a1 left
        ]);
        let start = tokio::time::Instant::now();
        let (handle, mut rx) = spawn_refresher(fake, 1, &["10.0.0.1:9106", "10.0.0.2:9106"]);

        match rx.recv().await.expect("remove a1") {
            Change::Remove(k) => assert_eq!(k, EndpointKey::Addr(a1, 0)),
            Change::Insert(k, _) => panic!("unexpected insert {k:?}"),
        }
        // Not at the empty (10s) or failed (20s) lookup: only the third one acts.
        assert_eq!(start.elapsed(), Duration::from_secs(30));

        // Exactly one change, now and after two more identical lookups.
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        tokio::time::advance(Duration::from_secs(25)).await;
        settle().await;
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn a_swap_where_nothing_new_can_be_built_keeps_the_old_endpoints() {
        let (a1, b) = (a("10.0.0.1:9106"), a("10.0.0.2:9106"));
        // The answer is disjoint from `known`, and every new address fails to build.
        let fake = FakeResolver::new(vec![Ok(vec![b])]);
        let (r, mut rx) = refresher(fake, 1, &["10.0.0.1:9106"], 64);
        let handle = tokio::spawn(r.run(false, |_| Err(anyhow::anyhow!("cannot build"))));

        tokio::time::advance(Duration::from_secs(35)).await;
        settle().await;
        assert!(
            matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
            "no remove may be sent when no address would remain"
        );
        handle.abort();

        // Once the new address does build, the swap completes: insert, then remove.
        let fake = FakeResolver::new(vec![Ok(vec![b])]);
        let (handle, mut rx) = spawn_refresher(fake, 1, &["10.0.0.1:9106"]);
        match rx.recv().await.unwrap() {
            Change::Insert(k, _) => assert_eq!(k, EndpointKey::Addr(b, 0)),
            Change::Remove(k) => panic!("insert must come first, got remove {k:?}"),
        }
        match rx.recv().await.unwrap() {
            Change::Remove(k) => assert_eq!(k, EndpointKey::Addr(a1, 0)),
            Change::Insert(k, _) => panic!("unexpected insert {k:?}"),
        }
        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn a_full_change_queue_never_blocks_and_the_next_tick_catches_up() {
        let (a1, b) = (a("10.0.0.1:9106"), a("10.0.0.2:9106"));
        // Queue holds one address's pool (2); nobody drains it, like an idle channel.
        let fake = FakeResolver::new(vec![Ok(vec![a1, b])]);
        let (r, mut rx) = refresher(fake, 2, &[], 2);
        let handle = tokio::spawn(r.run(true, build));

        settle().await;
        // The refresher is not parked on a full queue: it is waiting for the next tick.
        assert!(!handle.is_finished());
        let mut got = Vec::new();
        while let Ok(Change::Insert(k, _)) = rx.try_recv() {
            got.push(k);
        }
        assert_eq!(
            got,
            vec![EndpointKey::Addr(a1, 0), EndpointKey::Addr(a1, 1)]
        );

        // Space again: the next tick inserts the address that did not fit.
        tokio::time::advance(Duration::from_secs(15)).await;
        settle().await;
        let mut got = Vec::new();
        while let Ok(Change::Insert(k, _)) = rx.try_recv() {
            got.push(k);
        }
        assert_eq!(got, vec![EndpointKey::Addr(b, 0), EndpointKey::Addr(b, 1)]);
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

    #[tokio::test(start_paused = true)]
    async fn first_lookup_is_retried_with_backoff_not_a_full_interval_later() {
        let a1 = a("10.0.0.1:9106");
        let fake = FakeResolver::new(vec![
            Err(io::Error::other("nxdomain")),
            Ok(vec![]),
            Ok(vec![a1]),
        ]);
        let (mut r, mut rx) = refresher(fake, 1, &[], 64);
        r.refresh = Duration::from_secs(60);
        r.seed_slots = 1;
        let start = tokio::time::Instant::now();
        let handle = tokio::spawn(r.run(true, build));

        match rx.recv().await.expect("insert") {
            Change::Insert(k, _) => assert_eq!(k, EndpointKey::Addr(a1, 0)),
            Change::Remove(k) => panic!("unexpected remove {k:?}"),
        }
        // t=0 fail, +1s empty, +2s success: 3s, not 120s.
        assert_eq!(start.elapsed(), Duration::from_secs(3));

        // The placeholder goes only after the real endpoint is in.
        match rx.recv().await.expect("seed removal") {
            Change::Remove(k) => assert_eq!(k, EndpointKey::Seed(0)),
            Change::Insert(k, _) => panic!("unexpected insert {k:?}"),
        }
        handle.abort();
    }

    #[test]
    fn dual_stack_answers_keep_only_ipv4() {
        let mixed = set(&["10.0.0.1:9106", "[fd00::1]:9106", "10.0.0.2:9106"]);
        assert_eq!(prefer_ipv4(mixed), set(&["10.0.0.1:9106", "10.0.0.2:9106"]));
        // IPv6-only is left alone rather than emptied.
        let v6 = set(&["[fd00::1]:9106"]);
        assert_eq!(prefer_ipv4(v6.clone()), v6);
    }

    /// Real grpc-go server fixture. Opt in with
    /// `PC_GRPC_GOAWAY_TEST=1 cargo test -p pc-grpc goaway_recovers -- --ignored --nocapture`.
    ///
    /// Proves recovery after a real grpc-go idle close with two stable DNS
    /// addresses, at pool sizes 1 and 2, through both eager and lazy constructors.
    /// Only MaxConnectionIdle is shortened to 300 ms for a practical local run.
    #[tokio::test]
    #[ignore = "requires Go and PC_GRPC_GOAWAY_TEST=1; runs the real grpc-go idle-GOAWAY fixture"]
    async fn goaway_recovers_with_stable_addresses_at_pool_one_and_two() {
        assert_eq!(
            std::env::var("PC_GRPC_GOAWAY_TEST").as_deref(),
            Ok("1"),
            "set PC_GRPC_GOAWAY_TEST=1 to run the grpc-go fixture"
        );
        let _env_lock = ENV_LOCK.lock().await;
        let mut fixture = GoAwayFixture::start();
        let addrs = fixture.addrs.clone();
        println!("fake resolver DNS answers={addrs:?} (stable for the full run)");
        let resolver = FakeResolver::new(vec![Ok(addrs.clone())]);
        let started = Instant::now();

        for construction in ["eager", "lazy"] {
            for pool in [1, 2] {
                let _target = EnvVar::set(
                    "TRANSACTION_GRPC",
                    format!("pa498.test:{}", addrs[0].port()),
                );
                let _pool = EnvVar::set("GRPC_TRANSACTION_MAX_POOL", pool.to_string());
                let _tls = EnvVar::set("GRPC_TLS_MODE", "insecure");
                let _keepalive_time = EnvVar::set("GRPC_KEEP_ALIVE_TIME", "30");
                let _keepalive_timeout = EnvVar::set("GRPC_KEEP_ALIVE_TIMEOUT", "10");
                let _permit = EnvVar::set("GRPC_PERMIT_WITHOUT_STREAM", "true");
                let _retries = EnvVar::set("GRPC_MAX_RETRIES", "3");
                let _initial_backoff = EnvVar::set("GRPC_INITIAL_BACKOFF", "100");
                let _max_backoff = EnvVar::set("GRPC_MAX_BACKOFF", "30");
                let _backoff_multiplier = EnvVar::set("GRPC_BACKOFF_MULTIPLIER", "2");
                let _max_idle = EnvVar::set("GRPC_MAX_IDLE", "30");
                let _max_life = EnvVar::set("GRPC_MAX_LIFE", "24");
                let factory = crate::GrpcClientFactory::from_env().unwrap();
                let channel = match construction {
                    "eager" => factory
                        .connect_discovered_with(
                            "TRANSACTION_GRPC",
                            Duration::from_secs(30),
                            resolver.clone(),
                        )
                        .await
                        .expect("connect discovered to both grpc-go fixture addresses"),
                    "lazy" => factory
                        .connect_discovered_lazy_with(
                            "TRANSACTION_GRPC",
                            Duration::from_secs(30),
                            resolver.clone(),
                        )
                        .expect("construct lazy discovered channel"),
                    _ => unreachable!(),
                };
                tokio::time::sleep(Duration::from_millis(100)).await;

                assert_channel_recovers(&channel, construction, pool, started).await;
                drop(channel);
            }
        }

        fixture.stop_and_report();
    }

    async fn assert_channel_recovers(
        channel: &tonic::transport::Channel,
        construction: &str,
        pool: usize,
        started: Instant,
    ) {
        let mut consecutive_failures = 0;
        let mut total_successes = 0;
        for probe in 1..=5 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let request = HealthCheckRequest {
                service: String::new(),
            };
            let result = tokio::time::timeout(
                Duration::from_millis(950),
                HealthClient::new(channel.clone()).check(request),
            )
            .await
            .expect("health RPC must stay within the production deadline");
            match result {
                Ok(_) => {
                    consecutive_failures = 0;
                    total_successes += 1;
                    println!(
                        "elapsed_ms={} construction={construction} pool={pool} probe={probe} result=ok",
                        started.elapsed().as_millis()
                    );
                }
                Err(status) => {
                    let message = status.message().to_ascii_lowercase();
                    assert!(
                        matches!(
                            status.code(),
                            tonic::Code::Cancelled
                                | tonic::Code::Unknown
                                | tonic::Code::Unavailable
                        ),
                        "pool={pool} probe={probe} got non-transport status: {status}"
                    );
                    println!(
                        "elapsed_ms={} construction={construction} pool={pool} probe={probe} result=error code={:?} message={:?}",
                        started.elapsed().as_millis(),
                        status.code(),
                        status.message()
                    );
                    consecutive_failures += 1;
                    assert!(
                        consecutive_failures < 2,
                        "construction={construction} pool={pool} remained failed across probes after grpc-go MaxConnectionIdle GOAWAY: {status}"
                    );
                    if matches!(status.code(), tonic::Code::Cancelled | tonic::Code::Unknown) {
                        assert!(
                            message.contains("operation was canceled")
                                || message.contains("transport error")
                                || message.contains("connection closed"),
                            "construction={construction} pool={pool} error was not the observed idle-close transport failure: {status}"
                        );
                    }
                }
            }
        }
        assert!(
            total_successes > 0,
            "construction={construction} pool={pool} had no successful probes"
        );
        assert_eq!(
            consecutive_failures, 0,
            "construction={construction} pool={pool} ended without recovering after its last failed probe"
        );
    }

    struct EnvVar {
        key: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl EnvVar {
        fn set(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
            let previous = std::env::var_os(key);
            std::env::set_var(key, value);
            Self { key, previous }
        }
    }

    impl Drop for EnvVar {
        fn drop(&mut self) {
            if let Some(value) = &self.previous {
                std::env::set_var(self.key, value);
            } else {
                std::env::remove_var(self.key);
            }
        }
    }

    struct GoAwayFixture {
        child: Option<Child>,
        addrs: Vec<SocketAddr>,
        log_path: std::path::PathBuf,
    }

    impl GoAwayFixture {
        fn start() -> Self {
            let addrs: Vec<_> = (0..2)
                .map(|_| {
                    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve fixture port");
                    listener.local_addr().unwrap()
                })
                .collect();
            let log_path = std::env::temp_dir().join(format!(
                "pc-grpc-goaway-{}-{}.log",
                std::process::id(),
                Instant::now().elapsed().as_nanos()
            ));
            let log = File::create(&log_path).expect("create sanitized fixture log");
            let stderr = log.try_clone().expect("clone fixture log handle");
            let child = Command::new("go")
                .args([
                    "run",
                    ".",
                    "-addr-one",
                    &addrs[0].to_string(),
                    "-addr-two",
                    &addrs[1].to_string(),
                    "-idle",
                    "300ms",
                ])
                .current_dir(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/tests/fixtures/goaway"
                ))
                .stdout(Stdio::from(log))
                .stderr(Stdio::from(stderr))
                .spawn()
                .expect("start grpc-go fixture; install Go and download its pinned module first");
            let fixture = Self {
                child: Some(child),
                addrs,
                log_path,
            };

            for addr in &fixture.addrs {
                let deadline = Instant::now() + Duration::from_secs(30);
                loop {
                    if std::net::TcpStream::connect(addr).is_ok() {
                        break;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "grpc-go fixture did not listen on {addr}"
                    );
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
            println!(
                "grpc-go fixture addresses={:?} max_connection_idle_ms=300",
                fixture.addrs
            );
            fixture
        }

        fn stop_and_report(&mut self) {
            if let Some(mut child) = self.child.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
            let logs = std::fs::read_to_string(&self.log_path).unwrap_or_default();
            println!("grpc-go connection lifecycle:\n{logs}");
            for addr in &self.addrs {
                let prefix = addr.to_string();
                assert!(
                    logs.lines()
                        .any(|line| { line.contains(&prefix) && line.contains("connection_open") }),
                    "fixture address {addr} received no gRPC connection"
                );
                assert!(
                    logs.lines().any(|line| {
                        line.contains(&prefix) && line.contains("connection_close")
                    }),
                    "fixture address {addr} recorded no gRPC connection close"
                );
            }
            let _ = std::fs::remove_file(&self.log_path);
        }
    }

    impl Drop for GoAwayFixture {
        fn drop(&mut self) {
            if let Some(mut child) = self.child.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
            let _ = std::fs::remove_file(&self.log_path);
        }
    }
}
