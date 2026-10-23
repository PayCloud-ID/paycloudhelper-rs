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
use tonic::transport::channel::Change;
use tonic::transport::Endpoint;

/// Key of one balanced endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EndpointKey {
    /// Slot `n` of the plain host-name endpoint a lazy channel starts with, so
    /// a call never waits on an empty balancer. Removed once real addresses
    /// have been inserted.
    Seed(usize),
    /// Slot `n` (of `pool`) of one resolved pod address.
    Addr(SocketAddr, usize),
}

/// First retry delay while no lookup has succeeded yet; doubles up to `refresh`.
const FIRST_RETRY: Duration = Duration::from_secs(1);

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
            Ok(prefer_ipv4(addrs.collect()))
        })
    }
}

/// Keep only the IPv4 addresses when there is any, otherwise all of them.
///
/// `lookup_host` returns A and AAAA records; on a dual-stack Service one pod
/// would be balanced twice (and the cluster here is single-stack IPv4).
pub fn prefer_ipv4(addrs: BTreeSet<SocketAddr>) -> BTreeSet<SocketAddr> {
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
/// * While no lookup has succeeded (`known` empty) a failure is retried after
///   1s, 2s, 4s, … capped at `refresh`, not a whole `refresh` later; once an
///   address is known the normal interval applies.
/// * `seed_slots` > 0 means the channel holds `EndpointKey::Seed(0..seed_slots)`
///   placeholders; they are removed right after the first real insert.
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
    mut seed_slots: usize,
    mut known: BTreeSet<SocketAddr>,
    build: F,
    tx: Sender<Change<EndpointKey, Endpoint>>,
) where
    F: Fn(SocketAddr) -> Result<Endpoint>,
{
    let mut first = immediate;
    let mut retry = FIRST_RETRY.min(refresh);

    loop {
        let delay = if first {
            first = false;
            Duration::ZERO
        } else if known.is_empty() {
            let d = retry;
            retry = (retry * 2).min(refresh);
            d
        } else {
            refresh
        };
        tokio::select! {
            () = tx.closed() => return,
            () = tokio::time::sleep(delay) => {}
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
                    .send(Change::Insert(
                        EndpointKey::Addr(addr, slot),
                        endpoint.clone(),
                    ))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            known.insert(addr);
            tracing::info!("pc-grpc: discovery target={host} +{}", addr.ip());
        }
        for addr in gone {
            for slot in 0..pool {
                if tx
                    .send(Change::Remove(EndpointKey::Addr(addr, slot)))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            known.remove(&addr);
            tracing::info!("pc-grpc: discovery target={host} -{}", addr.ip());
        }
        if seed_slots > 0 && !known.is_empty() {
            for slot in 0..seed_slots {
                if tx
                    .send(Change::Remove(EndpointKey::Seed(slot)))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            seed_slots = 0;
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
            0,
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
        assert_eq!(keys, vec![EndpointKey::Addr(b, 0), EndpointKey::Addr(b, 1)]);
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
            Change::Remove(k) => assert_eq!(k, EndpointKey::Addr(a1, 0)),
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

    #[tokio::test(start_paused = true)]
    async fn first_lookup_is_retried_with_backoff_not_a_full_interval_later() {
        let a1 = a("10.0.0.1:9106");
        let fake = FakeResolver::new(vec![
            Err(io::Error::other("nxdomain")),
            Ok(vec![]),
            Ok(vec![a1]),
        ]);
        let (tx, mut rx) = mpsc::channel(64);
        let start = tokio::time::Instant::now();
        let handle = tokio::spawn(run_refresher(
            fake,
            "svc-headless".into(),
            9106,
            Duration::from_secs(60),
            true,
            1,
            1,
            BTreeSet::new(),
            build,
            tx,
        ));

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
}
