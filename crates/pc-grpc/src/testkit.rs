//! Test fixture: a loopback "server" that behaves like grpc-go with a short
//! `ConnectionTimeout` (PA-498): a connection that has not sent a byte within
//! `handshake_timeout` is closed, and every bridged connection is closed after
//! `lifetime` (idle/age close between probe ticks). Behind it sits a real
//! tonic-health server.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// Serialises tests that mutate process-global environment variables.
pub(crate) static ENV_LOCK: Mutex<()> = Mutex::new(());

pub(crate) struct Backend {
    pub(crate) addr: SocketAddr,
    pub(crate) accepted: Arc<AtomicUsize>,
    tasks: Vec<JoinHandle<()>>,
}

impl Backend {
    pub(crate) async fn start(handshake_timeout: Duration, lifetime: Duration) -> Self {
        let inner = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let inner_addr = inner.local_addr().unwrap();
        let (reporter, service) = tonic_health::server::health_reporter();
        let server = tokio::spawn(async move {
            let _reporter = reporter; // keep the reporter alive with the server
            let _ = tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(tonic::transport::server::TcpIncoming::from(inner))
                .await;
        });

        let front = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = front.local_addr().unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let count = accepted.clone();
        let proxy = tokio::spawn(async move {
            loop {
                let Ok((client, _)) = front.accept().await else {
                    return;
                };
                count.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(bridge(client, inner_addr, handshake_timeout, lifetime));
            }
        });
        Self {
            addr,
            accepted,
            tasks: vec![server, proxy],
        }
    }

    /// Stop answering: the listener goes away and new connections are refused
    /// (a pod that went away). Bridged connections end within `lifetime`.
    pub(crate) fn stop(&mut self) {
        for t in self.tasks.drain(..) {
            t.abort();
        }
    }
}

impl Drop for Backend {
    fn drop(&mut self) {
        self.stop();
    }
}

async fn bridge(client: TcpStream, inner: SocketAddr, handshake: Duration, lifetime: Duration) {
    // grpc-go: rawConn.SetDeadline(now + connectionTimeout) until the preface arrives.
    let mut probe = [0_u8; 1];
    match tokio::time::timeout(handshake, client.peek(&mut probe)).await {
        Ok(Ok(n)) if n > 0 => {}
        _ => return, // never handshaked in time: drop (close)
    }
    let Ok(mut server) = TcpStream::connect(inner).await else {
        return;
    };
    let mut client = client;
    let _ = tokio::time::timeout(lifetime, copy_bidirectional(&mut client, &mut server)).await;
}

/// Env-mutating tests hold `ENV_LOCK` only while the factory reads the
/// environment and builds the channel; the probe rounds run unlocked.
#[cfg(test)]
mod tests {
    #![allow(clippy::await_holding_lock)] // see the module doc above

    use super::*;
    use crate::discovery::{LookupFuture, Resolver};
    use crate::GrpcClientFactory;
    use std::collections::BTreeSet;
    use tonic_health::pb::{health_client::HealthClient, HealthCheckRequest};

    struct Fixed(BTreeSet<SocketAddr>);

    impl Resolver for Fixed {
        fn lookup<'a>(&'a self, _h: &'a str, _p: u16) -> LookupFuture<'a> {
            let a = self.0.clone();
            Box::pin(async move { Ok(a) })
        }
    }

    // Scaled replay of the production timeline (production tick 120 s, server
    // ConnectionTimeout 10 s): handshake 150 ms, connection life 400 ms, probe
    // gap 600 ms. Every connection is dead at every tick and every orphan has
    // expired between ticks.
    const HANDSHAKE: Duration = Duration::from_millis(150);
    const LIFETIME: Duration = Duration::from_millis(400);
    const GAP: Duration = Duration::from_millis(600);
    const ROUNDS: usize = 30;

    async fn two_backends() -> (Backend, Backend) {
        (
            Backend::start(HANDSHAKE, LIFETIME).await,
            Backend::start(HANDSHAKE, LIFETIME).await,
        )
    }

    fn fixed(backends: &[&Backend]) -> Arc<Fixed> {
        Arc::new(Fixed(backends.iter().map(|b| b.addr).collect()))
    }

    fn health_request() -> tonic::Request<HealthCheckRequest> {
        let mut req = tonic::Request::new(HealthCheckRequest {
            service: String::new(),
        });
        crate::with_deadline(&mut req, Duration::from_millis(500));
        req
    }

    /// 2 addresses x pool 2, probes only, as qoinhub does.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "characterization of the PA-498 starvation; probabilistic by nature, run with --ignored"]
    async fn stock_discovery_channel_fails_probes_against_a_short_connection_timeout() {
        let (a, b) = two_backends().await;
        let channel = {
            let _guard = ENV_LOCK
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::env::set_var("TRANSACTION_GRPC", "svc-headless:9106");
            let factory = GrpcClientFactory::from_env().unwrap();
            let channel = factory
                .connect_discovered_with(
                    "TRANSACTION_GRPC",
                    Duration::from_secs(30),
                    fixed(&[&a, &b]),
                )
                .await
                .unwrap();
            std::env::remove_var("TRANSACTION_GRPC");
            channel
        };

        let mut failed = 0;
        for _ in 0..ROUNDS {
            tokio::time::sleep(GAP).await;
            if HealthClient::new(channel.clone())
                .check(health_request())
                .await
                .is_err()
            {
                failed += 1;
            }
        }
        println!(
            "stock channel: {failed}/{ROUNDS} probes failed ({} + {} connections accepted)",
            a.accepted.load(Ordering::SeqCst),
            b.accepted.load(Ordering::SeqCst)
        );
        assert!(
            failed >= 3,
            "expected the starvation to show; tune HANDSHAKE/LIFETIME/GAP until it does"
        );
    }

    async fn pooled_over(resolver: Arc<dyn Resolver>, refresh: Duration) -> crate::PooledChannel {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::env::set_var("TRANSACTION_GRPC", "svc-headless:9106");
        let factory = GrpcClientFactory::from_env().unwrap();
        let pooled = factory
            .connect_pooled_with("TRANSACTION_GRPC", refresh, resolver)
            .await
            .unwrap();
        std::env::remove_var("TRANSACTION_GRPC");
        pooled
    }

    /// The regression for the characterization above: same fixture, same
    /// timers, 2 addresses x pool 2. Deterministic, because each slot's request
    /// waits on its own re-dial instead of skipping it.
    #[tokio::test(flavor = "multi_thread")]
    async fn pooled_channel_never_fails_a_probe_against_a_short_connection_timeout() {
        let (a, b) = two_backends().await;
        let pooled = pooled_over(fixed(&[&a, &b]), Duration::from_secs(30)).await;
        assert_eq!(pooled.slot_count(), 4);

        for round in 0..ROUNDS {
            tokio::time::sleep(GAP).await;
            HealthClient::new(pooled.clone())
                .check(health_request())
                .await
                .unwrap_or_else(|e| panic!("probe {round} failed: {e:?}"));
        }
        // Every tick found its connection closed by the fixture, so the probes
        // above went over fresh connections each time; the fixture saw them.
        let accepted = a.accepted.load(Ordering::SeqCst) + b.accepted.load(Ordering::SeqCst);
        assert!(
            accepted >= ROUNDS,
            "only {accepted} connections for {ROUNDS} probes"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_pod_that_goes_away_costs_at_most_one_failed_call() {
        let (a, mut b) = two_backends().await;
        let pooled = pooled_over(fixed(&[&a, &b]), Duration::from_secs(30)).await;
        HealthClient::new(pooled.clone())
            .check(health_request())
            .await
            .expect("both pods up");

        b.stop();
        // Let every bridged connection to b reach its end of life.
        tokio::time::sleep(LIFETIME + Duration::from_millis(100)).await;

        let mut failed = 0;
        for _ in 0..10 {
            if HealthClient::new(pooled.clone())
                .check(health_request())
                .await
                .is_err()
            {
                failed += 1;
            }
        }
        assert!(failed <= 1, "{failed}/10 calls failed after b went away");
    }

    /// Answers `answers[stage]`; the test moves `stage`.
    struct Staged {
        answers: Vec<BTreeSet<SocketAddr>>,
        stage: AtomicUsize,
    }

    impl Resolver for Staged {
        fn lookup<'a>(&'a self, _h: &'a str, _p: u16) -> LookupFuture<'a> {
            let a = self.answers[self.stage.load(Ordering::SeqCst)].clone();
            Box::pin(async move { Ok(a) })
        }
    }

    async fn wait_for_slots(pooled: &crate::PooledChannel, want: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while pooled.slot_count() != want {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("slot count stayed {} (want {want})", pooled.slot_count()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn discovery_adds_and_removes_slots_without_any_request() {
        let (a, b) = two_backends().await;
        let resolver = Arc::new(Staged {
            answers: vec![
                [a.addr].into_iter().collect(),
                [a.addr, b.addr].into_iter().collect(),
                [b.addr].into_iter().collect(),
            ],
            stage: AtomicUsize::new(0),
        });
        let pooled = pooled_over(resolver.clone(), Duration::from_millis(200)).await;
        assert_eq!(pooled.slot_count(), 2);

        // No call is made from here on. The balancer-based channel could not do
        // this: it drains its change queue only while a request polls it.
        resolver.stage.store(1, Ordering::SeqCst);
        wait_for_slots(&pooled, 4).await;
        resolver.stage.store(2, Ordering::SeqCst);
        wait_for_slots(&pooled, 2).await;
    }
}
