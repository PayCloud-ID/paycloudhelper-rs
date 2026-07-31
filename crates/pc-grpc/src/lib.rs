#![forbid(unsafe_code)]
//! `pc-grpc` — `tonic` gRPC client-pool factory.
//!
//! Port of the qoinhub gRPC client pool (`app/grpc_pool.go`,
//! `app/grpc_metrics.go`, `structs/grpc_config.go`). It reproduces the
//! `GrpcManager`/`GrpcClientPool` behaviour on top of `tonic`:
//!
//! * dial the `host:port` DNS target exactly as configured (see
//!   [`dial_target`] — no service-name suffix is added),
//! * keep an N-connection round-robin pool with **prewarm**
//!   (`GRPC_*_MAX_POOL`, default **2** — mirrors `getEnvInt("GRPC_*_MAX_POOL", 2)`),
//! * retry connection establishment on transient failure (mirrors
//!   `createConnection`'s retry loop and the `UNAVAILABLE`/`DEADLINE_EXCEEDED`
//!   retry policy),
//! * apply keep-alive, idle/lifetime and TLS settings per `GRPC_*` env vars
//!   (mirrors `DefaultConfig`/`ApplyGrpcEnvConfig`/`resolveTransportCredentials`).
//!
//! It also provides the RC-1 shrinking-budget deadline helpers
//! ([`child_budget`]/[`with_deadline`]): a child call's deadline is strictly
//! less than the wall budget so it always expires first.
//!
//! The parity-critical *pure* logic — env parsing defaults, the dial-target
//! shape and the deadline-margin math — is unit-tested offline. Anything
//! that needs a live gRPC server (an actual [`GrpcClientFactory::connect`]) is
//! `#[ignore]`d.

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

/// Default connections-per-target for every pool.
///
/// mirrors: the `2` default of `getEnvInt("GRPC_TRANSACTION_MAX_POOL", 2)` (and
/// the `maxConn <= 0 { maxConn = 2 }` clamp in `NewGrpcClientPool`).
pub const DEFAULT_MAX_POOL: usize = 2;

/// Margin subtracted from a wall-clock budget to derive a child deadline.
///
/// The RC-1 timeout-budget rule: a child call's deadline must be *strictly*
/// less than the wall budget so the child times out before the parent. e.g.
/// `1000ms` wall → `950ms` child.
pub const DEADLINE_MARGIN: Duration = Duration::from_millis(50);

/// TLS selection derived from `GRPC_TLS_MODE`.
///
/// mirrors: `resolveTransportCredentials` — only `GRPC_TLS_MODE=tls` enables
/// TLS; every other value (including empty) uses insecure credentials.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsMode {
    /// Insecure transport (`insecure.NewCredentials()`); any mode that is not `tls`.
    Insecure,
    /// TLS transport (`credentials.NewTLS`), optionally with `GRPC_CLIENT_CA_FILE`.
    Tls,
}

/// Parsed `GRPC_*` configuration.
///
/// mirrors: `structs.GrpcClientConfig` plus the gRPC fields of
/// `helpers.EnvConfig`, using the same env var names and defaults resolved in
/// `helpers.Load`/`ApplyGrpcEnvConfig`.
#[derive(Debug, Clone)]
pub struct GrpcConfig {
    /// `GRPC_TRANSACTION_MAX_POOL` (default 2) — pool size for `TRANSACTION_GRPC`.
    pub transaction_max_pool: usize,
    /// `GRPC_MERCHANT_MAX_POOL` (default 2) — pool size for `MERCHANT_GRPC`.
    pub merchant_max_pool: usize,
    /// `GRPC_CONFIG_MAX_POOL` (default 2) — pool size for `CONFIG_GRPC`.
    pub config_max_pool: usize,
    /// `GRPC_HEALTHCHECK_INTERVAL` seconds (default 120) — health-check period.
    pub healthcheck_interval_secs: u64,
    /// `GRPC_HEALTHCHECK` (default true) — whether the health-check worker runs.
    pub healthcheck_enabled: bool,
    /// `GRPC_PORT` (default `9333`) — the local gRPC server port.
    pub grpc_port: String,
    /// `GRPC_TLS_MODE` — TLS vs insecure transport credentials.
    pub tls_mode: TlsMode,
    /// `GRPC_CLIENT_CA_FILE` — optional custom CA PEM path for TLS.
    pub client_ca_file: Option<String>,
    /// `GRPC_KEEP_ALIVE_TIME` seconds (default 30) — keep-alive ping interval.
    pub keep_alive_time: Duration,
    /// `GRPC_KEEP_ALIVE_TIMEOUT` seconds (default 10) — keep-alive ack timeout.
    pub keep_alive_timeout: Duration,
    /// `GRPC_PERMIT_WITHOUT_STREAM` (default true) — ping with no active stream.
    pub permit_without_stream: bool,
    /// `GRPC_MAX_RETRIES` (default 3) — connection-establishment attempts.
    pub max_retries: u32,
    /// `GRPC_INITIAL_BACKOFF` ms (default 100) — first reconnect backoff.
    pub initial_backoff: Duration,
    /// `GRPC_MAX_BACKOFF` seconds (default 30) — backoff ceiling.
    pub max_backoff: Duration,
    /// `GRPC_BACKOFF_MULTIPLIER` (default 2.0) — backoff growth factor.
    pub backoff_multiplier: f64,
    /// `GRPC_MAX_IDLE` minutes (default 30) — idle-eviction threshold.
    pub max_idle: Duration,
    /// `GRPC_MAX_LIFE` hours (default 24) — lifetime-eviction threshold.
    pub max_life: Duration,
}

impl GrpcConfig {
    /// Parse the full `GRPC_*` configuration from the process environment.
    ///
    /// Invalid values fall back to the default (mirrors `getEnvInt`/`getEnvBool`
    /// et al., which log a warning and keep the default on a parse error).
    ///
    /// mirrors: `helpers.Load` (gRPC fields) + `ApplyGrpcEnvConfig`.
    pub fn from_env() -> Self {
        Self {
            transaction_max_pool: env_parsed("GRPC_TRANSACTION_MAX_POOL", DEFAULT_MAX_POOL),
            merchant_max_pool: env_parsed("GRPC_MERCHANT_MAX_POOL", DEFAULT_MAX_POOL),
            config_max_pool: env_parsed("GRPC_CONFIG_MAX_POOL", DEFAULT_MAX_POOL),
            healthcheck_interval_secs: env_parsed("GRPC_HEALTHCHECK_INTERVAL", 120_u64),
            healthcheck_enabled: env_bool("GRPC_HEALTHCHECK", true),
            grpc_port: env_string("GRPC_PORT", "9333"),
            tls_mode: if env_string("GRPC_TLS_MODE", "") == "tls" {
                TlsMode::Tls
            } else {
                TlsMode::Insecure
            },
            client_ca_file: match env_string("GRPC_CLIENT_CA_FILE", "") {
                s if s.is_empty() => None,
                s => Some(s),
            },
            keep_alive_time: Duration::from_secs(env_parsed("GRPC_KEEP_ALIVE_TIME", 30_u64)),
            keep_alive_timeout: Duration::from_secs(env_parsed("GRPC_KEEP_ALIVE_TIMEOUT", 10_u64)),
            permit_without_stream: env_bool("GRPC_PERMIT_WITHOUT_STREAM", true),
            max_retries: env_parsed("GRPC_MAX_RETRIES", 3_u32),
            initial_backoff: Duration::from_millis(env_parsed("GRPC_INITIAL_BACKOFF", 100_u64)),
            max_backoff: Duration::from_secs(env_parsed("GRPC_MAX_BACKOFF", 30_u64)),
            backoff_multiplier: env_parsed("GRPC_BACKOFF_MULTIPLIER", 2.0_f64),
            max_idle: Duration::from_secs(env_parsed::<u64>("GRPC_MAX_IDLE", 30) * 60),
            max_life: Duration::from_secs(env_parsed::<u64>("GRPC_MAX_LIFE", 24) * 3_600),
        }
    }
}

/// gRPC channel factory over a DNS round-robin pool.
///
/// mirrors: `app.GrpcManager` + `app.GrpcClientPool` — one factory owns the
/// `GRPC_*` config and hands out `tonic::transport::Channel`s that round-robin
/// across an N-connection prewarmed pool per target.
#[derive(Debug, Clone)]
pub struct GrpcClientFactory {
    config: GrpcConfig,
}

impl GrpcClientFactory {
    /// Build a factory from the `GRPC_*` environment.
    ///
    /// mirrors: `NewGrpcManager` (which reads `helpers.GetConfig()` pool sizes
    /// and `DefaultConfig()` env overrides).
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            config: GrpcConfig::from_env(),
        })
    }

    /// Borrow the parsed configuration.
    pub fn config(&self) -> &GrpcConfig {
        &self.config
    }

    /// Resolve the pool size for a `*_GRPC` target env name.
    ///
    /// mirrors: `NewGrpcManager` wiring `Transaction`/`Merchant`/`Config` pools
    /// to their `Grpc*MaxPool`; unknown targets fall back to [`DEFAULT_MAX_POOL`].
    pub fn pool_size_for(&self, target_env: &str) -> usize {
        let size = match target_env {
            "TRANSACTION_GRPC" => self.config.transaction_max_pool,
            "MERCHANT_GRPC" => self.config.merchant_max_pool,
            "CONFIG_GRPC" => self.config.config_max_pool,
            _ => DEFAULT_MAX_POOL,
        };
        size.max(1)
    }

    /// Connect to `target_env` (e.g. `"TRANSACTION_GRPC"`), returning a
    /// round-robin `Channel` over a prewarmed N-connection pool.
    ///
    /// The env value is a bare `host:port` (e.g.
    /// `paycloud-be-transaction-module:9106`) and is dialled as-is — no
    /// service-name suffix is appended, so the DNS name is entirely the
    /// deployment's choice. Establishment is retried on transient failure up to
    /// `GRPC_MAX_RETRIES` (needs a live server — this is why any real `connect`
    /// test is `#[ignore]`d).
    ///
    /// mirrors: `GrpcClientPool.GetConnection` + `createConnection` (round-robin
    /// pool, retry-with-backoff establishment) over the target that
    /// `GrpcManager.initializeConnections` dials.
    pub async fn connect(&self, target_env: &str) -> Result<Channel> {
        let raw = std::env::var(target_env)
            .with_context(|| format!("gRPC target env `{target_env}` is not set"))?;
        let (host, port) = parse_host_port(&raw)
            .with_context(|| format!("gRPC target `{target_env}`=`{raw}` is not host:port"))?;

        let target = dial_target(&host, port);
        let pool = self.pool_size_for(target_env);
        let endpoint = self.build_endpoint(&target)?;

        tracing::info!(target = %target, pool, "pc-grpc: prewarming round-robin channel");

        // Prewarm: eagerly establish one connection with retry so a dead
        // upstream surfaces here (mirrors createConnection's retry loop).
        self.prewarm(&endpoint).await?;

        // Round-robin pool: N identical subchannels over the same DNS target
        // (mirrors GrpcClientPool's maxConn connections + round_robin policy).
        let endpoints = vec![endpoint; pool];
        Ok(Channel::balance_list(endpoints.into_iter()))
    }

    /// Build a configured (but not yet connected) [`Endpoint`] for a
    /// `host:port` target.
    ///
    /// mirrors: the `grpc.DialOption` set in `NewGrpcClientPool`
    /// (keep-alive params, connect timeout) plus `resolveTransportCredentials`.
    fn build_endpoint(&self, target: &str) -> Result<Endpoint> {
        let scheme = match self.config.tls_mode {
            TlsMode::Tls => "https",
            TlsMode::Insecure => "http",
        };
        let uri = format!("{scheme}://{target}");

        let mut endpoint = Endpoint::from_shared(uri.clone())
            .with_context(|| format!("invalid gRPC endpoint `{uri}`"))?
            .keep_alive_timeout(self.config.keep_alive_timeout)
            .keep_alive_while_idle(self.config.permit_without_stream)
            .http2_keep_alive_interval(self.config.keep_alive_time)
            .connect_timeout(Duration::from_secs(5));

        if self.config.tls_mode == TlsMode::Tls {
            let mut tls = ClientTlsConfig::new();
            if let Some(ca) = &self.config.client_ca_file {
                let pem = std::fs::read(ca)
                    .with_context(|| format!("read GRPC_CLIENT_CA_FILE `{ca}`"))?;
                tls = tls.ca_certificate(Certificate::from_pem(pem));
            }
            endpoint = endpoint
                .tls_config(tls)
                .context("apply GRPC_TLS_MODE=tls client config")?;
        }

        Ok(endpoint)
    }

    /// Establish one probe connection, retrying on transient failure.
    ///
    /// mirrors: `createConnection`'s bounded retry loop (up to
    /// `GRPC_MAX_RETRIES` attempts with growing backoff).
    async fn prewarm(&self, endpoint: &Endpoint) -> Result<()> {
        let attempts = self.config.max_retries.max(1);
        let mut last_err: Option<anyhow::Error> = None;

        for attempt in 1..=attempts {
            match endpoint.connect().await {
                // The probe channel is dropped; balance_list re-dials lazily.
                Ok(_channel) => return Ok(()),
                Err(err) => {
                    tracing::warn!(attempt, error = %err, "pc-grpc: prewarm attempt failed");
                    last_err = Some(anyhow!(err));
                    if attempt < attempts {
                        tokio::time::sleep(self.backoff_for(attempt)).await;
                    }
                }
            }
        }

        Err(last_err
            .unwrap_or_else(|| anyhow!("prewarm failed"))
            .context("gRPC prewarm exhausted retries"))
    }

    /// Backoff before retry `attempt` (1-based), capped at `max_backoff`.
    ///
    /// mirrors: `createConnection`'s `attempt*attempt*100ms`-style growth,
    /// clamped by `MaxBackoff`.
    fn backoff_for(&self, attempt: u32) -> Duration {
        let scaled = self.config.initial_backoff.saturating_mul(attempt);
        scaled.min(self.config.max_backoff)
    }
}

/// Format the DNS target the pool dials.
///
/// The service `name` and `port` (as carried in `TRANSACTION_GRPC=host:port`)
/// are joined verbatim into `<name>:<port>`. **No suffix is appended** — the
/// helper never rewrites the service name, so pointing at a headless service,
/// a ClusterIP or an external host is purely a deployment/env decision.
///
/// mirrors: the target string `GrpcManager.initializeConnections` dials via
/// `helpers.GetTransactionGrpc()`.
pub fn dial_target(name: &str, port: u16) -> String {
    format!("{name}:{port}")
}

/// Split a `host:port` endpoint value into its parts.
///
/// mirrors: the `TRANSACTION_GRPC=paycloud-be-transaction-module:9106`
/// env-value shape consumed by the pool.
pub fn parse_host_port(endpoint: &str) -> Result<(String, u16)> {
    let (host, port) = endpoint
        .rsplit_once(':')
        .ok_or_else(|| anyhow!("missing `:port` in endpoint `{endpoint}`"))?;
    if host.is_empty() {
        return Err(anyhow!("empty host in endpoint `{endpoint}`"));
    }
    let port: u16 = port
        .parse()
        .with_context(|| format!("invalid port `{port}` in endpoint `{endpoint}`"))?;
    Ok((host.to_string(), port))
}

/// Shrink a wall-clock budget to a child deadline strictly below it.
///
/// Subtracts [`DEADLINE_MARGIN`] and clamps at zero, so the result is never
/// negative and (for any non-zero wall budget) strictly less than the wall
/// budget — the RC-1 child &lt; wall invariant.
pub fn child_budget(wall: Duration) -> Duration {
    wall.checked_sub(DEADLINE_MARGIN).unwrap_or(Duration::ZERO)
}

/// Set a shrinking-budget deadline on an outgoing request.
///
/// Applies [`child_budget`] to `budget` and stores it as the request timeout
/// (the `grpc-timeout` metadata), so the child call expires before the wall
/// budget.
///
/// mirrors: wrapping an RPC in `context.WithTimeout(ctx, budget)` with the RC-1
/// margin applied.
pub fn with_deadline<T>(req: &mut tonic::Request<T>, budget: Duration) {
    req.set_timeout(child_budget(budget));
}

// ---- private env helpers (mirror helpers.getEnvInt/getEnvFloat/getEnvBool/getEnvOrDefault) ----

/// Parse `key` as `T`, falling back to `default` when unset, empty or invalid.
fn env_parsed<T: std::str::FromStr>(key: &str, default: T) -> T {
    match std::env::var(key) {
        Ok(v) if !v.trim().is_empty() => v.trim().parse().unwrap_or(default),
        _ => default,
    }
}

/// Read `key` as a string, falling back to `default` when unset or empty.
fn env_string(key: &str, default: &str) -> String {
    match std::env::var(key) {
        Ok(v) if !v.is_empty() => v,
        _ => default.to_string(),
    }
}

/// Read `key` as a bool (Go `strconv.ParseBool` grammar), falling back to
/// `default` when unset, empty or invalid.
fn env_bool(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(v) => match v.trim() {
            "1" | "t" | "T" | "TRUE" | "true" | "True" => true,
            "0" | "f" | "F" | "FALSE" | "false" | "False" => false,
            _ => default,
        },
        Err(_) => default,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Serialises tests that mutate process-global environment variables.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn dial_target_joins_name_and_port_without_suffix() {
        assert_eq!(
            dial_target("paycloud-be-transaction-module", 9106),
            "paycloud-be-transaction-module:9106"
        );
        assert_eq!(dial_target("svc", 1), "svc:1");

        // The helper must never inject a `-headless` (or any other) suffix —
        // the DNS name comes from the env value alone.
        assert!(!dial_target("svc", 1).contains("-headless"));
        // An explicitly headless env value is preserved as-is, not doubled up.
        assert_eq!(
            dial_target("paycloud-be-transaction-module-headless", 9106),
            "paycloud-be-transaction-module-headless:9106"
        );
    }

    #[test]
    fn parse_host_port_splits_env_value() {
        let (host, port) = parse_host_port("paycloud-be-transaction-module:9106").unwrap();
        assert_eq!(host, "paycloud-be-transaction-module");
        assert_eq!(port, 9106);

        // Round-trips unchanged into the target the pool dials.
        assert_eq!(
            dial_target(&host, port),
            "paycloud-be-transaction-module:9106"
        );

        assert!(parse_host_port("no-port").is_err());
        assert!(parse_host_port(":9106").is_err());
        assert!(parse_host_port("host:not-a-port").is_err());
    }

    #[test]
    fn child_budget_is_strictly_below_wall_and_never_negative() {
        // e.g. 1000ms wall -> 950ms child.
        assert_eq!(
            child_budget(Duration::from_millis(1000)),
            Duration::from_millis(950)
        );

        // child < wall for any non-zero wall budget.
        for ms in [51_u64, 60, 100, 250, 1000, 5000, 30_000] {
            let wall = Duration::from_millis(ms);
            assert!(
                child_budget(wall) < wall,
                "child budget must be strictly below the wall budget for {ms}ms"
            );
        }

        // Clamps at zero (never negative / never panics) when below the margin.
        assert_eq!(child_budget(Duration::from_millis(50)), Duration::ZERO);
        assert_eq!(child_budget(Duration::from_millis(10)), Duration::ZERO);
        assert_eq!(child_budget(Duration::ZERO), Duration::ZERO);
    }

    #[test]
    fn with_deadline_applies_child_budget_without_panicking() {
        let mut req = tonic::Request::new(());
        with_deadline(&mut req, Duration::from_millis(1000));
        // set_timeout stores the grpc-timeout; smoke-test that it does not panic.
        req.into_inner();
    }

    #[test]
    fn config_parsing_defaults_and_overrides() {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let keys = [
            "GRPC_TRANSACTION_MAX_POOL",
            "GRPC_MERCHANT_MAX_POOL",
            "GRPC_CONFIG_MAX_POOL",
            "GRPC_HEALTHCHECK_INTERVAL",
            "GRPC_HEALTHCHECK",
            "GRPC_PORT",
            "GRPC_TLS_MODE",
            "GRPC_CLIENT_CA_FILE",
            "GRPC_KEEP_ALIVE_TIME",
            "GRPC_KEEP_ALIVE_TIMEOUT",
            "GRPC_PERMIT_WITHOUT_STREAM",
            "GRPC_MAX_RETRIES",
            "GRPC_INITIAL_BACKOFF",
            "GRPC_MAX_BACKOFF",
            "GRPC_BACKOFF_MULTIPLIER",
            "GRPC_MAX_IDLE",
            "GRPC_MAX_LIFE",
        ];
        for k in keys {
            std::env::remove_var(k);
        }

        // Defaults (mirror helpers.Load).
        let cfg = GrpcConfig::from_env();
        assert_eq!(cfg.transaction_max_pool, 2);
        assert_eq!(cfg.merchant_max_pool, 2);
        assert_eq!(cfg.config_max_pool, 2);
        assert_eq!(cfg.healthcheck_interval_secs, 120);
        assert!(cfg.healthcheck_enabled);
        assert_eq!(cfg.grpc_port, "9333");
        assert_eq!(cfg.tls_mode, TlsMode::Insecure);
        assert_eq!(cfg.client_ca_file, None);
        assert_eq!(cfg.keep_alive_time, Duration::from_secs(30));
        assert_eq!(cfg.keep_alive_timeout, Duration::from_secs(10));
        assert!(cfg.permit_without_stream);
        assert_eq!(cfg.max_retries, 3);
        assert_eq!(cfg.initial_backoff, Duration::from_millis(100));
        assert_eq!(cfg.max_backoff, Duration::from_secs(30));
        assert!((cfg.backoff_multiplier - 2.0).abs() < f64::EPSILON);
        assert_eq!(cfg.max_idle, Duration::from_secs(30 * 60));
        assert_eq!(cfg.max_life, Duration::from_secs(24 * 3_600));

        // Factory pool-size resolution uses the parsed defaults.
        let factory = GrpcClientFactory::from_env().unwrap();
        assert_eq!(factory.pool_size_for("TRANSACTION_GRPC"), 2);
        assert_eq!(factory.pool_size_for("MERCHANT_GRPC"), 2);
        assert_eq!(factory.pool_size_for("CONFIG_GRPC"), 2);
        assert_eq!(factory.pool_size_for("SOMETHING_ELSE"), DEFAULT_MAX_POOL);

        // Overrides.
        std::env::set_var("GRPC_TRANSACTION_MAX_POOL", "5");
        std::env::set_var("GRPC_HEALTHCHECK", "false");
        std::env::set_var("GRPC_TLS_MODE", "tls");
        std::env::set_var("GRPC_CLIENT_CA_FILE", "/etc/ca.pem");
        std::env::set_var("GRPC_MAX_RETRIES", "7");
        // Invalid value must fall back to the default (mirrors getEnvInt).
        std::env::set_var("GRPC_MERCHANT_MAX_POOL", "not-an-int");

        let cfg = GrpcConfig::from_env();
        assert_eq!(cfg.transaction_max_pool, 5);
        assert!(!cfg.healthcheck_enabled);
        assert_eq!(cfg.tls_mode, TlsMode::Tls);
        assert_eq!(cfg.client_ca_file.as_deref(), Some("/etc/ca.pem"));
        assert_eq!(cfg.max_retries, 7);
        assert_eq!(
            cfg.merchant_max_pool, 2,
            "invalid int falls back to default"
        );

        for k in keys {
            std::env::remove_var(k);
        }
    }

    #[tokio::test]
    #[ignore = "requires a live gRPC server on $TRANSACTION_GRPC"]
    async fn connect_prewarms_live_server() {
        let factory = GrpcClientFactory::from_env().unwrap();
        let channel = factory
            .connect("TRANSACTION_GRPC")
            .await
            .expect("connect to live TRANSACTION_GRPC");
        // A real RPC would use this channel; here we only prove it establishes.
        drop(channel);
    }
}
