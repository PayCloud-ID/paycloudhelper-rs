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
//! [`GrpcClientFactory::connect`] prewarms and therefore *fails* when the
//! upstream is unreachable — correct when the dependency is required.
//! [`GrpcClientFactory::connect_lazy`] builds the identical pool without the
//! probe, for services that must boot degraded instead of refusing to start.
//! Neither needs the caller to rebuild endpoints by hand.
//!
//! # DNS discovery (`GRPC_DNS_REFRESH_SEC`)
//!
//! By default the pool is `pool` subchannels on one DNS name, each resolving
//! that name once. Against a Kubernetes **headless** Service that pins every
//! subchannel to the pods that existed at connect time: a pod added later gets
//! no traffic. Setting `GRPC_DNS_REFRESH_SEC` to a positive number of seconds
//! makes [`GrpcClientFactory::connect`] / [`GrpcClientFactory::connect_lazy`]
//! resolve the name to its A records, keep `pool` connections per address and
//! re-resolve on that interval (see [`discovery`]). Unset or `0` is exactly the
//! behaviour above; nothing else changes.
//!
//! It also provides the RC-1 shrinking-budget deadline helpers
//! ([`child_budget`]/[`with_deadline`]): a child call's deadline is strictly
//! less than the wall budget so it always expires first.
//!
//! The parity-critical *pure* logic — env parsing defaults, the dial-target
//! shape and the deadline-margin math — is unit-tested offline. Anything
//! that needs a live gRPC server (an actual [`GrpcClientFactory::connect`]) is
//! `#[ignore]`d.

pub mod discovery;

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use tonic::transport::channel::Change;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

use crate::discovery::{run_refresher, EndpointKey, Resolver, SystemResolver};

/// Default connections-per-target for every pool.
///
/// mirrors: the `2` default of `getEnvInt("GRPC_TRANSACTION_MAX_POOL", 2)` (and
/// the `maxConn <= 0 { maxConn = 2 }` clamp in `NewGrpcClientPool`).
pub const DEFAULT_MAX_POOL: usize = 2;

// Defaults for the settings this crate parses but does not yet act on. Named
// so `from_env` and `warn_inert_settings` share one source of truth — a check
// carrying its own copy of a default drifts the moment either is edited.
const DEFAULT_HEALTHCHECK_INTERVAL_SECS: u64 = 120;
const DEFAULT_HEALTHCHECK_ENABLED: bool = true;
const DEFAULT_BACKOFF_MULTIPLIER: f64 = 2.0;
const DEFAULT_MAX_IDLE_MINUTES: u64 = 30;
const DEFAULT_MAX_LIFE_HOURS: u64 = 24;

/// Buffer of the discovery channel's change queue (inserts/removes in flight).
const DISCOVERY_BUFFER: usize = 1024;

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
    /// `GRPC_DNS_REFRESH_SEC` (default 0 = off) — how often to re-resolve the
    /// target's A records and follow pods that joined or left. `Duration::ZERO`
    /// keeps the fixed, resolve-once pool. No Go counterpart.
    pub dns_refresh: Duration,
}

impl GrpcConfig {
    /// Parse the full `GRPC_*` configuration from the process environment.
    ///
    /// Invalid values fall back to the default (mirrors `getEnvInt`/`getEnvBool`
    /// et al., which log a warning and keep the default on a parse error).
    ///
    /// mirrors: `helpers.Load` (gRPC fields) + `ApplyGrpcEnvConfig`.
    pub fn from_env() -> Self {
        let config = Self::parse_env();
        config.warn_inert_settings();
        config
    }

    fn parse_env() -> Self {
        Self {
            transaction_max_pool: env_parsed("GRPC_TRANSACTION_MAX_POOL", DEFAULT_MAX_POOL),
            merchant_max_pool: env_parsed("GRPC_MERCHANT_MAX_POOL", DEFAULT_MAX_POOL),
            config_max_pool: env_parsed("GRPC_CONFIG_MAX_POOL", DEFAULT_MAX_POOL),
            healthcheck_interval_secs: env_parsed(
                "GRPC_HEALTHCHECK_INTERVAL",
                DEFAULT_HEALTHCHECK_INTERVAL_SECS,
            ),
            healthcheck_enabled: env_bool("GRPC_HEALTHCHECK", DEFAULT_HEALTHCHECK_ENABLED),
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
            backoff_multiplier: env_parsed("GRPC_BACKOFF_MULTIPLIER", DEFAULT_BACKOFF_MULTIPLIER),
            max_idle: Duration::from_secs(
                env_parsed::<u64>("GRPC_MAX_IDLE", DEFAULT_MAX_IDLE_MINUTES) * 60,
            ),
            max_life: Duration::from_secs(
                env_parsed::<u64>("GRPC_MAX_LIFE", DEFAULT_MAX_LIFE_HOURS) * 3_600,
            ),
            dns_refresh: parse_dns_refresh(std::env::var("GRPC_DNS_REFRESH_SEC").ok().as_deref()),
        }
    }

    /// Warn about settings that are parsed and stored but never acted on.
    ///
    /// Five `GRPC_*` variables are read here, kept on the struct, asserted in
    /// tests — and then no code reads them. Implementing five features is not a
    /// parity pass's job, but silently accepting `GRPC_HEALTHCHECK=false` and
    /// then behaving identically is the kind of thing that costs an afternoon
    /// and ends with someone concluding the config system is broken. A line at
    /// construction turns a silent no-op into a visible one.
    ///
    /// Only **non-default** values warn. A default is what the code does anyway,
    /// so warning about it would put five lines in every boot log and train
    /// everyone to skip them.
    fn warn_inert_settings(&self) {
        let mut inert: Vec<String> = Vec::new();
        if self.healthcheck_interval_secs != DEFAULT_HEALTHCHECK_INTERVAL_SECS {
            inert.push(format!(
                "GRPC_HEALTHCHECK_INTERVAL={}",
                self.healthcheck_interval_secs
            ));
        }
        if self.healthcheck_enabled != DEFAULT_HEALTHCHECK_ENABLED {
            inert.push(format!("GRPC_HEALTHCHECK={}", self.healthcheck_enabled));
        }
        // Exact comparison is right here: the question is "did an operator set
        // this?", and any value that round-trips to something other than the
        // literal default came from the environment.
        if self.backoff_multiplier != DEFAULT_BACKOFF_MULTIPLIER {
            inert.push(format!(
                "GRPC_BACKOFF_MULTIPLIER={}",
                self.backoff_multiplier
            ));
        }
        if self.max_idle != Duration::from_secs(DEFAULT_MAX_IDLE_MINUTES * 60) {
            inert.push(format!("GRPC_MAX_IDLE={:?}", self.max_idle));
        }
        if self.max_life != Duration::from_secs(DEFAULT_MAX_LIFE_HOURS * 3_600) {
            inert.push(format!("GRPC_MAX_LIFE={:?}", self.max_life));
        }

        if !inert.is_empty() {
            tracing::warn!(
                settings = %inert.join(" "),
                "[GrpcConfig] these gRPC settings are parsed but not implemented yet \
                 and will have no effect"
            );
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

    /// The DNS refresh interval when discovery is on (`GRPC_DNS_REFRESH_SEC` > 0).
    pub fn discovery_refresh(&self) -> Option<Duration> {
        (!self.config.dns_refresh.is_zero()).then_some(self.config.dns_refresh)
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
    /// With `GRPC_DNS_REFRESH_SEC` > 0 this is [`GrpcClientFactory::connect_discovered`]
    /// (same fail-fast contract, but the pool follows the Service's A records).
    ///
    /// mirrors: `GrpcClientPool.GetConnection` + `createConnection` (round-robin
    /// pool, retry-with-backoff establishment) over the target that
    /// `GrpcManager.initializeConnections` dials.
    pub async fn connect(&self, target_env: &str) -> Result<Channel> {
        if let Some(refresh) = self.discovery_refresh() {
            return self.connect_discovered(target_env, refresh).await;
        }
        let (target, pool) = self.resolve_target(target_env)?;
        let endpoint = self.build_endpoint(&target, None)?;

        tracing::info!(target = %target, pool, "pc-grpc: prewarming round-robin channel");

        // Prewarm: eagerly establish one connection with retry so a dead
        // upstream surfaces here (mirrors createConnection's retry loop).
        self.prewarm(&endpoint).await?;

        // Round-robin pool: N identical subchannels over the same DNS target
        // (mirrors GrpcClientPool's maxConn connections + round_robin policy).
        let endpoints = vec![endpoint; pool];
        Ok(Channel::balance_list(endpoints.into_iter()))
    }

    /// Build the same round-robin pool as [`GrpcClientFactory::connect`] but
    /// **without** the prewarm probe, so an unresolvable or dead target does not
    /// abort the caller. `tonic` re-dials the returned channel in the
    /// background; calls made before the first connection succeeds fail with
    /// `UNAVAILABLE` rather than panicking.
    ///
    /// Use this when the service must start in a degraded state instead of
    /// refusing to boot — a Kubernetes rollout where dependencies come up in an
    /// arbitrary order, for example. [`GrpcClientFactory::connect`] remains the
    /// default: it surfaces a dead upstream at startup, which is what you want
    /// when the dependency is genuinely required.
    ///
    /// Synchronous, because no connection is established. It must still be
    /// called **from inside a Tokio runtime**: `Channel::balance_list` spawns
    /// the balancer task eagerly and panics without a reactor. Calling it from
    /// `main` before the runtime starts, or from a plain `#[test]`, panics.
    ///
    /// mirrors: `createConnection` with the retry loop skipped — the same
    /// `grpc.DialOption` set and `maxConn` round-robin pool that
    /// `GrpcManager.initializeConnections` builds.
    ///
    /// With `GRPC_DNS_REFRESH_SEC` > 0 this is
    /// [`GrpcClientFactory::connect_discovered_lazy`]: still synchronous and
    /// still no DNS at construction (the first lookup happens in the background).
    pub fn connect_lazy(&self, target_env: &str) -> Result<Channel> {
        if let Some(refresh) = self.discovery_refresh() {
            return self.connect_discovered_lazy(target_env, refresh);
        }
        let (target, pool) = self.resolve_target(target_env)?;
        let endpoint = self.build_endpoint(&target, None)?;

        tracing::info!(
            target = %target,
            pool,
            "pc-grpc: lazy round-robin channel (no prewarm)"
        );

        let endpoints = vec![endpoint; pool];
        Ok(Channel::balance_list(endpoints.into_iter()))
    }

    /// Like [`GrpcClientFactory::connect`], but resolve the target host to its
    /// A records, keep `pool` connections **per address**, and re-resolve every
    /// `refresh` so pods that join are used and pods that leave are dropped.
    /// Use against a Kubernetes headless Service (a ClusterIP name resolves to
    /// one address and gains nothing).
    ///
    /// **Initial resolution is fail-fast**, matching `connect`: the first lookup
    /// is retried up to `GRPC_MAX_RETRIES` times with the usual backoff, and an
    /// error — or an empty answer — after that is returned, as is a failed
    /// prewarm of the first address. After startup a failed or empty lookup only
    /// logs and keeps the current endpoints, and the last address is never
    /// removed. Under TLS the original host name stays the TLS domain
    /// (SNI/verification), although the connections dial IPs.
    ///
    /// The refresher is a spawned Tokio task that ends when the returned
    /// `Channel` (and every clone) is dropped. `refresh` must be non-zero.
    pub async fn connect_discovered(&self, target_env: &str, refresh: Duration) -> Result<Channel> {
        self.connect_discovered_with(target_env, refresh, Arc::new(SystemResolver))
            .await
    }

    /// The no-boot-failure counterpart of [`GrpcClientFactory::connect_discovered`],
    /// for [`GrpcClientFactory::connect_lazy`] callers.
    ///
    /// Synchronous and never touches DNS itself. The channel starts with the
    /// plain host-name pool (`pool` endpoints on the Service name, exactly what
    /// [`GrpcClientFactory::connect_lazy`] builds without discovery), so a call
    /// made before DNS answers — or while it never does — fails fast with
    /// `UNAVAILABLE` instead of waiting on an empty balancer, deadline or not.
    /// The refresher's first lookup runs at once in the background; if it fails
    /// or comes back empty it is retried after 1s, 2s, 4s, … (capped at
    /// `refresh`) until it succeeds, and on that first success the per-address
    /// endpoints are inserted and the host-name seed is removed. Must be called
    /// inside a Tokio runtime, like `connect_lazy`.
    pub fn connect_discovered_lazy(&self, target_env: &str, refresh: Duration) -> Result<Channel> {
        self.connect_discovered_lazy_with(target_env, refresh, Arc::new(SystemResolver))
    }

    pub(crate) async fn connect_discovered_with(
        &self,
        target_env: &str,
        refresh: Duration,
        resolver: Arc<dyn Resolver>,
    ) -> Result<Channel> {
        let (host, port, pool) = self.discovery_target(target_env, refresh)?;
        let initial = self.initial_lookup(resolver.as_ref(), &host, port).await?;

        let build = self.discovery_builder(&host);
        let first = *initial.iter().next().expect("initial_lookup is non-empty");
        self.prewarm(&build(first)?).await?;

        tracing::info!(
            host = %host, port, pool, addrs = initial.len(),
            refresh_secs = refresh.as_secs(),
            "pc-grpc: discovery-aware round-robin channel"
        );

        let (channel, tx) = Channel::balance_channel(DISCOVERY_BUFFER);
        for addr in &initial {
            let endpoint = build(*addr)?;
            for slot in 0..pool {
                tx.send(Change::Insert(
                    EndpointKey::Addr(*addr, slot),
                    endpoint.clone(),
                ))
                .await
                .map_err(|_| anyhow!("gRPC balancer closed during setup"))?;
            }
        }
        tokio::spawn(run_refresher(
            resolver, host, port, refresh, false, pool, 0, initial, build, tx,
        ));
        Ok(channel)
    }

    pub(crate) fn connect_discovered_lazy_with(
        &self,
        target_env: &str,
        refresh: Duration,
        resolver: Arc<dyn Resolver>,
    ) -> Result<Channel> {
        let (host, port, pool) = self.discovery_target(target_env, refresh)?;
        tracing::info!(
            host = %host, port, pool, refresh_secs = refresh.as_secs(),
            "pc-grpc: lazy discovery-aware round-robin channel (no prewarm)"
        );
        let (channel, tx) = Channel::balance_channel(DISCOVERY_BUFFER);

        // Seed with today's plain host-name pool so the balancer is never empty:
        // until the first lookup lands (or if it never does) calls behave exactly
        // like the non-discovery lazy path, including failing fast with
        // `UNAVAILABLE`. The refresher drops the seed after its first real insert.
        let seed = self.build_endpoint(&dial_target(&host, port), None)?;
        for slot in 0..pool {
            tx.try_send(Change::Insert(EndpointKey::Seed(slot), seed.clone()))
                .map_err(|_| anyhow!("gRPC balancer rejected the seed endpoint"))?;
        }

        let build = self.discovery_builder(&host);
        tokio::spawn(run_refresher(
            resolver,
            host,
            port,
            refresh,
            true,
            pool,
            pool,
            BTreeSet::new(),
            build,
            tx,
        ));
        Ok(channel)
    }

    /// `(host, port, pool)` for a discovery connect; rejects a zero `refresh`.
    fn discovery_target(
        &self,
        target_env: &str,
        refresh: Duration,
    ) -> Result<(String, u16, usize)> {
        if refresh.is_zero() {
            return Err(anyhow!("DNS discovery needs a non-zero refresh interval"));
        }
        let (target, pool) = self.resolve_target(target_env)?;
        let (host, port) = parse_host_port(&target)?;
        Ok((host, port, pool))
    }

    /// Builds the per-address endpoint: dial the IP, keep `host` as TLS domain.
    fn discovery_builder(
        &self,
        host: &str,
    ) -> impl Fn(SocketAddr) -> Result<Endpoint> + Send + 'static {
        let factory = self.clone();
        let host = host.to_string();
        move |addr| factory.build_endpoint(&addr.to_string(), Some(&host))
    }

    /// First lookup for [`GrpcClientFactory::connect_discovered`]: retried like
    /// `prewarm`; an empty answer counts as a failure.
    async fn initial_lookup(
        &self,
        resolver: &dyn Resolver,
        host: &str,
        port: u16,
    ) -> Result<BTreeSet<SocketAddr>> {
        let attempts = self.config.max_retries.max(1);
        let mut last_err = anyhow!("no addresses for `{host}`");
        for attempt in 1..=attempts {
            match resolver.lookup(host, port).await {
                Ok(addrs) if !addrs.is_empty() => return Ok(addrs),
                Ok(_) => last_err = anyhow!("`{host}` resolved to no addresses"),
                Err(err) => last_err = anyhow!(err).context(format!("resolve `{host}`")),
            }
            tracing::warn!(attempt, host = %host, error = %last_err, "pc-grpc: initial discovery lookup failed");
            if attempt < attempts {
                tokio::time::sleep(self.backoff_for(attempt)).await;
            }
        }
        Err(last_err.context("gRPC discovery: initial resolution exhausted retries"))
    }

    /// Resolve a `*_GRPC` target env name to its dial target and pool size
    /// without connecting.
    ///
    /// Exposed so callers can log or assert on what *would* be dialled — and so
    /// they never have to re-derive it by hand from
    /// [`parse_host_port`] + [`dial_target`] + [`GrpcClientFactory::pool_size_for`].
    pub fn resolve_target(&self, target_env: &str) -> Result<(String, usize)> {
        let raw = std::env::var(target_env)
            .with_context(|| format!("gRPC target env `{target_env}` is not set"))?;
        let (host, port) = parse_host_port(&raw)
            .with_context(|| format!("gRPC target `{target_env}`=`{raw}` is not host:port"))?;
        Ok((dial_target(&host, port), self.pool_size_for(target_env)))
    }

    /// Build a configured (but not yet connected) [`Endpoint`] for a
    /// `host:port` target.
    ///
    /// mirrors: the `grpc.DialOption` set in `NewGrpcClientPool`
    /// (keep-alive params, connect timeout) plus `resolveTransportCredentials`.
    ///
    /// `tls_domain` overrides the TLS server name; discovery dials pod IPs but
    /// must still verify the certificate against the Service name.
    fn build_endpoint(&self, target: &str, tls_domain: Option<&str>) -> Result<Endpoint> {
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
            if let Some(domain) = tls_domain {
                tls = tls.domain_name(domain);
            }
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
    /// mirrors: `createConnection`'s `time.Duration(attempt*attempt*100) *
    /// time.Millisecond` (`app/grpc_pool.go:562`) — **quadratic**, not linear.
    /// This grew linearly while its own doc claimed to mirror the quadratic
    /// curve, so at the default 100ms base a 3-attempt prewarm waited
    /// 100/200/300ms where Go waits 100/400/900ms.
    ///
    /// Two deliberate differences from Go, both harmless: Go hardcodes the 100ms
    /// base here (its `InitialBackoff` field feeds a different mechanism, the
    /// grpc service-config retry policy at `grpc_pool.go:361`), whereas this
    /// scales the configured `initial_backoff` — identical at the default. And
    /// Go applies no ceiling to this loop, because with 3 attempts it never
    /// needs one; the `max_backoff` clamp is kept as a guard for anyone who
    /// raises `GRPC_MAX_RETRIES`.
    fn backoff_for(&self, attempt: u32) -> Duration {
        backoff_curve(
            self.config.initial_backoff,
            self.config.max_backoff,
            attempt,
        )
    }
}

/// `initial × attempt²`, clamped to `max` (pure — testable without a pool).
///
/// Split out because the linear-vs-quadratic bug lived here undetected: there
/// was no test, and the only statement of intent was a doc comment that
/// disagreed with the code.
fn backoff_curve(initial: Duration, max: Duration, attempt: u32) -> Duration {
    initial
        .saturating_mul(attempt.saturating_mul(attempt))
        .min(max)
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

/// Parse `GRPC_DNS_REFRESH_SEC`: unset, empty or `0` is off; anything that is not
/// a non-negative whole number of seconds is treated as off, with a warning.
fn parse_dns_refresh(raw: Option<&str>) -> Duration {
    let Some(raw) = raw.map(str::trim).filter(|v| !v.is_empty()) else {
        return Duration::ZERO;
    };
    if let Ok(secs) = raw.parse::<u64>() {
        return Duration::from_secs(secs);
    }
    tracing::warn!(
        value = raw,
        "pc-grpc: invalid GRPC_DNS_REFRESH_SEC; DNS discovery stays off"
    );
    Duration::ZERO
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
///
/// The grammar itself lives in [`pc_core::parse_bool_go`] so this crate and
/// `pc-log` cannot drift apart on what `1` means. The `.trim()` is this crate's
/// own tolerance, preserved from the original: deploy values arrive through
/// compose and `env-data` files, where a trailing space is easy to introduce and
/// invisible in review.
fn env_bool(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(v) => pc_core::parse_bool_go(v.trim()).unwrap_or(default),
        Err(_) => default,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Serialises tests that mutate process-global environment variables.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// Go: `time.Duration(attempt*attempt*100) * time.Millisecond`
    /// (`app/grpc_pool.go:562`). At the default 100ms base a 3-attempt prewarm
    /// must wait 100/400/900ms; it waited 100/200/300ms, because the growth was
    /// linear while the doc comment claimed to mirror the quadratic curve.
    #[test]
    fn backoff_matches_go_quadratic_curve() {
        let initial = Duration::from_millis(100);
        let max = Duration::from_secs(30);
        assert_eq!(backoff_curve(initial, max, 1), Duration::from_millis(100));
        assert_eq!(backoff_curve(initial, max, 2), Duration::from_millis(400));
        assert_eq!(backoff_curve(initial, max, 3), Duration::from_millis(900));
    }

    /// Go's loop is capped at 3 attempts so it never needs a ceiling; this crate
    /// keeps one for anyone who raises `GRPC_MAX_RETRIES`, and it must hold even
    /// where `attempt²` would overflow.
    #[test]
    fn backoff_is_clamped_and_cannot_overflow() {
        let initial = Duration::from_millis(100);
        let max = Duration::from_secs(30);
        assert_eq!(backoff_curve(initial, max, 100), max, "100²×100ms > 30s");
        assert_eq!(backoff_curve(initial, max, u32::MAX), max);
    }

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
            "GRPC_DNS_REFRESH_SEC",
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
        assert_eq!(cfg.dns_refresh, Duration::ZERO, "discovery is opt-in");

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

    #[test]
    fn resolve_target_reports_dial_target_and_pool_size() {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        std::env::remove_var("GRPC_TRANSACTION_MAX_POOL");
        std::env::set_var("TRANSACTION_GRPC", "paycloud-be-transaction-module:9106");
        let factory = GrpcClientFactory::from_env().unwrap();

        let (target, pool) = factory.resolve_target("TRANSACTION_GRPC").unwrap();
        assert_eq!(target, "paycloud-be-transaction-module:9106");
        assert_eq!(pool, DEFAULT_MAX_POOL);

        // A malformed or absent env value is an error, not a panic.
        std::env::set_var("TRANSACTION_GRPC", "no-port");
        assert!(factory.resolve_target("TRANSACTION_GRPC").is_err());
        std::env::remove_var("TRANSACTION_GRPC");
        assert!(factory.resolve_target("TRANSACTION_GRPC").is_err());
    }

    // Not a plain `#[test]`: `balance_list` spawns its balancer eagerly and
    // panics with "there is no reactor running" outside a Tokio context. The
    // call itself is still synchronous — see `connect_lazy`'s docs.
    #[tokio::test]
    async fn connect_lazy_survives_an_unresolvable_target() {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        // The whole point: a DNS name that cannot resolve must still yield a
        // channel, because the caller has to boot degraded rather than abort.
        // `connect` would exhaust its retries here and return Err.
        std::env::set_var("TRANSACTION_GRPC", "unresolvable.invalid:9106");
        std::env::set_var("GRPC_TLS_MODE", "");
        let factory = GrpcClientFactory::from_env().unwrap();

        let channel = factory
            .connect_lazy("TRANSACTION_GRPC")
            .expect("a lazy channel needs no reachable upstream");
        drop(channel);

        std::env::remove_var("TRANSACTION_GRPC");
        std::env::remove_var("GRPC_TLS_MODE");
    }

    // ---- DNS discovery wiring (GRPC_DNS_REFRESH_SEC) ----

    #[test]
    fn dns_refresh_parsing_defaults_off_and_warns_on_garbage() {
        assert_eq!(parse_dns_refresh(None), Duration::ZERO);
        assert_eq!(parse_dns_refresh(Some("")), Duration::ZERO);
        assert_eq!(parse_dns_refresh(Some("  ")), Duration::ZERO);
        assert_eq!(parse_dns_refresh(Some("0")), Duration::ZERO);
        assert_eq!(parse_dns_refresh(Some("30")), Duration::from_secs(30));
        assert_eq!(parse_dns_refresh(Some(" 5 ")), Duration::from_secs(5));
        // Invalid values do not enable discovery.
        assert_eq!(parse_dns_refresh(Some("abc")), Duration::ZERO);
        assert_eq!(parse_dns_refresh(Some("-3")), Duration::ZERO);
        assert_eq!(parse_dns_refresh(Some("1.5")), Duration::ZERO);
    }

    #[test]
    fn discovery_is_selected_only_by_a_positive_refresh() {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        std::env::remove_var("GRPC_DNS_REFRESH_SEC");
        assert_eq!(factory_from_env().discovery_refresh(), None);
        std::env::set_var("GRPC_DNS_REFRESH_SEC", "0");
        assert_eq!(factory_from_env().discovery_refresh(), None);
        std::env::set_var("GRPC_DNS_REFRESH_SEC", "nope");
        assert_eq!(factory_from_env().discovery_refresh(), None);
        std::env::set_var("GRPC_DNS_REFRESH_SEC", "15");
        assert_eq!(
            factory_from_env().discovery_refresh(),
            Some(Duration::from_secs(15))
        );
        std::env::remove_var("GRPC_DNS_REFRESH_SEC");
    }

    fn factory_from_env() -> GrpcClientFactory {
        GrpcClientFactory::from_env().unwrap()
    }

    use crate::discovery::LookupFuture;

    /// Resolver returning a fixed answer, counting calls.
    struct FixedResolver {
        answer: Vec<SocketAddr>,
        calls: std::sync::atomic::AtomicUsize,
    }

    impl FixedResolver {
        fn new(answer: &[&str]) -> Arc<Self> {
            Arc::new(Self {
                answer: answer.iter().map(|a| a.parse().unwrap()).collect(),
                calls: std::sync::atomic::AtomicUsize::new(0),
            })
        }
        fn calls(&self) -> usize {
            self.calls.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl Resolver for FixedResolver {
        fn lookup<'a>(&'a self, _host: &'a str, _port: u16) -> LookupFuture<'a> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let out = self.answer.iter().copied().collect();
            Box::pin(async move { Ok(out) })
        }
    }

    fn quick_factory() -> GrpcClientFactory {
        let mut f = factory_from_env();
        f.config.max_retries = 1;
        f
    }

    // Env-mutating tests hold ENV_LOCK across awaits on purpose: they must
    // exclude each other, and a current-thread test runtime cannot deadlock on it.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn connect_discovered_fails_fast_on_an_empty_initial_lookup() {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::env::set_var("TRANSACTION_GRPC", "svc-headless:9106");

        let resolver = FixedResolver::new(&[]);
        let err = quick_factory()
            .connect_discovered_with(
                "TRANSACTION_GRPC",
                Duration::from_secs(30),
                resolver.clone(),
            )
            .await
            .expect_err("no addresses must be an error, like `connect`");
        assert!(format!("{err:#}").contains("no addresses"), "{err:#}");
        assert_eq!(resolver.calls(), 1);

        // A zero interval is rejected rather than spinning.
        let err = quick_factory()
            .connect_discovered_with("TRANSACTION_GRPC", Duration::ZERO, resolver)
            .await
            .expect_err("zero refresh");
        assert!(format!("{err:#}").contains("non-zero"), "{err:#}");
        std::env::remove_var("TRANSACTION_GRPC");
    }

    // Env-mutating tests hold ENV_LOCK across awaits on purpose: they must
    // exclude each other, and a current-thread test runtime cannot deadlock on it.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn connect_discovered_prewarms_the_first_address_and_returns_a_channel() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            // Hold accepted sockets open; the prewarm only needs the TCP/h2 preface to go out.
            let mut held = Vec::new();
            while let Ok((sock, _)) = listener.accept().await {
                held.push(sock);
            }
        });
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::env::set_var("TRANSACTION_GRPC", format!("svc-headless:{}", addr.port()));

        let resolver = FixedResolver::new(&[&addr.to_string()]);
        let channel = quick_factory()
            .connect_discovered_with("TRANSACTION_GRPC", Duration::from_secs(30), resolver)
            .await
            .expect("discovered channel");
        drop(channel);
        std::env::remove_var("TRANSACTION_GRPC");
    }

    // Env-mutating tests hold ENV_LOCK across awaits on purpose: they must
    // exclude each other, and a current-thread test runtime cannot deadlock on it.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn connect_lazy_with_refresh_does_not_resolve_at_construction() {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::env::set_var("TRANSACTION_GRPC", "unresolvable.invalid:9106");
        std::env::set_var("GRPC_DNS_REFRESH_SEC", "30");
        let factory = factory_from_env();
        std::env::remove_var("GRPC_DNS_REFRESH_SEC");

        // Takes the discovery branch (refresh is on) and still returns
        // synchronously, Ok, without any reachable upstream or DNS answer.
        assert_eq!(factory.discovery_refresh(), Some(Duration::from_secs(30)));
        let channel = factory
            .connect_lazy("TRANSACTION_GRPC")
            .expect("lazy discovery must not need DNS to construct");
        drop(channel);
        std::env::remove_var("TRANSACTION_GRPC");
    }

    // Env-mutating tests hold ENV_LOCK across awaits on purpose: they must
    // exclude each other, and a current-thread test runtime cannot deadlock on it.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn connect_discovered_lazy_resolves_in_the_background() {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::env::set_var("TRANSACTION_GRPC", "svc-headless:9106");

        let resolver = FixedResolver::new(&["10.0.0.1:9106"]);
        let channel = quick_factory()
            .connect_discovered_lazy_with(
                "TRANSACTION_GRPC",
                Duration::from_secs(30),
                resolver.clone(),
            )
            .expect("lazy discovered channel");
        // The refresher's first lookup is immediate, off the caller's thread.
        for _ in 0..50 {
            if resolver.calls() > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            resolver.calls(),
            1,
            "first lookup runs without waiting a tick"
        );
        drop(channel);
        std::env::remove_var("TRANSACTION_GRPC");
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
