#![forbid(unsafe_code)]
//! `pc-redis` — bit-for-bit port of the Go `paycloudhelper` Redis surface
//! (`redis.go` + `mutex.go`).
//!
//! Mirrors the optional-Redis lifecycle, the TTL soft-clamp guardrail
//! (`MaxTTL` / `clampStoreTTL`), the `StoreRedisWithContext` / `GetRedis` /
//! `DeleteRedis` accessors, and the redsync distributed lock (here backed by
//! [`rslock`], which is the same Redlock algorithm with drift factor `0.01`).
//!
//! ## Deviations from the Go original (documented for parity reviewers)
//!
//! - Go keeps a process-global `atomic.Pointer[redis.Client]`; the Rust port
//!   returns an owned [`RedisPool`] handle (a `deadpool_redis` pool plus an
//!   [`rslock::LockManager`]) so callers hold the connection explicitly.
//! - `std::time::Duration` is unsigned, so a *negative* TTL — expressible in
//!   Go's signed `time.Duration` — cannot reach [`clamp_ttl`]. The
//!   negative → 0 rule is still implemented (and unit-tested) in the
//!   millisecond-domain [`clamp_ttl_with_max`], which the public wrapper calls.
//! - A redis-nil (key miss) is quiet: [`RedisPool::get`] returns `Ok(None)`,
//!   never an error, matching Go's `GetRedis` returning `("", nil)`.

use std::fmt::Display;
use std::time::Duration;

use redis::AsyncCommands;
use serde::Serialize;

/// Re-export of the acquired distributed lock handle.
///
/// mirrors: the `*redsync.Mutex` stored by Go's `AcquireLock` / `StoreMutex`.
pub use rslock::Lock;

// ---------------------------------------------------------------------------
// Key-format helpers (pure). Parity contract: design 02 §5.
// ---------------------------------------------------------------------------

/// CSRF cache key: `csrf-<token>`.
///
/// mirrors: `csrf.go` — `GetRedis("csrf-" + header.Csrf)`.
#[must_use]
pub fn csrf_key(token: &str) -> String {
    format!("csrf-{token}")
}

/// JWT revocation key: `revoke_token_<merchantId>`.
///
/// mirrors: `middleware_revoke_jwt` — `StoreRedis("revoke_token_" + id, ...)`.
pub fn revoke_token_key(merchant_id: impl Display) -> String {
    format!("revoke_token_{merchant_id}")
}

/// Distributed-lock key prefix: `redis_lock:<AppName>:`.
///
/// mirrors: Go's `redisLockKey = fmt.Sprintf("redis_lock:%s:", GetAppName())`
/// set in `InitializeRedisWithRetry`. `AppName` is read from process identity.
#[must_use]
pub fn lock_key_prefix() -> String {
    format!("redis_lock:{}:", pc_core::identity::app_name())
}

/// Full distributed-lock key: `redis_lock:<AppName>:<name>`.
///
/// mirrors: Go's `redisLockKey + id` in `StoreRedisWithLock`.
#[must_use]
pub fn lock_key(name: &str) -> String {
    format!("{}{name}", lock_key_prefix())
}

// ---------------------------------------------------------------------------
// TTL guardrail (Go: MaxTTL / clampStoreTTL). Parity contract: design 02 §5.
// ---------------------------------------------------------------------------

/// Default TTL ceiling in minutes: 30 days (`30 * 24 * 60`).
const DEFAULT_MAX_TTL_MINUTES: i64 = 30 * 24 * 60; // 43200

/// The configured TTL ceiling in minutes.
///
/// mirrors: `MaxTTL` — reads `REDIS_MAX_TTL_MINUTES`; a value `<= 0` (or an
/// unparseable / unset value) falls back to the 43200-minute default.
#[must_use]
pub fn max_ttl_minutes() -> i64 {
    match env_i64("REDIS_MAX_TTL_MINUTES") {
        Some(n) if n > 0 => n,
        _ => DEFAULT_MAX_TTL_MINUTES,
    }
}

/// Soft-clamp a caller TTL against the env-configured ceiling.
///
/// mirrors: `clampStoreTTL` composed with `MaxTTL`.
#[must_use]
pub fn clamp_ttl(d: Duration) -> Duration {
    // `Duration` is unsigned, so `as_millis()` is always `>= 0`; saturate to
    // `i64::MAX` for the (astronomically large) overflow case, where the clamp
    // caps it at the ceiling anyway.
    let ms = i64::try_from(d.as_millis()).unwrap_or(i64::MAX);
    let clamped = clamp_ttl_with_max(ms, max_ttl_minutes());
    // `clamped` is in `[0, max_minutes * 60_000]`, always non-negative.
    Duration::from_millis(u64::try_from(clamped).unwrap_or(0))
}

/// Millisecond-domain soft-clamp with an injectable ceiling (unit-testable
/// without env-mutation races — the reason this is split out).
///
/// mirrors: `clampStoreTTL` — negative → 0 (no-expiry) with a warning;
/// above-ceiling → ceiling with a warning; otherwise unchanged.
#[must_use]
pub fn clamp_ttl_with_max(ttl_ms: i64, max_minutes: i64) -> i64 {
    if ttl_ms < 0 {
        tracing::warn!(
            ttl_ms,
            "[StoreRedis] negative TTL, clamping to 0 (no-expiry)"
        );
        return 0;
    }
    let max_ms = max_minutes.saturating_mul(60_000);
    if ttl_ms > max_ms {
        tracing::warn!(
            ttl_ms,
            max_ms,
            "[StoreRedis] TTL exceeds max, clamping (possible overflow bug)"
        );
        return max_ms;
    }
    ttl_ms
}

/// Outcome of a [`RedisPool::ttl`] lookup.
///
/// Redis `PTTL` overloads one integer with three distinct answers, and
/// collapsing them into `Option<Duration>` loses the one that matters: a key
/// that is absent and a key that never expires both become `None`, so a caller
/// deciding whether to refresh cannot tell "gone" from "permanent".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyTtl {
    /// The key does not exist (`PTTL` → `-2`).
    Missing,
    /// The key exists with no expiry set (`PTTL` → `-1`).
    NoExpiry,
    /// The key exists and expires after this much longer (`PTTL` → `>= 0`).
    Expires(Duration),
}

impl KeyTtl {
    /// Interpret a raw `PTTL` reply.
    #[must_use]
    pub fn from_pttl(ms: i64) -> Self {
        match ms {
            -2 => Self::Missing,
            -1 => Self::NoExpiry,
            // Redis never returns another negative; treat one as no-expiry
            // rather than panicking on `Duration::from_millis` underflow.
            ms if ms < 0 => Self::NoExpiry,
            ms => Self::Expires(Duration::from_millis(ms.unsigned_abs())),
        }
    }

    /// The remaining lifetime, or `None` when the key is missing or permanent.
    ///
    /// Use when "how long is left" is the only question and both other answers
    /// mean "do not reuse this TTL".
    #[must_use]
    pub fn remaining(self) -> Option<Duration> {
        match self {
            Self::Expires(d) => Some(d),
            Self::Missing | Self::NoExpiry => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Lock timing config (Go: GetTrxRedisLockTimeout / GetTrxRedisBackoff).
// ---------------------------------------------------------------------------

/// Default distributed-lock TTL: 2000ms.
const DEFAULT_LOCK_TIMEOUT_MS: i64 = 2000;
/// Minimum accepted lock TTL: 700ms (`minTimeout`).
const MIN_LOCK_TIMEOUT_MS: i64 = 700;
/// Default and minimum lock retry backoff: 10ms.
const DEFAULT_BACKOFF_MS: i64 = 10;

/// Clamp math for the lock TTL (pure, injectable — env-race-free).
///
/// mirrors: `GetTrxRedisLockTimeout` — `TRANSACTION_REDIS_LOCK_TIMEOUT` is used
/// only when it parses and is `>= 700`; otherwise the 2000ms default.
fn trx_lock_timeout_ms_from(parsed: Option<i64>) -> i64 {
    match parsed {
        Some(v) if v >= MIN_LOCK_TIMEOUT_MS => v,
        _ => DEFAULT_LOCK_TIMEOUT_MS,
    }
}

/// Clamp math for the lock retry backoff (pure, injectable — env-race-free).
///
/// mirrors: `GetTrxRedisBackoff` — `TRANSACTION_REDIS_BACKOFF` is used only when
/// it parses and is `>= 10`; otherwise the 10ms default.
fn trx_backoff_ms_from(parsed: Option<i64>) -> i64 {
    match parsed {
        Some(v) if v >= DEFAULT_BACKOFF_MS => v,
        _ => DEFAULT_BACKOFF_MS,
    }
}

/// Distributed-lock TTL, read from `TRANSACTION_REDIS_LOCK_TIMEOUT`.
///
/// mirrors: `GetTrxRedisLockTimeout`.
#[must_use]
pub fn trx_lock_timeout() -> Duration {
    let ms = trx_lock_timeout_ms_from(env_i64("TRANSACTION_REDIS_LOCK_TIMEOUT"));
    Duration::from_millis(u64::try_from(ms).unwrap_or(2000))
}

/// Distributed-lock retry backoff, read from `TRANSACTION_REDIS_BACKOFF`.
///
/// mirrors: `GetTrxRedisBackoff`.
#[must_use]
pub fn trx_backoff() -> Duration {
    let ms = trx_backoff_ms_from(env_i64("TRANSACTION_REDIS_BACKOFF"));
    Duration::from_millis(u64::try_from(ms).unwrap_or(10))
}

/// Read an env var as `i64`, returning `None` when unset/empty/unparseable
/// (matching Go's `strconv.Atoi(os.Getenv(...))` err-to-default flow).
fn env_i64(name: &str) -> Option<i64> {
    std::env::var(name).ok()?.parse().ok()
}

/// Read an env var, treating unset and empty-string identically as absent.
fn env_nonempty(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

// ---------------------------------------------------------------------------
// The pool handle.
// ---------------------------------------------------------------------------

/// Owned Redis handle: a connection pool for key/value ops plus an `rslock`
/// [`LockManager`](rslock::LockManager) for distributed locks.
///
/// mirrors: Go's process-global `redisPoolClient` + `redisSync` pair, returned
/// as an explicit value instead of package state.
#[derive(Clone)]
pub struct RedisPool {
    pool: deadpool_redis::Pool,
    locks: rslock::LockManager,
}

impl std::fmt::Debug for RedisPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisPool").finish_non_exhaustive()
    }
}

/// Initialize Redis from standard environment variables.
///
/// Returns `Ok(None)` (a no-op) when `REDIS_HOST` is unset — Redis is optional.
/// When `REDIS_HOST` is set but the connection cannot be built, an `Err` is
/// returned so the caller can decide whether to abort startup.
///
/// Supported vars: `REDIS_HOST` (required to enable), `REDIS_PORT` (default
/// 6379), `REDIS_PASSWORD` (default empty), `REDIS_DB` (default 0).
///
/// mirrors: `InitRedisFromEnv`.
#[allow(clippy::unused_async)] // async API stays stable; future eager PING belongs here.
pub async fn init_from_env() -> anyhow::Result<Option<RedisPool>> {
    let Some(host) = env_nonempty("REDIS_HOST") else {
        tracing::info!("[InitRedisFromEnv] REDIS_HOST not set, skipping Redis init");
        return Ok(None);
    };

    let port = env_nonempty("REDIS_PORT").unwrap_or_else(|| "6379".to_string());
    let port_num: u16 = port
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid REDIS_PORT: {port}"))?;

    // Go: invalid REDIS_DB warns and falls back to 0 rather than failing.
    let db = match std::env::var("REDIS_DB") {
        Ok(s) if !s.is_empty() => s.parse::<i64>().unwrap_or_else(|_| {
            tracing::warn!(redis_db = %s, "[InitRedisFromEnv] invalid REDIS_DB, using db=0");
            0
        }),
        _ => 0,
    };

    let conn_info = redis::ConnectionInfo {
        addr: redis::ConnectionAddr::Tcp(host, port_num),
        redis: redis::RedisConnectionInfo {
            db,
            // Go's InitRedisOptions defaults Username to "default".
            username: Some("default".to_string()),
            password: env_nonempty("REDIS_PASSWORD"),
            ..Default::default()
        },
    };

    RedisPool::connect(conn_info).map(Some)
}

impl RedisPool {
    /// Build a pool + lock manager from a fully-formed connection info.
    ///
    /// mirrors: `initRedisClient` + `InitRedSyncOnce` (client build; the
    /// eager `PING` health check is deferred to first pool checkout).
    fn connect(conn_info: redis::ConnectionInfo) -> anyhow::Result<Self> {
        let client = redis::Client::open(conn_info.clone())?;
        let pool = deadpool_redis::Config::from_connection_info(conn_info)
            .create_pool(Some(deadpool_redis::Runtime::Tokio1))?;
        let locks = rslock::LockManager::from_clients(vec![client]);
        Ok(Self { pool, locks })
    }

    /// Ping Redis through a pooled connection.
    ///
    /// mirrors: `checkRedisHealth`'s two-second `PING`.
    pub async fn ping(&self) -> anyhow::Result<()> {
        let mut conn = self.pool.get().await?;
        redis::cmd("PING").query_async::<String>(&mut conn).await?;
        Ok(())
    }

    /// Snapshot connection-pool capacity and utilization.
    #[must_use]
    pub fn pool_status(&self) -> RedisPoolStatus {
        let status = self.pool.status();
        RedisPoolStatus {
            max_size: status.max_size,
            total: status.size,
            idle: status.available,
            waiting: status.waiting,
        }
    }

    /// Store JSON-marshalled `value` under `key` with a soft-clamped TTL.
    ///
    /// A TTL of zero (or one clamped to zero) writes with no expiry. mirrors:
    /// `StoreRedisWithContext` — `phjson.Marshal` then `SET` with
    /// `clampStoreTTL(id, duration)`.
    pub async fn store<T: Serialize>(
        &self,
        key: &str,
        value: &T,
        ttl: Duration,
    ) -> anyhow::Result<()> {
        let json = serde_json::to_string(value)?;
        let ms = clamp_ttl(ttl).as_millis();
        let mut conn = self.pool.get().await?;
        if ms == 0 {
            conn.set::<_, _, ()>(key, json).await?;
        } else {
            let ms = u64::try_from(ms).unwrap_or(u64::MAX);
            conn.pset_ex::<_, _, ()>(key, json, ms).await?;
        }
        Ok(())
    }

    /// Fetch the raw string stored under `key`.
    ///
    /// A key miss (redis-nil) is quiet: returns `Ok(None)`, never an error.
    /// mirrors: `GetRedisWithContext` / `GetRedis` returning `("", nil)`.
    pub async fn get(&self, key: &str) -> anyhow::Result<Option<String>> {
        let mut conn = self.pool.get().await?;
        // `Option<String>` decodes redis-nil to `None` instead of erroring.
        let val: Option<String> = conn.get(key).await?;
        Ok(val)
    }

    /// Read the remaining time-to-live of `key` (Redis `PTTL`).
    ///
    /// Millisecond precision, so a TTL written by [`RedisPool::store`] round
    /// trips without the one-second truncation `TTL` would impose.
    ///
    /// This has **no Go counterpart** — `paycloudhelper` never reads a TTL back.
    /// It exists because a caller layering an in-process cache over Redis cannot
    /// otherwise learn how long a shared entry has left: recomputing it from the
    /// value's own expiry field (an `expiresIn`, say) restarts the clock and
    /// silently extends the effective lifetime beyond what the writer intended.
    pub async fn ttl(&self, key: &str) -> anyhow::Result<KeyTtl> {
        let mut conn = self.pool.get().await?;
        let ms: i64 = conn.pttl(key).await?;
        Ok(KeyTtl::from_pttl(ms))
    }

    /// Delete `key`. Deleting a missing key is not an error.
    ///
    /// mirrors: `DeleteRedisWithContext` / `DeleteRedis`.
    pub async fn delete(&self, key: &str) -> anyhow::Result<()> {
        let mut conn = self.pool.get().await?;
        conn.del::<_, ()>(key).await?;
        Ok(())
    }

    /// Acquire the distributed lock `key` for `ttl`, or `None` on contention /
    /// failure (a user should retry after a short wait).
    ///
    /// Uses the manager's default retry (Redlock, drift factor `0.01`).
    /// mirrors: `AcquireLock` — `(false, nil)` contention becomes `None`.
    pub async fn acquire_lock(&self, key: &str, ttl: Duration) -> Option<Lock> {
        self.locks.lock(key.as_bytes(), ttl).await.ok()
    }

    /// Acquire the lock with an explicit retry count and delay.
    ///
    /// mirrors: `AcquireLockWithRetry` — `redsync.WithTries` / `WithRetryDelay`.
    pub async fn acquire_lock_with_retry(
        &self,
        key: &str,
        ttl: Duration,
        tries: u32,
        delay: Duration,
    ) -> Option<Lock> {
        let mut mgr = self.locks.clone();
        mgr.set_retry(tries, delay);
        mgr.lock(key.as_bytes(), ttl).await.ok()
    }

    /// Release a previously acquired lock (best-effort, as in Redlock).
    ///
    /// mirrors: `ReleaseLock` — `mutex.UnlockContext`.
    pub async fn release(&self, lock: &Lock) {
        self.locks.unlock(lock).await;
    }
}

/// Transport-neutral Redis pool status used by `pc-health`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RedisPoolStatus {
    pub max_size: usize,
    pub total: usize,
    pub idle: usize,
    pub waiting: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csrf_key_format() {
        assert_eq!(csrf_key("abc123"), "csrf-abc123");
    }

    #[test]
    fn revoke_token_key_format() {
        assert_eq!(revoke_token_key(42), "revoke_token_42");
        assert_eq!(revoke_token_key("m-7"), "revoke_token_m-7");
    }

    #[test]
    fn lock_key_uses_app_identity() {
        // identity is process-global; set-then-assert in one test to avoid
        // cross-test ordering races.
        pc_core::identity::set_app_name("test");
        assert_eq!(lock_key_prefix(), "redis_lock:test:");
        assert_eq!(lock_key("resource"), "redis_lock:test:resource");
    }

    #[test]
    fn clamp_within_max_unchanged() {
        // 1 minute, well under the 30-day ceiling.
        assert_eq!(clamp_ttl_with_max(60_000, DEFAULT_MAX_TTL_MINUTES), 60_000);
    }

    #[test]
    fn clamp_over_max_capped() {
        let max_ms = DEFAULT_MAX_TTL_MINUTES * 60_000;
        assert_eq!(
            clamp_ttl_with_max(max_ms + 1, DEFAULT_MAX_TTL_MINUTES),
            max_ms
        );
        // exactly at the ceiling is unchanged.
        assert_eq!(clamp_ttl_with_max(max_ms, DEFAULT_MAX_TTL_MINUTES), max_ms);
    }

    #[test]
    fn clamp_negative_collapses_to_zero() {
        assert_eq!(clamp_ttl_with_max(-1, DEFAULT_MAX_TTL_MINUTES), 0);
        assert_eq!(clamp_ttl_with_max(-999_999, 10), 0);
    }

    #[test]
    fn clamp_ttl_duration_wrapper() {
        // Zero stays zero (no-expiry).
        assert_eq!(clamp_ttl(Duration::ZERO), Duration::ZERO);
        // Small TTL passes through unchanged.
        assert_eq!(clamp_ttl(Duration::from_secs(60)), Duration::from_secs(60));
        // A 60-day TTL is capped at the 30-day default ceiling.
        let ceiling = Duration::from_secs(u64::try_from(DEFAULT_MAX_TTL_MINUTES).unwrap() * 60);
        assert_eq!(clamp_ttl(Duration::from_secs(60 * 24 * 60 * 60)), ceiling);
    }

    #[test]
    fn lock_timeout_default_and_min_clamp() {
        assert_eq!(trx_lock_timeout_ms_from(None), 2000);
        assert_eq!(trx_lock_timeout_ms_from(Some(500)), 2000); // below 700 min
        assert_eq!(trx_lock_timeout_ms_from(Some(699)), 2000);
        assert_eq!(trx_lock_timeout_ms_from(Some(700)), 700); // at min
        assert_eq!(trx_lock_timeout_ms_from(Some(5000)), 5000);
    }

    #[test]
    fn backoff_default_and_min_clamp() {
        assert_eq!(trx_backoff_ms_from(None), 10);
        assert_eq!(trx_backoff_ms_from(Some(5)), 10); // below 10 min
        assert_eq!(trx_backoff_ms_from(Some(9)), 10);
        assert_eq!(trx_backoff_ms_from(Some(10)), 10); // at min
        assert_eq!(trx_backoff_ms_from(Some(50)), 50);
    }

    #[test]
    fn key_ttl_distinguishes_missing_from_permanent() {
        assert_eq!(KeyTtl::from_pttl(-2), KeyTtl::Missing);
        assert_eq!(KeyTtl::from_pttl(-1), KeyTtl::NoExpiry);
        assert_eq!(
            KeyTtl::from_pttl(1_500),
            KeyTtl::Expires(Duration::from_millis(1_500))
        );
        // A key that has just expired but not yet been reaped reads as 0ms.
        assert_eq!(KeyTtl::from_pttl(0), KeyTtl::Expires(Duration::ZERO));

        // Only a real remaining lifetime is reusable as a TTL.
        assert_eq!(
            KeyTtl::from_pttl(1_500).remaining(),
            Some(Duration::from_millis(1_500))
        );
        assert_eq!(KeyTtl::from_pttl(-1).remaining(), None);
        assert_eq!(KeyTtl::from_pttl(-2).remaining(), None);

        // Out-of-contract negatives must not underflow into a huge Duration.
        assert_eq!(KeyTtl::from_pttl(i64::MIN), KeyTtl::NoExpiry);
    }

    // ---- live-Redis round-trips (require a broker; ignored by default) ----

    #[tokio::test]
    #[ignore = "requires a live Redis broker (set REDIS_HOST)"]
    async fn ttl_reads_back_the_stored_expiry() {
        let pool = init_from_env().await.unwrap().expect("REDIS_HOST set");
        let key = "pc-redis:test:ttl";

        assert_eq!(pool.ttl(key).await.unwrap(), KeyTtl::Missing);

        pool.store(key, &"hello", Duration::from_secs(30))
            .await
            .unwrap();
        let remaining = pool.ttl(key).await.unwrap().remaining().expect("expiring");
        assert!(
            remaining <= Duration::from_secs(30) && remaining > Duration::from_secs(25),
            "PTTL should report just under the written 30s, got {remaining:?}"
        );

        // A zero TTL writes with no expiry (see `store`), not a 0ms expiry.
        pool.store(key, &"hello", Duration::ZERO).await.unwrap();
        assert_eq!(pool.ttl(key).await.unwrap(), KeyTtl::NoExpiry);

        pool.delete(key).await.unwrap();
        assert_eq!(pool.ttl(key).await.unwrap(), KeyTtl::Missing);
    }

    #[tokio::test]
    #[ignore = "requires a live Redis broker (set REDIS_HOST)"]
    async fn store_get_delete_roundtrip() {
        let pool = init_from_env().await.unwrap().expect("REDIS_HOST set");
        let key = "pc-redis:test:roundtrip";
        pool.store(key, &"hello", Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(pool.get(key).await.unwrap().as_deref(), Some("\"hello\""));
        pool.delete(key).await.unwrap();
        assert_eq!(pool.get(key).await.unwrap(), None); // quiet miss
    }

    #[tokio::test]
    #[ignore = "requires a live Redis broker (set REDIS_HOST)"]
    async fn lock_acquire_and_release() {
        pc_core::identity::set_app_name("test");
        let pool = init_from_env().await.unwrap().expect("REDIS_HOST set");
        let key = lock_key("resource");
        let lock = pool
            .acquire_lock(&key, trx_lock_timeout())
            .await
            .expect("acquired");
        // A second acquire on the held key should fail (contention → None).
        assert!(pool.acquire_lock(&key, trx_lock_timeout()).await.is_none());
        pool.release(&lock).await;
    }
}
