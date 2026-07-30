#![forbid(unsafe_code)]
//! `pc-db` — centralized, env-driven `sqlx` connection-pool configuration.
//!
//! Bit-for-bit port of the Go `paycloudhelper/phdb` package (`phdb/pool.go`).
//! The design goal is identical to the Go original: pool sizing is consistent,
//! env-driven and **never unbounded**. Go's `database/sql` treats
//! `MaxOpenConns <= 0` as *unlimited*, a fast path to exhausting a small
//! PostgreSQL instance; [`PoolConfig::validate`] forbids it. The four pool
//! knobs map onto `sqlx`'s [`PoolOptions`](sqlx::pool::PoolOptions).
//!
//! ## Deviations from Go (documented parity gaps)
//!
//! * Go's `phdb.Apply` mutates an already-open `*sql.DB`. `sqlx` configures the
//!   pool *before* it connects, so the analogue of `Apply` is a **builder**
//!   ([`PoolConfig::pg_pool_options`] / [`PoolConfig::mysql_pool_options`]) that
//!   returns a configured [`PoolOptions`](sqlx::pool::PoolOptions). Connecting
//!   is the separate live path ([`PoolConfig::connect_pg`]).
//! * `sqlx` has **no max-idle-connections knob**. Go's `SetMaxIdleConns`
//!   caps the idle set; `sqlx` instead reaps idle connections via
//!   `idle_timeout`. [`PoolConfig::max_idle_conns`] is still parsed, validated
//!   and clamped ([`PoolConfig::effective_max_idle_conns`]) for parity, but it
//!   is not applied to `PoolOptions` (there is no equivalent setter).
//! * `dsn` is a Rust-port addition: `sqlx` builds pools from a connection
//!   string, whereas Go's `phdb` left DSN building to each service. It is read
//!   from `<prefix>_DSN`.

use std::time::Duration;

use sqlx::mysql::{MySqlPool, MySqlPoolOptions};
use sqlx::pool::PoolOptions;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::{Database, MySql, Postgres};

/// Errors produced while validating or building a pool.
///
/// mirrors: the `fmt.Errorf("phdb: ...")` values returned by
/// `phdb.PoolConfig.Validate` and `phdb.Apply`. Error message text is kept
/// byte-identical to the Go originals where an equivalent exists.
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    /// mirrors: `phdb: MaxOpenConns must be > 0 (0 means unlimited); got N`.
    /// The load-bearing safety rule — a value of `0` would mean *unbounded*.
    #[error("phdb: MaxOpenConns must be > 0 (0 means unlimited); got {0}")]
    UnboundedMaxOpen(i64),

    /// mirrors: `phdb: MaxIdleConns must be >= 0; got N`.
    #[error("phdb: MaxIdleConns must be >= 0; got {0}")]
    NegativeMaxIdle(i64),

    /// No Go equivalent: `MaxOpenConns` is valid (`> 0`) but exceeds `u32`,
    /// which is the width `sqlx`'s `max_connections` accepts.
    #[error("phdb: MaxOpenConns {0} exceeds the u32 range accepted by sqlx")]
    MaxOpenOverflow(i64),

    /// No Go equivalent: a connect was requested but [`PoolConfig::dsn`] is
    /// unset (Go built the DSN per-service, outside `phdb`).
    #[error("phdb: connection string (DSN) is not set")]
    MissingDsn,

    /// A `sqlx` connect/pool error surfaced from the live path.
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

/// The pool knobs, mirroring Go `phdb.PoolConfig`.
///
/// mirrors: `phdb.PoolConfig` (minus the `Logger` func field — logging goes
/// through [`pc_log`]) plus the Rust-only [`dsn`](Self::dsn) field. Counts are
/// `i64` so the Go `int` semantics — where `<= 0` / `< 0` are *rejected* rather
/// than being unrepresentable — port exactly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PoolConfig {
    /// Maximum number of open connections. mirrors: `PoolConfig.MaxOpenConns`.
    /// Must be `> 0`; `0` (Go's "unlimited") is rejected by [`Self::validate`].
    pub max_open_conns: i64,
    /// Maximum number of idle connections. mirrors: `PoolConfig.MaxIdleConns`.
    /// Must be `>= 0`. Clamped to `max_open_conns` by
    /// [`Self::effective_max_idle_conns`]; see the crate-level note on why
    /// `sqlx` cannot apply this directly.
    pub max_idle_conns: i64,
    /// Maximum lifetime of a connection before it is retired. mirrors:
    /// `PoolConfig.ConnMaxLifetime`. Stretched by [`apply_lifetime_jitter`]
    /// before being applied.
    pub conn_max_lifetime: Duration,
    /// Maximum time a connection may sit idle before being closed. mirrors:
    /// `PoolConfig.ConnMaxIdleTime`. Applied as `sqlx`'s `idle_timeout`.
    pub conn_max_idle_time: Duration,
    /// Connection string used by the live [`Self::connect_pg`] /
    /// [`Self::connect_mysql`] path. Rust-port addition; read from
    /// `<prefix>_DSN`.
    pub dsn: Option<String>,
}

impl Default for PoolConfig {
    /// mirrors: `phdb.DefaultPoolConfig` — conservative PgBouncer-sized defaults
    /// (`MaxOpen=15`, `MaxIdle=15`, `Lifetime=30m`, `IdleTime=5m`).
    fn default() -> Self {
        Self {
            max_open_conns: 15,
            max_idle_conns: 15,
            conn_max_lifetime: Duration::from_secs(30 * 60),
            conn_max_idle_time: Duration::from_secs(5 * 60),
            dsn: None,
        }
    }
}

impl PoolConfig {
    /// mirrors: `phdb.DefaultPoolConfig`. Convenience alias for
    /// [`PoolConfig::default`].
    pub fn default_pool_config() -> Self {
        Self::default()
    }

    /// Read a config for the given env prefix.
    ///
    /// mirrors: `phdb.LoadPoolConfig(prefix)`. Reads
    /// `<prefix>_MAX_OPEN_CONN`, `<prefix>_MAX_IDLE_CONN`,
    /// `<prefix>_CONN_MAX_LIFETIME` (minutes) and
    /// `<prefix>_CONN_MAX_IDLE_TIME` (minutes); additionally reads
    /// `<prefix>_DSN` (Rust-port addition). Missing or non-positive integer
    /// values fall back to [`PoolConfig::default`] exactly as Go's
    /// `envIntPositive` does — a bad env value can never produce an unbounded
    /// pool. The env prefixes used in production are `DB` (primary), `DB_ACC`
    /// (accounting) and `DB_RPL` (replica).
    pub fn from_env(prefix: &str) -> Self {
        let d = Self::default();
        let lifetime_min = env_int_positive(
            &format!("{prefix}_CONN_MAX_LIFETIME"),
            duration_minutes(d.conn_max_lifetime),
        );
        let idle_min = env_int_positive(
            &format!("{prefix}_CONN_MAX_IDLE_TIME"),
            duration_minutes(d.conn_max_idle_time),
        );
        Self {
            max_open_conns: env_int_positive(&format!("{prefix}_MAX_OPEN_CONN"), d.max_open_conns),
            max_idle_conns: env_int_positive(&format!("{prefix}_MAX_IDLE_CONN"), d.max_idle_conns),
            conn_max_lifetime: minutes(lifetime_min),
            conn_max_idle_time: minutes(idle_min),
            dsn: std::env::var(format!("{prefix}_DSN"))
                .ok()
                .filter(|s| !s.is_empty()),
        }
    }

    /// Reject configurations unsafe for PostgreSQL.
    ///
    /// mirrors: `phdb.PoolConfig.Validate`. Forbids `max_open_conns <= 0`
    /// (Go's *unlimited*) and `max_idle_conns < 0`.
    pub fn validate(&self) -> Result<(), DbError> {
        if self.max_open_conns <= 0 {
            return Err(DbError::UnboundedMaxOpen(self.max_open_conns));
        }
        if self.max_idle_conns < 0 {
            return Err(DbError::NegativeMaxIdle(self.max_idle_conns));
        }
        Ok(())
    }

    /// The idle-connection count clamped to `max_open_conns`.
    ///
    /// mirrors: the `if maxIdle > cfg.MaxOpenConns { maxIdle = cfg.MaxOpenConns }`
    /// clamp inside `phdb.Apply`.
    pub fn effective_max_idle_conns(&self) -> i64 {
        self.max_idle_conns.min(self.max_open_conns)
    }

    /// Build configured PostgreSQL pool options (real lifetime jitter applied).
    ///
    /// mirrors: `phdb.Apply` for the PostgreSQL engine — validates, then sets
    /// `max_connections` and a `+≤10%`-jittered `max_lifetime` plus
    /// `idle_timeout`. The actual `connect()` is [`Self::connect_pg`].
    pub fn pg_pool_options(&self) -> Result<PgPoolOptions, DbError> {
        pool_options::<Postgres>(self, lifetime_with_jitter(self.conn_max_lifetime))
    }

    /// Build configured MySQL pool options (real lifetime jitter applied).
    ///
    /// mirrors: `phdb.Apply` for the MySQL engine (dual-engine parity).
    pub fn mysql_pool_options(&self) -> Result<MySqlPoolOptions, DbError> {
        pool_options::<MySql>(self, lifetime_with_jitter(self.conn_max_lifetime))
    }

    /// Live path: build options and connect a PostgreSQL pool.
    ///
    /// mirrors: `phdb.OpenAndApply` for PostgreSQL, folded together with the
    /// per-service `sql.Open`. Requires [`Self::dsn`] to be set.
    pub async fn connect_pg(&self) -> Result<PgPool, DbError> {
        let dsn = self.dsn.as_deref().ok_or(DbError::MissingDsn)?;
        Ok(self.pg_pool_options()?.connect(dsn).await?)
    }

    /// Live path: build options and connect a MySQL pool.
    ///
    /// mirrors: `phdb.OpenAndApply` for MySQL. Requires [`Self::dsn`] to be set.
    pub async fn connect_mysql(&self) -> Result<MySqlPool, DbError> {
        let dsn = self.dsn.as_deref().ok_or(DbError::MissingDsn)?;
        Ok(self.mysql_pool_options()?.connect(dsn).await?)
    }
}

/// Build a `sqlx` [`PoolOptions`](sqlx::pool::PoolOptions) for any engine, using
/// a caller-supplied (already-jittered) lifetime.
///
/// mirrors: the knob-setting core of `phdb.Apply`. Kept generic and
/// lifetime-injectable so it is unit-testable deterministically (the
/// convenience [`PoolConfig::pg_pool_options`] / [`PoolConfig::mysql_pool_options`]
/// feed it real jitter). Validates first — an invalid config never yields
/// options. A zero lifetime / idle-time is left unset (`sqlx` reads
/// `None` as "no limit", matching Go leaving a zero `Duration` as the
/// database/sql default of "no expiry").
pub fn pool_options<DB: Database>(
    cfg: &PoolConfig,
    effective_lifetime: Duration,
) -> Result<PoolOptions<DB>, DbError> {
    cfg.validate()?;
    let max_open = u32::try_from(cfg.max_open_conns)
        .map_err(|_| DbError::MaxOpenOverflow(cfg.max_open_conns))?;

    // SQLx itself defaults max_lifetime to 30 minutes, so explicitly write
    // `None` for zero to preserve Go/database/sql's no-expiry semantics.
    let opts = PoolOptions::<DB>::new()
        .max_connections(max_open)
        .max_lifetime((!effective_lifetime.is_zero()).then_some(effective_lifetime))
        .idle_timeout((!cfg.conn_max_idle_time.is_zero()).then_some(cfg.conn_max_idle_time));

    // mirrors: phdb.logPool -> phlogger.LogI observability line.
    pc_log::log_i!(
        "[phdb]",
        "pool applied MaxOpen={} MaxIdle={} Lifetime={:?} IdleTime={:?}",
        max_open,
        cfg.effective_max_idle_conns(),
        effective_lifetime,
        cfg.conn_max_idle_time,
    );

    Ok(opts)
}

/// Apply a deterministic `+≤10%` lifetime jitter (pure, unit-testable).
///
/// mirrors: `phdb.lifetimeWithJitter`, but with the random draw factored out so
/// it is deterministic. `rand_fraction` is the position within the jitter
/// budget: `0.0` returns `base` unchanged, `1.0` returns `base * 1.10`. The
/// result is always in `[base, base * 1.10]` and is monotonic in
/// `rand_fraction`. A zero `base` returns zero (matching Go's
/// `if lifetime <= 0 { return lifetime }`). Out-of-range fractions are clamped
/// to `[0.0, 1.0]`.
pub fn apply_lifetime_jitter(base: Duration, rand_fraction: f64) -> Duration {
    if base.is_zero() {
        return base;
    }
    let frac = rand_fraction.clamp(0.0, 1.0);
    base + base.mul_f64(0.1 * frac)
}

/// Apply a real random `+≤10%` lifetime jitter.
///
/// mirrors: `phdb.lifetimeWithJitter` exactly — draws a per-process random
/// offset. The draw is `[0.0, 1.0)`, so the result is `[base, base * 1.10)`
/// (exclusive upper bound, like Go's `rand.N(budget)`). Thin convenience over
/// [`apply_lifetime_jitter`].
pub fn lifetime_with_jitter(base: Duration) -> Duration {
    apply_lifetime_jitter(base, rand::random::<f64>())
}

/// mirrors: `phdb.envIntPositive` — parse `key` as an int; use it only when it
/// parses and is `> 0`, otherwise return `default`. No trimming (matches Go's
/// `strconv.Atoi`).
fn env_int_positive(key: &str, default: i64) -> i64 {
    match std::env::var(key) {
        Ok(v) => match v.parse::<i64>() {
            Ok(n) if n > 0 => n,
            _ => default,
        },
        Err(_) => default,
    }
}

/// Whole minutes in `d` (mirrors Go's `int(d / time.Minute)` for defaults).
fn duration_minutes(d: Duration) -> i64 {
    i64::try_from(d.as_secs() / 60).unwrap_or(i64::MAX)
}

/// A `Duration` of `n` minutes (mirrors Go's `time.Duration(n) * time.Minute`).
fn minutes(n: i64) -> Duration {
    Duration::from_secs(u64::try_from(n.max(0)).unwrap_or(0).saturating_mul(60))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIN: u64 = 60;

    #[test]
    fn default_matches_go_defaults() {
        let c = PoolConfig::default();
        assert_eq!(c.max_open_conns, 15);
        assert_eq!(c.max_idle_conns, 15);
        assert_eq!(c.conn_max_lifetime, Duration::from_secs(30 * MIN));
        assert_eq!(c.conn_max_idle_time, Duration::from_secs(5 * MIN));
        assert_eq!(PoolConfig::default_pool_config(), c);
    }

    #[test]
    fn validate_rejects_zero_max_open() {
        let cfg = PoolConfig {
            max_open_conns: 0,
            ..PoolConfig::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, DbError::UnboundedMaxOpen(0)));
        // Message text parity with Go.
        assert_eq!(
            err.to_string(),
            "phdb: MaxOpenConns must be > 0 (0 means unlimited); got 0"
        );
    }

    #[test]
    fn validate_rejects_negative_max_open() {
        let cfg = PoolConfig {
            max_open_conns: -1,
            ..PoolConfig::default()
        };
        assert!(matches!(cfg.validate(), Err(DbError::UnboundedMaxOpen(-1))));
    }

    #[test]
    fn validate_rejects_negative_max_idle() {
        let cfg = PoolConfig {
            max_idle_conns: -1,
            ..PoolConfig::default()
        };
        assert!(matches!(cfg.validate(), Err(DbError::NegativeMaxIdle(-1))));
    }

    #[test]
    fn validate_accepts_positive() {
        let cfg = PoolConfig {
            max_open_conns: 20,
            max_idle_conns: 20,
            ..PoolConfig::default()
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn idle_gt_open_is_valid_and_clamps() {
        // Go: "idle gt open clamps ok" — Validate passes, Apply clamps.
        let cfg = PoolConfig {
            max_open_conns: 10,
            max_idle_conns: 50,
            ..PoolConfig::default()
        };
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.effective_max_idle_conns(), 10);
    }

    // --- per-prefix env parsing ---------------------------------------------
    // Only this test touches the real DB / DB_ACC / DB_RPL prefixes, so it does
    // not race the synthetic-prefix tests below. Vars are removed at the end.

    #[test]
    fn from_env_reads_db_families() {
        std::env::set_var("DB_MAX_OPEN_CONN", "20");
        std::env::set_var("DB_MAX_IDLE_CONN", "18");
        std::env::set_var("DB_CONN_MAX_LIFETIME", "30");
        std::env::set_var("DB_CONN_MAX_IDLE_TIME", "5");
        std::env::set_var("DB_DSN", "postgres://localhost/primary");

        std::env::set_var("DB_ACC_MAX_OPEN_CONN", "8");
        std::env::set_var("DB_ACC_MAX_IDLE_CONN", "8");

        std::env::set_var("DB_RPL_MAX_OPEN_CONN", "12");

        let primary = PoolConfig::from_env("DB");
        assert_eq!(primary.max_open_conns, 20);
        assert_eq!(primary.max_idle_conns, 18);
        assert_eq!(primary.conn_max_lifetime, Duration::from_secs(30 * MIN));
        assert_eq!(primary.conn_max_idle_time, Duration::from_secs(5 * MIN));
        assert_eq!(primary.dsn.as_deref(), Some("postgres://localhost/primary"));

        let acc = PoolConfig::from_env("DB_ACC");
        assert_eq!(acc.max_open_conns, 8);
        assert_eq!(acc.max_idle_conns, 8);
        // Durations unset for DB_ACC -> defaults.
        assert_eq!(acc.conn_max_lifetime, Duration::from_secs(30 * MIN));

        let rpl = PoolConfig::from_env("DB_RPL");
        assert_eq!(rpl.max_open_conns, 12);
        // Unset fields fall back to defaults.
        assert_eq!(rpl.max_idle_conns, 15);

        for k in [
            "DB_MAX_OPEN_CONN",
            "DB_MAX_IDLE_CONN",
            "DB_CONN_MAX_LIFETIME",
            "DB_CONN_MAX_IDLE_TIME",
            "DB_DSN",
            "DB_ACC_MAX_OPEN_CONN",
            "DB_ACC_MAX_IDLE_CONN",
            "DB_RPL_MAX_OPEN_CONN",
        ] {
            std::env::remove_var(k);
        }
    }

    #[test]
    fn from_env_defaults_when_unset() {
        // Synthetic prefix nothing else touches.
        let c = PoolConfig::from_env("PCDB_UNSET_XYZ");
        assert_eq!(c, PoolConfig::default());
    }

    #[test]
    fn from_env_invalid_values_fall_back_to_default_not_unbounded() {
        let key = "PCDB_INVALID_MAX_OPEN_CONN";
        let prefix = "PCDB_INVALID";
        for bad in ["0", "-5", "abc", ""] {
            std::env::set_var(key, bad);
            let c = PoolConfig::from_env(prefix);
            assert_eq!(
                c.max_open_conns,
                PoolConfig::default().max_open_conns,
                "{bad:?} must fall back to default (never unbounded)"
            );
        }
        std::env::remove_var(key);
    }

    // --- jitter --------------------------------------------------------------

    #[test]
    fn jitter_endpoints_and_zero() {
        let base = Duration::from_secs(30 * MIN);
        assert_eq!(apply_lifetime_jitter(base, 0.0), base);
        assert_eq!(apply_lifetime_jitter(base, 1.0), base.mul_f64(1.10));
        // Out-of-range fractions clamp.
        assert_eq!(apply_lifetime_jitter(base, -3.0), base);
        assert_eq!(apply_lifetime_jitter(base, 9.0), base.mul_f64(1.10));
        // Zero base stays zero (Go: lifetime <= 0 short-circuit).
        assert_eq!(apply_lifetime_jitter(Duration::ZERO, 0.5), Duration::ZERO);
    }

    #[test]
    fn jitter_in_range_and_monotonic() {
        let base = Duration::from_secs(30 * MIN);
        let upper = base.mul_f64(1.10);
        let mut prev = base;
        for i in 0..=10 {
            let frac = f64::from(i) / 10.0;
            let got = apply_lifetime_jitter(base, frac);
            assert!(
                got >= base && got <= upper,
                "frac {frac}: {got:?} out of [{base:?}, {upper:?}]"
            );
            assert!(got >= prev, "must be monotonic in fraction");
            prev = got;
        }
    }

    #[test]
    fn real_jitter_stays_within_half_open_range() {
        let base = Duration::from_secs(30 * MIN);
        let upper = base + base / 10; // exclusive upper, like Go's rand.N
        for _ in 0..500 {
            let got = lifetime_with_jitter(base);
            assert!(
                got >= base && got < upper,
                "jitter {got:?} out of [{base:?}, {upper:?})"
            );
        }
        assert_eq!(lifetime_with_jitter(Duration::ZERO), Duration::ZERO);
    }

    // --- builder (no DB) -----------------------------------------------------

    #[test]
    fn pool_options_rejects_unbounded() {
        let cfg = PoolConfig {
            max_open_conns: 0,
            ..PoolConfig::default()
        };
        assert!(pool_options::<Postgres>(&cfg, Duration::from_secs(60)).is_err());
        assert!(cfg.pg_pool_options().is_err());
        assert!(cfg.mysql_pool_options().is_err());
    }

    #[test]
    fn pool_options_applies_validated_knobs() {
        let cfg = PoolConfig {
            max_open_conns: 7,
            max_idle_conns: 7,
            conn_max_lifetime: Duration::from_secs(30 * MIN),
            conn_max_idle_time: Duration::from_secs(2 * MIN),
            dsn: None,
        };
        let lifetime = apply_lifetime_jitter(cfg.conn_max_lifetime, 0.5);
        let pg = pool_options::<Postgres>(&cfg, lifetime).unwrap();
        assert_eq!(pg.get_max_connections(), 7);
        assert_eq!(pg.get_max_lifetime(), Some(lifetime));
        assert_eq!(pg.get_idle_timeout(), Some(Duration::from_secs(2 * MIN)));

        let my = pool_options::<MySql>(&cfg, lifetime).unwrap();
        assert_eq!(my.get_max_connections(), 7);
    }

    #[test]
    fn pg_pool_options_lifetime_within_jitter_range() {
        let cfg = PoolConfig::default();
        let opts = cfg.pg_pool_options().unwrap();
        let base = cfg.conn_max_lifetime;
        let upper = base + base / 10;
        let life = opts.get_max_lifetime().expect("lifetime set");
        assert!(
            life >= base && life < upper,
            "{life:?} out of [{base:?}, {upper:?})"
        );
    }

    #[test]
    fn zero_lifetime_leaves_options_unset() {
        let cfg = PoolConfig {
            conn_max_lifetime: Duration::ZERO,
            conn_max_idle_time: Duration::ZERO,
            ..PoolConfig::default()
        };
        let opts = pool_options::<Postgres>(&cfg, Duration::ZERO).unwrap();
        assert_eq!(opts.get_max_lifetime(), None);
        assert_eq!(opts.get_idle_timeout(), None);
    }

    // --- live path (requires a real database) --------------------------------

    #[tokio::test]
    #[ignore = "requires a live PostgreSQL; set PC_DB_TEST_DSN"]
    async fn connect_pg_live() {
        let dsn = std::env::var("PC_DB_TEST_DSN").expect("PC_DB_TEST_DSN");
        let cfg = PoolConfig {
            dsn: Some(dsn),
            ..PoolConfig::default()
        };
        let pool = cfg.connect_pg().await.expect("connect");
        pool.close().await;
    }

    #[tokio::test]
    async fn connect_pg_missing_dsn_errors() {
        // No DB needed: MissingDsn is returned before any connect attempt.
        let cfg = PoolConfig {
            dsn: None,
            ..PoolConfig::default()
        };
        assert!(matches!(cfg.connect_pg().await, Err(DbError::MissingDsn)));
    }
}
