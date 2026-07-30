//! Per-key log sampling.
//!
//! mirrors: Go `phlogger/sampler.go` (`SamplerConfig`, `sampler`,
//! `SamplerConfigForEnv`, `InitializeSampler`).

use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, Instant};

use dashmap::DashMap;

/// Controls log sampling behavior per key per period.
///
/// mirrors: `phlogger.SamplerConfig`.
///
/// `initial` is the number of log lines allowed per key in each `period`.
/// After `initial` is exhausted, only every `thereafter`-th message is emitted.
/// If `initial == 0`, sampling is disabled and all logs pass through.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SamplerConfig {
    /// Log first N per period per key (`0` = disabled).
    pub initial: u32,
    /// After `initial`, log every Nth (`0` = drop all after the initial burst).
    pub thereafter: u32,
    /// Sampling window (Go default: 1s).
    pub period: Duration,
}

/// Returns production-tuned sampler defaults for the given environment.
///
/// mirrors: `phlogger.SamplerConfigForEnv` (mapped onto the typed
/// [`pc_core::AppEnv`]). `None` (unset / unrecognized env) yields `None`,
/// i.e. sampling disabled — all logs pass through.
///
/// - `Production` → 5 / 50 / 1s
/// - `Staging`    → 10 / 10 / 1s
/// - `Develop`    → 20 / 20 / 1s
/// - `None`       → `None` (pass-through)
#[must_use]
pub fn sampler_config_for_env(env: Option<pc_core::AppEnv>) -> Option<SamplerConfig> {
    match env {
        Some(pc_core::AppEnv::Production) => Some(SamplerConfig {
            initial: 5,
            thereafter: 50,
            period: Duration::from_secs(1),
        }),
        Some(pc_core::AppEnv::Staging) => Some(SamplerConfig {
            initial: 10,
            thereafter: 10,
            period: Duration::from_secs(1),
        }),
        Some(pc_core::AppEnv::Develop) => Some(SamplerConfig {
            initial: 20,
            thereafter: 20,
            period: Duration::from_secs(1),
        }),
        None => None,
    }
}

/// Per-key counter state within a period.
///
/// mirrors: `phlogger.samplerEntry`. Go uses atomics behind a `sync.Map`;
/// here [`DashMap`] gives per-entry exclusive access so plain fields suffice.
struct SamplerEntry {
    count: i64,
    reset: Instant,
}

/// Per-key Initial/Thereafter log sampler.
///
/// mirrors: `phlogger.sampler`.
pub(crate) struct Sampler {
    config: Option<SamplerConfig>,
    entries: DashMap<String, SamplerEntry>,
}

impl Sampler {
    /// Creates a sampler. `None` (or a config with `initial == 0`) disables
    /// sampling so every call passes through.
    ///
    /// mirrors: the `&sampler{config: ...}` construction in Go.
    pub(crate) fn new(config: Option<SamplerConfig>) -> Self {
        Self {
            config,
            entries: DashMap::new(),
        }
    }

    /// Returns `(allowed, suppressed)` for a log line keyed by `key`.
    ///
    /// mirrors: `(*sampler).check`.
    ///
    /// - Sampling disabled (`initial == 0`): `(true, 0)`.
    /// - Within the initial burst: `(true, 0)`.
    /// - After initial, on every `thereafter`-th call: `(true, thereafter-1)`.
    /// - Otherwise: `(false, 0)`.
    pub(crate) fn check(&self, key: &str) -> (bool, i64) {
        let cfg = match self.config {
            Some(c) if c.initial > 0 => c,
            _ => return (true, 0),
        };

        let now = Instant::now();
        let mut entry = self
            .entries
            .entry(key.to_string())
            .or_insert_with(|| SamplerEntry {
                count: 0,
                reset: now,
            });

        // Reset counter if the period has elapsed.
        if now.duration_since(entry.reset) >= cfg.period {
            entry.reset = now;
            entry.count = 1;
            return (true, 0);
        }

        entry.count += 1;
        let n = entry.count;

        // Within the initial burst — allow.
        if n <= i64::from(cfg.initial) {
            return (true, 0);
        }

        // After initial: allow every `thereafter`-th, suppress the rest.
        if cfg.thereafter == 0 {
            return (false, 0);
        }
        let over = n - i64::from(cfg.initial);
        if over % i64::from(cfg.thereafter) == 0 {
            return (true, i64::from(cfg.thereafter) - 1);
        }
        (false, 0)
    }
}

/// Process-global sampler consulted by the emit macros.
///
/// mirrors: `phlogger.globalSampler`. Starts disabled so pre-`init` log calls
/// always pass through.
static GLOBAL_SAMPLER: OnceLock<RwLock<Arc<Sampler>>> = OnceLock::new();

fn global() -> &'static RwLock<Arc<Sampler>> {
    GLOBAL_SAMPLER.get_or_init(|| RwLock::new(Arc::new(Sampler::new(None))))
}

/// Installs the global sampler config.
///
/// mirrors: `phlogger.InitializeSampler` — including the "default the period to
/// 1s when omitted but `initial > 0`" rule. Passing `None` disables sampling.
/// Safe to call repeatedly; the last call wins.
pub fn initialize_sampler(config: Option<SamplerConfig>) {
    let config = config.map(|mut c| {
        if c.period.is_zero() && c.initial > 0 {
            c.period = Duration::from_secs(1);
        }
        c
    });
    *global().write().expect("sampler lock poisoned") = Arc::new(Sampler::new(config));
}

/// Consults the global sampler. Returns `Some(suppressed)` when the caller
/// should emit (0 = nothing suppressed), or `None` when the line is dropped.
pub(crate) fn sample(key: &str) -> Option<i64> {
    let sampler = global().read().expect("sampler lock poisoned").clone();
    match sampler.check(key) {
        (true, suppressed) => Some(suppressed),
        (false, _) => None,
    }
}
