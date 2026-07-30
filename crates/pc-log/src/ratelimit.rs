//! Per-key time-window rate limiter used by the windowed `*_rated_w!` macros.
//!
//! mirrors: Go `phlogger/ratelimit.go` (`rateLimiter`, `rateLimitEntry`,
//! `globalRateLimiter`).

use std::sync::OnceLock;
use std::time::{Duration, Instant};

use dashmap::DashMap;

/// Per-key state for the window limiter.
///
/// mirrors: `phlogger.rateLimitEntry`.
struct RateLimitEntry {
    last_emit: Option<Instant>,
    suppressed: i64,
}

/// Thread-safe per-key time-window rate limiter.
///
/// mirrors: `phlogger.rateLimiter`.
struct RateLimiter {
    entries: DashMap<String, RateLimitEntry>,
}

impl RateLimiter {
    fn new() -> Self {
        Self {
            entries: DashMap::new(),
        }
    }

    /// Returns `(allowed, suppressed)`.
    ///
    /// mirrors: `(*rateLimiter).check`.
    ///
    /// - `window == 0`: rate limiting disabled; always `(true, 0)`.
    /// - within the active window: `(false, 0)` and the suppressed counter is
    ///   incremented.
    /// - first call or window expired: `(true, suppressed)` where `suppressed`
    ///   is the drained count of previously suppressed calls.
    fn check(&self, key: &str, window: Duration) -> (bool, i64) {
        if window.is_zero() {
            return (true, 0);
        }

        let now = Instant::now();
        let mut entry = self
            .entries
            .entry(key.to_string())
            .or_insert_with(|| RateLimitEntry {
                last_emit: None,
                suppressed: 0,
            });

        match entry.last_emit {
            Some(last) if now.duration_since(last) < window => {
                entry.suppressed += 1;
                (false, 0)
            }
            _ => {
                let suppressed = entry.suppressed;
                entry.suppressed = 0;
                entry.last_emit = Some(now);
                (true, suppressed)
            }
        }
    }
}

/// The singleton used by all windowed log variants.
///
/// mirrors: `phlogger.globalRateLimiter`.
static GLOBAL_RATE_LIMITER: OnceLock<RateLimiter> = OnceLock::new();

fn global() -> &'static RateLimiter {
    GLOBAL_RATE_LIMITER.get_or_init(RateLimiter::new)
}

/// Consults the global window limiter. Returns `Some(suppressed)` when the
/// caller should emit, or `None` when the line is within the active window.
pub(crate) fn sample_window(key: &str, window: Duration) -> Option<i64> {
    match global().check(key, window) {
        (true, suppressed) => Some(suppressed),
        (false, _) => None,
    }
}
