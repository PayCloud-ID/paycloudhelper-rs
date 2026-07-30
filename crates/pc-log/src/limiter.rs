//! Per-key token-bucket rate limiter.
//!
//! mirrors: Go `phlogger/keyed_limiter.go` (`KeyedLimiter`), which wraps
//! `golang.org/x/time/rate`. Here the equivalent token bucket is provided by
//! the `governor` crate.

use std::num::NonZeroU32;

use governor::{DefaultKeyedRateLimiter, Quota};

/// Per-key token bucket rate limiter.
///
/// mirrors: `phlogger.KeyedLimiter`. Each unique key gets its own independent
/// bucket with identical rate and burst. Thread-safe.
pub struct KeyedLimiter {
    inner: DefaultKeyedRateLimiter<String>,
}

impl KeyedLimiter {
    /// Creates a limiter allowing `rate_per_sec` events per second with `burst`
    /// capacity per key. A burst of 1 gives strict per-second limiting.
    ///
    /// mirrors: `phlogger.NewKeyedLimiter(r float64, burst int)`. Go accepts a
    /// fractional rate; this port takes an integer `rate_per_sec` (the sampler
    /// callers only ever use whole-number rates). A `0` rate or burst is
    /// clamped up to 1 because `governor` requires non-zero quotas.
    #[must_use]
    pub fn new(rate_per_sec: u32, burst: u32) -> Self {
        let rate = NonZeroU32::new(rate_per_sec).unwrap_or(NonZeroU32::MIN);
        let burst = NonZeroU32::new(burst).unwrap_or(NonZeroU32::MIN);
        let quota = Quota::per_second(rate).allow_burst(burst);
        Self {
            inner: DefaultKeyedRateLimiter::keyed(quota),
        }
    }

    /// Reports whether an event for `key` should be permitted. Creates a bucket
    /// for unseen keys automatically.
    ///
    /// mirrors: `(*KeyedLimiter).Allow` (`rate.Limiter.Allow`).
    #[must_use]
    pub fn allow(&self, key: &str) -> bool {
        self.inner.check_key(&key.to_string()).is_ok()
    }
}
