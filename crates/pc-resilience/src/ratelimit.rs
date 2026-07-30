//! Keyed rate limiting.
//!
//! mirrors: the rate-limited "audit not ready" logging (`logAuditNotReadyRateLimited`
//! in `audittrail_publisher.go`) and similar per-key throttles. A thin wrapper
//! over [`governor`] giving one token bucket per key.

use std::hash::Hash;
use std::num::NonZeroU32;

use governor::{DefaultKeyedRateLimiter, Quota};

/// Per-key token-bucket rate limiter.
///
/// Each distinct `key` gets its own bucket sized by the configured [`Quota`]
/// (burst capacity + replenishment rate). [`allow`](KeyedRateLimiter::allow)
/// consumes one token and reports whether the action is permitted, the
/// general-purpose form of the "log at most once per interval per key" pattern.
pub struct KeyedRateLimiter<K>
where
    K: Eq + Hash + Clone,
{
    limiter: DefaultKeyedRateLimiter<K>,
}

impl<K> KeyedRateLimiter<K>
where
    K: Eq + Hash + Clone,
{
    /// Build a limiter from an explicit [`governor::Quota`].
    #[must_use]
    pub fn new(quota: Quota) -> Self {
        Self {
            limiter: DefaultKeyedRateLimiter::keyed(quota),
        }
    }

    /// Convenience: allow up to `rate` actions per second per key, with a burst
    /// capacity equal to `rate`.
    #[must_use]
    pub fn per_second(rate: NonZeroU32) -> Self {
        Self::new(Quota::per_second(rate))
    }

    /// Attempt to consume one token for `key`. Returns `true` if the action is
    /// permitted, `false` if the key's bucket is currently exhausted.
    pub fn allow(&self, key: &K) -> bool {
        self.limiter.check_key(key).is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_burst_then_denies() {
        // Burst capacity of 5 per key.
        let rl: KeyedRateLimiter<&str> = KeyedRateLimiter::per_second(NonZeroU32::new(5).unwrap());

        // First 5 immediate calls consume the full burst.
        for i in 0..5 {
            assert!(rl.allow(&"a"), "call {i} should be allowed");
        }
        // 6th is denied (bucket exhausted within the same instant).
        assert!(!rl.allow(&"a"));

        // A different key has its own independent bucket.
        assert!(rl.allow(&"b"));
    }
}
