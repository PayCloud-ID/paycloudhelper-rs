//! Consecutive-failure circuit breaker.
//!
//! mirrors: the breaker embedded in `AuditPublisher` (`audittrail_publisher.go`).
//! The Go version keeps an atomic `consecutiveFailures` counter and an atomic
//! `circuitOpen` flag; it trips (`OPEN`) once failures reach `maxConsecFailures`
//! (default 10) and a background goroutine flips it back to `CLOSED` after
//! `cooldownDuration` (default 30s). This port adds an explicit `HalfOpen` probe
//! state and checks the cooldown lazily on the next call rather than via a timer,
//! which is behaviourally equivalent and easier to test deterministically.

use std::future::Future;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Default consecutive-failure threshold before the breaker opens.
///
/// mirrors: `NewAuditPublisher` `maxConsecFailures: 10`.
pub const DEFAULT_THRESHOLD: u32 = 10;

/// Default cooldown the breaker stays open before probing recovery.
///
/// mirrors: `NewAuditPublisher` `cooldownDuration: 30 * time.Second`.
pub const DEFAULT_COOLDOWN: Duration = Duration::from_secs(30);

/// Breaker state.
///
/// * `Closed` — calls flow through; failures are counted.
/// * `Open` — calls are short-circuited until the cooldown elapses.
/// * `HalfOpen` — a single probe is allowed; success closes the breaker, any
///   failure re-opens it immediately (mirrors the audit publisher resuming only
///   after a clean push).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum State {
    Closed,
    Open,
    HalfOpen,
}

/// Classifies whether a guarded operation's error is a *cancellation* rather
/// than a real failure.
///
/// **RC-2 (critical):** a cancellation is not a failure. A cancelled call (e.g.
/// the caller dropped, a shutdown signal fired, a [`tokio`](https://tokio.rs)
/// task was aborted) must never advance the failure counter or trip the
/// breaker — otherwise a graceful shutdown would look like an outage. Implement
/// this trait on your operation's error type and [`CircuitBreaker::call`] will
/// route cancellations through [`CircuitBreaker::record_cancel`] (a no-op on the
/// counters) instead of [`CircuitBreaker::record_failure`].
pub trait Cancellable {
    /// Returns `true` when this outcome is a cancellation that should not count
    /// against the breaker.
    fn is_cancellation(&self) -> bool;
}

/// The error returned by [`CircuitBreaker::call`].
#[derive(Debug, thiserror::Error)]
pub enum BreakerError<E> {
    /// The breaker was open and the call was short-circuited without running
    /// the guarded operation. mirrors: `Submit` dropping the message when
    /// `circuitOpen == 1`.
    #[error("circuit breaker is open")]
    Open,
    /// The guarded operation ran and returned an error.
    #[error("guarded operation failed: {0}")]
    Inner(E),
}

impl<E> BreakerError<E> {
    /// True when the call was short-circuited by an open breaker.
    #[must_use]
    pub fn is_open(&self) -> bool {
        matches!(self, Self::Open)
    }
}

#[derive(Debug)]
struct Inner {
    state: State,
    consecutive_failures: u32,
    opened_at: Option<Instant>,
}

/// A consecutive-failure circuit breaker.
///
/// Wrap a fallible async operation with [`call`](CircuitBreaker::call); the
/// breaker counts consecutive failures, trips after `threshold`, short-circuits
/// for `cooldown`, then permits a single probe. For callers that classify
/// outcomes themselves, the low-level [`try_acquire`](CircuitBreaker::try_acquire)
/// / [`record_success`](CircuitBreaker::record_success) /
/// [`record_failure`](CircuitBreaker::record_failure) /
/// [`record_cancel`](CircuitBreaker::record_cancel) methods are also public.
#[derive(Debug)]
pub struct CircuitBreaker {
    threshold: u32,
    cooldown: Duration,
    inner: Mutex<Inner>,
}

impl Default for CircuitBreaker {
    /// The audit-publisher defaults: 10 consecutive failures / 30s cooldown.
    fn default() -> Self {
        Self::new(DEFAULT_THRESHOLD, DEFAULT_COOLDOWN)
    }
}

impl CircuitBreaker {
    /// Create a breaker that opens after `threshold` consecutive failures and
    /// stays open for `cooldown`.
    ///
    /// A `threshold` of 0 is clamped to 1 (mirrors the Go option guards, which
    /// only apply positive values).
    #[must_use]
    pub fn new(threshold: u32, cooldown: Duration) -> Self {
        Self {
            threshold: threshold.max(1),
            cooldown,
            inner: Mutex::new(Inner {
                state: State::Closed,
                consecutive_failures: 0,
                opened_at: None,
            }),
        }
    }

    /// Current breaker state (snapshot).
    #[must_use]
    pub fn state(&self) -> State {
        self.lock().state
    }

    /// Number of consecutive failures recorded since the last success.
    #[must_use]
    pub fn consecutive_failures(&self) -> u32 {
        self.lock().consecutive_failures
    }

    /// Run `f` under the breaker.
    ///
    /// * If the breaker is `Open` and the cooldown has not elapsed, returns
    ///   [`BreakerError::Open`] without running `f`.
    /// * Otherwise runs `f`. `Ok` closes the breaker and resets the counter;
    ///   an error whose [`Cancellable::is_cancellation`] is `true` is recorded
    ///   as a cancellation (**RC-2**: does not count against the breaker); any
    ///   other error advances the failure counter and may trip the breaker.
    pub async fn call<F, Fut, T, E>(&self, f: F) -> Result<T, BreakerError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        E: Cancellable,
    {
        if !self.try_acquire() {
            return Err(BreakerError::Open);
        }
        match f().await {
            Ok(v) => {
                self.record_success();
                Ok(v)
            }
            Err(e) if e.is_cancellation() => {
                self.record_cancel();
                Err(BreakerError::Inner(e))
            }
            Err(e) => {
                self.record_failure();
                Err(BreakerError::Inner(e))
            }
        }
    }

    /// Low-level admission check. Returns `true` if a call is permitted,
    /// transitioning `Open -> HalfOpen` when the cooldown has elapsed.
    /// mirrors: the `circuitOpen.Load() == 1` gate in `Submit`.
    pub fn try_acquire(&self) -> bool {
        let mut g = self.lock();
        match g.state {
            State::Closed | State::HalfOpen => true,
            State::Open => {
                if g.opened_at.is_none_or(|t| t.elapsed() >= self.cooldown) {
                    g.state = State::HalfOpen;
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Record a successful outcome: reset the failure counter and close the
    /// breaker. mirrors: `p.consecutiveFailures.Store(0)` after a clean push.
    pub fn record_success(&self) {
        let mut g = self.lock();
        g.consecutive_failures = 0;
        g.state = State::Closed;
        g.opened_at = None;
    }

    /// Record a failure: increment the counter and open the breaker when the
    /// threshold is reached (or on any failure while probing in `HalfOpen`).
    /// mirrors: `recordFailure` / `CompareAndSwap(0, 1)`.
    pub fn record_failure(&self) {
        let mut g = self.lock();
        g.consecutive_failures += 1;
        if g.state == State::HalfOpen || g.consecutive_failures >= self.threshold {
            g.state = State::Open;
            g.opened_at = Some(Instant::now());
        }
    }

    /// Record a cancellation (**RC-2**): a no-op on the failure counter and
    /// state. The breaker is neither advanced toward tripping nor reset.
    #[allow(clippy::unused_self)]
    pub fn record_cancel(&self) {
        // Intentionally does nothing: a cancellation is not a failure and must
        // not influence the breaker. Present as an explicit, testable path.
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, thiserror::Error)]
    enum TestErr {
        #[error("boom")]
        Boom,
        #[error("cancelled")]
        Cancelled,
    }

    impl Cancellable for TestErr {
        fn is_cancellation(&self) -> bool {
            matches!(self, TestErr::Cancelled)
        }
    }

    async fn fail() -> Result<(), TestErr> {
        Err(TestErr::Boom)
    }
    async fn cancel() -> Result<(), TestErr> {
        Err(TestErr::Cancelled)
    }
    async fn ok() -> Result<u32, TestErr> {
        Ok(1)
    }

    #[test]
    fn defaults_match_go() {
        let cb = CircuitBreaker::default();
        assert_eq!(cb.threshold, 10);
        assert_eq!(cb.cooldown, Duration::from_secs(30));
    }

    #[tokio::test]
    async fn trips_after_n_consecutive_failures() {
        let cb = CircuitBreaker::new(3, Duration::from_secs(30));
        // First (threshold - 1) failures keep it closed.
        for _ in 0..2 {
            let r = cb.call(fail).await;
            assert!(matches!(r, Err(BreakerError::Inner(TestErr::Boom))));
            assert_eq!(cb.state(), State::Closed);
        }
        // The third failure trips it.
        let r = cb.call(fail).await;
        assert!(matches!(r, Err(BreakerError::Inner(TestErr::Boom))));
        assert_eq!(cb.state(), State::Open);

        // Now it short-circuits without running the op.
        let r = cb.call(fail).await;
        assert!(matches!(r, Err(BreakerError::Open)));
        assert!(r.unwrap_err().is_open());
    }

    #[tokio::test]
    async fn rejects_while_open_then_recovers_after_cooldown() {
        let cb = CircuitBreaker::new(1, Duration::from_millis(60));
        // Trip immediately (threshold 1).
        let _ = cb.call(fail).await;
        assert_eq!(cb.state(), State::Open);
        // Rejected while still within cooldown.
        assert!(matches!(cb.call(ok).await, Err(BreakerError::Open)));

        tokio::time::sleep(Duration::from_millis(90)).await;

        // Cooldown elapsed: the probe runs and, on success, closes the breaker.
        let r = cb.call(ok).await;
        assert_eq!(r.ok(), Some(1));
        assert_eq!(cb.state(), State::Closed);
        assert_eq!(cb.consecutive_failures(), 0);
    }

    #[tokio::test]
    async fn cancellation_does_not_trip_the_breaker() {
        let cb = CircuitBreaker::new(3, Duration::from_secs(30));
        // Far more cancellations than the threshold — must never trip.
        for _ in 0..10 {
            let r = cb.call(cancel).await;
            assert!(matches!(r, Err(BreakerError::Inner(TestErr::Cancelled))));
            assert_eq!(cb.state(), State::Closed);
            assert_eq!(cb.consecutive_failures(), 0);
        }
        // A real failure still counts normally afterwards.
        let _ = cb.call(fail).await;
        assert_eq!(cb.consecutive_failures(), 1);
        assert_eq!(cb.state(), State::Closed);
    }

    #[tokio::test]
    async fn half_open_failure_reopens_immediately() {
        let cb = CircuitBreaker::new(2, Duration::from_millis(40));
        let _ = cb.call(fail).await;
        let _ = cb.call(fail).await;
        assert_eq!(cb.state(), State::Open);
        tokio::time::sleep(Duration::from_millis(60)).await;
        // Probe fails -> straight back to Open (no need to re-reach threshold).
        let r = cb.call(fail).await;
        assert!(matches!(r, Err(BreakerError::Inner(TestErr::Boom))));
        assert_eq!(cb.state(), State::Open);
    }
}
