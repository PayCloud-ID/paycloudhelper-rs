#![forbid(unsafe_code)]
//! `pc-log` — structured logging, per-key sampling and rate limiting.
//!
//! Bit-for-bit port of the Go `paycloudhelper` logging surface: the top-level
//! `logger.go` wrappers and the `phlogger` package (`sampler.go`,
//! `ratelimit.go`, `keyed_limiter.go`, `context.go`, `phlogger.go`,
//! `metrics.go`).
//!
//! The parity-critical pure logic — sampler math, the [`LogContext`] prefix
//! shape, the [`KeyedLimiter`] token bucket and the `" [+N suppressed]"` suffix
//! — lives in this crate and is unit-tested against the documented shapes.
//!
//! ## Where sampling happens (deviation from a pure Layer)
//!
//! Go's `LogD/I/W/E/F` sample *inside the log function*, then mutate the
//! message (appending the suppressed-count suffix) *before* it is formatted.
//! A `tracing` subscriber `Layer` cannot reproduce that suffix: by the time a
//! layer sees an event its message is already fixed and a layer cannot rewrite
//! it. So sampling and the suffix are applied at the emit site (the leveled
//! macros), exactly as Go does, and [`init`] installs the env-tuned sampler as
//! the process-global sampler those macros consult (plus the JSON subscriber).

mod context;
mod limiter;
mod ratelimit;
mod sampler;
mod timer;

pub use context::LogContext;
pub use limiter::KeyedLimiter;
pub use sampler::{initialize_sampler, sampler_config_for_env, SamplerConfig};

/// Re-export of [`pc_core::build_log_prefix`] — builds the `[pchelper.Fn]`
/// prefix (blank fn → `[pchelper.Log]`). Use it to build the `prefix` argument
/// for the leveled macros.
///
/// mirrors: `phhelper.BuildLogPrefix`.
pub use pc_core::build_log_prefix;

use tracing_subscriber::prelude::*;

/// Installs the process-wide logging stack.
///
/// mirrors: `phlogger.InitializeLogger` — sets the Go time format
/// `2006-01-02 15:04:05.000` (Rust strftime `%Y-%m-%d %H:%M:%S%.3f`) and
/// initializes the sampler from `APP_ENV`.
///
/// Installs a `tracing` JSON subscriber with a frozen field schema — `level`,
/// `timestamp`, `prefix`, `message` — and initializes the env-tuned global
/// sampler (see [`sampler_config_for_env`]). Idempotent: uses `try_init`, so a
/// second call (or a subscriber already set) is a silent no-op.
pub fn init() {
    initialize_sampler(sampler_config_for_env(pc_core::identity::app_env()));

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .json()
        .flatten_event(true)
        .with_timer(timer::GoTimer)
        .with_target(false)
        .with_level(true);

    // try_init returns Err if a global subscriber is already set — idempotent.
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .try_init();
}

/// Implementation details used by the emit macros. Not part of the public API.
#[doc(hidden)]
pub mod __private {
    use std::time::Duration;

    pub use tracing;

    /// Consults the global sampler. See [`crate::sampler`].
    #[must_use]
    pub fn sample(key: &str) -> Option<i64> {
        crate::sampler::sample(key)
    }

    /// Consults the global window limiter. See [`crate::ratelimit`].
    #[must_use]
    pub fn sample_window(key: &str, window: Duration) -> Option<i64> {
        crate::ratelimit::sample_window(key, window)
    }

    /// Appends the Go `" [+%d suppressed]"` suffix when `suppressed > 0`.
    ///
    /// mirrors: the `format += " [+%d suppressed]"` branch shared by every
    /// sampled/rate-limited log function in `phlogger`.
    #[must_use]
    pub fn with_suffix(mut message: String, suppressed: i64) -> String {
        use std::fmt::Write as _;
        if suppressed > 0 {
            let _ = write!(message, " [+{suppressed} suppressed]");
        }
        message
    }
}

/// Internal: emit one already-sampled, already-suffixed line at `$level`.
#[doc(hidden)]
#[macro_export]
macro_rules! __pc_emit {
    ($level:ident, $prefix:expr, $msg:expr) => {
        $crate::__private::tracing::$level!(prefix = %$prefix, message = %$msg)
    };
}

/// Internal: sample by the format-literal key, then emit.
#[doc(hidden)]
#[macro_export]
macro_rules! __pc_log {
    ($level:ident, $prefix:expr, $fmt:literal $(, $arg:expr)* $(,)?) => {{
        if let Some(__suppressed) = $crate::__private::sample($fmt) {
            let __msg = $crate::__private::with_suffix(
                ::std::format!($fmt $(, $arg)*),
                __suppressed,
            );
            $crate::__pc_emit!($level, $prefix, __msg);
        }
    }};
}

/// Internal: sample by an explicit key (custom-key rate-limited variants).
#[doc(hidden)]
#[macro_export]
macro_rules! __pc_log_rated {
    ($level:ident, $key:expr, $prefix:expr, $fmt:literal $(, $arg:expr)* $(,)?) => {{
        if let Some(__suppressed) = $crate::__private::sample($key) {
            let __msg = $crate::__private::with_suffix(
                ::std::format!($fmt $(, $arg)*),
                __suppressed,
            );
            $crate::__pc_emit!($level, $prefix, __msg);
        }
    }};
}

/// Internal: time-window rate limiting by an explicit key + window.
#[doc(hidden)]
#[macro_export]
macro_rules! __pc_log_rated_w {
    ($level:ident, $key:expr, $window:expr, $prefix:expr, $fmt:literal $(, $arg:expr)* $(,)?) => {{
        if let Some(__suppressed) = $crate::__private::sample_window($key, $window) {
            let __msg = $crate::__private::with_suffix(
                ::std::format!($fmt $(, $arg)*),
                __suppressed,
            );
            $crate::__pc_emit!($level, $prefix, __msg);
        }
    }};
}

/// Logs at Debug level, sampled by the format-literal key.
///
/// mirrors: `LogD`. Usage: `log_d!(prefix, "message {}", arg)`.
#[macro_export]
macro_rules! log_d {
    ($prefix:expr, $fmt:literal $(, $arg:expr)* $(,)?) => {
        $crate::__pc_log!(debug, $prefix, $fmt $(, $arg)*)
    };
}

/// Logs at Info level, sampled by the format-literal key.
///
/// mirrors: `LogI`. Usage: `log_i!(prefix, "message {}", arg)`.
#[macro_export]
macro_rules! log_i {
    ($prefix:expr, $fmt:literal $(, $arg:expr)* $(,)?) => {
        $crate::__pc_log!(info, $prefix, $fmt $(, $arg)*)
    };
}

/// Logs at Warning level, sampled by the format-literal key.
///
/// mirrors: `LogW`. Usage: `log_w!(prefix, "message {}", arg)`.
#[macro_export]
macro_rules! log_w {
    ($prefix:expr, $fmt:literal $(, $arg:expr)* $(,)?) => {
        $crate::__pc_log!(warn, $prefix, $fmt $(, $arg)*)
    };
}

/// Logs at Error level, sampled by the format-literal key.
///
/// mirrors: `LogE`. Usage: `log_e!(prefix, "message {}", arg)`.
#[macro_export]
macro_rules! log_e {
    ($prefix:expr, $fmt:literal $(, $arg:expr)* $(,)?) => {
        $crate::__pc_log!(error, $prefix, $fmt $(, $arg)*)
    };
}

/// Logs at Fatal level, sampled by the format-literal key.
///
/// mirrors: `LogF`. NOTE: unlike Go's `LogF` this does NOT call `os.Exit`;
/// process termination is a policy decision left to the caller. The line is
/// emitted at `tracing`'s `error` level (its highest severity).
#[macro_export]
macro_rules! log_f {
    ($prefix:expr, $fmt:literal $(, $arg:expr)* $(,)?) => {
        $crate::__pc_log!(error, $prefix, $fmt $(, $arg)*)
    };
}

/// Logs at Debug level, sampled by an explicit key. mirrors: `LogDRated`.
///
/// Usage: `log_d_rated!(key, prefix, "message {}", arg)`.
#[macro_export]
macro_rules! log_d_rated {
    ($key:expr, $prefix:expr, $fmt:literal $(, $arg:expr)* $(,)?) => {
        $crate::__pc_log_rated!(debug, $key, $prefix, $fmt $(, $arg)*)
    };
}

/// Logs at Info level, sampled by an explicit key. mirrors: `LogIRated`.
///
/// Usage: `log_i_rated!(key, prefix, "message {}", arg)`.
#[macro_export]
macro_rules! log_i_rated {
    ($key:expr, $prefix:expr, $fmt:literal $(, $arg:expr)* $(,)?) => {
        $crate::__pc_log_rated!(info, $key, $prefix, $fmt $(, $arg)*)
    };
}

/// Logs at Warning level, sampled by an explicit key. mirrors: `LogWRated`.
#[macro_export]
macro_rules! log_w_rated {
    ($key:expr, $prefix:expr, $fmt:literal $(, $arg:expr)* $(,)?) => {
        $crate::__pc_log_rated!(warn, $key, $prefix, $fmt $(, $arg)*)
    };
}

/// Logs at Error level, sampled by an explicit key. mirrors: `LogERated`.
#[macro_export]
macro_rules! log_e_rated {
    ($key:expr, $prefix:expr, $fmt:literal $(, $arg:expr)* $(,)?) => {
        $crate::__pc_log_rated!(error, $key, $prefix, $fmt $(, $arg)*)
    };
}

/// Logs at Info level with a time-window limiter. mirrors: `LogIRatedW`.
///
/// Usage: `log_i_rated_w!(key, window, prefix, "message {}", arg)`.
#[macro_export]
macro_rules! log_i_rated_w {
    ($key:expr, $window:expr, $prefix:expr, $fmt:literal $(, $arg:expr)* $(,)?) => {
        $crate::__pc_log_rated_w!(info, $key, $window, $prefix, $fmt $(, $arg)*)
    };
}

/// Logs at Error level with a time-window limiter. mirrors: `LogERatedW`.
#[macro_export]
macro_rules! log_e_rated_w {
    ($key:expr, $window:expr, $prefix:expr, $fmt:literal $(, $arg:expr)* $(,)?) => {
        $crate::__pc_log_rated_w!(error, $key, $window, $prefix, $fmt $(, $arg)*)
    };
}

/// Logs at Warning level with a time-window limiter. mirrors: `LogWRatedW`.
#[macro_export]
macro_rules! log_w_rated_w {
    ($key:expr, $window:expr, $prefix:expr, $fmt:literal $(, $arg:expr)* $(,)?) => {
        $crate::__pc_log_rated_w!(warn, $key, $window, $prefix, $fmt $(, $arg)*)
    };
}

/// Logs at Debug level with a time-window limiter. mirrors: `LogDRatedW`.
#[macro_export]
macro_rules! log_d_rated_w {
    ($key:expr, $window:expr, $prefix:expr, $fmt:literal $(, $arg:expr)* $(,)?) => {
        $crate::__pc_log_rated_w!(debug, $key, $window, $prefix, $fmt $(, $arg)*)
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sampler::Sampler;
    use std::time::Duration;

    // ── Parity row: env sampler defaults (incl. None → pass-through) ──────────

    #[test]
    fn sampler_defaults_production() {
        let cfg = sampler_config_for_env(Some(pc_core::AppEnv::Production)).unwrap();
        assert_eq!(cfg.initial, 5);
        assert_eq!(cfg.thereafter, 50);
        assert_eq!(cfg.period, Duration::from_secs(1));
    }

    #[test]
    fn sampler_defaults_staging() {
        let cfg = sampler_config_for_env(Some(pc_core::AppEnv::Staging)).unwrap();
        assert_eq!(cfg.initial, 10);
        assert_eq!(cfg.thereafter, 10);
        assert_eq!(cfg.period, Duration::from_secs(1));
    }

    #[test]
    fn sampler_defaults_develop() {
        let cfg = sampler_config_for_env(Some(pc_core::AppEnv::Develop)).unwrap();
        assert_eq!(cfg.initial, 20);
        assert_eq!(cfg.thereafter, 20);
        assert_eq!(cfg.period, Duration::from_secs(1));
    }

    #[test]
    fn sampler_defaults_none_is_passthrough() {
        assert!(sampler_config_for_env(None).is_none());
        // A disabled sampler allows everything.
        let s = Sampler::new(None);
        for _ in 0..100 {
            assert_eq!(s.check("k"), (true, 0));
        }
    }

    // ── Parity row: sampler math (mirrors sampler_test.go) ────────────────────

    #[test]
    fn sampler_initial_burst_then_drop_when_thereafter_zero() {
        let s = Sampler::new(Some(SamplerConfig {
            initial: 3,
            thereafter: 0,
            period: Duration::from_secs(1),
        }));
        for _ in 0..3 {
            assert_eq!(s.check("k"), (true, 0));
        }
        assert_eq!(s.check("k"), (false, 0));
    }

    #[test]
    fn sampler_thereafter_sampling_reports_suppressed() {
        let s = Sampler::new(Some(SamplerConfig {
            initial: 2,
            thereafter: 5,
            period: Duration::from_secs(10),
        }));
        // First 2 allowed (initial).
        assert_eq!(s.check("k"), (true, 0));
        assert_eq!(s.check("k"), (true, 0));
        // Next 4 suppressed.
        for _ in 0..4 {
            assert_eq!(s.check("k"), (false, 0));
        }
        // 5th over initial is allowed and reports 4 suppressed.
        assert_eq!(s.check("k"), (true, 4));
    }

    #[test]
    fn sampler_keys_are_independent() {
        let s = Sampler::new(Some(SamplerConfig {
            initial: 1,
            thereafter: 0,
            period: Duration::from_secs(1),
        }));
        assert_eq!(s.check("a"), (true, 0));
        assert_eq!(s.check("b"), (true, 0));
        assert_eq!(s.check("a"), (false, 0));
        assert_eq!(s.check("b"), (false, 0));
    }

    #[test]
    fn sampler_period_reset() {
        let s = Sampler::new(Some(SamplerConfig {
            initial: 1,
            thereafter: 0,
            period: Duration::from_millis(50),
        }));
        assert_eq!(s.check("k"), (true, 0));
        assert_eq!(s.check("k"), (false, 0));
        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(s.check("k"), (true, 0));
    }

    // ── Parity row: LogContext prefix shape ───────────────────────────────────

    #[test]
    fn log_context_prefix_shapes() {
        assert_eq!(LogContext::new(&[]).prefix(), "");
        assert_eq!(
            LogContext::new(&[("req_id", "abc-123")]).prefix(),
            "[req_id=abc-123] "
        );
        assert_eq!(
            LogContext::new(&[("req_id", "abc"), ("merchant", "M001"), ("user", "U42")]).prefix(),
            "[req_id=abc merchant=M001 user=U42] "
        );
    }

    #[test]
    fn log_context_with_merges_and_is_immutable() {
        let parent = LogContext::new(&[("req_id", "abc")]);
        let child = parent.with(&[("step", "validate")]);
        assert_eq!(child.prefix(), "[req_id=abc step=validate] ");
        // Parent unchanged.
        assert_eq!(parent.prefix(), "[req_id=abc] ");
        // Empty fields → same context.
        assert_eq!(parent.with(&[]), parent);
        // Empty parent → just the extra.
        assert_eq!(
            LogContext::new(&[]).with(&[("step", "init")]).prefix(),
            "[step=init] "
        );
        // Chained.
        assert_eq!(
            LogContext::new(&[("req_id", "abc")])
                .with(&[("step", "1")])
                .with(&[("detail", "x")])
                .prefix(),
            "[req_id=abc step=1 detail=x] "
        );
    }

    // ── Parity row: KeyedLimiter allow/deny under burst ───────────────────────

    #[test]
    fn keyed_limiter_allows_first_and_independent_keys() {
        let kl = KeyedLimiter::new(10, 1);
        assert!(kl.allow("a"));
        assert!(kl.allow("b"));
    }

    #[test]
    fn keyed_limiter_rate_limits_per_key() {
        let kl = KeyedLimiter::new(1, 1);
        assert!(kl.allow("k"));
        assert!(!kl.allow("k"));
    }

    #[test]
    fn keyed_limiter_burst_allows_multiple() {
        let kl = KeyedLimiter::new(1, 3);
        for _ in 0..3 {
            assert!(kl.allow("k"));
        }
        assert!(!kl.allow("k"));
    }

    // ── Parity row: suppressed-suffix formatting ──────────────────────────────

    #[test]
    fn suppressed_suffix_formatting() {
        assert_eq!(__private::with_suffix("msg".to_string(), 0), "msg");
        assert_eq!(
            __private::with_suffix("msg".to_string(), 4),
            "msg [+4 suppressed]"
        );
    }

    // ── Window rate limiter (mirrors ratelimit.go) ────────────────────────────

    #[test]
    fn window_limiter_zero_window_passes_through() {
        assert_eq!(
            crate::ratelimit::sample_window("k", Duration::ZERO),
            Some(0)
        );
    }

    #[test]
    fn window_limiter_suppresses_within_window_then_reports() {
        let key = "window-test-unique-key";
        let window = Duration::from_millis(50);
        assert_eq!(crate::ratelimit::sample_window(key, window), Some(0)); // first
        assert_eq!(crate::ratelimit::sample_window(key, window), None); // suppressed
        assert_eq!(crate::ratelimit::sample_window(key, window), None); // suppressed
        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(crate::ratelimit::sample_window(key, window), Some(2)); // drains 2
    }

    // ── Smoke: macros + init compile and run without panicking ────────────────

    #[test]
    fn macros_smoke() {
        init(); // idempotent; safe under parallel tests via try_init
        let prefix = build_log_prefix("Smoke");
        log_d!(prefix, "debug {}", 1);
        log_i!(prefix, "info {}", 2);
        log_w!(prefix, "warn {}", 3);
        log_e!(prefix, "error {}", 4);
        log_f!(prefix, "fatal {}", 5);
        log_i_rated!("k.info", prefix, "rated {}", 6);
        log_e_rated!("k.err", prefix, "rated {}", 7);
        log_i_rated_w!("k.win", Duration::from_millis(10), prefix, "windowed {}", 8);
        let ctx = LogContext::new(&[("req", "1")]);
        log_i!(ctx.prefix(), "with context");
    }
}
