//! QR-MPM phase-latency histogram.
//!
//! mirrors: `phtrace/metrics.go` (`DefaultPhaseBuckets`, `PhaseHistogram`,
//! `NewPhaseHistogram`, `PhaseHistogram.Record`, `PhaseHistogram.Observe`).

use std::time::{Duration, Instant};

use opentelemetry::metrics::Histogram;
use opentelemetry::KeyValue;

/// Histogram bucket boundaries (milliseconds) used by QR-MPM phase timing.
///
/// Units: milliseconds.
///
/// mirrors: `phtrace.DefaultPhaseBuckets`.
pub const DEFAULT_PHASE_BUCKETS: [f64; 11] = [
    5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0,
];

/// Wraps a `Histogram<f64>` pre-configured with the milliseconds unit and the
/// QR-MPM phase bucket boundaries. Cheap to clone; create once at startup per
/// service and reuse across tasks.
///
/// When `pc-trace` is disabled the underlying meter is the global no-op meter,
/// so recording is a cheap no-op.
///
/// mirrors: `phtrace.PhaseHistogram`.
#[derive(Clone, Debug)]
pub struct PhaseHistogram {
    hist: Histogram<f64>,
    #[allow(dead_code)] // retained for parity with Go's `PhaseHistogram.name`.
    name: String,
}

impl PhaseHistogram {
    /// Creates a phase-duration histogram on the named meter (typically the
    /// service name). An empty `buckets` slice falls back to
    /// [`DEFAULT_PHASE_BUCKETS`].
    ///
    /// When `pc-trace` is disabled this returns a histogram backed by the
    /// global no-op meter that records nothing.
    ///
    /// mirrors: `phtrace.NewPhaseHistogram` (the Go version additionally caches
    /// instruments by `(meterName, histName)`; the OTel-Rust SDK performs that
    /// de-duplication internally, so this constructor omits the cache).
    #[must_use]
    pub fn new(meter_name: &'static str, hist_name: &str, buckets: &[f64]) -> Self {
        let meter = opentelemetry::global::meter_provider().meter(meter_name);
        let boundaries: Vec<f64> = if buckets.is_empty() {
            DEFAULT_PHASE_BUCKETS.to_vec()
        } else {
            buckets.to_vec()
        };
        let hist = meter
            .f64_histogram(hist_name.to_string())
            .with_unit("ms")
            .with_description("QR-MPM transaction phase duration in milliseconds")
            .with_boundaries(boundaries)
            .build();
        PhaseHistogram {
            hist,
            name: hist_name.to_string(),
        }
    }

    /// Records `duration` (converted to whole milliseconds) tagged with the
    /// given `phase` plus any extra attributes.
    ///
    /// mirrors: `PhaseHistogram.Record` — like Go's
    /// `float64(duration.Milliseconds())`, sub-millisecond fractions are
    /// truncated toward zero.
    #[allow(clippy::cast_precision_loss)] // OTel histogram is f64; Go records float64 milliseconds.
    pub fn record(&self, phase: &str, duration: Duration, extra: &[KeyValue]) {
        let mut attrs = Vec::with_capacity(1 + extra.len());
        attrs.push(KeyValue::new("phase", phase.to_string()));
        attrs.extend_from_slice(extra);
        // Go records duration.Milliseconds() (an i64 count of whole ms).
        let millis = duration.as_millis() as f64;
        self.hist.record(millis, &attrs);
    }

    /// Returns a closure that, when called, records the elapsed time since this
    /// call under `phase`. Mirrors the deferred-`Observe` pattern from Go.
    ///
    /// ```ignore
    /// let done = phase_hist.observe("rmq_publish", &[]);
    /// // ... work ...
    /// done();
    /// ```
    ///
    /// mirrors: `PhaseHistogram.Observe`.
    pub fn observe(&self, phase: &str, extra: &[KeyValue]) -> impl FnOnce() + '_ {
        let start = Instant::now();
        let phase = phase.to_string();
        let extra = extra.to_vec();
        move || {
            self.record(&phase, start.elapsed(), &extra);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::float_cmp)] // frozen integer-valued f64 constants.
    fn default_phase_buckets_match_go() {
        assert_eq!(
            DEFAULT_PHASE_BUCKETS,
            [5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0]
        );
    }

    #[test]
    fn record_on_disabled_meter_is_noop() {
        // With no provider initialized, the global meter is a no-op; recording
        // must not panic.
        let h = PhaseHistogram::new("test-svc", "qrmpm_phase_ms", &[]);
        h.record("issue", Duration::from_millis(42), &[]);
        let done = h.observe("settle", &[KeyValue::new("vendor", "acme")]);
        done();
    }
}
