#![forbid(unsafe_code)]
//! `pc-trace` — OpenTelemetry tracing and metrics helpers for PayCloud
//! services. Bit-for-bit port of Go `paycloudhelper/phtrace`.
//!
//! Design goals (mirrors the `phtrace` package doc):
//!   - Zero-cost when disabled (`OTEL_ENABLED=false`, or [`init_from_env`] not
//!     called): [`is_enabled`] reflects an atomic flag and helpers degrade to
//!     no-ops backed by the global no-op tracer/meter.
//!   - Works with Grafana Tempo (traces) and Prometheus (metrics) via the OTLP
//!     gRPC exporter hitting an OTel Collector.
//!   - W3C `traceparent` + `baggage` propagation, including over RabbitMQ (see
//!     the [`amqp`] module, behind the `amqp` feature).
//!   - Graceful shutdown: dropping (or explicitly shutting down) the
//!     [`TraceGuard`] flushes pending spans and metrics.
//!
//! # Parity map (Go symbol → Rust item)
//!   - `phtrace.Init` + `phtrace.FromEnv` → [`init_from_env`]
//!   - `phtrace.IsEnabled`               → [`is_enabled`]
//!   - `phtrace.Shutdown`                → [`TraceGuard`]
//!   - `phtrace.Config`                  → [`Config`]
//!   - log field-key constants           → [`fields`]
//!   - `phtrace.PhaseHistogram`          → [`PhaseHistogram`]
//!   - `phtrace.InjectAMQP` / `ExtractAMQP` → [`amqp::inject_amqp`] / [`amqp::extract_amqp`]

use std::sync::atomic::{AtomicBool, Ordering};

use opentelemetry::propagation::TextMapCompositePropagator;
use opentelemetry_sdk::propagation::{BaggagePropagator, TraceContextPropagator};

mod config;
mod metrics;

#[cfg(feature = "amqp")]
pub mod amqp;

pub use config::Config;
pub use metrics::{PhaseHistogram, DEFAULT_PHASE_BUCKETS};

#[cfg(feature = "amqp")]
pub use amqp::{extract_amqp, inject_amqp};

/// Canonical log/telemetry field-key constants, shared across all PayCloud
/// services so Loki/Grafana/Tempo queries stay consistent. These names are
/// frozen (design 02 §5).
///
/// mirrors: the `Field*` constants in `phtrace/log.go`.
pub mod fields {
    /// mirrors: `phtrace.FieldTraceID`.
    pub const TRACE_ID: &str = "trace_id";
    /// mirrors: `phtrace.FieldSpanID`.
    pub const SPAN_ID: &str = "span_id";
    /// mirrors: `phtrace.FieldTicketID`.
    pub const TICKET_ID: &str = "ticket_id";
    /// mirrors: `phtrace.FieldReffNo`.
    pub const REFF_NO: &str = "reff_no";
    /// mirrors: `phtrace.FieldMerchantID`.
    pub const MERCHANT_ID: &str = "merchant_id";
    /// mirrors: `phtrace.FieldOrderID`.
    pub const ORDER_ID: &str = "order_id";
    /// mirrors: `phtrace.FieldTrxID`.
    pub const TRX_ID: &str = "trx_id";
    /// mirrors: `phtrace.FieldTrxNo`.
    pub const TRX_NO: &str = "trx_no";
    /// mirrors: `phtrace.FieldService`.
    pub const SERVICE: &str = "service";
    /// mirrors: `phtrace.FieldRoute`.
    pub const ROUTE: &str = "route";
    /// mirrors: `phtrace.FieldVendor`.
    pub const VENDOR: &str = "vendor";
}

/// Global "is exporting" flag. mirrors: the package-level `enabled atomic.Bool`.
static ENABLED: AtomicBool = AtomicBool::new(false);

/// Reports whether `pc-trace` has been successfully initialized and is actively
/// exporting telemetry. Cheap (atomic load) and safe on hot paths.
///
/// mirrors: `phtrace.IsEnabled`.
#[must_use]
pub fn is_enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

/// Returns the composite W3C propagator (`traceparent` + `baggage`).
///
/// mirrors: `phtrace.Propagator` — because [`init_from_env`] installs this exact
/// composite as the global propagator, constructing a fresh one here is
/// behaviorally identical and works even before init (so AMQP carriers still
/// round-trip in the disabled state).
#[cfg_attr(not(feature = "amqp"), allow(dead_code))]
pub(crate) fn propagator() -> TextMapCompositePropagator {
    TextMapCompositePropagator::new(vec![
        Box::new(TraceContextPropagator::new()),
        Box::new(BaggagePropagator::new()),
    ])
}

/// Guard returned by [`init_from_env`]. Dropping it (or calling
/// [`TraceGuard::shutdown`]) flushes pending spans and metrics and clears the
/// [`is_enabled`] flag. Idempotent: only the first shutdown does work.
///
/// mirrors: `phtrace.Shutdown` (a closure in Go; an RAII guard here).
#[must_use = "dropping the guard immediately flushes and shuts down telemetry"]
pub struct TraceGuard {
    tracer: Option<opentelemetry_sdk::trace::TracerProvider>,
    meter: Option<opentelemetry_sdk::metrics::SdkMeterProvider>,
}

impl TraceGuard {
    /// A guard for the disabled / no-op path. Holds no providers.
    fn disabled() -> Self {
        TraceGuard {
            tracer: None,
            meter: None,
        }
    }

    /// Flushes and shuts down the tracer and meter providers, clearing the
    /// global enabled flag. Safe to call multiple times.
    ///
    /// mirrors: the `Shutdown` closure body in `phtrace.Init`.
    pub fn shutdown(&mut self) -> anyhow::Result<()> {
        let mut errs: Vec<String> = Vec::new();
        if let Some(tp) = self.tracer.take() {
            if let Err(e) = tp.shutdown() {
                errs.push(format!("tracer shutdown: {e}"));
            }
        }
        if let Some(mp) = self.meter.take() {
            if let Err(e) = mp.shutdown() {
                errs.push(format!("meter shutdown: {e}"));
            }
        }
        ENABLED.store(false, Ordering::Relaxed);
        if errs.is_empty() {
            Ok(())
        } else {
            Err(anyhow::anyhow!(errs.join("; ")))
        }
    }
}

impl Drop for TraceGuard {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

/// Initializes OTLP gRPC exporters for traces and metrics from `OTEL_*`
/// environment variables and installs global providers plus the W3C composite
/// propagator. Returns a [`TraceGuard`] that flushes on drop.
///
/// When export is disabled (`OTEL_ENABLED` false, or no endpoint configured),
/// this is a **no-op**: it returns a disabled guard, leaves the global no-op
/// providers in place, and [`is_enabled`] stays `false`.
///
/// The enabled path builds tonic-backed OTLP exporters and a Tokio-runtime
/// batch/periodic pipeline, so it must be called from within a Tokio runtime.
///
/// mirrors: `phtrace.Init(ctx, phtrace.FromEnv())`.
///
/// # Errors
/// Returns an error when export is enabled but required configuration is
/// missing/invalid (empty service name or endpoint, sampling ratio outside
/// `[0,1]`, non-positive dial timeout), or when an OTLP exporter fails to build.
pub fn init_from_env() -> anyhow::Result<TraceGuard> {
    init(Config::from_env())
}

/// Shared init used by [`init_from_env`]. Split out so the config source is
/// testable independently of process environment.
fn init(cfg: Config) -> anyhow::Result<TraceGuard> {
    let cfg = cfg.with_defaults();

    if !cfg.enabled {
        // No-op path: leave global no-op providers, do not flip ENABLED.
        return Ok(TraceGuard::disabled());
    }

    ensure_required(&cfg)?;

    let guard = build_pipeline(&cfg)?;
    ENABLED.store(true, Ordering::Relaxed);
    Ok(guard)
}

/// Validates the fields that must be present before wiring exporters.
///
/// mirrors: `phtrace.ensureRequired`.
fn ensure_required(cfg: &Config) -> anyhow::Result<()> {
    if cfg.service_name.is_empty() {
        anyhow::bail!("pc-trace: service_name must not be empty (set OTEL_SERVICE_NAME)");
    }
    if cfg.endpoint.is_empty() {
        anyhow::bail!("pc-trace: endpoint must not be empty (set OTEL_EXPORTER_OTLP_ENDPOINT)");
    }
    if !(0.0..=1.0).contains(&cfg.sampling_ratio) {
        anyhow::bail!(
            "pc-trace: sampling_ratio must be in [0,1], got {}",
            cfg.sampling_ratio
        );
    }
    if cfg.dial_timeout.is_zero() {
        anyhow::bail!("pc-trace: dial_timeout must be > 0");
    }
    Ok(())
}

/// Builds the trace + metric export pipeline and installs global providers and
/// the composite propagator.
///
/// mirrors: `buildResource` + `buildTracerProvider` + `buildMeterProvider` and
/// the `otel.Set*` calls in `phtrace.Init`.
fn build_pipeline(cfg: &Config) -> anyhow::Result<TraceGuard> {
    use opentelemetry::KeyValue;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::trace::Sampler;
    use opentelemetry_sdk::{runtime, Resource};
    use opentelemetry_semantic_conventions::resource as semres;

    // ---- resource (service identity + extra attributes) ----
    let mut attrs = vec![
        KeyValue::new(semres::SERVICE_NAME, cfg.service_name.clone()),
        KeyValue::new(semres::SERVICE_VERSION, cfg.service_version.clone()),
        // Go used semconv v1.26 "deployment.environment"; use the literal key
        // for byte-parity without the deprecated-const clippy warning.
        KeyValue::new("deployment.environment", cfg.environment.clone()),
    ];
    for (k, v) in &cfg.resource_attributes {
        attrs.push(KeyValue::new(k.clone(), v.clone()));
    }
    let resource = Resource::new(attrs);

    // ---- tracer provider (OTLP tonic + ParentBased(TraceIDRatioBased)) ----
    let mut span_builder = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(cfg.endpoint.clone())
        .with_timeout(cfg.dial_timeout);
    if cfg.insecure {
        span_builder = span_builder.with_protocol(opentelemetry_otlp::Protocol::Grpc);
    }
    let span_exporter = span_builder
        .build()
        .map_err(|e| anyhow::anyhow!("pc-trace: build span exporter: {e}"))?;

    let sampler = Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(cfg.sampling_ratio)));
    let tracer_provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_batch_exporter(span_exporter, runtime::Tokio)
        .with_resource(resource.clone())
        .with_sampler(sampler)
        .build();

    // ---- meter provider (OTLP tonic + periodic reader) ----
    let mut metric_builder = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(cfg.endpoint.clone())
        .with_timeout(cfg.dial_timeout);
    if cfg.insecure {
        metric_builder = metric_builder.with_protocol(opentelemetry_otlp::Protocol::Grpc);
    }
    let metric_exporter = metric_builder
        .build()
        .map_err(|e| anyhow::anyhow!("pc-trace: build metric exporter: {e}"))?;

    let reader =
        opentelemetry_sdk::metrics::PeriodicReader::builder(metric_exporter, runtime::Tokio)
            .with_interval(cfg.metric_export_interval)
            .build();
    let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(resource)
        .build();

    // ---- install globals ----
    opentelemetry::global::set_tracer_provider(tracer_provider.clone());
    opentelemetry::global::set_meter_provider(meter_provider.clone());
    opentelemetry::global::set_text_map_propagator(propagator());

    Ok(TraceGuard {
        tracer: Some(tracer_provider),
        meter: Some(meter_provider),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_constants_are_frozen() {
        assert_eq!(fields::TRACE_ID, "trace_id");
        assert_eq!(fields::SPAN_ID, "span_id");
        assert_eq!(fields::TICKET_ID, "ticket_id");
        assert_eq!(fields::REFF_NO, "reff_no");
        assert_eq!(fields::MERCHANT_ID, "merchant_id");
        assert_eq!(fields::ORDER_ID, "order_id");
        assert_eq!(fields::TRX_ID, "trx_id");
        assert_eq!(fields::TRX_NO, "trx_no");
        assert_eq!(fields::SERVICE, "service");
        assert_eq!(fields::ROUTE, "route");
        assert_eq!(fields::VENDOR, "vendor");
    }

    #[test]
    fn disabled_by_default() {
        // OTEL_ENABLED unset and no endpoint => disabled, is_enabled() false.
        assert!(!is_enabled());
        let cfg = Config {
            enabled: false,
            ..Config::from_env()
        };
        let guard = init(cfg).expect("disabled init is infallible");
        assert!(!is_enabled());
        drop(guard);
        assert!(!is_enabled());
    }

    #[test]
    #[allow(clippy::float_cmp)] // frozen integer-valued f64 constants.
    fn default_phase_buckets_exposed() {
        assert_eq!(
            DEFAULT_PHASE_BUCKETS,
            [5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0]
        );
    }

    #[cfg(feature = "amqp")]
    #[test]
    fn amqp_traceparent_round_trip() {
        use lapin::types::FieldTable;
        use opentelemetry::trace::{SpanContext, TraceContextExt, TraceFlags, TraceState};
        use opentelemetry::trace::{SpanId, TraceId};
        use opentelemetry::Context;

        let trace_id = TraceId::from_hex("0af7651916cd43dd8448eb211c80319c").unwrap();
        let span_id = SpanId::from_hex("b7ad6b7169203331").unwrap();
        let sc = SpanContext::new(
            trace_id,
            span_id,
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        );
        let cx = Context::new().with_remote_span_context(sc);

        let mut headers = FieldTable::default();
        inject_amqp(&cx, &mut headers);

        // The W3C traceparent header must have been written.
        assert!(headers.inner().contains_key("traceparent"));

        let extracted = extract_amqp(&headers);
        let out = extracted.span().span_context().clone();
        assert_eq!(out.trace_id(), trace_id);
        assert_eq!(out.span_id(), span_id);
        assert!(out.is_sampled());
    }
}
