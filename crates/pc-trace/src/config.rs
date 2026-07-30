//! OTLP exporter configuration loaded from `OTEL_*` environment variables.
//!
//! mirrors: `phtrace/config.go` (`Config`, `FromEnv`, `withDefaults`, and the
//! `env*` parsing helpers). The functional-option builders from the Go file are
//! intentionally omitted here — the Rust port's public entry point is
//! [`crate::init_from_env`], which reads the environment directly.

use std::collections::BTreeMap;
use std::env;
use std::time::Duration;

/// Controls OTLP exporter behavior for traces and metrics. Only `service_name`
/// and `endpoint` are strictly required; the rest have safe defaults.
///
/// mirrors: `phtrace.Config`.
#[derive(Clone, Debug, PartialEq)]
pub struct Config {
    /// Toggles OTel export. When false, [`crate::init_from_env`] returns a
    /// no-op guard and all helpers degrade to no-ops.
    /// Env: `OTEL_ENABLED` (default: true when an endpoint is set, else false).
    ///
    /// mirrors: `Config.Enabled`.
    pub enabled: bool,

    /// Logical service identifier. Required when enabled.
    /// Env: `OTEL_SERVICE_NAME`.
    ///
    /// mirrors: `Config.ServiceName`.
    pub service_name: String,

    /// Build/semver version. Env: `OTEL_SERVICE_VERSION`.
    ///
    /// mirrors: `Config.ServiceVersion`.
    pub service_version: String,

    /// Deployment environment (prod, stg, dev). Env: `OTEL_DEPLOYMENT_ENV`
    /// (falling back to `APP_ENV`, then `"dev"`).
    ///
    /// mirrors: `Config.Environment`.
    pub environment: String,

    /// OTLP collector host:port (gRPC). Required when enabled.
    /// Env: `OTEL_EXPORTER_OTLP_ENDPOINT`.
    ///
    /// mirrors: `Config.Endpoint`.
    pub endpoint: String,

    /// Disables TLS for the OTLP gRPC connection.
    /// Env: `OTEL_EXPORTER_OTLP_INSECURE` (default: true).
    ///
    /// mirrors: `Config.Insecure`.
    pub insecure: bool,

    /// Head-based sampling ratio for traces (0..=1).
    /// Env: `OTEL_TRACES_SAMPLER_ARG` (default: 1.0).
    ///
    /// mirrors: `Config.SamplingRatio`.
    pub sampling_ratio: f64,

    /// Timeout applied while connecting to the OTLP endpoint during init.
    /// Env: `OTEL_DIAL_TIMEOUT` (default: 5s).
    ///
    /// mirrors: `Config.DialTimeout`.
    pub dial_timeout: Duration,

    /// Maximum delay between span export batches.
    /// Env: `OTEL_BATCH_TIMEOUT` (default: 5s).
    ///
    /// mirrors: `Config.BatchTimeout`.
    pub batch_timeout: Duration,

    /// Maximum spans per export batch.
    /// Env: `OTEL_BATCH_MAX_EXPORT_SIZE` (default: 512).
    ///
    /// mirrors: `Config.BatchMaxExportSize`.
    pub batch_max_export_size: usize,

    /// How often the periodic metric reader pushes.
    /// Env: `OTEL_METRIC_EXPORT_INTERVAL` (default: 15s).
    ///
    /// mirrors: `Config.MetricExportInterval`.
    pub metric_export_interval: Duration,

    /// Extra key=value pairs attached to every span and metric.
    /// Env: `OTEL_RESOURCE_ATTRIBUTES` (comma-separated key=value).
    ///
    /// mirrors: `Config.ResourceAttributes`.
    pub resource_attributes: BTreeMap<String, String>,
}

impl Config {
    /// Loads a `Config` from environment variables. The loader is intentionally
    /// forgiving: unset variables fall back to safe defaults so local developer
    /// runs do not require every `OTEL_*` variable to be set.
    ///
    /// mirrors: `phtrace.FromEnv` (without the functional-option overrides).
    #[must_use]
    pub fn from_env() -> Self {
        let endpoint = trimmed_env("OTEL_EXPORTER_OTLP_ENDPOINT");
        // Default: enabled iff endpoint is set. OTEL_ENABLED overrides.
        let enabled = match trimmed_env("OTEL_ENABLED") {
            v if !v.is_empty() => truthy(&v),
            _ => !endpoint.is_empty(),
        };
        Config {
            enabled,
            service_name: trimmed_env("OTEL_SERVICE_NAME"),
            service_version: trimmed_env("OTEL_SERVICE_VERSION"),
            environment: first_non_empty(&[
                env::var("OTEL_DEPLOYMENT_ENV").unwrap_or_default(),
                env::var("APP_ENV").unwrap_or_default(),
                "dev".to_string(),
            ]),
            endpoint,
            insecure: env_bool("OTEL_EXPORTER_OTLP_INSECURE", true),
            sampling_ratio: env_float("OTEL_TRACES_SAMPLER_ARG", 1.0),
            dial_timeout: env_duration("OTEL_DIAL_TIMEOUT", Duration::from_secs(5)),
            batch_timeout: env_duration("OTEL_BATCH_TIMEOUT", Duration::from_secs(5)),
            batch_max_export_size: env_int("OTEL_BATCH_MAX_EXPORT_SIZE", 512),
            metric_export_interval: env_duration(
                "OTEL_METRIC_EXPORT_INTERVAL",
                Duration::from_secs(15),
            ),
            resource_attributes: parse_resource_attrs(&trimmed_env("OTEL_RESOURCE_ATTRIBUTES")),
        }
    }

    /// Backfills zero-value fields with sensible defaults to keep init robust
    /// when a `Config` is constructed by hand.
    ///
    /// mirrors: `Config.withDefaults`.
    #[must_use]
    pub fn with_defaults(mut self) -> Self {
        if self.dial_timeout.is_zero() {
            self.dial_timeout = Duration::from_secs(5);
        }
        if self.batch_timeout.is_zero() {
            self.batch_timeout = Duration::from_secs(5);
        }
        if self.batch_max_export_size == 0 {
            self.batch_max_export_size = 512;
        }
        if self.metric_export_interval.is_zero() {
            self.metric_export_interval = Duration::from_secs(15);
        }
        if self.sampling_ratio == 0.0 {
            self.sampling_ratio = 1.0;
        }
        if self.environment.is_empty() {
            self.environment = "dev".to_string();
        }
        self
    }
}

/// Returns the trimmed value of `key`, or an empty string when unset.
fn trimmed_env(key: &str) -> String {
    env::var(key).unwrap_or_default().trim().to_string()
}

/// Returns the first non-empty (trimmed) value. mirrors: `firstNonEmpty`.
fn first_non_empty(vals: &[String]) -> String {
    for v in vals {
        let s = v.trim();
        if !s.is_empty() {
            return s.to_string();
        }
    }
    String::new()
}

/// Reports whether `v` is a Go-`truthy` string. mirrors: `truthy`.
fn truthy(v: &str) -> bool {
    matches!(
        v.trim().to_ascii_lowercase().as_str(),
        "1" | "t" | "true" | "y" | "yes" | "on"
    )
}

/// mirrors: `envBool`.
fn env_bool(key: &str, def: bool) -> bool {
    let v = trimmed_env(key);
    if v.is_empty() {
        return def;
    }
    truthy(&v)
}

/// mirrors: `envInt` (non-negative counts; parse failure falls back to `def`).
fn env_int(key: &str, def: usize) -> usize {
    let v = trimmed_env(key);
    if v.is_empty() {
        return def;
    }
    v.parse::<usize>().unwrap_or(def)
}

/// mirrors: `envFloat`.
fn env_float(key: &str, def: f64) -> f64 {
    let v = trimmed_env(key);
    if v.is_empty() {
        return def;
    }
    v.parse::<f64>().unwrap_or(def)
}

/// mirrors: `envDuration`. Parses a subset of Go's `time.ParseDuration` grammar
/// (a decimal number followed by a `ns`/`us`/`ms`/`s`/`m`/`h` unit). Parse
/// failures fall back to `def`.
fn env_duration(key: &str, def: Duration) -> Duration {
    let v = trimmed_env(key);
    if v.is_empty() {
        return def;
    }
    parse_go_duration(&v).unwrap_or(def)
}

/// Parses a single-unit Go duration string (e.g. `"5s"`, `"1500ms"`, `"1.5h"`).
fn parse_go_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    // Split the trailing unit (longest match first for the two-char units).
    for (suffix, seconds_per_unit) in [
        ("ns", 1e-9_f64),
        ("us", 1e-6),
        ("ms", 1e-3),
        ("s", 1.0),
        ("m", 60.0),
        ("h", 3_600.0),
    ] {
        if let Some(num) = s.strip_suffix(suffix) {
            let value: f64 = num.trim().parse().ok()?;
            if value < 0.0 || !value.is_finite() {
                return None;
            }
            return Duration::try_from_secs_f64(value * seconds_per_unit).ok();
        }
    }
    None
}

/// Parses the `OTEL_RESOURCE_ATTRIBUTES` comma-separated key=value list.
/// mirrors: `parseResourceAttrs`.
fn parse_resource_attrs(raw: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    if raw.trim().is_empty() {
        return out;
    }
    for kv in raw.split(',') {
        let kv = kv.trim();
        if let Some((k, v)) = kv.split_once('=') {
            let k = k.trim();
            if !k.is_empty() {
                out.insert(k.to_string(), v.trim().to_string());
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_go_duration_units() {
        assert_eq!(parse_go_duration("5s"), Some(Duration::from_secs(5)));
        assert_eq!(parse_go_duration("15s"), Some(Duration::from_secs(15)));
        assert_eq!(
            parse_go_duration("1500ms"),
            Some(Duration::from_millis(1500))
        );
        assert_eq!(parse_go_duration("2m"), Some(Duration::from_secs(120)));
        assert_eq!(parse_go_duration("1h"), Some(Duration::from_secs(3600)));
        assert_eq!(parse_go_duration("bogus"), None);
    }

    #[test]
    fn truthy_matches_go() {
        for t in ["1", "t", "true", "Y", "YES", "on", "True"] {
            assert!(truthy(t), "{t} should be truthy");
        }
        for f in ["0", "false", "no", "", "maybe"] {
            assert!(!truthy(f), "{f} should be falsy");
        }
    }

    #[test]
    fn resource_attrs_parse() {
        let got = parse_resource_attrs("a=1, b = two ,,c=");
        assert_eq!(got.get("a").map(String::as_str), Some("1"));
        assert_eq!(got.get("b").map(String::as_str), Some("two"));
        assert_eq!(got.get("c").map(String::as_str), Some(""));
        assert_eq!(got.len(), 3);
    }

    #[test]
    fn with_defaults_backfills() {
        let c = Config {
            enabled: false,
            service_name: String::new(),
            service_version: String::new(),
            environment: String::new(),
            endpoint: String::new(),
            insecure: true,
            sampling_ratio: 0.0,
            dial_timeout: Duration::ZERO,
            batch_timeout: Duration::ZERO,
            batch_max_export_size: 0,
            metric_export_interval: Duration::ZERO,
            resource_attributes: BTreeMap::new(),
        }
        .with_defaults();
        assert_eq!(c.dial_timeout, Duration::from_secs(5));
        assert_eq!(c.batch_timeout, Duration::from_secs(5));
        assert_eq!(c.batch_max_export_size, 512);
        assert_eq!(c.metric_export_interval, Duration::from_secs(15));
        assert!((c.sampling_ratio - 1.0).abs() < f64::EPSILON);
        assert_eq!(c.environment, "dev");
    }
}
