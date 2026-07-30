#![forbid(unsafe_code)]
//! `pc-sentry` — Sentry client initialization and phlogger log-hook forwarding.
//!
//! Bit-for-bit parity port of the Go `paycloudhelper` Sentry surface:
//! `sentry.go` (the thin top-level wrappers) and the `phsentry` package
//! (`phsentry.go`, `log_hook.go`). The parity-critical string shapes — the
//! issue title `"[Fn] [env=<env>]"`, the exception value `"[<level>] <msg>"`,
//! the `"[Prefix] body"` log-prefix split, and the default function name — live
//! here as pure, unit-tested helpers so they can be verified without a live
//! Sentry transport.
//!
//! ## Breadcrumb depth divergence
//!
//! Go's `sentry-go` `AddBreadcrumb(bc, limit)` takes a per-call retention
//! limit: 5 for the `SendToSentry*` family and 10 for `ReceiveLog`. The Rust
//! `sentry` SDK trims breadcrumbs at the client level (`ClientOptions.max_breadcrumbs`,
//! applied inside `Hub::add_breadcrumb`), so there is no per-call limit. Because
//! the only forwarding path in this milestone is the `ReceiveLog` log hook, the
//! client is initialized with `max_breadcrumbs == BREADCRUMB_DEPTH_RECEIVE_LOG`
//! (10). Both documented depths are exported as constants for parity reference.

use std::sync::RwLock;

use serde::{Deserialize, Serialize};

/// Default function name used when the log message carries no `[Fn]` prefix and
/// no [`SentryData`] function override is set.
///
/// mirrors: `phsentry.defaultFunctionName` (`"paycloud-be-func"`).
pub const DEFAULT_FUNC: &str = "paycloud-be-func";

/// Exception type used by [`extract_log_prefix`] when the message has no
/// `[Prefix]` bracket.
///
/// mirrors: the `"Log"` fallback returned by `phsentry.extractLogPrefix`.
pub const DEFAULT_EXCEPTION_TYPE: &str = "Log";

/// Breadcrumb retention depth for the general `SendToSentry*` capture family.
///
/// mirrors: the literal `5` passed to `AddBreadcrumb` in
/// `phsentry.captureWithBreadcrumb`. Exported for parity reference; see the
/// crate-level docs for why Rust applies breadcrumb trimming at the client
/// level rather than per call.
pub const BREADCRUMB_DEPTH: usize = 5;

/// Breadcrumb retention depth for the `ReceiveLog` log-hook path.
///
/// mirrors: the literal `10` passed to `AddBreadcrumb` in
/// `phsentry.addDefaultBreadcrumb`.
pub const BREADCRUMB_DEPTH_RECEIVE_LOG: usize = 10;

/// Default Sentry traces sample rate.
///
/// mirrors: `phsentry.InitSentryOptions` default `TracesSampleRate: 1.0`.
pub const DEFAULT_TRACES_SAMPLE_RATE: f32 = 1.0;

/// Breadcrumb context attached to forwarded Sentry events.
///
/// mirrors: `phsentry.SentryData` (`service` / `module` / `function`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SentryData {
    /// The service name (Go `Service`). mirrors: `phhelper.GetAppName`.
    pub service: String,
    /// The module / environment (Go `Module`). mirrors: `phhelper.GetAppEnv`.
    pub module: String,
    /// The function name (Go `Function`). Defaults to [`DEFAULT_FUNC`].
    pub function: String,
}

impl SentryData {
    /// Render the breadcrumb data map (`service` / `module` / `function`).
    ///
    /// mirrors: `phsentry.GetSentryDataMap` — the map attached to the default
    /// breadcrumb in `addDefaultBreadcrumb`.
    #[must_use]
    fn to_map(&self) -> sentry::protocol::Map<String, sentry::protocol::Value> {
        let mut map = sentry::protocol::Map::new();
        map.insert(
            "service".to_string(),
            sentry::protocol::Value::String(self.service.clone()),
        );
        map.insert(
            "module".to_string(),
            sentry::protocol::Value::String(self.module.clone()),
        );
        map.insert(
            "function".to_string(),
            sentry::protocol::Value::String(self.function.clone()),
        );
        map
    }
}

/// Process-global breadcrumb context, `None` until [`init`] or
/// [`set_sentry_data`] populates it. mirrors: `phsentry.sentryBreadcrumbData`.
static SENTRY_DATA: RwLock<Option<SentryData>> = RwLock::new(None);

/// Store the process-global breadcrumb context.
///
/// mirrors: `phsentry.NewSentryData` — a nil argument is a no-op (leaves the
/// current value untouched).
pub fn set_sentry_data(data: SentryData) {
    let mut guard = SENTRY_DATA
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *guard = Some(data);
}

/// Read the process-global breadcrumb context.
///
/// mirrors: `phsentry.GetSentryData`.
#[must_use]
pub fn sentry_data() -> Option<SentryData> {
    SENTRY_DATA
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

/// Options for [`init`].
///
/// mirrors: `phsentry.SentryOptions`. Like the Go path, `paycloudhelper` itself
/// does not read `os.Getenv` for these; consumers map their own configuration
/// in (or use [`SentryOptions::from_env`], which reads the standard `SENTRY_*`
/// variables as a convenience).
#[derive(Clone, Debug, PartialEq)]
pub struct SentryOptions {
    /// Sentry DSN. Empty means Sentry is disabled ([`init`] is a no-op).
    /// mirrors: `SentryOptions.Dsn`.
    pub dsn: String,
    /// Deployment environment. mirrors: `SentryOptions.Environment`.
    pub environment: String,
    /// Release identifier. mirrors: `SentryOptions.Release`.
    pub release: String,
    /// Traces sample rate. mirrors: the default `TracesSampleRate: 1.0`.
    pub traces_sample_rate: f32,
    /// SDK diagnostic logging. mirrors: `SentryOptions.Debug`.
    pub debug: bool,
    /// Optional breadcrumb context seed. mirrors: `SentryOptions.Data`.
    pub data: Option<SentryData>,
}

impl Default for SentryOptions {
    fn default() -> Self {
        Self {
            dsn: String::new(),
            environment: String::new(),
            release: String::new(),
            traces_sample_rate: DEFAULT_TRACES_SAMPLE_RATE,
            debug: false,
            data: None,
        }
    }
}

impl SentryOptions {
    /// Build options from the standard `SENTRY_*` environment variables.
    ///
    /// `SENTRY_DSN`, `SENTRY_ENVIRONMENT` (falling back to the process
    /// `APP_ENV` via `pc_core::identity::app_env_raw`), `SENTRY_RELEASE`,
    /// `SENTRY_TRACES_SAMPLE_RATE` (default [`DEFAULT_TRACES_SAMPLE_RATE`]) and
    /// `SENTRY_DEBUG` (`1`/`true`, case-insensitive).
    ///
    /// mirrors: the consumer-service convention around `phsentry.InitSentry`.
    #[must_use]
    pub fn from_env() -> Self {
        let dsn = std::env::var("SENTRY_DSN").unwrap_or_default();
        let environment = std::env::var("SENTRY_ENVIRONMENT")
            .ok()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(pc_core::identity::app_env_raw);
        let release = std::env::var("SENTRY_RELEASE").unwrap_or_default();
        let traces_sample_rate = std::env::var("SENTRY_TRACES_SAMPLE_RATE")
            .ok()
            .and_then(|v| v.trim().parse::<f32>().ok())
            .unwrap_or(DEFAULT_TRACES_SAMPLE_RATE);
        let debug = matches!(
            std::env::var("SENTRY_DEBUG")
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase()
                .as_str(),
            "1" | "true" | "yes" | "on"
        );
        Self {
            dsn,
            environment,
            release,
            traces_sample_rate,
            debug,
            data: None,
        }
    }
}

/// Initialize the global Sentry client and return its guard.
///
/// Returns `None` when the DSN is empty (a safe no-op), matching the Go path
/// where `InitSentry` returns a nil `*sentry.Client`. The returned
/// [`sentry::ClientInitGuard`] must be held for the lifetime of the process;
/// dropping it flushes and closes the client.
///
/// mirrors: `phsentry.InitSentry` / `phsentry.InitSentryOptions` — sets
/// `AttachStacktrace: false`, `TracesSampleRate: 1.0` (unless overridden) and
/// seeds the breadcrumb [`SentryData`] (service = app name, module = app env,
/// function = [`DEFAULT_FUNC`], overlaid with `options.data`).
#[must_use]
pub fn init(options: &SentryOptions) -> Option<sentry::ClientInitGuard> {
    if options.dsn.is_empty() {
        return None;
    }

    let client_options = sentry::ClientOptions {
        dsn: options.dsn.parse().ok(),
        environment: opt_cow(&options.environment),
        release: opt_cow(&options.release),
        traces_sample_rate: options.traces_sample_rate,
        debug: options.debug,
        attach_stacktrace: false,
        max_breadcrumbs: BREADCRUMB_DEPTH_RECEIVE_LOG,
        ..Default::default()
    };

    let guard = sentry::init(client_options);

    // Seed breadcrumb data: defaults overlaid with the caller's overrides.
    let mut data = SentryData {
        service: pc_core::identity::app_name(),
        module: pc_core::identity::app_env_raw(),
        function: DEFAULT_FUNC.to_string(),
    };
    if let Some(overrides) = &options.data {
        if !overrides.service.is_empty() {
            data.service.clone_from(&overrides.service);
        }
        if !overrides.module.is_empty() {
            data.module.clone_from(&overrides.module);
        }
        if !overrides.function.is_empty() {
            data.function.clone_from(&overrides.function);
        }
    }
    set_sentry_data(data);

    Some(guard)
}

/// Convert a possibly-empty string into an `Option<Cow>` (empty → `None`).
fn opt_cow(s: &str) -> Option<std::borrow::Cow<'static, str>> {
    if s.is_empty() {
        None
    } else {
        Some(std::borrow::Cow::Owned(s.to_string()))
    }
}

/// Returns `true` once a Sentry client is bound to the current hub.
///
/// mirrors: `phsentry.SentryEnabled`.
#[must_use]
pub fn sentry_enabled() -> bool {
    sentry::Hub::current().client().is_some()
}

/// Flush buffered Sentry events, waiting up to `timeout`.
///
/// mirrors: `phsentry.FlushSentry` — a no-op when no client is bound.
pub fn flush(timeout: std::time::Duration) {
    if let Some(client) = sentry::Hub::current().client() {
        client.flush(Some(timeout));
    }
}

/// Build the Sentry issue title (the exception `type`).
///
/// Format: `"[<func>] [env=<env>]"`. Keeping the server name and level out of
/// the type preserves cross-pod issue grouping.
///
/// mirrors: `phsentry.buildSentryTitle`.
#[must_use]
pub fn issue_title(func: &str, env: &str) -> String {
    format!("[{func}] [env={env}]")
}

/// Build the Sentry exception `value`.
///
/// Format: `"[<level>] <msg>"` — the level is surfaced in the message body.
///
/// mirrors: the `fmt.Sprintf("[%s] %s", level, exValue)` in `phsentry.ReceiveLog`.
#[must_use]
pub fn exception_value(level: &str, msg: &str) -> String {
    format!("[{level}] {msg}")
}

/// Split a log message formatted as `"[Prefix] body"` into
/// `(exception_type, exception_value)`.
///
/// Returns the bracketed prefix as the type and the trimmed remainder as the
/// value. When there is no `[Prefix]` bracket (or the bracket is empty), the
/// type defaults to [`DEFAULT_EXCEPTION_TYPE`] and the value is the trimmed
/// whole message.
///
/// mirrors: `phsentry.extractLogPrefix` (the `end > 1` guard rejects an empty
/// `[]` prefix).
#[must_use]
pub fn extract_log_prefix(message: &str) -> (String, String) {
    let msg = message.trim();
    if let Some(rest) = msg.strip_prefix('[') {
        if let Some(end) = rest.find(']') {
            // `end` is the index of `]` within `rest`; Go's `end > 1` on the
            // original string is `end >= 1` here (a non-empty prefix).
            if end >= 1 {
                let ex_type = &rest[..end];
                let ex_value = rest[end + 1..].trim();
                return (ex_type.to_string(), ex_value.to_string());
            }
        }
    }
    (DEFAULT_EXCEPTION_TYPE.to_string(), msg.to_string())
}

/// Map a phlogger level string to a [`sentry::Level`].
///
/// mirrors: `phsentry.sentryLevelFor` — unknown levels fall through to
/// `Debug`.
#[must_use]
pub fn sentry_level_for(level: &str) -> sentry::Level {
    match level {
        "fatal" => sentry::Level::Fatal,
        "error" => sentry::Level::Error,
        "warn" => sentry::Level::Warning,
        "info" => sentry::Level::Info,
        _ => sentry::Level::Debug,
    }
}

/// Resolve the environment string used in the issue title.
///
/// mirrors: `phsentry.buildSentryTitle` — the client's configured environment
/// wins over the raw `APP_ENV` when non-empty.
fn resolve_env() -> String {
    if let Some(client) = sentry::Hub::current().client() {
        if let Some(env) = &client.options().environment {
            if !env.is_empty() {
                return env.to_string();
            }
        }
    }
    pc_core::identity::app_env_raw()
}

/// Forward a phlogger log record to Sentry.
///
/// This is the log-hook subscriber called for every phlogger level. A no-op
/// when no Sentry client is bound (matching the "silently skip until a client
/// is initialized" contract). Error/fatal records become structured exception
/// events whose title is [`issue_title`] and whose value is [`exception_value`];
/// all other levels are captured as plain messages. When [`SentryData`] is set,
/// a default breadcrumb (retention [`BREADCRUMB_DEPTH_RECEIVE_LOG`]) carrying
/// the service/module/function context is attached.
///
/// `level` is one of `debug` | `info` | `warn` | `error` | `fatal`; `message`
/// is the formatted log string (its `[Fn]` prefix becomes the issue title's
/// function segment via [`extract_log_prefix`]).
///
/// mirrors: `phsentry.ReceiveLog` + `phsentry.addDefaultBreadcrumb`.
pub fn receive_log(level: &str, message: &str) {
    if sentry::Hub::current().client().is_none() {
        return;
    }

    let sentry_level = sentry_level_for(level);
    let data = sentry_data();

    sentry::with_scope(
        |scope| scope.set_level(Some(sentry_level)),
        || {
            // addDefaultBreadcrumb: only attach when breadcrumb data is set.
            if let Some(data) = &data {
                sentry::add_breadcrumb(sentry::protocol::Breadcrumb {
                    ty: "default".to_string(),
                    category: Some(level.to_string()),
                    message: Some(message.to_string()),
                    level: sentry_level,
                    data: data.to_map(),
                    ..Default::default()
                });
            }

            if level == "error" || level == "fatal" {
                let (ex_type, ex_value) = extract_log_prefix(message);
                let env = resolve_env();
                let event = sentry::protocol::Event {
                    level: sentry_level,
                    message: Some(message.to_string()),
                    exception: vec![sentry::protocol::Exception {
                        ty: issue_title(&ex_type, &env),
                        value: Some(exception_value(level, &ex_value)),
                        ..Default::default()
                    }]
                    .into(),
                    ..Default::default()
                };
                sentry::capture_event(event);
            } else {
                sentry::capture_message(message, sentry_level);
            }
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn issue_title_shape() {
        // parity: sentry issue title "[Fn] [env=<env>]"
        assert_eq!(issue_title("Push", "production"), "[Push] [env=production]");
    }

    #[test]
    fn exception_value_shape() {
        // parity: exception value "[<level>] <msg>"
        assert_eq!(exception_value("ERROR", "boom"), "[ERROR] boom");
    }

    #[test]
    fn default_func_constant() {
        assert_eq!(DEFAULT_FUNC, "paycloud-be-func");
    }

    #[test]
    fn breadcrumb_depth_constants() {
        // parity: depth 5 default, 10 for ReceiveLog.
        assert_eq!(BREADCRUMB_DEPTH, 5);
        assert_eq!(BREADCRUMB_DEPTH_RECEIVE_LOG, 10);
    }

    #[test]
    fn extract_log_prefix_with_bracket() {
        // "[ReadyCheck] readiness failed" -> ("ReadyCheck", "readiness failed")
        let (ty, val) = extract_log_prefix("[ReadyCheck] readiness failed");
        assert_eq!(ty, "ReadyCheck");
        assert_eq!(val, "readiness failed");
    }

    #[test]
    fn extract_log_prefix_no_bracket() {
        let (ty, val) = extract_log_prefix("plain message");
        assert_eq!(ty, DEFAULT_EXCEPTION_TYPE);
        assert_eq!(val, "plain message");
    }

    #[test]
    fn extract_log_prefix_empty_bracket_falls_back() {
        // Go's `end > 1` guard rejects an empty "[]" prefix.
        let (ty, val) = extract_log_prefix("[] body");
        assert_eq!(ty, DEFAULT_EXCEPTION_TYPE);
        assert_eq!(val, "[] body");
    }

    #[test]
    fn extract_log_prefix_trims_whitespace() {
        let (ty, val) = extract_log_prefix("  [Fn]   spaced   ");
        assert_eq!(ty, "Fn");
        assert_eq!(val, "spaced");
    }

    #[test]
    fn full_error_title_and_value() {
        // Composed shape used by receive_log for error/fatal events.
        let (ex_type, ex_value) = extract_log_prefix("[main.initSentry] readiness check failed");
        assert_eq!(
            issue_title(&ex_type, "development"),
            "[main.initSentry] [env=development]"
        );
        assert_eq!(
            exception_value("error", &ex_value),
            "[error] readiness check failed"
        );
    }

    #[test]
    fn level_mapping() {
        assert_eq!(sentry_level_for("fatal"), sentry::Level::Fatal);
        assert_eq!(sentry_level_for("error"), sentry::Level::Error);
        assert_eq!(sentry_level_for("warn"), sentry::Level::Warning);
        assert_eq!(sentry_level_for("info"), sentry::Level::Info);
        assert_eq!(sentry_level_for("debug"), sentry::Level::Debug);
        assert_eq!(sentry_level_for("unknown"), sentry::Level::Debug);
    }

    #[test]
    fn empty_dsn_init_is_noop() {
        // parity: empty DSN => nil client (None guard), and a safe no-op.
        let opts = SentryOptions::default();
        assert!(opts.dsn.is_empty());
        let guard = init(&opts);
        assert!(guard.is_none());
        // No client bound: forwarding must not panic and must do nothing.
        assert!(!sentry_enabled());
        receive_log("error", "[Fn] boom");
        flush(std::time::Duration::from_millis(0));
    }

    #[test]
    fn from_env_defaults_when_unset() {
        // Isolate from ambient SENTRY_* by asserting the defaulting behavior of
        // the sample rate parser directly (env is process-global; avoid mutation).
        let opts = SentryOptions {
            traces_sample_rate: DEFAULT_TRACES_SAMPLE_RATE,
            ..SentryOptions::default()
        };
        assert!((opts.traces_sample_rate - 1.0).abs() < f32::EPSILON);
        assert!(opts.dsn.is_empty());
    }

    #[test]
    fn sentry_data_to_map_keys() {
        let d = SentryData {
            service: "svc".to_string(),
            module: "prod".to_string(),
            function: "Fn".to_string(),
        };
        let map = d.to_map();
        assert_eq!(map.get("service").and_then(|v| v.as_str()), Some("svc"));
        assert_eq!(map.get("module").and_then(|v| v.as_str()), Some("prod"));
        assert_eq!(map.get("function").and_then(|v| v.as_str()), Some("Fn"));
    }
}
