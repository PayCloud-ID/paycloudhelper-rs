#![forbid(unsafe_code)]
//! `pc-core` — the acyclic base crate. Depends on nothing PayCloud, only
//! lightweight third-party crates. Every other `pc-*` crate builds on this.
//!
//! Mirrors the root of Go `paycloudhelper`: `helpers.go` (`JsonMinify`),
//! `phhelper/helper.go` (`BuildLogPrefix`), `phhelper/globenv.go` (app identity),
//! and the `(result, status, err)` triple-return discipline (05 §1.4).

pub mod identity;
pub mod json;

pub use json::json_minify;

/// Deployment environment.
///
/// mirrors: the valid `APP_ENV` set in `config.go` (`develop`/`staging`/`production`).
/// `parse` is intentionally lenient — it also accepts the `dev`/`stg`/`prod`
/// short forms the log sampler keys on. Strict boot validation (rejecting
/// anything outside the three canonical names) lives in `pc-config`.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub enum AppEnv {
    Develop,
    Staging,
    Production,
}

impl AppEnv {
    /// Parse an environment string. Case-sensitive to match Go's map lookup,
    /// with the documented short forms added for the sampler.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "develop" | "dev" => Some(Self::Develop),
            "staging" | "stg" => Some(Self::Staging),
            "production" | "prod" => Some(Self::Production),
            _ => None,
        }
    }

    /// The canonical string form (the value stored in app identity).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Develop => "develop",
            Self::Staging => "staging",
            Self::Production => "production",
        }
    }
}

/// Log module prefix constant. mirrors: `phhelper.LogModulePrefix = "pchelper"`.
pub const LOG_MODULE_PREFIX: &str = "pchelper";

/// Build the standardized log prefix.
///
/// mirrors: `phhelper.BuildLogPrefix` — trims the function name, substitutes
/// `"Log"` when blank, and wraps as `[pchelper.Fn]` (or `[Fn]` if the module
/// prefix were ever empty).
#[must_use]
pub fn build_log_prefix(function_name: &str) -> String {
    let fn_name = function_name.trim();
    let fn_name = if fn_name.is_empty() { "Log" } else { fn_name };
    if LOG_MODULE_PREFIX.is_empty() {
        format!("[{fn_name}]")
    } else {
        format!("[{LOG_MODULE_PREFIX}.{fn_name}]")
    }
}

/// The platform's `(result, status, err)` triple-return, encoded as a `Result`
/// error type. mirrors: 05 §1.4.
///
/// `Business` carries a status string the caller maps to a response code (the
/// expected-outcome path); `System` wraps an unexpected failure. Marked
/// `#[must_use]` at the `Result` level so a dropped error is a compile-time
/// error class (the `orderman-set-expired.go` dropped-error bug becomes
/// structurally impossible).
#[derive(thiserror::Error, Debug)]
pub enum AppError {
    /// Business outcome carrying a status the caller maps to a response code.
    #[error("business: {0}")]
    Business(String),
    /// System / unexpected failure.
    #[error(transparent)]
    System(#[from] anyhow::Error),
}

impl AppError {
    /// True when this is an expected business outcome (not a system fault).
    #[must_use]
    pub fn is_business(&self) -> bool {
        matches!(self, Self::Business(_))
    }
}

/// Convenience alias for the platform result type.
pub type AppResult<T> = Result<T, AppError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_prefix_matches_go() {
        assert_eq!(
            build_log_prefix("InitializeApp"),
            "[pchelper.InitializeApp]"
        );
        // blank / whitespace fn -> "Log"
        assert_eq!(build_log_prefix(""), "[pchelper.Log]");
        assert_eq!(build_log_prefix("   "), "[pchelper.Log]");
        // surrounding whitespace trimmed (Go strings.TrimSpace)
        assert_eq!(build_log_prefix("  Push  "), "[pchelper.Push]");
    }

    #[test]
    fn app_env_parse() {
        assert_eq!(AppEnv::parse("develop"), Some(AppEnv::Develop));
        assert_eq!(AppEnv::parse("dev"), Some(AppEnv::Develop));
        assert_eq!(AppEnv::parse("staging"), Some(AppEnv::Staging));
        assert_eq!(AppEnv::parse("production"), Some(AppEnv::Production));
        assert_eq!(AppEnv::parse("prod"), Some(AppEnv::Production));
        assert_eq!(AppEnv::parse("qa"), None);
        assert_eq!(AppEnv::Production.as_str(), "production");
    }
}
