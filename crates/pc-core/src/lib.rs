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
            "develop" | "developement" | "dev" => Some(Self::Develop),
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

/// Parse a string with Go's `strconv.ParseBool` grammar.
///
/// mirrors: `strconv.ParseBool` — accepts exactly `1 t T TRUE true True` and
/// `0 f F FALSE false False`, and **nothing else**. `None` is the caller's cue
/// to apply its own default, the way Go's `err != nil` branch does.
///
/// Lives here rather than in each consumer because the same env flags are read
/// by more than one crate, and a private near-copy that accepts a different set
/// of spellings is how `LOG_FORWARD_WARN=1` comes to mean `true` in one crate
/// and `false` in another.
///
/// Does **not** trim, because Go does not: a value with stray whitespace fails
/// to parse and falls back to the default. Callers that want to tolerate
/// whitespace should `.trim()` at the call site and say why.
#[must_use]
pub fn parse_bool_go(value: &str) -> Option<bool> {
    match value {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Some(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Some(false),
        _ => None,
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
        assert_eq!(AppEnv::parse("developement"), Some(AppEnv::Develop));
        assert_eq!(AppEnv::parse("staging"), Some(AppEnv::Staging));
        assert_eq!(AppEnv::parse("production"), Some(AppEnv::Production));
        assert_eq!(AppEnv::parse("prod"), Some(AppEnv::Production));
        assert_eq!(AppEnv::parse("qa"), None);
        assert_eq!(AppEnv::Production.as_str(), "production");
    }

    #[test]
    fn parse_bool_go_accepts_exactly_the_go_grammar() {
        for truthy in ["1", "t", "T", "TRUE", "true", "True"] {
            assert_eq!(parse_bool_go(truthy), Some(true), "{truthy}");
        }
        for falsy in ["0", "f", "F", "FALSE", "false", "False"] {
            assert_eq!(parse_bool_go(falsy), Some(false), "{falsy}");
        }
    }

    /// The spellings Go rejects. `"yes"`/`"on"` look obviously boolean and are
    /// not; `"tRuE"` is the one that bites, because an `eq_ignore_ascii_case`
    /// near-copy accepts it while Go does not.
    #[test]
    fn parse_bool_go_rejects_everything_else() {
        for invalid in [
            "", " ", "yes", "no", "on", "off", "tRuE", "2", "-1", " true",
        ] {
            assert_eq!(parse_bool_go(invalid), None, "{invalid:?}");
        }
    }
}
