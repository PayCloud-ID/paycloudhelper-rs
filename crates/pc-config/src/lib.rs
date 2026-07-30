#![forbid(unsafe_code)]
//! Configuration bootstrap + validation. mirrors: `init.go` (`findEnvPath`,
//! `InitializeApp`) and `config.go` (`ValidateConfiguration`,
//! `GetConfigurationStatus`).
//!
//! Unlike Go, there is **no import-time `init()`** (01 §4.4): a service calls
//! [`initialize_app`] explicitly from `main()` (via the `paycloudhelper`
//! umbrella). This crate deliberately depends only on `pc-core` — it does not
//! pull `pc-log`, so validation results are *returned* for the caller to log,
//! rather than logged here (Go's `LogConfigurationWarnings` lives at the
//! umbrella layer).

use std::path::{Path, PathBuf};

use pc_core::identity;
use serde::Serialize;

/// Severity of a configuration finding. Serializes to Go's exact strings.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    Warning,
    Error,
}

impl Level {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Warning => "warning",
            Self::Error => "error",
        }
    }
}

/// A configuration validation finding. mirrors: `config.go ConfigError`
/// (`{field, message, level}`) — field names preserved for the health endpoint.
#[derive(Clone, Debug, Serialize)]
pub struct ConfigError {
    pub field: String,
    pub message: String,
    pub level: Level,
}

impl ConfigError {
    fn warn(field: &str, message: &str) -> Self {
        Self {
            field: field.to_string(),
            message: message.to_string(),
            level: Level::Warning,
        }
    }
}

/// mirrors: `findEnvPath` — resolution order:
/// `ENV_FILE`/`DOTENV_PATH` → CWD/.env → ≤5 parent dirs → binary dir →
/// `/app/.env` → `/.env`. `None` means no `.env` was found.
#[must_use]
pub fn find_env_path() -> Option<PathBuf> {
    // Explicit operator overrides win; both names accepted.
    for key in ["ENV_FILE", "DOTENV_PATH"] {
        if let Ok(p) = std::env::var(key) {
            if !p.is_empty() && Path::new(&p).exists() {
                return Some(PathBuf::from(p));
            }
        }
    }

    if let Ok(wd) = std::env::current_dir() {
        let candidate = wd.join(".env");
        if candidate.exists() {
            return Some(candidate);
        }
        // Walk up to 5 parent directories, stopping at the filesystem root.
        let mut dir = wd.as_path();
        for _ in 0..5 {
            match dir.parent() {
                Some(parent) if parent != dir => {
                    let candidate = parent.join(".env");
                    if candidate.exists() {
                        return Some(candidate);
                    }
                    dir = parent;
                }
                _ => break,
            }
        }
    }

    // Binary directory.
    if let Ok(exe) = std::env::current_exe() {
        if let Some(exe_dir) = exe.parent() {
            let candidate = exe_dir.join(".env");
            if candidate.exists() {
                return Some(candidate);
            }
        }
    }

    // Common container secret-mount targets, checked last so local dev wins.
    for p in ["/app/.env", "/.env"] {
        if Path::new(p).exists() {
            return Some(PathBuf::from(p));
        }
    }

    None
}

/// Load the `.env` file discovered by [`find_env_path`] (falling back to
/// `dotenvy`'s default CWD search). mirrors: the `godotenv.Load` calls in
/// `InitializeApp`. A missing `.env` is not an error.
pub fn load_dotenv() {
    match find_env_path() {
        Some(path) => {
            let _ = dotenvy::from_path(&path);
        }
        None => {
            let _ = dotenvy::dotenv();
        }
    }
}

/// mirrors: `InitializeApp` — load `.env`, set `APP_NAME`/`APP_ENV` (with the
/// `APP_MODE` legacy fallback) into `pc-core` identity, then validate.
///
/// Returns `Ok(())` when validation is clean, or `Err(findings)` when there are
/// warnings/errors — the caller (umbrella) logs them (Go's
/// `LogConfigurationWarnings`).
///
/// # Errors
/// Returns the list of configuration findings when any are present.
pub fn initialize_app() -> Result<(), Vec<ConfigError>> {
    load_dotenv();

    if let Ok(app_name) = std::env::var("APP_NAME") {
        if !app_name.is_empty() {
            identity::set_app_name(&app_name);
        }
    }
    // APP_ENV is canonical; APP_MODE is accepted as a fallback.
    match std::env::var("APP_ENV") {
        Ok(v) if !v.is_empty() => identity::set_app_env_raw(&v),
        _ => {
            if let Ok(mode) = std::env::var("APP_MODE") {
                if !mode.is_empty() {
                    identity::set_app_env_raw(&mode);
                }
            }
        }
    }

    let findings = validate_configuration();
    if findings.is_empty() {
        Ok(())
    } else {
        Err(findings)
    }
}

fn env_nonempty(key: &str) -> bool {
    std::env::var(key).map(|v| !v.is_empty()).unwrap_or(false)
}

/// mirrors: `config.go ValidateConfiguration`.
///
/// Note: the Redis `Addr`/`Password` checks in the Go version read
/// `redisOptions` (state owned by the redis pool). In the Rust split that state
/// lives in `pc-redis`, so those two rows are deferred to `pc-redis`/`pc-health`
/// probes; the env-derived checks below are reproduced faithfully.
#[must_use]
pub fn validate_configuration() -> Vec<ConfigError> {
    let mut errors: Vec<ConfigError> = Vec::new();

    // APP_NAME (env is authoritative for this warning).
    if !env_nonempty("APP_NAME") {
        errors.push(ConfigError::warn(
            "APP_NAME",
            "APP_NAME environment variable not set - using empty default",
        ));
    }

    // APP_ENV: env, falling back to stored identity.
    let app_env = match std::env::var("APP_ENV") {
        Ok(v) if !v.is_empty() => v,
        _ => identity::app_env_raw(),
    };
    if app_env.is_empty() {
        errors.push(ConfigError::warn(
            "APP_ENV",
            "APP_ENV environment variable not set - using empty default",
        ));
    } else if !matches!(app_env.as_str(), "develop" | "staging" | "production") {
        errors.push(ConfigError::warn(
            "APP_ENV",
            &format!(
                "APP_ENV has unexpected value '{app_env}' (expected: develop, staging, production)"
            ),
        ));
    }

    // Sentry.
    if !env_nonempty("SENTRY_DSN") {
        errors.push(ConfigError::warn(
            "SENTRY_DSN",
            "SENTRY_DSN not set - error tracking disabled",
        ));
    }

    // RabbitMQ audit trail: warn only on partial configuration (some-but-not-all).
    let rabbit_keys = [
        "RABBITMQ_HOST",
        "RABBITMQ_PORT",
        "RABBITMQ_VIRTUAL_HOST_AUDITTRAIL",
        "RABBITMQ_USERNAME_AUDITTRAIL",
        "RABBITMQ_PASSWORD_AUDITTRAIL",
        "RABBITMQ_QUEUE_AUDITTRAIL",
    ];
    let total = rabbit_keys.len();
    let configured = rabbit_keys.iter().filter(|k| env_nonempty(k)).count();
    if configured > 0 && configured < total {
        errors.push(ConfigError::warn(
            "RabbitMQ",
            "RabbitMQ audit trail partially configured - audit trail may not work",
        ));
    } else if configured == 0 {
        errors.push(ConfigError::warn(
            "RabbitMQ",
            "RabbitMQ audit trail not configured - audit trail disabled",
        ));
    }

    errors
}

/// mirrors: `config.go GetConfigurationStatus` — summary for the health
/// endpoint. Worst-of: any error → `unhealthy`, else any warning → `degraded`,
/// else `healthy`. JSON shape `{status, errors, warnings, issues}`.
#[must_use]
pub fn configuration_status() -> serde_json::Value {
    let issues = validate_configuration();
    let error_count = issues.iter().filter(|e| e.level == Level::Error).count();
    let warning_count = issues.iter().filter(|e| e.level == Level::Warning).count();

    let status = if error_count > 0 {
        "unhealthy"
    } else if warning_count > 0 {
        "degraded"
    } else {
        "healthy"
    };

    serde_json::json!({
        "status": status,
        "errors": error_count,
        "warnings": warning_count,
        "issues": issues,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_error_serializes_go_shape() {
        let e = ConfigError::warn("APP_ENV", "bad");
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["field"], "APP_ENV");
        assert_eq!(v["message"], "bad");
        assert_eq!(v["level"], "warning");
    }

    #[test]
    fn status_shape_has_expected_keys() {
        let v = configuration_status();
        assert!(v.get("status").is_some());
        assert!(v.get("errors").is_some());
        assert!(v.get("warnings").is_some());
        assert!(v["issues"].is_array());
    }
}
