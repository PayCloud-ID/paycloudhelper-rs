//! Process-global app identity (thread-safe).
//!
//! mirrors: `phhelper/globenv.go` — an `RWMutex`-guarded `globAppName` /
//! `globAppEnv` pair, both empty by default, set once at boot by
//! `pc-config`'s `initialize_app` (the `init.go` `InitializeApp` analog).
//!
//! Go stores `APP_ENV` verbatim as a string (it may hold a non-canonical value
//! like `"qa"` that boot validation later warns about), so the raw string is
//! the source of truth. `app_env()` layers a typed parse on top.

use std::sync::RwLock;

use crate::AppEnv;

static APP_NAME: RwLock<String> = RwLock::new(String::new());
static APP_ENV: RwLock<String> = RwLock::new(String::new());

/// mirrors: `phhelper.GetAppName`.
#[must_use]
pub fn app_name() -> String {
    APP_NAME
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

/// mirrors: `phhelper.SetAppName`.
pub fn set_app_name(v: &str) {
    let mut guard = APP_NAME
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    v.clone_into(&mut guard);
}

/// The raw `APP_ENV` string as stored (may be empty or non-canonical).
/// mirrors: `phhelper.GetAppEnv`.
#[must_use]
pub fn app_env_raw() -> String {
    APP_ENV
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

/// Set the raw `APP_ENV` string verbatim. mirrors: `phhelper.SetAppEnv`.
/// `pc-config` uses this for the `APP_MODE` legacy-fallback path where the
/// value hasn't been validated against the canonical set.
pub fn set_app_env_raw(v: &str) {
    let mut guard = APP_ENV
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    v.clone_into(&mut guard);
}

/// The typed environment, or `None` if unset / non-canonical.
#[must_use]
pub fn app_env() -> Option<AppEnv> {
    AppEnv::parse(&app_env_raw())
}

/// Set the canonical environment (stores its canonical string form).
pub fn set_app_env(v: AppEnv) {
    set_app_env_raw(v.as_str());
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: identity is process-global; this single test drives the whole
    // lifecycle to avoid cross-test ordering flakiness.
    #[test]
    fn identity_roundtrip() {
        set_app_name("paycloud-be-qoinhubinterface-manager");
        assert_eq!(app_name(), "paycloud-be-qoinhubinterface-manager");

        set_app_env_raw("qa"); // non-canonical, stored verbatim like Go
        assert_eq!(app_env_raw(), "qa");
        assert_eq!(app_env(), None);

        set_app_env(AppEnv::Production);
        assert_eq!(app_env_raw(), "production");
        assert_eq!(app_env(), Some(AppEnv::Production));
    }
}
