#![forbid(unsafe_code)]
//! Custom validation rules. mirrors: `validator.go` (`char_libs`,
//! `numeric_null_libs` govalidator custom rules).
//!
//! Go registers these as govalidator rules; here they are plain predicates a
//! service wires into whatever validation layer it uses (`pc-http` request
//! guards, `validator` derive, or manual checks). The parity-critical bit is
//! the **regex and the blank-passes semantics**, not the registration
//! mechanism (02 §5 validation row).

use std::sync::LazyLock;

use regex::Regex;

/// mirrors: `Numeric = "^-?[0-9]+$"`.
pub const NUMERIC: &str = "^-?[0-9]+$";
/// mirrors: `Key = "^[-a-zA-Z0-9_-]+$"`.
pub const KEY: &str = "^[-a-zA-Z0-9_-]+$";

static RE_NUMERIC: LazyLock<Regex> = LazyLock::new(|| Regex::new(NUMERIC).unwrap());
static RE_KEY: LazyLock<Regex> = LazyLock::new(|| Regex::new(KEY).unwrap());

/// `numeric_null_libs`: blank passes; otherwise must match `^-?[0-9]+$`.
/// mirrors: `AddValidatorLibs` numeric_null_libs rule (blank → nil/ok).
#[must_use]
pub fn numeric_null_libs(value: &str) -> bool {
    value.is_empty() || RE_NUMERIC.is_match(value)
}

/// `char_libs`: blank passes; otherwise must match `^[-a-zA-Z0-9_-]+$`.
/// mirrors: `AddValidatorLibs` char_libs rule (blank → nil/ok).
#[must_use]
pub fn char_libs(value: &str) -> bool {
    value.is_empty() || RE_KEY.is_match(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_null_libs_rules() {
        assert!(numeric_null_libs("")); // blank passes
        assert!(numeric_null_libs("123"));
        assert!(numeric_null_libs("-123"));
        assert!(!numeric_null_libs("12.3"));
        assert!(!numeric_null_libs("12a"));
        assert!(!numeric_null_libs("+1")); // only leading '-' allowed
    }

    #[test]
    fn char_libs_rules() {
        assert!(char_libs("")); // blank passes
        assert!(char_libs("abc-DEF_123"));
        assert!(char_libs("---"));
        assert!(!char_libs("has space"));
        assert!(!char_libs("dot.")); // '.' not in the character class
        assert!(!char_libs("slash/"));
    }
}
