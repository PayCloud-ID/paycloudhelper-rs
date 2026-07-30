//! Request-scoped child logger carrying a `[k=v ...]` prefix.
//!
//! mirrors: Go `phlogger/context.go` (`LogContext`, `NewLogContext`, `With`).

/// A child logger that prepends key-value context fields to every log message.
///
/// mirrors: `phlogger.LogContext`. The prefix is built once at creation time
/// and is immutable; [`prefix`](LogContext::prefix) reads it back.
///
/// The prefix shape is `"[k=v k2=v2] "` — space-separated `key=value` pairs in
/// insertion order, wrapped in brackets with a trailing space. An empty field
/// set yields an empty prefix.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct LogContext {
    prefix: String,
}

/// Builds `"[k=v k2=v2] "` from ordered pairs, or `""` when empty.
///
/// mirrors: the `strings.Builder` loop in `phlogger.NewLogContext`.
fn build_prefix(fields: &[(&str, &str)]) -> String {
    if fields.is_empty() {
        return String::new();
    }
    let mut out = String::from("[");
    for (i, (k, v)) in fields.iter().enumerate() {
        if i > 0 {
            out.push(' ');
        }
        out.push_str(k);
        out.push('=');
        out.push_str(v);
    }
    out.push_str("] ");
    out
}

impl LogContext {
    /// Creates a child logger with key-value context fields.
    ///
    /// mirrors: `phlogger.NewLogContext` (the Rust API takes explicit
    /// `(key, value)` tuples rather than Go's flat variadic list, so there is
    /// no odd-trailing-key case to drop).
    #[must_use]
    pub fn new(fields: &[(&str, &str)]) -> Self {
        Self {
            prefix: build_prefix(fields),
        }
    }

    /// Returns a new `LogContext` merging this context's fields with additional
    /// pairs, appended after the existing ones.
    ///
    /// mirrors: `(*LogContext).With`.
    #[must_use]
    pub fn with(&self, fields: &[(&str, &str)]) -> Self {
        if fields.is_empty() {
            return self.clone();
        }
        let extra = build_prefix(fields);
        if self.prefix.is_empty() {
            return Self { prefix: extra };
        }
        // Merge: "[parent] " + "[extra] " → "[parent extra] ".
        // Drop the parent's trailing "] " and the extra's leading "[".
        let merged = format!("{} {}", &self.prefix[..self.prefix.len() - 2], &extra[1..]);
        Self { prefix: merged }
    }

    /// Reads the prefix string that is prepended to every message.
    ///
    /// mirrors: the private `LogContext.prefix` field (exposed here so the
    /// emit macros can consume it).
    #[must_use]
    pub fn prefix(&self) -> &str {
        &self.prefix
    }
}
