//! The byte-exact twin of Go `encoding/json.Compact` — **the signature-hash
//! input**. mirrors: `helpers.go` / `phhelper.JsonMinify`.
//!
//! This is the single highest-risk item in the whole library (01 §3.1): the
//! SNAP-BI body hash is `SHA256` over the minified body, so the Rust output
//! must match Go byte-for-byte or *every signature fails*.
//!
//! ## Why not round-trip through `serde_json::Value`
//!
//! `serde_json::Value` backs objects with a `BTreeMap` (without the
//! `preserve_order` feature), which would **reorder object keys** and re-escape
//! strings — diverging from Go, which copies the input verbatim and only elides
//! insignificant whitespace. So this streams a compactor over the raw bytes,
//! exactly like `encoding/json.Compact`:
//!
//! - whitespace *outside* strings (`0x20 0x09 0x0A 0x0D`) is dropped;
//! - everything else — key order, number formatting, unicode escapes, raw UTF-8,
//!   `<` `>` `&` (Go's `Compact`, unlike `HTMLEscape`, does **not** escape these),
//!   duplicate keys, whitespace *inside* strings — is copied byte-for-byte.
//!
//! A validity pass (`serde::de::IgnoredAny`) rejects malformed input the way
//! Go's scanner returns an error, without materializing a `Value`.

use serde::de::IgnoredAny;

/// Error compacting JSON. mirrors: the `"failure encountered compacting json"`
/// error string in Go `JsonMinify`.
#[derive(thiserror::Error, Debug)]
pub enum JsonMinifyError {
    #[error("failure encountered compacting json := {0}")]
    Compact(String),
}

/// Byte-exact twin of Go `json.Compact`. See module docs.
///
/// Returns the minified bytes, or an error if `raw` is not a single well-formed
/// JSON value.
pub fn json_minify(raw: &[u8]) -> Result<Vec<u8>, JsonMinifyError> {
    // Well-formedness gate: one JSON value, trailing whitespace tolerated, no
    // trailing garbage — matching Go's single-value scanner. IgnoredAny does not
    // allocate a Value, so object key order is never touched.
    serde_json::from_slice::<IgnoredAny>(raw)
        .map_err(|e| JsonMinifyError::Compact(e.to_string()))?;
    Ok(compact_bytes(raw))
}

/// The verbatim-copy / whitespace-elide core. Assumes `raw` is well-formed JSON
/// (callers gate with the validity pass above).
fn compact_bytes(raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(raw.len());
    let mut in_string = false;
    let mut escaped = false;
    for &b in raw {
        if in_string {
            out.push(b);
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_string = false;
            }
            continue;
        }
        match b {
            // insignificant whitespace outside strings — dropped
            b' ' | b'\t' | b'\n' | b'\r' => {}
            b'"' => {
                in_string = true;
                out.push(b);
            }
            _ => out.push(b),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minify(s: &str) -> String {
        String::from_utf8(json_minify(s.as_bytes()).expect("valid json")).unwrap()
    }

    #[test]
    fn elides_insignificant_whitespace() {
        assert_eq!(
            minify("{ \"a\" : 1 , \"b\" : [ 1 , 2 ] }"),
            r#"{"a":1,"b":[1,2]}"#
        );
        assert_eq!(minify("[\n  1,\n\t2\r\n]"), "[1,2]");
    }

    #[test]
    fn preserves_key_order_not_sorted() {
        // The distinguishing test vs a Value round-trip: keys stay as-authored.
        assert_eq!(minify(r#"{"b":1,"a":2,"c":3}"#), r#"{"b":1,"a":2,"c":3}"#);
    }

    #[test]
    fn preserves_number_formatting_verbatim() {
        // Go json.Compact does NOT normalize numbers (1.50 stays 1.50).
        assert_eq!(
            minify(r#"{ "n": 1.50, "e": 1E10, "z": -0.0 }"#),
            r#"{"n":1.50,"e":1E10,"z":-0.0}"#
        );
    }

    #[test]
    fn does_not_escape_html_chars() {
        // Compact (unlike HTMLEscape) leaves < > & alone.
        assert_eq!(minify(r#"{"x":"<b>&</b>"}"#), r#"{"x":"<b>&</b>"}"#);
    }

    #[test]
    fn preserves_unicode_escapes_and_raw_utf8() {
        assert_eq!(
            minify(r#"{ "x": "\u00e9\u2028" }"#),
            r#"{"x":"\u00e9\u2028"}"#
        );
        assert_eq!(
            minify(r#"{ "x": "héllo → 世界" }"#),
            r#"{"x":"héllo → 世界"}"#
        );
    }

    #[test]
    fn preserves_whitespace_inside_strings() {
        assert_eq!(minify(r#"{ "x": "a  b\tc" }"#), r#"{"x":"a  b\tc"}"#);
    }

    #[test]
    fn escaped_quote_does_not_terminate_string() {
        assert_eq!(
            minify(r#"{ "x": "a\"b", "y": 1 }"#),
            r#"{"x":"a\"b","y":1}"#
        );
        // trailing backslash-escape then close
        assert_eq!(minify(r#"{"p":"c:\\tmp"}"#), r#"{"p":"c:\\tmp"}"#);
    }

    #[test]
    fn preserves_duplicate_keys() {
        // Neither Go json.Compact nor this compactor dedupes.
        assert_eq!(minify(r#"{ "a": 1, "a": 2 }"#), r#"{"a":1,"a":2}"#);
    }

    #[test]
    fn scalars_and_empties() {
        assert_eq!(minify("  true "), "true");
        assert_eq!(minify(" \"hi\" "), "\"hi\"");
        assert_eq!(minify("{ }"), "{}");
        assert_eq!(minify("[ ]"), "[]");
        assert_eq!(minify(" 42 "), "42");
    }

    #[test]
    fn rejects_malformed() {
        assert!(json_minify(b"{ bad ").is_err());
        assert!(json_minify(b"").is_err());
        assert!(json_minify(b"{} trailing").is_err());
    }
}
