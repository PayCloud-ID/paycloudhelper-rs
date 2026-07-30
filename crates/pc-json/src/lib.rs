#![forbid(unsafe_code)]
//! JSON serialization profiles. mirrors: `phjson/phjson.go` (the sonic wrapper)
//! and `json_codec.go` (`defaultAuditJSONMarshalNoEsc`).
//!
//! The parity-critical profile is **audit-trail** (02 §5 audit row): Go uses an
//! `encoding/json` `Encoder` with `SetEscapeHTML(false)`, whose `Encode` appends
//! a **trailing newline**. `serde_json` already does not HTML-escape `< > &`
//! (that matches `EscapeHTML(false)`); the only extra step is the trailing `\n`.

use serde::Serialize;

/// Default JSON marshal. mirrors: `phjson.Marshal` / `ToJson` (no trailing newline).
pub fn marshal<T: Serialize>(v: &T) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(v)
}

/// Audit-trail JSON marshal: no HTML escaping, **trailing newline**.
/// mirrors: `defaultAuditJSONMarshalNoEsc` / `phhelper.JsonMarshalNoEsc`
/// (`json.NewEncoder` + `SetEscapeHTML(false)` + `Encode`'s trailing `\n`).
///
/// Used for audit-trail message bodies (V1 `pushMessageAudit`, V2
/// `AuditPublisher` workers). Downstream consumers parse JSON tolerantly, so the
/// trailing newline is safe and byte-matches the Go default.
pub fn marshal_audit<T: Serialize>(v: &T) -> Result<Vec<u8>, serde_json::Error> {
    let mut buf = serde_json::to_vec(v)?;
    buf.push(b'\n');
    Ok(buf)
}

/// Pretty JSON (2-space indent). mirrors: `phhelper.JSONEncode` / `ToJsonIndent`.
pub fn marshal_indent<T: Serialize>(v: &T) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec_pretty(v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[derive(Serialize)]
    struct Payload {
        html: String,
        n: i64,
    }

    #[test]
    fn audit_marshal_no_html_escape_and_trailing_newline() {
        let p = Payload {
            html: "<b>&</b>".to_string(),
            n: 7,
        };
        let out = marshal_audit(&p).unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "{\"html\":\"<b>&</b>\",\"n\":7}\n"
        );
    }

    #[test]
    fn default_marshal_has_no_trailing_newline() {
        let p = Payload {
            html: "x".to_string(),
            n: 1,
        };
        let out = String::from_utf8(marshal(&p).unwrap()).unwrap();
        assert!(!out.ends_with('\n'));
        assert_eq!(out, "{\"html\":\"x\",\"n\":1}");
    }
}
