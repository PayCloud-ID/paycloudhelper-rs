#![forbid(unsafe_code)]
//! Axum authentication and replay-protection middleware.
//!
//! Mirrors `RevokeToken`, `VerifCsrf`, and `VerifIdemKey` from the Go helper.

use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::extract::{Request, State};
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{HeaderName, HeaderValue};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use md5::{Digest, Md5};
use pc_http::ResponseApi;
use serde::Deserialize;

const REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");
const CSRF_HEADER: HeaderName = HeaderName::from_static("x-xsrf-token");
const IDEM_HEADER: HeaderName = HeaderName::from_static("idempotency-key");
const SESSION_HEADER: HeaderName = HeaderName::from_static("session");

/// Revocation statuses frozen by the parity contract.
pub const REVOKED_STATUSES: [i32; 3] = [3, 4, 7];

/// Shared state for the middleware set.
#[derive(Clone)]
pub struct AuthState {
    pub redis: pc_redis::RedisPool,
    pub public_key_pem: Arc<str>,
}

impl AuthState {
    #[must_use]
    pub fn new(redis: pc_redis::RedisPool, public_key_pem: impl Into<Arc<str>>) -> Self {
        Self {
            redis,
            public_key_pem: public_key_pem.into(),
        }
    }
}

#[derive(Deserialize)]
struct RevokeToken {
    status: i32,
}

/// MD5 hex of Go-compatible minified JSON.
pub fn idempotency_key(body: &[u8]) -> Result<String, pc_core::json::JsonMinifyError> {
    let minified = pc_core::json_minify(body)?;
    Ok(hex::encode(Md5::digest(minified)))
}

/// Compare a submitted idempotency key with the body-derived MD5.
pub fn verify_idempotency_key(
    submitted: &str,
    body: &[u8],
) -> Result<bool, pc_core::json::JsonMinifyError> {
    Ok(submitted == idempotency_key(body)?)
}

/// Validate the `X-Xsrf-Token` against Redis key `csrf-<token>`.
pub async fn verify_csrf(
    State(state): State<AuthState>,
    mut request: Request,
    next: Next,
) -> Response {
    let request_id = ensure_request_id(&mut request);
    let token = header(&request, &CSRF_HEADER);
    if token.is_empty() || token.len() > 50 || !pc_validate::char_libs(&token) {
        return with_request_id(
            pc_http::bad_request("invalid validation", "").into_response(),
            &request_id,
        );
    }

    match state.redis.get(&pc_redis::csrf_key(&token)).await {
        Ok(Some(_)) => with_request_id(next.run(request).await, &request_id),
        Ok(None) => with_request_id(
            pc_http::unauthorized("token invalid").into_response(),
            &request_id,
        ),
        Err(err) => {
            tracing::error!(error = %err, "[VerifCsrf] redis error");
            with_request_id(
                pc_http::internal_error(&err.to_string()).into_response(),
                &request_id,
            )
        }
    }
}

/// Verify an RS256 bearer token, its custom `Expired` claim, and Redis revoke
/// status `3`, `4`, or `7`.
pub async fn revoke_token(
    State(state): State<AuthState>,
    mut request: Request,
    next: Next,
) -> Response {
    let request_id = ensure_request_id(&mut request);
    let auth = header(&request, &AUTHORIZATION);
    let Some(token) = auth.strip_prefix("Bearer ") else {
        let message = if auth.split_whitespace().count() == 2 {
            "authorization token type does not match"
        } else {
            "invalid authorization token"
        };
        return with_request_id(pc_http::unauthorized(message).into_response(), &request_id);
    };

    let Ok(claims) = pc_snapbi::verify_jwt_rs256(&state.public_key_pem, token) else {
        return with_request_id(
            pc_http::unauthorized("authorization token credentials do not match").into_response(),
            &request_id,
        );
    };

    let Some(expired) = parse_go_datetime(&claims.expired) else {
        return with_request_id(
            pc_http::unauthorized("invalid authorization token credentials").into_response(),
            &request_id,
        );
    };
    if time::OffsetDateTime::now_utc() > expired {
        return with_request_id(
            pc_http::unauthorized("authorization token has expired").into_response(),
            &request_id,
        );
    }

    let Some(merchant_id) = merchant_id(&claims.extra) else {
        return with_request_id(
            pc_http::unauthorized("invalid authorization token merchant").into_response(),
            &request_id,
        );
    };

    match state
        .redis
        .get(&pc_redis::revoke_token_key(merchant_id))
        .await
    {
        Ok(None) => with_request_id(next.run(request).await, &request_id),
        Ok(Some(raw)) => match serde_json::from_str::<RevokeToken>(&raw) {
            Ok(revoke) if REVOKED_STATUSES.contains(&revoke.status) => {
                let response = ResponseApi::<()> {
                    code: 401,
                    status: "unauthorized".to_string(),
                    message: "revoke jwt token".to_string(),
                    internal_code: revoke.status.to_string(),
                    data: None,
                };
                with_request_id(response.into_response(), &request_id)
            }
            Ok(_) => with_request_id(next.run(request).await, &request_id),
            Err(err) => with_request_id(
                pc_http::internal_error(&err.to_string()).into_response(),
                &request_id,
            ),
        },
        Err(err) => with_request_id(
            pc_http::internal_error(&err.to_string()).into_response(),
            &request_id,
        ),
    }
}

/// Validate `Idempotency-Key` as the MD5 of the minified JSON request body,
/// then cache the body for the session TTL. A duplicate returns 202 with the
/// cached body.
#[allow(clippy::too_many_lines)] // linear flow mirrors the Go acceptance path.
pub async fn verify_idempotency(
    State(state): State<AuthState>,
    mut request: Request,
    next: Next,
) -> Response {
    let request_id = ensure_request_id(&mut request);
    let key = header(&request, &IDEM_HEADER);
    let session_raw = header(&request, &SESSION_HEADER);
    if key.is_empty() || key.len() > 50 || !pc_validate::char_libs(&key) {
        return with_request_id(
            pc_http::bad_request("invalid idempotency key format", "IDEM_INVALID_FORMAT")
                .into_response(),
            &request_id,
        );
    }
    if !session_raw.is_empty()
        && (session_raw.len() > 60 || !pc_validate::numeric_null_libs(&session_raw))
    {
        return with_request_id(
            pc_http::bad_request("invalid session header format", "IDEM_INVALID_SESSION")
                .into_response(),
            &request_id,
        );
    }
    let mut session = if session_raw.is_empty() || session_raw == "0" {
        9_u64
    } else {
        match session_raw.parse::<i64>() {
            Ok(value) if value >= 4 => u64::try_from(value).unwrap_or(9),
            Ok(_) => 9,
            Err(_) => {
                return with_request_id(
                    pc_http::bad_request("invalid session header format", "IDEM_INVALID_SESSION")
                        .into_response(),
                    &request_id,
                );
            }
        }
    };
    if session < 4 {
        session = 9;
    }

    let content_type = header(&request, &CONTENT_TYPE);
    if !content_type.starts_with("application/json") {
        return with_request_id(
            pc_http::bad_request("idempotency requires application/json", "IDEM_INVALID_BODY")
                .into_response(),
            &request_id,
        );
    }

    let (parts, body) = request.into_parts();
    let bytes = match to_bytes(body, 2 * 1024 * 1024).await {
        Ok(bytes) => bytes,
        Err(err) => {
            return with_request_id(
                pc_http::internal_error(&err.to_string()).into_response(),
                &request_id,
            );
        }
    };
    let value: serde_json::Value = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(err) => {
            return with_request_id(
                pc_http::bad_request(&err.to_string(), "IDEM_INVALID_BODY").into_response(),
                &request_id,
            );
        }
    };
    match verify_idempotency_key(&key, &bytes) {
        Ok(true) => {}
        Ok(false) => {
            return with_request_id(
                pc_http::bad_request(
                    "idempotency key does not match request body",
                    "IDEM_KEY_MISMATCH",
                )
                .into_response(),
                &request_id,
            );
        }
        Err(err) => {
            return with_request_id(
                pc_http::bad_request(&err.to_string(), "IDEM_INVALID_BODY").into_response(),
                &request_id,
            );
        }
    }

    match state.redis.get(&key).await {
        Ok(Some(cached)) => {
            let cached = serde_json::from_str::<serde_json::Value>(&cached)
                .unwrap_or(serde_json::Value::String(cached));
            let response = ResponseApi {
                code: 202,
                status: "accepted".to_string(),
                message: "your request in process".to_string(),
                internal_code: String::new(),
                data: Some(cached),
            };
            with_request_id(response.into_response(), &request_id)
        }
        Ok(None) => {
            if let Err(err) = state
                .redis
                .store(&key, &value, Duration::from_secs(session))
                .await
            {
                return with_request_id(
                    pc_http::internal_error(&err.to_string()).into_response(),
                    &request_id,
                );
            }
            let request = Request::from_parts(parts, Body::from(bytes));
            with_request_id(next.run(request).await, &request_id)
        }
        Err(err) => with_request_id(
            pc_http::internal_error(&err.to_string()).into_response(),
            &request_id,
        ),
    }
}

fn header(request: &Request, name: &HeaderName) -> String {
    request
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

fn ensure_request_id(request: &mut Request) -> String {
    let id = pc_http::get_or_generate_request_id(
        request
            .headers()
            .get(&REQUEST_ID)
            .and_then(|value| value.to_str().ok()),
    );
    if let Ok(value) = HeaderValue::from_str(&id) {
        request.headers_mut().insert(REQUEST_ID.clone(), value);
    }
    id
}

fn with_request_id(mut response: Response, request_id: &str) -> Response {
    if let Ok(value) = HeaderValue::from_str(request_id) {
        response.headers_mut().insert(REQUEST_ID.clone(), value);
    }
    response
}

fn parse_go_datetime(value: &str) -> Option<time::OffsetDateTime> {
    let format = time::macros::format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");
    time::PrimitiveDateTime::parse(value, &format)
        .ok()
        .map(time::PrimitiveDateTime::assume_utc)
}

fn merchant_id(extra: &std::collections::HashMap<String, serde_json::Value>) -> Option<i64> {
    let value = extra.get("MerchantId")?;
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|v| i64::try_from(v).ok()))
        .or_else(|| value.as_f64().and_then(f64_to_i64))
        .or_else(|| value.as_str().and_then(|v| v.parse().ok()))
}

#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
fn f64_to_i64(value: f64) -> Option<i64> {
    (value.is_finite()
        && value.fract() == 0.0
        && value >= i64::MIN as f64
        && value <= i64::MAX as f64)
        .then_some(value as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn md5_uses_minified_body() {
        let pretty = br#"{ "amount": 1000, "currency": "IDR" }"#;
        let compact = br#"{"amount":1000,"currency":"IDR"}"#;
        assert_eq!(
            idempotency_key(pretty).unwrap(),
            idempotency_key(compact).unwrap()
        );
        assert!(verify_idempotency_key(&idempotency_key(pretty).unwrap(), compact).unwrap());
    }

    #[test]
    fn revoked_statuses_are_frozen() {
        assert_eq!(REVOKED_STATUSES, [3, 4, 7]);
    }

    #[test]
    fn parses_custom_expired_claim_as_utc() {
        let dt = parse_go_datetime("2026-07-30 12:34:56").unwrap();
        assert_eq!(dt.year(), 2026);
        assert_eq!(dt.hour(), 12);
        assert!(parse_go_datetime("2026-07-30T12:34:56Z").is_none());
    }

    #[test]
    fn merchant_claim_accepts_number_and_string() {
        let mut claims = std::collections::HashMap::new();
        claims.insert("MerchantId".to_string(), serde_json::json!(42.0));
        assert_eq!(merchant_id(&claims), Some(42));
        claims.insert("MerchantId".to_string(), serde_json::json!("43"));
        assert_eq!(merchant_id(&claims), Some(43));
    }
}
