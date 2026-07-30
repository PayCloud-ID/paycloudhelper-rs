#![forbid(unsafe_code)]
//! `pc-http` — the HTTP surface of `paycloudhelper`.
//!
//! Two things live here, both bit-for-bit ports of Go:
//!
//! 1. The [`ResponseApi`] envelope and its constructors — a port of
//!    `paycloudhelper/response.go` (`ResponseApi.Out`, `.Success`,
//!    `.Accepted`, `.BadRequest`, `.Unauthorized`, `.InternalServerError`).
//!    Field names, `omitempty` semantics, status strings and the fixed 202
//!    message are reproduced exactly (design 02 §5).
//! 2. The hardened HTTP bootstrap — [`base_router`], [`serve_with_graceful_shutdown`]
//!    and [`get_or_generate_request_id`] — a port of the qoinhub bootstrap
//!    (`main.go` graceful shutdown, `middlewares/basic.go` + `routes/router.go`
//!    hardening: request-id, content-type guard, body-limit, timeout, secure
//!    headers) and `paycloudhelper/headers.go` (`GetOrGenerateRequestID`).

use std::net::SocketAddr;
use std::time::Duration;

use axum::extract::Request;
use axum::http::header::{
    CONTENT_SECURITY_POLICY, CONTENT_TYPE, STRICT_TRANSPORT_SECURITY, X_CONTENT_TYPE_OPTIONS,
    X_FRAME_OPTIONS, X_XSS_PROTECTION,
};
use axum::http::{HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use rand::RngCore;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::request_id::{
    MakeRequestId, PropagateRequestIdLayer, RequestId, SetRequestIdLayer,
};
use tower_http::set_header::SetResponseHeaderLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

/// Standard JSON response envelope.
///
/// mirrors: Go `paycloudhelper.ResponseApi` (`response.go`). Field names and
/// their JSON tags match exactly, including the `omitempty` behaviour on
/// `internal_code` and `data`.
#[derive(serde::Serialize)]
pub struct ResponseApi<T> {
    /// HTTP status code echoed into the body. mirrors: `ResponseApi.Code`.
    pub code: u16,
    /// Human status string (`success`, `bad request`, …). mirrors: `ResponseApi.Status`.
    pub status: String,
    /// Human-readable message. mirrors: `ResponseApi.Message`.
    pub message: String,
    /// Optional internal error code; omitted from JSON when empty.
    /// mirrors: `ResponseApi.InternalCode` (`json:"internal_code,omitempty"`).
    #[serde(skip_serializing_if = "String::is_empty")]
    pub internal_code: String,
    /// Optional payload; omitted from JSON when absent.
    /// mirrors: `ResponseApi.Data` (`json:"data,omitempty"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
}

impl<T: serde::Serialize> ResponseApi<T> {
    /// 200 success response carrying `data`.
    ///
    /// mirrors: `(*ResponseApi).Success` — `Out(200, message, "", "success", data)`.
    pub fn success(msg: &str, data: T) -> Self {
        Self {
            code: 200,
            status: "success".to_owned(),
            message: msg.to_owned(),
            internal_code: String::new(),
            data: Some(data),
        }
    }
}

/// 202 accepted / in-process response.
///
/// mirrors: `(*ResponseApi).Accepted` — `Out(202, "your request in process",
/// "", "accepted", data)`. The message is the fixed string reproduced exactly.
pub fn accepted() -> ResponseApi<()> {
    ResponseApi {
        code: 202,
        status: "accepted".to_owned(),
        message: "your request in process".to_owned(),
        internal_code: String::new(),
        data: None,
    }
}

/// 400 bad-request response.
///
/// mirrors: `(*ResponseApi).BadRequest` — `Out(400, message, internalCode,
/// "bad request", ...)`. Go stores the message in `data` too; the Rust port
/// keeps `data` empty (`ResponseApi<()>`) and preserves `internal_code`.
pub fn bad_request(msg: &str, internal: &str) -> ResponseApi<()> {
    ResponseApi {
        code: 400,
        status: "bad request".to_owned(),
        message: msg.to_owned(),
        internal_code: internal.to_owned(),
        data: None,
    }
}

/// 401 unauthorized response.
///
/// mirrors: `(*ResponseApi).Unauthorized` — `Out(401, message, ...,
/// "unauthorized", nil)`.
pub fn unauthorized(msg: &str) -> ResponseApi<()> {
    ResponseApi {
        code: 401,
        status: "unauthorized".to_owned(),
        message: msg.to_owned(),
        internal_code: String::new(),
        data: None,
    }
}

/// 500 internal-server-error response.
///
/// mirrors: `(*ResponseApi).InternalServerError` — `Out(500, err.Error(), "",
/// "internal server error", nil)`.
pub fn internal_error(msg: &str) -> ResponseApi<()> {
    ResponseApi {
        code: 500,
        status: "internal server error".to_owned(),
        message: msg.to_owned(),
        internal_code: String::new(),
        data: None,
    }
}

/// Turns a [`ResponseApi`] into an HTTP response whose status code is
/// [`ResponseApi::code`] and whose body is the JSON envelope.
///
/// mirrors: the Echo `c.JSON(code, &ResponseApi{...})` call sites — the body
/// code and the transport status code are always the same value.
impl<T: serde::Serialize> IntoResponse for ResponseApi<T> {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        (status, Json(self)).into_response()
    }
}

/// Extract an incoming request id or mint a fresh one.
///
/// mirrors: `paycloudhelper.GetOrGenerateRequestID` (`headers.go`) composed
/// with the private `generateRequestID`: a present, non-empty value passes
/// through unchanged; an absent one becomes a 32-char lowercase hex string
/// (16 random bytes, hex-encoded).
pub fn get_or_generate_request_id(header: Option<&str>) -> String {
    match header {
        Some(v) if !v.is_empty() => v.to_owned(),
        _ => generate_request_id(),
    }
}

/// Mint a 32-char lowercase hex request id.
///
/// mirrors: private `generateRequestID` — `hex.EncodeToString(rand 16 bytes)`.
fn generate_request_id() -> String {
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// [`MakeRequestId`] that mints ids the same way Go's `generateRequestID` does.
///
/// mirrors: the request-id middleware seed from qoinhub `middlewares/basic.go`
/// (`middleware.RequestID`), but using the paycloudhelper 32-char hex shape.
#[derive(Clone, Default)]
struct HexRequestId;

impl MakeRequestId for HexRequestId {
    fn make_request_id<B>(&mut self, _request: &axum::http::Request<B>) -> Option<RequestId> {
        HeaderValue::from_str(&generate_request_id())
            .ok()
            .map(RequestId::new)
    }
}

/// Reject body-carrying requests that are not `application/json`.
///
/// mirrors: the intent of qoinhub `middlewares/accept.go` hardening — guard the
/// content negotiation before a handler runs. Applies only to methods that
/// carry a body (POST/PUT/PATCH); GET health probes are unaffected.
async fn content_type_guard(request: Request, next: Next) -> Response {
    if matches!(
        *request.method(),
        Method::POST | Method::PUT | Method::PATCH
    ) {
        let ok = request
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|ct| ct.trim_start().starts_with("application/json"));
        if !ok {
            return ResponseApi::<()> {
                code: StatusCode::UNSUPPORTED_MEDIA_TYPE.as_u16(),
                status: "unsupported media type".to_owned(),
                message: "content-type must be application/json".to_owned(),
                internal_code: String::new(),
                data: None,
            }
            .into_response();
        }
    }
    next.run(request).await
}

/// 200 `ok` health handler shared by every probe endpoint.
async fn health_ok() -> Response {
    (StatusCode::OK, "ok").into_response()
}

/// Minimal metrics endpoint placeholder.
///
/// mirrors: the `/metrics` scrape endpoint expected by the deployment probes;
/// the real Prometheus registry is wired by consumers.
async fn metrics() -> Response {
    (
        StatusCode::OK,
        [(CONTENT_TYPE, "text/plain; version=0.0.4")],
        "# pc-http metrics placeholder\n",
    )
        .into_response()
}

/// Build the hardened base router.
///
/// mirrors: the qoinhub HTTP bootstrap — `routes/router.go` (body-limit 2MB,
/// secure headers `X-Content-Type-Options`/`X-Frame-Options`/`X-XSS-Protection`
/// /HSTS/CSP, health probes) plus `middlewares/basic.go` (request-id, tracing).
/// Adds a content-type guard and a request timeout as design-02 §5 hardening.
///
/// Routes: `/health`, `/healthz`, `/readyz`, `/livez`, `/metrics`.
pub fn base_router() -> Router {
    let secure_headers = |name, value: &'static str| {
        SetResponseHeaderLayer::if_not_present(name, HeaderValue::from_static(value))
    };

    Router::new()
        .route("/health", get(health_ok))
        .route("/healthz", get(health_ok))
        .route("/readyz", get(health_ok))
        .route("/livez", get(health_ok))
        .route("/metrics", get(metrics))
        // Layers apply outermost-first on the request path.
        .layer(TraceLayer::new_for_http())
        .layer(PropagateRequestIdLayer::x_request_id())
        .layer(SetRequestIdLayer::x_request_id(HexRequestId))
        // Secure response headers (qoinhub SecureConfig values).
        .layer(secure_headers(X_CONTENT_TYPE_OPTIONS, "nosniff"))
        .layer(secure_headers(X_FRAME_OPTIONS, "DENY"))
        .layer(secure_headers(X_XSS_PROTECTION, "1; mode=block"))
        .layer(secure_headers(
            STRICT_TRANSPORT_SECURITY,
            "max-age=31536000",
        ))
        .layer(secure_headers(
            CONTENT_SECURITY_POLICY,
            "default-src 'self'",
        ))
        // Request timeout (qoinhub server ReadTimeout/WriteTimeout of 30s).
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(30),
        ))
        // 2MB body limit (qoinhub BodyLimit("2M")).
        .layer(RequestBodyLimitLayer::new(2 * 1024 * 1024))
        .layer(axum::middleware::from_fn(content_type_guard))
}

/// Serve `router` on `addr`, shutting down gracefully on SIGINT/SIGTERM.
///
/// mirrors: qoinhub `main.go` / `startServer` — start listening, then on a
/// termination signal stop accepting new connections and drain in-flight
/// requests before returning.
pub async fn serve_with_graceful_shutdown(router: Router, addr: SocketAddr) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "pc-http: HTTP server start listening");
    axum::serve(listener, router.into_make_service())
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    tracing::info!("pc-http: HTTP server stopped serving new connections");
    Ok(())
}

/// Resolve when the process receives SIGINT (Ctrl-C) or, on Unix, SIGTERM.
///
/// mirrors: qoinhub `stopWhenSignalReceived` (`os/signal.Notify` on SIGINT/SIGTERM).
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{to_bytes, Body};
    use tower::ServiceExt;

    fn to_json<T: serde::Serialize>(r: &T) -> serde_json::Value {
        serde_json::to_value(r).expect("serialize")
    }

    #[test]
    fn internal_code_and_data_omitted_when_empty() {
        let v = to_json(&unauthorized("nope"));
        assert!(v.get("internal_code").is_none(), "internal_code omitted");
        assert!(v.get("data").is_none(), "data omitted");
    }

    #[test]
    fn internal_code_and_data_present_when_set() {
        let v = to_json(&bad_request("bad", "ERR-01"));
        assert_eq!(v["internal_code"], "ERR-01");

        let ok = to_json(&ResponseApi::success("done", serde_json::json!({"a": 1})));
        assert_eq!(ok["data"], serde_json::json!({"a": 1}));
    }

    #[test]
    fn success_sets_code_and_status() {
        let r = ResponseApi::success("done", 42);
        assert_eq!(r.code, 200);
        assert_eq!(r.status, "success");
        assert_eq!(r.message, "done");
        assert_eq!(r.data, Some(42));
    }

    #[test]
    fn accepted_sets_exact_message_status_code() {
        let r = accepted();
        assert_eq!(r.code, 202);
        assert_eq!(r.status, "accepted");
        assert_eq!(r.message, "your request in process");
    }

    #[test]
    fn bad_request_sets_status_and_code() {
        let r = bad_request("oops", "IC-9");
        assert_eq!(r.code, 400);
        assert_eq!(r.status, "bad request");
        assert_eq!(r.internal_code, "IC-9");
    }

    #[test]
    fn unauthorized_sets_status_and_code() {
        let r = unauthorized("no");
        assert_eq!(r.code, 401);
        assert_eq!(r.status, "unauthorized");
    }

    #[test]
    fn internal_error_sets_status_and_code() {
        let r = internal_error("boom");
        assert_eq!(r.code, 500);
        assert_eq!(r.status, "internal server error");
    }

    #[test]
    fn request_id_generated_is_32_lower_hex() {
        let id = get_or_generate_request_id(None);
        assert_eq!(id.len(), 32, "32 hex chars");
        assert!(
            id.chars()
                .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c)),
            "lowercase hex only: {id}"
        );
        // Empty header also generates.
        assert_eq!(get_or_generate_request_id(Some("")).len(), 32);
    }

    #[test]
    fn request_id_passes_through_when_present() {
        assert_eq!(get_or_generate_request_id(Some("abc")), "abc");
    }

    #[test]
    fn into_response_uses_body_code_as_status() {
        let resp = bad_request("x", "y").into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn health_endpoint_returns_200() {
        let app = base_router();
        let req = Request::builder()
            .uri("/healthz")
            .body(Body::empty())
            .expect("request");
        let resp = app.oneshot(req).await.expect("response");
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn health_endpoint_gets_request_id_header() {
        let app = base_router();
        let req = Request::builder()
            .uri("/health")
            .body(Body::empty())
            .expect("request");
        let resp = app.oneshot(req).await.expect("response");
        let id = resp
            .headers()
            .get("x-request-id")
            .expect("x-request-id set")
            .to_str()
            .expect("ascii");
        assert_eq!(id.len(), 32);
    }

    #[tokio::test]
    async fn post_without_json_content_type_is_415() {
        let app = base_router();
        let req = Request::builder()
            .method(Method::POST)
            .uri("/metrics")
            .body(Body::from("hello"))
            .expect("request");
        let resp = app.oneshot(req).await.expect("response");
        assert_eq!(resp.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
        let body = to_bytes(resp.into_body(), usize::MAX).await.expect("body");
        let v: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(v["status"], "unsupported media type");
    }
}
