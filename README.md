# paycloudhelper-rs

The shared Rust platform library — the `paycloudhelper` successor. A single Cargo
workspace of focused `pc-*` crates that reproduce every wire-visible behavior of
the Go [`paycloudhelper`](https://github.com/PayCloud-ID/paycloudhelper) module
**bit-for-bit** (the transport-identical-strangler rule), so future Rust services
consume it exactly as Go services consume `paycloudhelper` today.

> Design: `paycloud-docs/prototype/rust-core-transaction/plans/paycloudhelper-rs/`
> (00-README, 01 analysis, 02 design, 03 implementation plan).

## Which crate for which need

| Crate | Responsibility |
|---|---|
| `pc-core` | `AppEnv`, app identity, `AppError`, `json_minify` (signature-hash input), log prefix |
| `pc-config` | `.env` discovery + boot validation (mirrors `init.go`/`config.go`) |
| `pc-log` | `tracing` subscriber, env sampler, rate-limited variants, `LogContext` |
| `pc-trace` | OpenTelemetry OTLP init, AMQP traceparent carrier, phase histograms |
| `pc-sentry` | Sentry init + log-hook forwarding |
| `pc-json` | audit-trail JSON profile (`EscapeHTML=false` + trailing newline) |
| `pc-validate` | `char_libs` / `numeric_null_libs` custom rules |
| `pc-resilience` | circuit breaker, singleflight, rate-limit helpers |
| `pc-redis` | redis pool, TTL clamp, `rslock` locks, key formats |
| `pc-db` | `sqlx` pool config (reject-unbounded + lifetime jitter) |
| `pc-amqp` | `lapin`: reconnect, confirms, TTL, `SendWait` RPC |
| `pc-audit` | audit trail V1/V2/TRX over `pc-amqp` |
| `pc-snapbi` | SNAP-BI HMAC/RSA/AES/JWT + PEM parsing |
| `pc-http` | `axum` bootstrap, `ResponseApi` envelope, headers, health probes |
| `pc-grpc` | `tonic` client-pool factory (headless DNS, prewarm, deadline budget) |
| `pc-auth` | axum middlewares: RevokeToken, CSRF, Idempotency |
| `pc-health` | aggregated redis/rabbitmq/sentry health |
| `pc-s3minio` | s3minio SDK client facade |
| `paycloudhelper` | umbrella: feature-gated re-exports + explicit `init()` |

## Consuming from a service (git-tag model)

```toml
[dependencies]
pc-config = { git = "ssh://git@github.com/PayCloud-ID/paycloudhelper-rs", tag = "v0.1.0" }
pc-log    = { git = "ssh://git@github.com/PayCloud-ID/paycloudhelper-rs", tag = "v0.1.0" }
# ...only the crates the service needs
```

Or via the umbrella, feature-gated:

```toml
paycloudhelper = { git = "ssh://git@github.com/PayCloud-ID/paycloudhelper-rs", tag = "v0.1.0",
                   default-features = false, features = ["config", "log", "redis", "snapbi", "http"] }
```

## Development

```sh
cargo fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test  --workspace --all-features
cargo deny check      # supply-chain gate
```

Tests that require Redis or RabbitMQ are isolated from the default suite. Unit
tests exercise the frozen key, payload, status, signature, TTL, retry, and
envelope contracts without external services; live broker checks should run in
the deployment pipeline.

## Bootstrap

The umbrella replaces Go import-time side effects with one explicit call:

```rust
fn main() -> anyhow::Result<()> {
    paycloudhelper::init()?;
    Ok(())
}
```

## AMQP and audit

```rust
use std::sync::Arc;

let amqp = pc_amqp::AmqpClient::new("audittrail", &rabbit_uri).await?;
let audit = pc_audit::AuditPublisher::new(
    Arc::new(amqp),
    &pc_audit::AuditPublisherConfig::default(),
);

if let Some(payload) =
    pc_audit::process_payload("CreateOrder", "order created", "ok", None)?
{
    audit.submit(payload);
}
```

`AmqpClient` declares a durable queue, enables publisher confirms, applies the
five-second heartbeat, and reconnects on demand after a broken connection.
`AuditPublisher` defaults to 10 workers, a 1000-message buffer, and a circuit
breaker that opens after 10 consecutive failures for 30 seconds.

## Axum authentication middleware

`pc-auth` exports functions for `axum::middleware::from_fn_with_state`:

```rust
let auth = pc_auth::AuthState::new(redis_pool, public_key_pem);
let app = routes
    .layer(axum::middleware::from_fn_with_state(
        auth.clone(),
        pc_auth::verify_idempotency,
    ))
    .layer(axum::middleware::from_fn_with_state(
        auth.clone(),
        pc_auth::verify_csrf,
    ))
    .layer(axum::middleware::from_fn_with_state(
        auth,
        pc_auth::revoke_token,
    ));
```

The middleware preserves the Go Redis key formats and response envelopes,
echoes `X-Request-ID`, accepts RS256 only, and treats statuses 3, 4, and 7 as
revoked.

## Health and S3MinIO

`pc-health::check_health` aggregates Redis, RabbitMQ, and Sentry using
`healthy`/`degraded`/`unhealthy` worst-of-N semantics. `pc-s3minio` exposes a
transport-neutral client trait, helper facade, and HTTP adapter. Its generic
tonic facade accepts a service-owned generated protobuf adapter through
`GrpcService`, keeping `pc-proto` ownership outside this library while sharing
the Go-compatible health, readiness, and unsupported-stream behavior.
