---
description: What paycloudhelper-rs is, how the workspace is laid out, and which crate owns what.
applyTo: '**'
alwaysApply: true
---

# Project Context — paycloudhelper-rs

`paycloudhelper-rs` is the shared Rust platform library for PayCloud: a single Cargo
workspace of focused `pc-*` crates plus a `paycloudhelper` umbrella crate. It is the
successor to the Go module
[`paycloudhelper`](https://github.com/PayCloud-ID/paycloudhelper).

**This is a library, not a service.** It has no `main`, no Dockerfile, no deployment.
It is consumed by Rust services over a git tag (see
[`release-and-versioning.md`](release-and-versioning.md)).

## The governing rule

Every crate reproduces the **wire-visible behavior** of its Go counterpart bit-for-bit
— the *transport-identical-strangler* rule. A Rust service consuming this library must
be indistinguishable, on the wire and in the logs, from the Go service it replaces.
This is the constraint that decides most design arguments in this repo. See
[`go-parity.md`](go-parity.md).

## Crate map

| Crate | Responsibility |
|---|---|
| `pc-core` | `AppEnv`, process-global app identity, `AppError`, `json_minify`, log prefix |
| `pc-config` | `.env` discovery + boot validation (mirrors `init.go` / `config.go`) |
| `pc-log` | golog-compatible text subscriber, env sampler, rate-limited variants, `LogContext` |
| `pc-trace` | OpenTelemetry OTLP init, AMQP traceparent carrier, phase histograms |
| `pc-sentry` | Sentry init + log-hook forwarding |
| `pc-json` | audit-trail JSON profile (`EscapeHTML=false` + trailing newline) |
| `pc-validate` | `char_libs` / `numeric_null_libs` custom rules |
| `pc-resilience` | circuit breaker, singleflight, keyed rate limiter |
| `pc-redis` | redis pool, TTL clamp, `rslock` locks, key formats |
| `pc-db` | `sqlx` pool config (reject-unbounded + lifetime jitter) |
| `pc-amqp` | `lapin`: reconnect, confirms, TTL, `SendWait` RPC |
| `pc-audit` | audit trail V1/V2/TRX over `pc-amqp` |
| `pc-snapbi` | SNAP-BI HMAC / RSA / AES / JWT + PEM parsing |
| `pc-http` | `axum` bootstrap, `ResponseApi` envelope, headers, health probes |
| `pc-grpc` | `tonic` client-pool factory (DNS round-robin, prewarm, deadline budget) |
| `pc-auth` | axum middlewares: RevokeToken, CSRF, Idempotency |
| `pc-health` | aggregated redis / rabbitmq / sentry health |
| `pc-s3minio` | s3minio SDK client facade (HTTP + generic tonic adapters) |
| `paycloudhelper` | umbrella: feature-gated re-exports + explicit `init()` |

## Repository layout

| Path | Role |
|---|---|
| `crates/*/src/` | crate sources; most crates are a single `lib.rs` |
| `crates/*/tests/` | integration tests — only where Go-oracle vectors exist |
| `crates/*/tests/vectors/*.json` | captured Go-oracle fixtures (see `go-parity.md`) |
| `Cargo.toml` | workspace: shared version, single source of dependency versions, shared lints |
| `deny.toml` / `SECURITY.md` | supply-chain gate + the reviewed advisory exceptions |
| `rust-toolchain.toml` | pinned dev/CI channel (`1.92.0`); MSRV is separate (`1.88`) |
| `CHANGELOG.md` | Keep-a-Changelog, one shared version for all crates |
| `.github/workflows/ci.yml` | fmt · clippy · test · cargo-deny · cargo-audit |

## Upstream design docs

The library was designed before it was written. When a decision looks arbitrary, the
reason is almost always in `paycloud-docs`:

| Reference in code | Document |
|---|---|
| `01 §…` | `paycloud-docs/prototype/rust-core-transaction/plans/paycloudhelper-rs/01-dependency-and-capability-analysis.md` |
| `02 §…` | `…/plans/paycloudhelper-rs/02-library-design.md` |
| `03 PC-…` | `…/plans/paycloudhelper-rs/03-implementation-plan.md` |
| `05 §…` | `paycloud-docs/prototype/rust-core-transaction/05-rust-target-architecture-and-comparison.md` |

Docs are the map; **the code and the Go helper are ground truth**. Where they conflict,
follow the code and flag the divergence — do not silently edit paycloud-docs.
