# Changelog

All notable changes to `paycloudhelper-rs` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the project adheres to
SemVer with a **workspace-shared version pre-1.0** (design [02 §4.1]).

The `### Retracted` heading is the Rust analog of `paycloudhelper`'s Go `retract`
directive — bad releases are yanked from the registry (once one exists) and
recorded here with the reason.

## [Unreleased]

### Added
- PC-0: Cargo workspace scaffold — `rust-toolchain.toml`, `deny.toml`, CI
  (`fmt` · `clippy -D warnings` · `test` · `cargo-deny` · `cargo-audit`), and
  the 18-crate + umbrella layout.
- PC-1: `pc-core` (byte-exact `json_minify` twin of Go `json.Compact`, `AppEnv`,
  process-global app identity, `AppError` triple-return, log-prefix builder),
  `pc-config`, `pc-validate`, `pc-json`.
- PC-2: `pc-log` sampling, rate-limited logging, request contexts and
  `pc-resilience` circuit breaker, singleflight, and keyed rate limiter.
- PC-3: OpenTelemetry tracing/metrics and AMQP propagation, Sentry forwarding,
  Redis pooling/locks/TTL guards, and bounded SQLx pool configuration.
- PC-4: reconnect-on-demand `pc-amqp` client with durable queues, confirms,
  TTLs, manual-ack consumers and correlated request/reply; `pc-audit` V1/V2/TRX
  payloads, bounded workers, monotonic IDs, and circuit breaker.
- PC-5: SNAP-BI HMAC-SHA512, RSA-PKCS1v15-SHA256, PEM, AES-256-GCM, and
  RS256-JWT helpers.
- PC-6: hardened Axum router/response envelope and tonic client-pool factory.
- PC-7: Axum CSRF, revocation, and idempotency middleware; aggregated resource
  health; transport-neutral S3MinIO SDK with HTTP and generic tonic adapters.
- PC-8: feature-gated `paycloudhelper` umbrella and explicit idempotent
  bootstrap.
- Go-oracle fixtures for byte-exact JSON compaction, SNAP-BI symmetric
  signatures, and transaction-audit serialization.

### Fixed
- Updated `pc-trace` for the OpenTelemetry 0.27 metrics and propagation APIs so
  `--all-features` builds successfully.
- Raised the MSRV from Rust 1.83 to 1.88 and pinned `time` to a patched release;
  Rust 1.83 can only resolve the vulnerable pre-0.3.47 line.

### Security
- Documented three temporary, no-fixed-release RustSec exceptions in
  `SECURITY.md`; `cargo-deny` and `cargo-audit` continue to reject every other
  advisory.

[02 §4.1]: https://github.com/PayCloud-ID/paycloud-docs
