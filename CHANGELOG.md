# Changelog

All notable changes to `paycloudhelper-rs` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the project uses a
**single workspace-shared version for every crate**, tracking the release tags.

The `### Retracted` heading is the Rust analog of `paycloudhelper`'s Go `retract`
directive — bad releases are yanked from the registry (once one exists) and
recorded here with the reason.

> **Version/tag reconciliation (2026-08-11).** Releases `v1.0.0`–`v1.0.2` were
> tagged while `workspace.package.version` stayed at `0.1.0`, and every change
> sat under `[Unreleased]`. A consumer pinning `v1.0.2` therefore got crates
> self-reporting `0.1.0`, with no record of what separated one tag from the
> next. The sections below reconstruct the three releases from their tagged
> commits; design [02 §4.1]'s "shared pre-1.0 version" is superseded, because
> the tags are load-bearing (services pin them) and retagging would break them.

## [Unreleased]

### Added
- `pc-amqp::AmqpClient::publish_to` (arbitrary routing key + caller properties)
  and `AmqpClient::reply` (answer a delivery on its own `reply_to` queue,
  inheriting the requester's correlation ID). The queue-bound publishers could
  only ever target the client's own queue, so a service *answering* AMQP
  requests had no path through the helper and had to drop to raw `lapin`.
- `pc-redis::RedisPool::ttl` + `KeyTtl` — read a key's remaining lifetime via
  `PTTL`, distinguishing missing (`-2`) from never-expires (`-1`). Needed by any
  caller layering an in-process cache over Redis: without it the shared TTL has
  to be recomputed from the value, which restarts the clock and extends the
  entry beyond what the writer intended. No Go counterpart.
- `pc-grpc::GrpcClientFactory::connect_lazy` and `resolve_target` — build the
  same round-robin pool as `connect` without the prewarm probe, for services
  that must boot degraded rather than refuse to start. Consumers were
  reimplementing endpoint construction to get this.
- Go-captured vectors for the three SNAP-BI algorithms that previously had
  roundtrip tests only: RSA-PKCS1v15-SHA256 (byte-exact), AES-256-GCM (decrypt
  direction — the random nonce makes an encrypt vector impossible), and JWT
  RS256 including an algorithm-confusion rejection case. Captured by calling
  the qoinhub Go helpers directly; provenance and the regeneration procedure
  are documented in `pc-snapbi/tests/go_algorithm_vectors.rs`. The capture
  program stays in the Go repo — a Go module here would fall outside
  `cargo-deny`/`cargo-audit`, go uncompiled by a Rust-only CI, and couple this
  library to one consumer's checkout.

### Fixed
- `pc-grpc::dial_target` no longer trips `clippy::needless_return`, which made
  `cargo clippy -D warnings` — and therefore CI — fail at `v1.0.2`. The lint
  aborted the `pc-grpc` crate, so no crate downstream of it was linted at all.
- `README.md` told consumers to pin `tag = "v0.1.0"`, which has never existed;
  it now names the current release tag.
- Every crate reports the workspace version `1.0.3` instead of `0.1.0` (see the
  reconciliation note above).

### Known divergences
- `pc-snapbi::encrypt_aes`/`decrypt_aes` apply `SHA256(key)` to any key that is
  not exactly 32 bytes. Go's `EncryptAES` only derives that way for the *env
  default* path (`secret == ""`, via `getAESKey`); given an explicit non-empty
  secret it passes the raw bytes to `aes.NewCipher`, which accepts 16/24/32 and
  errors otherwise. So for a 16- or 24-byte secret **both sides succeed and
  produce mutually unreadable ciphertext**, and for other lengths Go errors
  where Rust succeeds. Pinned by
  `pc-snapbi/tests/go_algorithm_vectors.rs::rust_cannot_read_go_ciphertext_written_with_a_non_32_byte_key`
  against captured Go ciphertext. Reachable via
  `helpers.DecryptAES(dmDynamicPrivKey, secretKey)` in
  `services/BiAccessTokenB2b.go`, where the secret is merchant-supplied.
  **Not fixed here:** aligning it changes behaviour for a consumer already
  pinned to `v1.0.2`, so the call is the helper owner's.

## [1.0.2] - 2026-08-04

Tag [`v1.0.2`] · commit `5f2e0d7` — "feat: rework on log compatible with golang
helpers". The release the qoinhub pilot pins.

### Fixed
- `pc-log` now emits the golog-compatible plain-text transport (`[LEVL]
  YYYY-MM-DD HH:MM:SS.mmm message`) in the process local timezone, honors
  `APP_DEBUG_LOG`, preserves the per-template sampler/suppression suffix, and
  shares one formatted message with `pc-sentry` forwarding.
- The sampler accepts the legacy `APP_ENV=developement` spelling used by the Go
  helper.
- `pc-audit` mirrors the v1.11.1 audit echo policy: per-event payload echoes are
  DEBUG and successful publish acknowledgements are a one-minute INFO heartbeat
  carrying the shared suppressed-count suffix.

### Known issues (fixed after this tag)
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` fails
  on `pc-grpc::dial_target`, so CI is red at the exact commit consumers pin.
- Every crate reports version `0.1.0`.

## [1.0.1] - 2026-07-31

Tag [`v1.0.1`] · commit `7dfd64c` — "fix: adjust not to force helper to
headless".

### Fixed
- `pc-grpc::dial_target` dials the configured `host:port` verbatim instead of
  appending a service-name suffix. Pointing at a headless service, a ClusterIP
  or an external host is now purely a deployment decision.

## [1.0.0] - 2026-07-30

Tag [`v1.0.0`] · commit `48c31a8` — "feat: init helper rust". The initial
18-crate + umbrella release, PC-0 through PC-8.

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
[`v1.0.0`]: https://github.com/PayCloud-ID/paycloudhelper-rs/tree/v1.0.0
[`v1.0.1`]: https://github.com/PayCloud-ID/paycloudhelper-rs/tree/v1.0.1
[`v1.0.2`]: https://github.com/PayCloud-ID/paycloudhelper-rs/tree/v1.0.2
