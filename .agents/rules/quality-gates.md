---
description: Build, lint, test, and supply-chain gates — the five CI jobs and how to satisfy them locally.
applyTo: 'crates/**, Cargo.toml, deny.toml, .github/workflows/**'
---

# Quality Gates

CI (`.github/workflows/ci.yml`) runs five independent jobs on every push to
`main`/`develop` and on every PR. Run all five locally before pushing:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test  --workspace --all-features
cargo deny check
cargo audit
```

## Clippy is pedantic and denied

`[workspace.lints.clippy]` sets `all` and `pedantic` to `warn`, and CI promotes every
warning to an error. Four lints are allowed workspace-wide, each for a stated reason:
`module_name_repetitions`, `missing_errors_doc`, `missing_panics_doc`,
`must_use_candidate`, and `doc_markdown` (the docs are dense with product nouns and Go
symbol names).

**Do not add crate-level or item-level `#[allow]` to silence a new lint** without a
comment explaining why the lint is wrong here. Every crate opts in with:

```toml
[lints]
workspace = true
```

A clippy failure is not cosmetic. At `v1.0.2` a single `needless_return` in
`pc-grpc::dial_target` aborted that crate — and therefore **every crate downstream of it
went unlinted**, at the exact commit consumers pin.

## `#![forbid(unsafe_code)]`

Crate roots carry it. There is no unsafe code in this workspace and no reason to add any.

## Tests that need a live service are `#[ignore]`d

The default suite must pass with no Redis, no RabbitMQ, no Postgres, no gRPC server.
Live checks are opt-in and say what they need in the ignore reason:

```rust
#[ignore = "requires a live Redis broker (set REDIS_HOST)"]
#[ignore = "requires a live PostgreSQL; set PC_DB_TEST_DSN"]
#[ignore = "requires a live gRPC server on $TRANSACTION_GRPC"]
```

Unit tests exercise the frozen key, payload, status, signature, TTL, retry, and envelope
contracts without external services. Live broker checks belong in the deployment
pipeline.

Prefer testing pure logic through an injected lookup rather than mutating process-global
state — `pc_config::count_configured` takes `present: impl Fn(&str) -> bool` precisely so
the alias table is testable without touching the real environment.

## Toolchain vs MSRV — two different numbers

| Setting | Value | Meaning |
|---|---|---|
| `rust-toolchain.toml` `channel` | `1.92.0` | the channel dev + CI actually use |
| `Cargo.toml` `rust-version` | `1.88` | MSRV consumers must satisfy |

The channel must stay `>=` the MSRV. The MSRV is `1.88` because the patched `time`
release requires it — Rust 1.83 can only resolve the vulnerable pre-0.3.47 line. Do not
lower it to widen compatibility.

## Dependency versions live in one place

Third-party versions are declared once in `[workspace.dependencies]` and referenced as
`serde.workspace = true` from each crate.

```toml
# ✅ crates/pc-foo/Cargo.toml
serde.workspace = true
tokio.workspace = true

# ❌ never re-pin a version in a member crate
serde = "1.0.200"
```

## Supply chain: `cargo-deny` + `cargo-audit` are release gates

Third-party dependencies must be permissively licensed (the allow-list in `deny.toml`);
the workspace itself is `LicenseRef-proprietary`. `yanked = "deny"`, unknown registries
are denied.

An advisory may be ignored **only** when no fixed release exists, the compatibility
requirement is documented, and the exception is mirrored in both `deny.toml` and
`SECURITY.md` with a removal condition. Three reviewed exceptions are active
(RUSTSEC-2023-0071 `rsa`, RUSTSEC-2024-0384 `instant`, RUSTSEC-2025-0134
`rustls-pemfile`). They are risk acceptances, not safety claims — every dependency
refresh must rerun both scanners and try to remove them.
