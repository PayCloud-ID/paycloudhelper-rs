---
description: The transport-identical-strangler rule — mirrors: markers, Go-oracle vectors, and how to record a divergence.
applyTo: 'crates/**/*.rs'
---

# Go Parity

Every wire-visible behavior must match the Go `paycloudhelper` module bit-for-bit:
JSON bytes, log lines, Redis key strings, AMQP properties, signature inputs, response
envelopes, status strings. "Equivalent" is not the standard; **identical** is.

Internal shape is free. Rust idiom is welcome everywhere it does not change what leaves
the process.

## 1. `mirrors:` markers are mandatory on parity-bearing items

Any public item that reproduces Go behavior carries a `mirrors:` marker naming the Go
source. This is the repo's densest convention — ~240 markers across 24 files — and it is
what makes a parity claim auditable.

```rust
/// Build the standardized log prefix.
///
/// mirrors: `phhelper.BuildLogPrefix` — trims the function name, substitutes
/// `"Log"` when blank, and wraps as `[pchelper.Fn]`.
#[must_use]
pub fn build_log_prefix(function_name: &str) -> String {
```

```rust
// ❌ a parity-bearing item with no provenance
/// Builds the log prefix.
pub fn build_log_prefix(function_name: &str) -> String {
```

Name the Go symbol (`phhelper.BuildLogPrefix`), file (`config.go ValidateConfiguration`),
or struct field (`ResponseApi.InternalCode` with its `json:"…"` tag). Where a design doc
settled the question, cite it as `05 §1.4` — see the reference table in
[`project-context.md`](project-context.md).

## 2. Preserve Go's exact strings

Field names, status strings, and message text are part of the wire contract — including
Go's typos and lowercase-with-spaces style.

```rust
// ✅ Go's exact serialized shape and casing
#[serde(rename_all = "lowercase")]
pub enum Level { Warning, Error }

"APP_NAME environment variable not set - using empty default"
"[ERRO] " / "[WARN] " / "[INFO] " / "[DBUG] " / "[FTAL] "
```

Legacy spellings are accepted, never "corrected": `AppEnv::parse` takes the Go helper's
misspelled `"developement"`; `pc-config` accepts both `APP_ENV` and `APP_MODE`, and both
`RABBITMQ_*` and the `RQ_*` names PayCloud actually deploys.

## 3. Roundtrip tests do not prove parity — oracles do

A roundtrip proves the Rust signer and Rust verifier agree with each other, which they
will even when both are wrong about Go.

Real parity is pinned by **vectors captured from the Go code path itself**, stored as
JSON under `crates/<crate>/tests/vectors/` and asserted by a test in
`crates/<crate>/tests/`:

| Fixture | Oracle |
|---|---|
| `pc-core/tests/vectors/json_minify.json` | Go `encoding/json.Compact` |
| `pc-snapbi/tests/vectors/symmetric_signatures.json` | Go SNAP-BI signature helpers |
| `pc-snapbi/tests/vectors/snapbi_go_vectors.json` | `helpers.SignatureGenerate`, `helpers.EncryptAES`, `golang-jwt/jwt/v5` |
| `pc-audit/tests/vectors/audit_trx.json` | Go transaction-audit serialization |

Each fixture carries an `oracle` field naming its Go source, and the test **asserts on
that field** so a fixture regenerated from a reimplementation fails loudly:

```rust
assert!(fixture.oracle.contains("encoding/json.Compact"));
assert_eq!(fixture.captured_at, "2026-07-30");
```

The capture programs stay in the **Go** repos. A Go module here would sit outside
`cargo-deny` / `cargo-audit`, go uncompiled by a Rust-only CI, and hard-code a path into
one consumer service. See the `go-parity-verification` skill for the capture procedure.

## 4. A divergence is documented, not hidden

Where Rust cannot or should not match Go, record it in `CHANGELOG.md` under
**`### Known divergences`**, and pin the *actual* behavior with a test named for what it
proves.

The live example — `pc-snapbi::encrypt_aes` applies `SHA256(key)` to any non-32-byte key,
while Go passes 16/24-byte keys through raw, so both sides succeed and produce mutually
unreadable ciphertext. It is pinned by
`pc-snapbi/tests/go_algorithm_vectors.rs::rust_cannot_read_go_ciphertext_written_with_a_non_32_byte_key`
against captured Go ciphertext.

Note the reasoning that kept it: aligning the behavior would change results for a
consumer already pinned to `v1.0.2`. **Fixing a divergence is a breaking change** — the
call belongs to the helper owner, not to the agent that noticed it. Report it; do not
silently align.
