# AI Agent Instructions — paycloudhelper-rs

> Single source of truth for Claude Code, GitHub Copilot, Cursor, Codex, Gemini, and any
> other AI agent working in this repo. Platform bridges delegate here.

**Version:** 1.0.0 — **Updated:** 2026-08-17

---

## Quick Reference

| Resource | Path | Description |
|----------|------|-------------|
| **This file** | `AGENTS.md` | Primary AI-agent entry point |
| **Rules** | `.agents/rules/` | Always-or-conditionally loaded rules |
| **Skills** | `.agents/skills/` | Domain-expertise packages |
| **Prompts** | `.agents/prompts/` | Reusable implementation prompts |
| **Copilot bridge** | `.github/copilot-instructions.md` | Delegates to this file |
| **Codex bridge** | `.codex/AGENTS.md` | Delegates to this file |
| **Gemini bridge** | `.gemini/instructions.md` | Delegates to this file |
| **Cursor rules** | `.cursor/rules/` | Symlink → `.agents/rules/` |
| **Cursor skills** | `.cursor/skills/` | Symlink → `.agents/skills/` |
| **Copilot skills** | `.github/skills/` | Symlink → `.agents/skills/` |

Edit the canonical files under `.agents/` — never through a symlinked bridge path.

---

## Repository Purpose

`paycloudhelper-rs` is PayCloud's shared **Rust platform library** — the successor to the
Go module [`paycloudhelper`](https://github.com/PayCloud-ID/paycloudhelper). It is one
Cargo workspace of 18 focused `pc-*` crates plus a `paycloudhelper` umbrella crate.

It is a **library, not a service**: no `main`, no Dockerfile, no deployment. Rust services
consume it by pinning a git tag. Its whole reason for existing is the
*transport-identical-strangler* rule — a Rust service built on it must be
indistinguishable, on the wire and in the logs, from the Go service it replaces.

| Directory | Role |
|---|---|
| `crates/pc-*/src/` | one crate per capability; most are a single `lib.rs` |
| `crates/*/tests/vectors/` | JSON fixtures captured from the **Go** code path |
| `crates/paycloudhelper/` | umbrella: feature-gated re-exports + `init()` |
| `.github/workflows/ci.yml` | fmt · clippy · test · cargo-deny · cargo-audit |
| `deny.toml` + `SECURITY.md` | supply-chain gate and its reviewed exceptions |

---

## Architecture (Summary)

Acyclic dependency DAG rooted at `pc-core`, which depends on nothing PayCloud:

```
pc-core ──┬─ pc-config, pc-json, pc-log, pc-redis, pc-resilience,
          │  pc-snapbi, pc-trace, pc-validate, pc-amqp
          ├─ pc-log ─── pc-sentry, pc-db
          ├─ pc-amqp ── pc-audit  (+ pc-core, pc-json, pc-log)
          ├─ pc-auth   (+ pc-http, pc-redis, pc-snapbi, pc-validate)
          └─ pc-health (+ pc-amqp, pc-redis, pc-sentry)

no internal deps: pc-grpc, pc-http, pc-s3minio
umbrella `paycloudhelper` depends on all 18, each optional
```

Full crate table and edge list: [`.agents/rules/project-context.md`](.agents/rules/project-context.md)
and [`.agents/rules/architecture.md`](.agents/rules/architecture.md).

---

## Critical Conventions

### 1. Parity is the product — `mirrors:` markers are mandatory

Any public item reproducing Go behavior names its Go source. ~240 markers across 24 files.

```rust
✅ /// Build the standardized log prefix.
   ///
   /// mirrors: `phhelper.BuildLogPrefix` — trims the function name, substitutes
   /// `"Log"` when blank, and wraps as `[pchelper.Fn]`.
   pub fn build_log_prefix(function_name: &str) -> String {

❌ /// Builds the log prefix.
   pub fn build_log_prefix(function_name: &str) -> String {
```

### 2. Preserve Go's exact strings — including the typos

```rust
✅ AppEnv::parse("developement")   // the Go helper's misspelling, accepted on purpose
✅ "[ERRO] " / "[WARN] " / "[INFO] " / "[DBUG] " / "[FTAL] "
✅ "APP_NAME environment variable not set - using empty default"
❌ "developement" → "development"  // silently breaks the log sampler
```

Legacy env spellings are accepted, never corrected: `APP_ENV` + `APP_MODE`, and both
`RABBITMQ_*` and the `RQ_*` names PayCloud actually deploys.

### 3. Roundtrip tests do not prove parity — Go oracles do

```rust
✅ let fixture: Fixture = serde_json::from_str(include_str!("vectors/json_minify.json"))?;
   assert!(fixture.oracle.contains("encoding/json.Compact"));   // provenance asserted
   assert_eq!(actual, vector.output.as_bytes(), "{}", vector.name);

❌ let sig = sign(k, m); assert!(verify(k, m, &sig));   // Rust agreeing with Rust
```

Capture programs stay in the **Go** repo — see the `go-parity-verification` skill.

### 4. Divergences are documented, never silently aligned

`pc-snapbi::encrypt_aes` derives `SHA256(key)` for non-32-byte keys where Go passes 16/24
raw — both sides succeed and produce mutually unreadable ciphertext. It stays, pinned by
a test named for the failure, because **fixing it would break consumers pinned to
`v1.0.2`**. Record such findings under `### Known divergences` in `CHANGELOG.md`; the fix
call belongs to the helper owner.

### 5. Configuration is passed in, not read from env

```rust
✅ let amqp = pc_amqp::AmqpClient::new("audittrail", &rabbit_uri).await?;
❌ let host = std::env::var("RQ_HOST")?;   // inside a pc-* crate
```

Only process-identity and observability vars are read: `APP_NAME`, `APP_ENV`/`APP_MODE`,
`APP_DEBUG_LOG`, `SENTRY_*`, `OTEL_DEPLOYMENT_ENV`, `REDIS_DB`.

### 6. No import-time initialization

```rust
✅ fn main() -> anyhow::Result<()> { paycloudhelper::init()?; Ok(()) }
```

`init()` must stay idempotent (asserted by `init_is_repeatable`). Config findings warn;
they never fail startup.

### 7. One version, shared, following the tags

`workspace.package.version` covers all 19 crates; members use `version.workspace = true`.
Bumping means editing that field **and** every internal path dep's `version = "…"` in the
root `Cargo.toml`. Third-party versions live only in `[workspace.dependencies]`; a member
crate writes `serde.workspace = true`, never a version.

### 8. Clippy pedantic is denied, and a failure cascades

CI runs `-D warnings` with `all` + `pedantic`. At `v1.0.2` one `needless_return` in
`pc-grpc` aborted that crate, so every crate downstream of it went unlinted at the exact
commit consumers pin.

### 9. Tests requiring a live service are `#[ignore]`d, with the reason

```rust
#[ignore = "requires a live Redis broker (set REDIS_HOST)"]
```

The default suite must pass with no Redis, RabbitMQ, Postgres, or gRPC server.

---

## Rules Reference

| Rule | File | Loaded When |
|------|------|-------------|
| Project Context | [`.agents/rules/project-context.md`](.agents/rules/project-context.md) | Always |
| Architecture | [`.agents/rules/architecture.md`](.agents/rules/architecture.md) | `crates/**`, `Cargo.toml` |
| Go Parity | [`.agents/rules/go-parity.md`](.agents/rules/go-parity.md) | `crates/**/*.rs` |
| Quality Gates | [`.agents/rules/quality-gates.md`](.agents/rules/quality-gates.md) | `crates/**`, `deny.toml`, `.github/workflows/**` |
| Release & Versioning | [`.agents/rules/release-and-versioning.md`](.agents/rules/release-and-versioning.md) | `Cargo.toml`, `CHANGELOG.md` |

## Skills Reference

| Skill | Path | Use When |
|-------|------|----------|
| `go-parity-verification` | [`.agents/skills/go-parity-verification/`](.agents/skills/go-parity-verification/) | Adding/changing parity-bearing behavior; capturing or asserting Go-oracle vectors |
| `pc-crate-authoring` | [`.agents/skills/pc-crate-authoring/`](.agents/skills/pc-crate-authoring/) | Creating a new `pc-*` crate or adding public API to one |
| `snapbi-crypto` | [`.agents/skills/snapbi-crypto/`](.agents/skills/snapbi-crypto/) | Touching signatures, PEM parsing, AES, or RS256 JWTs |

## Prompts

| Prompt | Path | Purpose |
|--------|------|---------|
| `release-checklist` | `.agents/prompts/release-checklist.prompt.md` | Cut a release tag end to end |

---

## Common Commands

```sh
# the five CI gates, in CI order
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test  --workspace --all-features
cargo deny check
cargo audit

# fix formatting
cargo fmt --all

# one crate
cargo test -p pc-snapbi --all-features
cargo clippy -p pc-grpc --all-targets --all-features -- -D warnings

# include the live-service tests (needs REDIS_HOST / PC_DB_TEST_DSN / TRANSACTION_GRPC)
cargo test --workspace --all-features -- --ignored

# umbrella feature check
cargo check -p paycloudhelper --no-default-features --features "config,log,redis"
cargo check -p paycloudhelper --features full
```

Toolchain is pinned to `1.92.0` by `rust-toolchain.toml`; MSRV is `1.88` — two different
numbers, both deliberate.

---

## Where to Look First When Debugging

1. **Output differs from the Go service** → find the `mirrors:` marker on the item, read
   the named Go symbol, then check for a fixture in `crates/*/tests/vectors/`.
2. **CI red but local green** → `--all-features`; clippy `pedantic`; `cargo deny` /
   `cargo audit` advisories that appeared since the last run.
3. **Consumer can't resolve the library** → a `version = "…"` in the root `Cargo.toml`
   left behind at the previous release.
4. **A crate is invisible to umbrella consumers** → one of the three umbrella wiring
   points (optional dep, feature, `#[cfg]` re-export) was missed.
5. **Behavior looks wrong but is tested** → check `### Known divergences` in
   `CHANGELOG.md` before "fixing" it.

---

## Upstream Design Docs

Code cites design sections as `01 §4.4`, `02 §4.1`, `03 PC-0`, `05 §1.4`. These map to
[`paycloud-docs`](https://github.com/PayCloud-ID/paycloud-docs):

| Ref | Document |
|---|---|
| `01` | `prototype/rust-core-transaction/plans/paycloudhelper-rs/01-dependency-and-capability-analysis.md` |
| `02` | `prototype/rust-core-transaction/plans/paycloudhelper-rs/02-library-design.md` |
| `03` | `prototype/rust-core-transaction/plans/paycloudhelper-rs/03-implementation-plan.md` |
| `05` | `prototype/rust-core-transaction/05-rust-target-architecture-and-comparison.md` |

Live code and the Go helper are ground truth; the docs are the map and parts go stale. On
a conflict, follow the code, flag the divergence, and **offer** to fix the doc — never
edit paycloud-docs silently.

---

## Agent Compatibility

### Claude Code
Reads `AGENTS.md` directly; all resources reachable under `.agents/**`.

### GitHub Copilot
Entry point `.github/copilot-instructions.md` → delegates here. Skills via
`.github/skills/` → `.agents/skills/`. Copilot-only prompts live in `.github/prompts/`
(a real directory, not a symlink); shared prompts live in `.agents/prompts/`.

### Cursor
Rules via `.cursor/rules/` → `.agents/rules/`. Skills via `.cursor/skills/` →
`.agents/skills/`.

### Codex
Entry point `.codex/AGENTS.md` → delegates here. Skills and prompts via
`.codex/skills/` and `.codex/prompts/`.

### Gemini Code
Entry point `.gemini/instructions.md` → delegates here. Skills via `.gemini/skills/`.

---

## Adding a New Rule or Skill

### New rule
1. Create `.agents/rules/<name>.md` with `description` + `applyTo` frontmatter
   (`alwaysApply: true` only for repo-overview rules).
2. Add a row to **Rules Reference** above.
3. Update cross-links if it supersedes content elsewhere.

### New skill
1. Create `.agents/skills/<name>/SKILL.md` with `name` + `description` + `applyTo`
   frontmatter. Name the domain, not the project (`snapbi-crypto`, not `paycloud-crypto`).
2. Add a row to **Skills Reference** above.
3. Ground it in real paths and real code from this repo — no aspirational guidance.

Both are picked up by every platform automatically through the existing symlinks; no
bridge file needs editing.
