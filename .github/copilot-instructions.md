# paycloudhelper-rs — GitHub Copilot Instructions

> **Primary instructions:** see [`AGENTS.md`](../AGENTS.md) for the full AI agent guide.
>
> This file is the Copilot bridge. All agents share the same source of truth in
> `AGENTS.md`; edit canonical content under `.agents/`, never through a symlink.

**Version:** 1.0.0 — **Updated:** 2026-08-17

---

## Quick Reference

| Resource | Path |
|----------|------|
| **Primary instructions** | [`AGENTS.md`](../AGENTS.md) |
| Rules | `.agents/rules/` (symlinked at `.cursor/rules/`) |
| Skills | `.agents/skills/` (symlinked at `.github/skills/`, `.cursor/skills/`) |
| Prompts (Copilot) | `.github/prompts/` |
| Prompts (shared) | `.agents/prompts/` |

---

## What this repo is

PayCloud's shared **Rust platform library** — the successor to the Go
`paycloudhelper` module. One Cargo workspace, 18 `pc-*` crates plus a `paycloudhelper`
umbrella. A library, not a service: services consume it by pinning a git tag.

Every crate reproduces the **wire-visible behavior** of its Go counterpart bit-for-bit.
Internal shape is free; anything that leaves the process is frozen.

Dependency graph is acyclic and rooted at `pc-core` (which depends on nothing PayCloud).
`pc-grpc`, `pc-http`, and `pc-s3minio` have no internal deps.

---

## The conventions that matter most

1. **`mirrors:` markers** — every parity-bearing public item names its Go source in a doc
   comment (`mirrors: \`phhelper.BuildLogPrefix\` — …`). ~240 of them across 24 files.
2. **Go's exact strings survive**, typos included: `AppEnv::parse("developement")`,
   `[ERRO] `/`[WARN] `/`[INFO] `/`[DBUG] `/`[FTAL] `, `RQ_*` and `RABBITMQ_*` both accepted.
3. **Roundtrip tests don't prove parity** — Go-captured vectors under
   `crates/*/tests/vectors/` do, and each test asserts the fixture's `oracle` field.
4. **Divergences get documented, not fixed** — `CHANGELOG.md` → `### Known divergences`.
   Consumers are pinned to tags; a behavior change breaks them.
5. **Config is passed in, not read from env** inside a `pc-*` crate. Only `APP_*`,
   `SENTRY_*`, `OTEL_DEPLOYMENT_ENV`, and `REDIS_DB` are read.
6. **Third-party versions live only in `[workspace.dependencies]`** — members write
   `serde.workspace = true`.
7. **Live-service tests are `#[ignore]`d** with the required env var in the reason.

Full detail with examples: [`AGENTS.md`](../AGENTS.md) §Critical Conventions.

---

## Commands

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test  --workspace --all-features
cargo deny check
cargo audit

cargo test -p pc-snapbi --all-features            # one crate
cargo test --workspace --all-features -- --ignored # include live-service tests
cargo check -p paycloudhelper --features full      # umbrella feature check
```

Clippy runs `all` + `pedantic` and CI denies every warning — a single lint failure aborts
its crate and leaves everything downstream unlinted.

Toolchain `1.92.0` (`rust-toolchain.toml`); MSRV `1.88` (`Cargo.toml`). Different numbers
on purpose.

---

## Debugging order

1. Output differs from Go → find the `mirrors:` marker, read the Go symbol, look for a
   fixture in `crates/*/tests/vectors/`.
2. CI red, local green → `--all-features`, clippy `pedantic`, new advisories.
3. Consumer can't resolve → stale `version = "…"` in the root `Cargo.toml`.
4. Crate invisible to umbrella users → one of the three umbrella wiring points missed
   (optional dep · feature · `#[cfg]` re-export).
5. Behavior looks wrong but is tested → check `### Known divergences` first.

---

## Design docs

Code cites `01 §…`, `02 §…`, `03 PC-…`, `05 §…`. These are documents in
[`paycloud-docs`](https://github.com/PayCloud-ID/paycloud-docs) under
`prototype/rust-core-transaction/` — mapped in [`AGENTS.md`](../AGENTS.md). Code is
ground truth; the docs are the map.
