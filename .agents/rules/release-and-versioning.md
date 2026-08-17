---
description: The tag-driven version model, CHANGELOG discipline, and how consumers pin this library.
applyTo: 'Cargo.toml, CHANGELOG.md, crates/*/Cargo.toml'
---

# Release and Versioning

## One shared version for every crate, and it follows the tags

`workspace.package.version` is the single version for all 19 crates. Every member
inherits it:

```toml
# ✅ crates/pc-foo/Cargo.toml
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true
repository.workspace = true
```

Internal path deps also carry the version so a consumer resolving by git tag gets a
coherent set:

```toml
pc-core = { path = "crates/pc-core", version = "1.0.3" }
```

**Bumping the version means editing every one of those `version = "…"` lines in the root
`Cargo.toml` plus `workspace.package.version`.** Missing one is a resolution failure, not
a warning.

### Why tags win over the design doc

Design `02 §4.1` specified a shared *pre-1.0* version. The repo was instead tagged
`v1.0.0`/`v1.0.1`/`v1.0.2` while every crate kept reporting `0.1.0` — so a consumer
pinning `v1.0.2` got crates self-identifying as `0.1.0`, and `cargo tree` could not tell
two releases apart. The tags are load-bearing (services pin them; retagging would break
them), so **the version follows the tags**, not the other way round. That design section
is superseded; the reconciliation note is at the top of `CHANGELOG.md`.

## Consumers pin a git tag

There is no registry. Services depend on this repo over SSH at an explicit tag:

```toml
# per-crate
pc-config = { git = "ssh://git@github.com/PayCloud-ID/paycloudhelper-rs", tag = "v1.0.3" }

# or the umbrella, feature-gated
paycloudhelper = { git = "ssh://git@github.com/PayCloud-ID/paycloudhelper-rs", tag = "v1.0.3",
                   default-features = false, features = ["config", "log", "redis"] }
```

Consequences that shape every change here:

- **A tag is immutable.** Never retag. A mistake gets a new patch tag.
- **Behavior changes are breaking for anyone already pinned**, even when the new behavior
  is more correct. That is why aligning a known divergence is the helper owner's call
  (see [`go-parity.md`](go-parity.md) §4).
- The `README.md` pin examples must name a tag that exists. `v0.1.0` was documented for a
  while and has never existed.

## CHANGELOG discipline

Format is [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Every user-visible
change lands under `## [Unreleased]` in the same commit as the code.

Sections in use — the first four are standard, the last two are repo-specific:

| Heading | Use for |
|---|---|
| `### Added` | new public API; say **why it was needed**, and whether Go has a counterpart |
| `### Fixed` | bugs, including CI-breaking ones |
| `### Security` | advisory exceptions and crypto-affecting changes |
| `### Known divergences` | deliberate, documented gaps vs the Go helper |
| `### Known issues (fixed after this tag)` | on a released section, what was wrong at that commit |
| `### Retracted` | the Rust analog of Go's `retract` directive — bad releases, with the reason |

Entries explain the motivation, not just the diff. Compare:

```markdown
✅ - `pc-redis::RedisPool::ttl` + `KeyTtl` — read a key's remaining lifetime via `PTTL`,
     distinguishing missing (`-2`) from never-expires (`-1`). Needed by any caller
     layering an in-process cache over Redis: without it the shared TTL has to be
     recomputed from the value, which restarts the clock. No Go counterpart.

❌ - Added a ttl method to RedisPool.
```

## Cutting a release

See `.agents/prompts/release-checklist.prompt.md`.
