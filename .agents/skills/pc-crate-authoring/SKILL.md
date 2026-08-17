---
name: pc-crate-authoring
description: >
  Covers adding a new pc-* crate or extending an existing crate's public API —
  workspace wiring, umbrella feature gates, layering constraints, lint and doc
  conventions. Invoke when creating a crate or adding public items to one.
applyTo: 'crates/**/Cargo.toml, crates/**/src/lib.rs, Cargo.toml'
---

# Authoring a `pc-*` Crate

## Adding a new crate — the seven wiring points

Members are globbed (`members = ["crates/*"]`), so the directory is picked up
automatically. Everything else is manual, and missing any of it produces a crate that
compiles but is invisible to consumers.

1. **`crates/pc-foo/Cargo.toml`** — inherit every shared field:

   ```toml
   [package]
   name = "pc-foo"
   version.workspace = true
   edition.workspace = true
   rust-version.workspace = true
   license.workspace = true
   repository.workspace = true
   description = "One line: what this crate owns."

   [lints]
   workspace = true

   [dependencies]
   pc-core.workspace = true
   serde.workspace = true
   ```

2. **Root `Cargo.toml` → `[workspace.dependencies]`** — register the path dep *with* the
   current workspace version:

   ```toml
   pc-foo = { path = "crates/pc-foo", version = "1.0.3" }
   ```

3. **Any third-party crate it needs** must be added to `[workspace.dependencies]` first,
   then referenced as `name.workspace = true`. Never pin a version in a member crate.

4. **`crates/paycloudhelper/Cargo.toml`** — optional dependency:

   ```toml
   pc-foo = { workspace = true, optional = true }
   ```

5. **…same file, `[features]`** — a feature named without the `pc-` prefix, and add it to
   `full`:

   ```toml
   foo = ["dep:pc-foo"]
   full = [ …, "foo" ]
   ```

6. **`crates/paycloudhelper/src/lib.rs`** — the gated re-export:

   ```rust
   #[cfg(feature = "foo")]
   pub use pc_foo as foo;
   ```

7. **`CHANGELOG.md`** under `## [Unreleased]` → `### Added`, and a row in the crate table
   in `README.md` and `.agents/rules/project-context.md`.

If the crate exports a constant worth smoke-testing, add it to
`full_feature_reexports_compile` in `crates/paycloudhelper/src/lib.rs`.

## Layering

The internal dependency graph is acyclic and rooted at `pc-core`, which depends on
nothing PayCloud. Check
[`../../rules/architecture.md`](../../rules/architecture.md) before adding an internal
edge; a cycle is a hard failure, and a *needless* edge (pulling `pc-log` into a crate
that could return findings instead) is a design regression.

## Crate root conventions

```rust
#![forbid(unsafe_code)]
//! `pc-foo` — one-line responsibility.
//!
//! Mirrors Go `paycloudhelper`: `foo.go` (`DoThing`, `ThingConfig`).
//!
//! Go symbols mirrored:
//! - [`do_thing`] ⇔ `helpers.DoThing`
```

Two module-doc styles are in use, both fine: a prose "Mirrors …" paragraph
(`pc-core`, `pc-config`) or an explicit `⇔` symbol list (`pc-snapbi`). Pick the one that
fits; a crate porting many named Go functions is clearer as a list.

Note deliberate design decisions in the module docs where a reader would otherwise
"fix" them — `pc-config`'s docs explain why it does not depend on `pc-log`.

## Public API conventions

- Parity-bearing items carry a **`mirrors:`** marker. See
  [`../../rules/go-parity.md`](../../rules/go-parity.md).
- Fallible functions return `anyhow::Result` or a `thiserror` enum; platform-level
  business-vs-system outcomes use `pc_core::AppError` / `AppResult<T>`.
- Pure accessors and constructors get `#[must_use]`.
- `serde` shapes preserve Go's exact field names and `omitempty` behavior:

  ```rust
  #[serde(skip_serializing_if = "String::is_empty")]
  pub internal_code: String,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub data: Option<T>,
  ```

- Take connection details as **parameters**. A `pc-*` crate does not call
  `std::env::var` to build a connection.

## Tests

Unit tests live in `#[cfg(test)] mod tests` at the bottom of the source file; that is
where most of this workspace's coverage sits. `crates/*/tests/` is reserved for
Go-oracle vector suites.

Live-service tests are `#[ignore]`d with a reason naming the required env var. Prefer an
injected lookup over mutating process-global env — see `count_configured` in
`pc-config`.
