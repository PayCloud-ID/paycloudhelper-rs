---
description: Cut a paycloudhelper-rs release tag — version bump, changelog, gates, tag.
argument-hint: <new-version, e.g. 1.0.4>
---

# Release checklist — `paycloudhelper-rs`

Cut release `$ARGUMENTS`. Tags are immutable and services pin them, so verify each step
before moving on. Stop and report if any gate fails.

## 1. Confirm the version is right

- Patch: fixes only. Minor: additive public API. Major: any change to wire-visible
  behavior — including "corrections" — because every consumer is pinned to a tag.
- Check `## [Unreleased]` in `CHANGELOG.md` for anything under `### Known divergences`
  that a consumer would experience as a break.

## 2. Bump the version everywhere

Both of these, in the root `Cargo.toml`:

- `workspace.package.version = "<new>"`
- every internal path dep in `[workspace.dependencies]`:
  `pc-core = { path = "crates/pc-core", version = "<new>" }` — all 18 of them.

Member crates inherit via `version.workspace = true`; do not edit them. Verify no stale
version remains:

```sh
grep -n 'version = "' Cargo.toml
grep -rn '^version = "' crates/*/Cargo.toml   # expect no matches
```

## 3. Close the changelog section

Rename `## [Unreleased]` to `## [<new>] - YYYY-MM-DD`, add the tag/commit line in the
style of the existing entries ("Tag [`vX.Y.Z`] · commit `abc1234` — \"<subject>\""), add
the link-reference at the bottom, and open a fresh empty `## [Unreleased]`.

Carry forward any `### Known divergences` that are still true. If something was broken at
this commit and fixed later, the released section gets a
`### Known issues (fixed after this tag)` heading.

## 4. Run every gate

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test  --workspace --all-features
cargo deny check
cargo audit
```

`cargo deny` / `cargo audit`: attempt to remove each exception in `deny.toml` and
`SECURITY.md` — check whether a fixed release now exists. If one does, drop the exception
from both files in this release.

## 5. Update the consumer-facing docs

`README.md`'s pin examples must name the tag being cut, not the previous one. A pin
example naming a tag that does not exist has shipped before.

## 6. Tag

```sh
git tag -a v<new> -m "<summary>"
git push origin v<new>
```

Never move or delete a published tag. A mistake gets a new patch release, recorded under
`### Retracted` in `CHANGELOG.md` with the reason.

## 7. Report

Summarize: version, what changed, gate results, exceptions removed or retained, and the
tag consumers should move to.
