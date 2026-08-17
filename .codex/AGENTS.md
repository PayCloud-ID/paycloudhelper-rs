# Codex — AI Agent Instructions

> **Primary source of truth:** [`../AGENTS.md`](../AGENTS.md)
>
> This bridge file delegates to the unified SOT. Edit canonical content under
> `.agents/`, never through a symlinked path.

**Quick Start:**
- Read [AGENTS.md](../AGENTS.md) for full context
- Rules: `.agents/rules/`
- Skills: `.codex/skills/` (symlink to `.agents/skills/`)
- Prompts: `.codex/prompts/` (symlink to `.agents/prompts/`)

**In one line:** PayCloud's shared Rust platform library (18 `pc-*` crates + a
`paycloudhelper` umbrella) that reproduces the Go `paycloudhelper` module's wire-visible
behavior bit-for-bit. Parity beats elegance; consumers pin git tags, so behavior changes
are breaking.

**Gates:**

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test  --workspace --all-features
cargo deny check
cargo audit
```
