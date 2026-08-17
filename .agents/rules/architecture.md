---
description: Crate layering, the acyclic dependency DAG, and the umbrella feature-gate contract.
applyTo: 'crates/**, Cargo.toml'
---

# Architecture

## Layering: an acyclic DAG rooted at `pc-core`

`pc-core` depends on **nothing PayCloud** — only lightweight third-party crates. Every
other crate builds on it (or on nothing at all). A new edge must not create a cycle.

```
pc-core  ──┬─ pc-config
           ├─ pc-json ──┐
           ├─ pc-log ───┼─ pc-sentry ──┐
           ├─ pc-redis ─┼──────────────┼─ pc-health
           ├─ pc-amqp ──┴─ pc-audit    │
           ├─ pc-resilience            │
           ├─ pc-snapbi ─┐             │
           ├─ pc-trace   │             │
           └─ pc-validate┴─ pc-auth ← pc-http
                                     (pc-db → pc-log)

no internal deps: pc-grpc, pc-http, pc-s3minio
```

Current edges, authoritative:

| Crate | Depends on |
|---|---|
| `pc-core`, `pc-grpc`, `pc-http`, `pc-s3minio` | *(nothing internal)* |
| `pc-config`, `pc-json`, `pc-log`, `pc-redis`, `pc-resilience`, `pc-snapbi`, `pc-trace`, `pc-validate` | `pc-core` |
| `pc-amqp` | `pc-core` |
| `pc-db` | `pc-log` |
| `pc-sentry` | `pc-core`, `pc-log` |
| `pc-audit` | `pc-core`, `pc-json`, `pc-log`, `pc-amqp` |
| `pc-auth` | `pc-core`, `pc-http`, `pc-redis`, `pc-snapbi`, `pc-validate` |
| `pc-health` | `pc-core`, `pc-amqp`, `pc-redis`, `pc-sentry` |
| `paycloudhelper` | all of the above, each `optional = true` |

### Deliberate non-edges

`pc-config` does **not** depend on `pc-log`. Validation findings are *returned* as
`Vec<ConfigError>` for the caller to log; Go's `LogConfigurationWarnings` equivalent
lives at the umbrella layer. Do not "simplify" this by logging inside `pc-config` — it
would drag the log stack into every consumer that only wanted config.

## No import-time initialization

Go's `paycloudhelper` ran work in package `init()`. Rust has no import-time side
effects, and that is treated as a correctness win (01 §4.4), not a gap to paper over.

```rust
// ✅ one explicit, idempotent bootstrap from main()
fn main() -> anyhow::Result<()> {
    paycloudhelper::init()?;
    Ok(())
}
```

`paycloudhelper::init()` loads `.env` + app identity (feature `config`) and installs
the subscriber (feature `log`). It must stay **idempotent** — `init_is_repeatable` in
`crates/paycloudhelper/src/lib.rs` asserts it. Config findings never fail startup;
they surface through `pc_config::configuration_status()`, matching Go's warn-only
behavior.

## Umbrella feature gates

Every `pc-*` crate is an optional dependency of `paycloudhelper` behind a feature named
after the crate without the `pc-` prefix, plus a `full` feature listing all of them.
`default = []` — consumers opt in.

```toml
# crates/paycloudhelper/Cargo.toml
pc-redis = { workspace = true, optional = true }

[features]
redis = ["dep:pc-redis"]
full  = [ ..., "redis", ... ]
```

```rust
// crates/paycloudhelper/src/lib.rs
#[cfg(feature = "redis")]
pub use pc_redis as redis;
```

All three edits are required. A new crate that is added to the workspace but not wired
into all three is invisible to umbrella consumers.

## Configuration is passed in, not read from env

The library reads only **process-identity and observability** env vars:
`APP_NAME`, `APP_ENV` (`APP_MODE` legacy fallback), `APP_DEBUG_LOG`, `SENTRY_DSN`,
`SENTRY_ENVIRONMENT`, `SENTRY_RELEASE`, `SENTRY_DEBUG`, `SENTRY_TRACES_SAMPLE_RATE`,
`OTEL_DEPLOYMENT_ENV`, `REDIS_DB`.

Connection settings are **constructor parameters**, not env lookups:

```rust
// ✅ caller owns the URI
let amqp = pc_amqp::AmqpClient::new("audittrail", &rabbit_uri).await?;

// ❌ never: std::env::var("RQ_HOST") inside a pc-* crate to build a connection
```

The service owns its deployment config. `pc-config::validate_configuration` *inspects*
the RabbitMQ env names to warn about partial setup, but never consumes them to connect.

`PC_DB_TEST_DSN` and `REDIS_HOST` appear only in `#[ignore]`d live-service tests.
