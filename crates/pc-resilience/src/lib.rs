#![forbid(unsafe_code)]
//! `pc-resilience` — general-purpose resilience primitives shared across the
//! `paycloudhelper-rs` workspace (design 02 §3, plan 03 PC-2).
//!
//! Three concepts, each mirroring a pattern that lives inline in the Go
//! `paycloudhelper` source rather than in a dedicated file:
//!
//! * [`CircuitBreaker`] — the consecutive-failure breaker used by the V2 audit
//!   publisher (`audittrail_publisher.go`): trips after N consecutive failures,
//!   short-circuits for a cooldown, then probes recovery. Defaults are the
//!   publisher's own: **10 failures / 30s cooldown**.
//! * [`Singleflight`] — in-flight de-duplication so concurrent callers sharing a
//!   key run the underlying future exactly once (mirrors Go's
//!   `golang.org/x/sync/singleflight` usage).
//! * [`KeyedRateLimiter`] — per-key token buckets over [`governor`], the general
//!   home for the rate-limited-log pattern (`logAuditNotReadyRateLimited`).

mod breaker;
mod ratelimit;
mod singleflight;

pub use breaker::{BreakerError, Cancellable, CircuitBreaker, State};
pub use ratelimit::KeyedRateLimiter;
pub use singleflight::Singleflight;
