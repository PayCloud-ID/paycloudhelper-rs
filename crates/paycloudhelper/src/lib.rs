#![forbid(unsafe_code)]
//! Feature-gated umbrella for the `pc-*` crate family.

#[cfg(feature = "amqp")]
pub use pc_amqp as amqp;
#[cfg(feature = "audit")]
pub use pc_audit as audit;
#[cfg(feature = "auth")]
pub use pc_auth as auth;
#[cfg(feature = "config")]
pub use pc_config as config;
#[cfg(feature = "core")]
pub use pc_core as core;
#[cfg(feature = "db")]
pub use pc_db as db;
#[cfg(feature = "grpc")]
pub use pc_grpc as grpc;
#[cfg(feature = "health")]
pub use pc_health as health;
#[cfg(feature = "http")]
pub use pc_http as http;
#[cfg(feature = "json")]
pub use pc_json as json;
#[cfg(feature = "log")]
pub use pc_log as log;
#[cfg(feature = "redis")]
pub use pc_redis as redis;
#[cfg(feature = "resilience")]
pub use pc_resilience as resilience;
#[cfg(feature = "s3minio")]
pub use pc_s3minio as s3minio;
#[cfg(feature = "sentry")]
pub use pc_sentry as sentry;
#[cfg(feature = "snapbi")]
pub use pc_snapbi as snapbi;
#[cfg(feature = "trace")]
pub use pc_trace as trace;
#[cfg(feature = "validate")]
pub use pc_validate as validate;

/// Explicit, idempotent replacement for Go import-time initialization.
///
/// With `config`, this discovers `.env` and initializes app identity. Config
/// findings are retained through `configuration_status` but do not make
/// startup fail, matching the Go warning behavior. With `log`, this installs
/// the process-wide structured subscriber and sampler.
pub fn init() -> anyhow::Result<()> {
    #[cfg(feature = "config")]
    {
        let _ = config::initialize_app();
    }
    #[cfg(feature = "log")]
    log::init();
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn init_is_repeatable() {
        super::init().unwrap();
        super::init().unwrap();
    }

    #[cfg(feature = "full")]
    #[test]
    fn full_feature_reexports_compile() {
        assert_eq!(super::amqp::DEFAULT_MESSAGE_TTL, "60000");
        assert_eq!(super::audit::CMD_DATA, "audit-trail-data");
        assert_eq!(super::s3minio::CODE_OK, 0);
    }
}
