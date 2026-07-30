#![forbid(unsafe_code)]
//! Aggregated Redis, RabbitMQ, and Sentry health.

use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

pub const HEALTHY: &str = "healthy";
pub const DEGRADED: &str = "degraded";
pub const UNHEALTHY: &str = "unhealthy";
pub const REDIS_DEGRADED_AFTER: Duration = Duration::from_millis(1000);

/// One component health result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthStatus {
    pub component: String,
    pub status: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
}

/// Overall health envelope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthCheck {
    pub app_name: String,
    pub app_env: String,
    pub timestamp: String,
    pub overall_status: String,
    pub checks: Vec<HealthStatus>,
}

/// Optional process resources. Missing resources are unhealthy, matching the
/// Go helper's backwards-compatible `CheckHealth`.
#[derive(Clone, Default)]
pub struct HealthResources {
    pub redis: Option<pc_redis::RedisPool>,
    pub rabbitmq: Option<pc_amqp::AmqpClient>,
}

/// Check all resources and return worst-of-N overall status.
pub async fn check_health(resources: &HealthResources) -> HealthCheck {
    let checks = vec![
        check_redis(resources.redis.as_ref()).await,
        check_rabbitmq(resources.rabbitmq.as_ref()),
        check_sentry(),
    ];
    let overall_status = worst_status(checks.iter().map(|check| check.status.as_str())).to_string();
    HealthCheck {
        app_name: pc_core::identity::app_name(),
        app_env: pc_core::identity::app_env_raw(),
        timestamp: timestamp_now(),
        overall_status,
        checks,
    }
}

/// Probe Redis with a two-second deadline; successful PING over 1000ms is
/// degraded.
pub async fn check_redis(redis: Option<&pc_redis::RedisPool>) -> HealthStatus {
    let Some(redis) = redis else {
        return unhealthy("redis", "redis client not initialized");
    };
    let start = Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(2), redis.ping()).await;
    let elapsed = start.elapsed();
    let latency = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
    match result {
        Ok(Ok(())) if elapsed > REDIS_DEGRADED_AFTER => HealthStatus {
            component: "redis".to_string(),
            status: DEGRADED.to_string(),
            message: "high latency detected".to_string(),
            latency_ms: Some(latency),
        },
        Ok(Ok(())) => HealthStatus {
            component: "redis".to_string(),
            status: HEALTHY.to_string(),
            message: String::new(),
            latency_ms: Some(latency),
        },
        Ok(Err(err)) => HealthStatus {
            component: "redis".to_string(),
            status: UNHEALTHY.to_string(),
            message: err.to_string(),
            latency_ms: Some(latency),
        },
        Err(_) => HealthStatus {
            component: "redis".to_string(),
            status: UNHEALTHY.to_string(),
            message: "redis health check timed out".to_string(),
            latency_ms: Some(latency),
        },
    }
}

/// RabbitMQ readiness check.
#[must_use]
pub fn check_rabbitmq(rabbitmq: Option<&pc_amqp::AmqpClient>) -> HealthStatus {
    match rabbitmq {
        None => unhealthy("rabbitmq", "rabbitmq client not initialized"),
        Some(client) if client.is_ready() => healthy("rabbitmq"),
        Some(_) => HealthStatus {
            component: "rabbitmq".to_string(),
            status: DEGRADED.to_string(),
            message: "connection not ready".to_string(),
            latency_ms: None,
        },
    }
}

/// Sentry global-client check.
#[must_use]
pub fn check_sentry() -> HealthStatus {
    if pc_sentry::sentry_enabled() {
        healthy("sentry")
    } else {
        unhealthy("sentry", "sentry client not initialized")
    }
}

/// Worst-of-N reduction: unhealthy > degraded > healthy.
#[must_use]
pub fn worst_status<'a>(statuses: impl IntoIterator<Item = &'a str>) -> &'static str {
    let mut worst = HEALTHY;
    for status in statuses {
        match status {
            UNHEALTHY => return UNHEALTHY,
            DEGRADED => worst = DEGRADED,
            _ => {}
        }
    }
    worst
}

/// Redis pool statistics in the legacy helper's public shape.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RedisPoolStats {
    pub total_conns: usize,
    pub idle_conns: usize,
    pub stale_conns: usize,
    pub hits: u64,
    pub misses: u64,
    pub timeouts: u64,
}

/// Snapshot pool stats. Deadpool does not expose go-redis hit/miss counters,
/// so those counters remain zero.
#[must_use]
pub fn redis_pool_stats(redis: Option<&pc_redis::RedisPool>) -> Option<RedisPoolStats> {
    let status = redis?.pool_status();
    Some(RedisPoolStats {
        total_conns: status.total,
        idle_conns: status.idle,
        ..Default::default()
    })
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RedisMetricsDetailed {
    pub pool_stats: RedisPoolStats,
    pub hit_rate_percent: f64,
    pub active_conns: usize,
    pub pool_utilized_percent: f64,
}

#[must_use]
pub fn redis_metrics_detailed(redis: Option<&pc_redis::RedisPool>) -> Option<RedisMetricsDetailed> {
    let pool_stats = redis_pool_stats(redis)?;
    let active_conns = pool_stats.total_conns.saturating_sub(pool_stats.idle_conns);
    let requests = pool_stats.hits + pool_stats.misses;
    let hit_rate_percent = if requests == 0 {
        0.0
    } else {
        ratio_u64(pool_stats.hits, requests)
    };
    let pool_utilized_percent = if pool_stats.total_conns == 0 {
        0.0
    } else {
        ratio_usize(active_conns, pool_stats.total_conns)
    };
    Some(RedisMetricsDetailed {
        pool_stats,
        hit_rate_percent,
        active_conns,
        pool_utilized_percent,
    })
}

#[allow(clippy::cast_precision_loss)]
fn ratio_u64(numerator: u64, denominator: u64) -> f64 {
    numerator as f64 / denominator as f64 * 100.0
}

#[allow(clippy::cast_precision_loss)]
fn ratio_usize(numerator: usize, denominator: usize) -> f64 {
    numerator as f64 / denominator as f64 * 100.0
}

fn healthy(component: &str) -> HealthStatus {
    HealthStatus {
        component: component.to_string(),
        status: HEALTHY.to_string(),
        message: String::new(),
        latency_ms: None,
    }
}

fn unhealthy(component: &str, message: &str) -> HealthStatus {
    HealthStatus {
        component: component.to_string(),
        status: UNHEALTHY.to_string(),
        message: message.to_string(),
        latency_ms: None,
    }
}

fn timestamp_now() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worst_of_n_contract() {
        assert_eq!(worst_status([HEALTHY, HEALTHY]), HEALTHY);
        assert_eq!(worst_status([HEALTHY, DEGRADED]), DEGRADED);
        assert_eq!(worst_status([DEGRADED, UNHEALTHY, HEALTHY]), UNHEALTHY);
    }

    #[test]
    fn absent_resources_are_unhealthy() {
        assert_eq!(check_rabbitmq(None).status, UNHEALTHY);
        assert_eq!(check_sentry().component, "sentry");
    }

    #[tokio::test]
    async fn aggregate_has_frozen_components() {
        let health = check_health(&HealthResources::default()).await;
        assert_eq!(health.overall_status, UNHEALTHY);
        let names: Vec<_> = health
            .checks
            .iter()
            .map(|check| check.component.as_str())
            .collect();
        assert_eq!(names, ["redis", "rabbitmq", "sentry"]);
    }
}
