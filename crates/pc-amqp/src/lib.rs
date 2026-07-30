#![forbid(unsafe_code)]
//! Reconnecting RabbitMQ client with publisher confirms.
//!
//! Mirrors `paycloudhelper`'s `AmqpClient`: durable queue declaration,
//! connection name `amqp-<AppName>`, a five-second heartbeat, JSON messages,
//! default TTL `60000`, bounded publish retries, manual-ack consumers, and
//! request/reply correlation.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use futures::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions,
    ConfirmSelectOptions, QueueDeclareOptions,
};
use lapin::types::{FieldTable, LongString, ShortString};
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, Consumer};
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

/// Default RabbitMQ message TTL in milliseconds.
pub const DEFAULT_MESSAGE_TTL: &str = "60000";
/// Reply-message TTL used by the request/reply helper.
pub const REPLY_MESSAGE_TTL: &str = "180000";
/// Go-compatible maximum publish attempts.
pub const PUSH_MAX_RETRIES: usize = 3;
/// Go-compatible total publish deadline.
pub const PUSH_TIMEOUT: Duration = Duration::from_secs(15);
/// Delay before retrying an unconfirmed publish.
pub const RESEND_DELAY: Duration = Duration::from_secs(5);

struct BrokerState {
    connection: Connection,
    channel: Channel,
}

impl BrokerState {
    fn ready(&self) -> bool {
        self.connection.status().connected() && self.channel.status().connected()
    }
}

/// A cloneable AMQP client that reconnects on demand after broker churn.
#[derive(Clone)]
pub struct AmqpClient {
    queue: Arc<str>,
    addr: Arc<str>,
    connection_name: Arc<str>,
    state: Arc<RwLock<Option<Arc<BrokerState>>>>,
    reconnect: Arc<Mutex<()>>,
}

impl std::fmt::Debug for AmqpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AmqpClient")
            .field("queue", &self.queue)
            .field("addr", &redact_amqp_uri(&self.addr))
            .field("connection_name", &self.connection_name)
            .field("ready", &self.is_ready())
            .finish_non_exhaustive()
    }
}

impl AmqpClient {
    /// Connect, enable confirms, and ensure the durable queue exists.
    pub async fn new(queue: &str, addr: &str) -> anyhow::Result<Self> {
        let app = pc_core::identity::app_name();
        let connection_name = if app.is_empty() {
            "amqp-".to_string()
        } else {
            format!("amqp-{app}")
        };
        Self::with_connection_name(queue, addr, &connection_name).await
    }

    /// Construct with an explicit connection name (used by audit-trail clients).
    pub async fn with_connection_name(
        queue: &str,
        addr: &str,
        connection_name: &str,
    ) -> anyhow::Result<Self> {
        if queue.trim().is_empty() {
            return Err(anyhow!("AMQP queue name must not be empty"));
        }
        if addr.trim().is_empty() {
            return Err(anyhow!("AMQP address must not be empty"));
        }

        let client = Self {
            queue: Arc::from(queue),
            addr: Arc::from(addr),
            connection_name: Arc::from(connection_name),
            state: Arc::new(RwLock::new(None)),
            reconnect: Arc::new(Mutex::new(())),
        };
        client.reconnect().await?;
        Ok(client)
    }

    /// Queue configured for this client.
    #[must_use]
    pub fn queue(&self) -> &str {
        &self.queue
    }

    /// AMQP connection name visible in RabbitMQ management.
    #[must_use]
    pub fn connection_name(&self) -> &str {
        &self.connection_name
    }

    /// Whether the current connection and channel are open.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.state
            .try_read()
            .ok()
            .and_then(|guard| guard.as_ref().map(|state| state.ready()))
            .unwrap_or(false)
    }

    /// Wait until ready or until `timeout` elapses.
    pub async fn wait_for_ready(&self, timeout: Duration) -> bool {
        tokio::time::timeout(timeout, async {
            loop {
                if self.is_ready() {
                    break;
                }
                let _ = self.ensure_state().await;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .is_ok()
    }

    async fn reconnect(&self) -> anyhow::Result<Arc<BrokerState>> {
        let _guard = self.reconnect.lock().await;
        if let Some(state) = self
            .state
            .read()
            .await
            .as_ref()
            .filter(|s| s.ready())
            .cloned()
        {
            return Ok(state);
        }

        let addr = with_heartbeat(&self.addr);
        tracing::info!(
            queue = %self.queue,
            addr = %redact_amqp_uri(&addr),
            "[AMQP] attempting to connect"
        );
        let props = ConnectionProperties::default()
            .with_connection_name(LongString::from(self.connection_name.as_ref()));
        let connection = Connection::connect(&addr, props)
            .await
            .with_context(|| format!("connect AMQP {}", redact_amqp_uri(&addr)))?;
        let channel = connection.create_channel().await?;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await?;
        channel
            .queue_declare(
                &self.queue,
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;

        let state = Arc::new(BrokerState {
            connection,
            channel,
        });
        *self.state.write().await = Some(state.clone());
        tracing::info!(queue = %self.queue, "[AMQP] client init done");
        Ok(state)
    }

    async fn ensure_state(&self) -> anyhow::Result<Arc<BrokerState>> {
        if let Some(state) = self
            .state
            .read()
            .await
            .as_ref()
            .filter(|s| s.ready())
            .cloned()
        {
            return Ok(state);
        }
        self.reconnect().await
    }

    async fn publish_once(
        &self,
        data: &[u8],
        ttl_ms: Option<&str>,
        extra: BasicProperties,
    ) -> anyhow::Result<()> {
        let state = self.ensure_state().await?;
        let mut props = extra.with_content_type(ShortString::from("application/json"));
        if let Some(ttl) = ttl_ms.filter(|ttl| !ttl.is_empty()) {
            props = props.with_expiration(ShortString::from(ttl));
        }
        let confirm = state
            .channel
            .basic_publish("", &self.queue, BasicPublishOptions::default(), data, props)
            .await?
            .await?;
        if confirm.is_nack() {
            return Err(anyhow!("[AMQP] broker negatively acknowledged publish"));
        }
        Ok(())
    }

    /// Publish with confirms, retrying at most three times within 15 seconds.
    pub async fn push(&self, data: &[u8]) -> anyhow::Result<()> {
        let work = async {
            let mut last = None;
            for attempt in 0..PUSH_MAX_RETRIES {
                match self
                    .publish_once(data, Some(DEFAULT_MESSAGE_TTL), BasicProperties::default())
                    .await
                {
                    Ok(()) => return Ok(()),
                    Err(err) => {
                        last = Some(err);
                        *self.state.write().await = None;
                        if attempt + 1 < PUSH_MAX_RETRIES {
                            tokio::time::sleep(RESEND_DELAY).await;
                        }
                    }
                }
            }
            Err(last.unwrap_or_else(|| anyhow!("[AMQP] publish failed")))
        };
        tokio::time::timeout(PUSH_TIMEOUT, work)
            .await
            .map_err(|_| anyhow!("[AMQP] push timeout after {PUSH_TIMEOUT:?}"))?
    }

    /// Publish a JSON message with the caller-provided expiration.
    ///
    /// An empty TTL disables message expiration.
    pub async fn push_with_ttl(&self, data: &[u8], ttl_ms: &str) -> anyhow::Result<()> {
        self.publish_once(
            data,
            (!ttl_ms.is_empty()).then_some(ttl_ms),
            BasicProperties::default(),
        )
        .await
    }

    /// Create a manual-ack consumer with prefetch count one.
    pub async fn consume(&self) -> anyhow::Result<Consumer> {
        let state = self.ensure_state().await?;
        state
            .channel
            .basic_qos(1, BasicQosOptions::default())
            .await?;
        let consumer = state
            .channel
            .basic_consume(
                &self.queue,
                "",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;
        Ok(consumer)
    }

    /// Send a correlated request and wait for a reply on an exclusive queue.
    pub async fn send_wait(&self, data: &[u8], timeout: Duration) -> anyhow::Result<Vec<u8>> {
        let state = self.ensure_state().await?;
        let reply = state
            .channel
            .queue_declare(
                "",
                QueueDeclareOptions {
                    passive: false,
                    durable: false,
                    exclusive: true,
                    auto_delete: true,
                    nowait: false,
                },
                FieldTable::default(),
            )
            .await?;
        let reply_queue = reply.name().as_str().to_string();
        let correlation = Uuid::new_v4().to_string();
        let mut consumer = state
            .channel
            .basic_consume(
                &reply_queue,
                "",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;

        let props = BasicProperties::default()
            .with_reply_to(ShortString::from(reply_queue))
            .with_correlation_id(ShortString::from(correlation.clone()))
            .with_expiration(ShortString::from(REPLY_MESSAGE_TTL));
        self.publish_once(data, None, props).await?;

        tokio::time::timeout(timeout, async {
            while let Some(delivery) = consumer.next().await {
                let delivery = delivery?;
                let matches = delivery
                    .properties
                    .correlation_id()
                    .as_ref()
                    .is_some_and(|id| id.as_str() == correlation);
                delivery.ack(BasicAckOptions::default()).await?;
                if matches {
                    return Ok(delivery.data);
                }
            }
            Err(anyhow!("[AMQP] reply consumer closed"))
        })
        .await
        .map_err(|_| anyhow!("[AMQP] send_wait timeout after {timeout:?}"))?
    }

    /// Close the channel and connection. Repeated calls are safe.
    pub async fn close(&self) -> anyhow::Result<()> {
        let state = self.state.write().await.take();
        if let Some(state) = state {
            if state.channel.status().connected() {
                state.channel.close(200, "client shutdown").await?;
            }
            if state.connection.status().connected() {
                state.connection.close(200, "client shutdown").await?;
            }
        }
        Ok(())
    }
}

/// Minimal publish abstraction used by `pc-audit` and its deterministic tests.
#[async_trait]
pub trait Publisher: Send + Sync {
    /// Publish bytes with a message TTL.
    async fn publish(&self, data: &[u8], ttl_ms: &str) -> anyhow::Result<()>;
    /// Report whether the transport is ready.
    fn ready(&self) -> bool;
}

#[async_trait]
impl Publisher for AmqpClient {
    async fn publish(&self, data: &[u8], ttl_ms: &str) -> anyhow::Result<()> {
        self.push_with_ttl(data, ttl_ms).await
    }

    fn ready(&self) -> bool {
        self.is_ready()
    }
}

fn with_heartbeat(addr: &str) -> String {
    if addr
        .split('?')
        .nth(1)
        .is_some_and(|q| q.split('&').any(|v| v.starts_with("heartbeat=")))
    {
        return addr.to_string();
    }
    let separator = if addr.contains('?') { '&' } else { '?' };
    format!("{addr}{separator}heartbeat=5")
}

/// Redact credentials from an AMQP URI before logging.
#[must_use]
pub fn redact_amqp_uri(uri: &str) -> String {
    let Some((scheme, rest)) = uri.split_once("://") else {
        return uri.to_string();
    };
    let Some((_, host)) = rest.rsplit_once('@') else {
        return uri.to_string();
    };
    format!("{scheme}://***:***@{host}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uri_redaction_hides_credentials() {
        assert_eq!(
            redact_amqp_uri("amqp://alice:secret@rabbit:5672/vhost"),
            "amqp://***:***@rabbit:5672/vhost"
        );
        assert_eq!(redact_amqp_uri("amqp://rabbit/v"), "amqp://rabbit/v");
    }

    #[test]
    fn heartbeat_is_added_once() {
        assert_eq!(
            with_heartbeat("amqp://rabbit/v"),
            "amqp://rabbit/v?heartbeat=5"
        );
        assert_eq!(
            with_heartbeat("amqp://rabbit/v?connection_timeout=30"),
            "amqp://rabbit/v?connection_timeout=30&heartbeat=5"
        );
        assert_eq!(
            with_heartbeat("amqp://rabbit/v?heartbeat=10"),
            "amqp://rabbit/v?heartbeat=10"
        );
    }

    #[test]
    fn frozen_ttls_match_go() {
        assert_eq!(DEFAULT_MESSAGE_TTL, "60000");
        assert_eq!(REPLY_MESSAGE_TTL, "180000");
        assert_eq!(PUSH_MAX_RETRIES, 3);
        assert_eq!(PUSH_TIMEOUT, Duration::from_secs(15));
    }
}
