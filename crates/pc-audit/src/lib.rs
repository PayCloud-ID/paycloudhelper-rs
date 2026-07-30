#![forbid(unsafe_code)]
//! Audit-trail entities and bounded asynchronous publisher.
//!
//! The JSON field names, command strings, timestamp layouts, unescaped HTML,
//! trailing newline, monotonic IDs, worker defaults, and circuit-breaker
//! thresholds mirror the Go helper.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pc_amqp::Publisher;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{mpsc, Mutex};

/// Audit process command.
pub const CMD_PROCESS: &str = "audit-trail-process";
/// Audit request/response command.
pub const CMD_DATA: &str = "audit-trail-data";
/// Transaction lifecycle command.
pub const CMD_TRX: &str = "audit-trail-trx";

pub const TRX_STATE_REQUEST_RECEIVED: &str = "request_received";
pub const TRX_STATE_REQUEST_VALIDATED: &str = "request_validated";
pub const TRX_STATE_ORDER_CREATED: &str = "order_created";
pub const TRX_STATE_CHANNEL_SELECTED: &str = "channel_selected";
pub const TRX_STATE_CHANNEL_PROCESSED: &str = "channel_processed";
pub const TRX_STATE_VENDOR_REQUEST_SENT: &str = "vendor_request_sent";
pub const TRX_STATE_VENDOR_TOKEN_ACQUIRED: &str = "vendor_token_acquired";
pub const TRX_STATE_QR_GENERATED: &str = "qr_generated";
pub const TRX_STATE_VENDOR_REQUEST_FAILED: &str = "vendor_request_failed";
pub const TRX_STATE_TRANSACTION_UPDATED: &str = "transaction_updated";
pub const TRX_STATE_PAYMENT_NOTIFIED: &str = "payment_notified";
pub const TRX_STATE_RESPONSE_RETURNED: &str = "response_returned";
pub const TRX_STATE_ORDER_EXPIRED: &str = "order_expired";
pub const TRX_STATE_PAYMENT_RECEIVED: &str = "payment_received";
pub const TRX_STATE_STATUS_CHECKED: &str = "status_checked";

pub const TRX_STATUS_PROCESSING: &str = "processing";
pub const TRX_STATUS_SUCCESS: &str = "success";
pub const TRX_STATUS_FAILED: &str = "failed";
pub const TRX_STATUS_EXPIRED: &str = "expired";

static NEXT_ID: AtomicI64 = AtomicI64::new(0);

/// Return the next process-wide, monotonically increasing audit ID.
#[must_use]
pub fn next_audit_id() -> i64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed) + 1
}

/// RabbitMQ envelope shared by V1, V2, and transaction audit messages.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct MessagePayloadAudit {
    pub id: i64,
    pub command: String,
    pub time: String,
    pub module_id: String,
    pub data: Value,
}

impl MessagePayloadAudit {
    /// Build an envelope with the current app identity and Go `time.DateTime`.
    pub fn new(command: &str, data: impl Serialize) -> Result<Self, serde_json::Error> {
        Ok(Self {
            id: next_audit_id(),
            command: command.to_string(),
            time: datetime_now(),
            module_id: pc_core_name(),
            data: serde_json::to_value(data)?,
        })
    }
}

/// Process audit body.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct AuditTrailProcess {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub subject: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub function: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub description: String,
    pub key: Option<Vec<String>>,
    pub data: DataAuditTrailProcess,
}

/// Process detail nested in [`AuditTrailProcess`].
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DataAuditTrailProcess {
    pub time: String,
    pub info: String,
}

/// Request/response audit body.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct AuditTrailData {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub subject: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub function: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub description: String,
    pub key: Option<Vec<String>>,
    pub source: String,
    pub communication_type: String,
    pub data: RequestAndResponse,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct RequestAndResponse {
    pub request: RequestAudit,
    pub response: ResponseAudit,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct RequestAudit {
    pub time: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_string: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub header: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub param: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<Value>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub ip_address: String,
    #[serde(skip_serializing_if = "is_zero_i64")]
    pub browser_id: i64,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub latitude: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub longitude: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ResponseAudit {
    pub time: String,
    pub detail: Detail,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Detail {
    pub status_code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

/// One transaction lifecycle event. JSON names intentionally use lower camel
/// case, matching `AuditTrailTrx`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditTrailTrx {
    pub reff_no: String,
    pub order_no: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub ticket_id: String,
    pub status: String,
    pub state: String,
    pub message: String,
    pub service: String,
    pub function: String,
    pub description: String,
    pub communication_type: String,
    pub event_time: String,
    #[serde(skip_serializing_if = "is_zero_i64")]
    pub duration_ms: i64,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub amount: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub currency: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub merchant_no: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub payment_code: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub qr_value: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub rrn: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub error_code: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub vendor_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response: Option<Value>,
    #[serde(skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub metadata: std::collections::BTreeMap<String, Value>,
    #[serde(default, serialize_with = "serialize_created_at")]
    pub created_at: String,
}

#[allow(clippy::trivially_copy_pass_by_ref)] // serde skip predicate requires `&T`.
fn is_zero_i64(value: &i64) -> bool {
    *value == 0
}

fn serialize_created_at<S>(value: &str, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(if value.is_empty() {
        "0001-01-01T00:00:00Z"
    } else {
        value
    })
}

/// Construct a validated process envelope. Empty function/description is
/// rejected just as `LogAudittrailProcess` silently skips it.
pub fn process_payload(
    function: &str,
    description: &str,
    info: &str,
    keys: Option<Vec<String>>,
) -> Result<Option<MessagePayloadAudit>, serde_json::Error> {
    if function.is_empty() || description.is_empty() {
        return Ok(None);
    }
    MessagePayloadAudit::new(
        CMD_PROCESS,
        AuditTrailProcess {
            subject: pc_core_name(),
            function: function.to_string(),
            description: description.to_string(),
            key: keys,
            data: DataAuditTrailProcess {
                time: datetime_now(),
                info: info.to_string(),
            },
        },
    )
    .map(Some)
}

/// Construct a validated request/response envelope.
pub fn data_payload(
    function: &str,
    description: &str,
    source: &str,
    communication_type: &str,
    keys: Option<Vec<String>>,
    data: RequestAndResponse,
) -> Result<Option<MessagePayloadAudit>, serde_json::Error> {
    if function.is_empty() || data.response.detail.status_code == 0 {
        return Ok(None);
    }
    MessagePayloadAudit::new(
        CMD_DATA,
        AuditTrailData {
            subject: pc_core_name(),
            function: function.to_string(),
            description: description.to_string(),
            key: keys,
            source: source.to_string(),
            communication_type: communication_type.to_string(),
            data,
        },
    )
    .map(Some)
}

/// Construct a transaction envelope. Both identifiers empty means skip.
pub fn trx_payload(
    mut data: AuditTrailTrx,
) -> Result<Option<MessagePayloadAudit>, serde_json::Error> {
    if data.reff_no.is_empty() && data.order_no.is_empty() {
        return Ok(None);
    }
    if data.service.is_empty() {
        data.service = pc_core_name();
    }
    if data.event_time.is_empty() {
        data.event_time = rfc3339_now();
    }
    MessagePayloadAudit::new(CMD_TRX, data).map(Some)
}

/// Worker-pool configuration.
#[derive(Clone, Debug)]
pub struct AuditPublisherConfig {
    pub worker_count: usize,
    pub buffer_size: usize,
    pub message_ttl: String,
    pub circuit_breaker_threshold: usize,
    pub circuit_breaker_cooldown: Duration,
}

impl Default for AuditPublisherConfig {
    fn default() -> Self {
        Self {
            worker_count: 10,
            buffer_size: 1000,
            message_ttl: String::new(),
            circuit_breaker_threshold: 10,
            circuit_breaker_cooldown: Duration::from_secs(30),
        }
    }
}

/// Non-blocking audit worker pool.
pub struct AuditPublisher {
    tx: mpsc::Sender<MessagePayloadAudit>,
    failures: Arc<AtomicUsize>,
    open_until_ms: Arc<AtomicU64>,
    stopped: Arc<AtomicBool>,
    workers: Vec<tokio::task::JoinHandle<()>>,
}

impl AuditPublisher {
    /// Start workers immediately.
    #[must_use]
    #[allow(clippy::needless_pass_by_value)] // ownership transfers to worker tasks.
    pub fn new(client: Arc<dyn Publisher>, config: &AuditPublisherConfig) -> Self {
        let workers_count = config.worker_count.max(1);
        let (tx, rx) = mpsc::channel::<MessagePayloadAudit>(config.buffer_size.max(1));
        let rx = Arc::new(Mutex::new(rx));
        let failures = Arc::new(AtomicUsize::new(0));
        let open_until_ms = Arc::new(AtomicU64::new(0));
        let stopped = Arc::new(AtomicBool::new(false));
        let mut workers = Vec::with_capacity(workers_count);

        for _ in 0..workers_count {
            let client = Arc::clone(&client);
            let rx = Arc::clone(&rx);
            let failures = Arc::clone(&failures);
            let open_until_ms = Arc::clone(&open_until_ms);
            let ttl = config.message_ttl.clone();
            let threshold = config.circuit_breaker_threshold.max(1);
            let cooldown = config.circuit_breaker_cooldown;
            workers.push(tokio::spawn(async move {
                loop {
                    let Some(payload) = rx.lock().await.recv().await else {
                        break;
                    };
                    if now_millis() < open_until_ms.load(Ordering::Acquire) {
                        continue;
                    }
                    if !client.ready() {
                        record_failure(&failures, &open_until_ms, threshold, cooldown);
                        continue;
                    }
                    let bytes = match pc_json::marshal_audit(&payload) {
                        Ok(bytes) => bytes,
                        Err(err) => {
                            tracing::error!(error = %err, "[AuditPublisher] marshal failed");
                            continue;
                        }
                    };
                    match client.publish(&bytes, &ttl).await {
                        Ok(()) => failures.store(0, Ordering::Release),
                        Err(err) => {
                            tracing::error!(id = payload.id, error = %err, "[AuditPublisher] push failed");
                            record_failure(&failures, &open_until_ms, threshold, cooldown);
                        }
                    }
                }
            }));
        }

        Self {
            tx,
            failures,
            open_until_ms,
            stopped,
            workers,
        }
    }

    /// Queue a message without blocking. Returns false when stopped, full, or
    /// while the breaker is open.
    pub fn submit(&self, payload: MessagePayloadAudit) -> bool {
        if self.stopped.load(Ordering::Acquire) || self.circuit_open() {
            return false;
        }
        self.tx.try_send(payload).is_ok()
    }

    #[must_use]
    pub fn circuit_open(&self) -> bool {
        now_millis() < self.open_until_ms.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn consecutive_failures(&self) -> usize {
        self.failures.load(Ordering::Acquire)
    }

    /// Close input and wait for queued messages to drain.
    pub async fn stop(mut self) {
        self.stopped.store(true, Ordering::Release);
        drop(self.tx);
        for worker in self.workers.drain(..) {
            let _ = worker.await;
        }
    }
}

fn record_failure(
    failures: &AtomicUsize,
    open_until_ms: &AtomicU64,
    threshold: usize,
    cooldown: Duration,
) {
    let count = failures.fetch_add(1, Ordering::AcqRel) + 1;
    if count >= threshold {
        let until =
            now_millis().saturating_add(u64::try_from(cooldown.as_millis()).unwrap_or(u64::MAX));
        open_until_ms.store(until, Ordering::Release);
    }
}

fn pc_core_name() -> String {
    pc_core::identity::app_name()
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn local_now() -> time::OffsetDateTime {
    let utc = time::OffsetDateTime::now_utc();
    time::UtcOffset::current_local_offset().map_or(utc, |offset| utc.to_offset(offset))
}

fn datetime_now() -> String {
    local_now()
        .format(time::macros::format_description!(
            "[year]-[month]-[day] [hour]:[minute]:[second]"
        ))
        .unwrap_or_default()
}

fn rfc3339_now() -> String {
    local_now()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex as StdMutex;

    #[derive(Default)]
    struct MockPublisher {
        messages: StdMutex<Vec<Vec<u8>>>,
        fail: AtomicBool,
    }

    #[async_trait]
    impl Publisher for MockPublisher {
        async fn publish(&self, data: &[u8], _ttl_ms: &str) -> anyhow::Result<()> {
            if self.fail.load(Ordering::Acquire) {
                anyhow::bail!("offline");
            }
            self.messages.lock().unwrap().push(data.to_vec());
            Ok(())
        }

        fn ready(&self) -> bool {
            true
        }
    }

    #[test]
    fn command_constants_are_frozen() {
        assert_eq!(CMD_PROCESS, "audit-trail-process");
        assert_eq!(CMD_DATA, "audit-trail-data");
        assert_eq!(CMD_TRX, "audit-trail-trx");
    }

    #[test]
    fn envelope_json_uses_pascal_case_and_newline() {
        let payload = MessagePayloadAudit {
            id: 7,
            command: CMD_PROCESS.to_string(),
            time: "2026-07-30 10:00:00".to_string(),
            module_id: "svc".to_string(),
            data: serde_json::json!({"html":"<tag>"}),
        };
        let encoded = pc_json::marshal_audit(&payload).unwrap();
        assert!(encoded.ends_with(b"\n"));
        let text = String::from_utf8(encoded).unwrap();
        assert!(text.contains(r#""Id":7"#));
        assert!(text.contains(r#""Command":"audit-trail-process""#));
        assert!(text.contains("<tag>"));
    }

    #[test]
    fn trx_camel_case_fields_match_go() {
        let value = serde_json::to_value(AuditTrailTrx {
            reff_no: "R1".to_string(),
            order_no: "O1".to_string(),
            duration_ms: 8,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(value["reffNo"], "R1");
        assert_eq!(value["orderNo"], "O1");
        assert_eq!(value["durationMs"], 8);
        assert_eq!(value["createdAt"], "0001-01-01T00:00:00Z");
    }

    #[test]
    fn nil_keys_and_zero_detail_match_go_encoding_json() {
        let process = serde_json::to_value(AuditTrailProcess {
            key: None,
            ..AuditTrailProcess::default()
        })
        .unwrap();
        assert!(process["Key"].is_null());

        let response = serde_json::to_value(ResponseAudit::default()).unwrap();
        assert_eq!(response["Detail"]["StatusCode"], 0);
        assert_eq!(response["Detail"]["Message"], "");
    }

    #[tokio::test]
    async fn workers_publish_audit_profile() {
        let mock = Arc::new(MockPublisher::default());
        let publisher = AuditPublisher::new(
            mock.clone(),
            &AuditPublisherConfig {
                worker_count: 1,
                buffer_size: 2,
                ..Default::default()
            },
        );
        assert!(publisher.submit(
            MessagePayloadAudit::new(CMD_PROCESS, serde_json::json!({"ok": true})).unwrap()
        ));
        tokio::time::sleep(Duration::from_millis(20)).await;
        publisher.stop().await;
        let messages = mock.messages.lock().unwrap();
        assert_eq!(messages.len(), 1);
        assert!(messages[0].ends_with(b"\n"));
    }

    #[tokio::test]
    async fn breaker_opens_at_ten_failures() {
        let mock = Arc::new(MockPublisher::default());
        mock.fail.store(true, Ordering::Release);
        let publisher = AuditPublisher::new(
            mock,
            &AuditPublisherConfig {
                worker_count: 1,
                buffer_size: 16,
                circuit_breaker_cooldown: Duration::from_secs(30),
                ..Default::default()
            },
        );
        for id in 1..=10 {
            assert!(publisher.submit(MessagePayloadAudit {
                id,
                command: CMD_PROCESS.to_string(),
                time: String::new(),
                module_id: String::new(),
                data: Value::Null,
            }));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(publisher.circuit_open());
        assert_eq!(publisher.consecutive_failures(), 10);
        publisher.stop().await;
    }
}
