use std::collections::BTreeMap;

use pc_audit::{AuditTrailTrx, MessagePayloadAudit, CMD_TRX};

#[test]
fn go_transaction_audit_json_is_byte_exact() {
    let payload = MessagePayloadAudit {
        id: 7,
        command: CMD_TRX.to_string(),
        time: "2026-07-30 10:00:00".to_string(),
        module_id: "svc".to_string(),
        data: serde_json::to_value(AuditTrailTrx {
            reff_no: "R-1".to_string(),
            order_no: "O-1".to_string(),
            status: "success".to_string(),
            state: "order_created".to_string(),
            message: "done".to_string(),
            service: "svc".to_string(),
            function: "CreateOrder".to_string(),
            description: "order persisted".to_string(),
            communication_type: "grpc".to_string(),
            event_time: "2026-07-30T10:00:00+07:00".to_string(),
            duration_ms: 8,
            amount: "1000".to_string(),
            currency: "IDR".to_string(),
            request: Some(serde_json::json!({"order": "O-1"})),
            metadata: BTreeMap::from([("retryCount".to_string(), serde_json::json!(2))]),
            created_at: "2026-07-30T10:00:00+07:00".to_string(),
            ..AuditTrailTrx::default()
        })
        .unwrap(),
    };

    let actual = pc_json::marshal_audit(&payload).unwrap();
    assert_eq!(
        actual,
        include_bytes!("vectors/audit_trx.json"),
        "must match Go encoding/json with SetEscapeHTML(false)"
    );
}
