use serde::Deserialize;

#[derive(Deserialize)]
struct Fixture {
    oracle: String,
    captured_at: String,
    vectors: Vec<Vector>,
}

#[derive(Deserialize)]
struct Vector {
    name: String,
    secret: String,
    method: String,
    url: String,
    token: String,
    body: String,
    timestamp: String,
    signature: String,
}

#[test]
fn go_symmetric_signature_vectors_are_byte_exact() {
    let fixture: Fixture = serde_json::from_str(include_str!("vectors/symmetric_signatures.json"))
        .expect("valid fixture");
    assert!(fixture.oracle.contains("Go crypto/hmac"));
    assert_eq!(fixture.captured_at, "2026-07-30");

    for vector in fixture.vectors {
        let actual = pc_snapbi::symmetric_sign(
            vector.secret.as_bytes(),
            &vector.method,
            &vector.url,
            &vector.token,
            vector.body.as_bytes(),
            &vector.timestamp,
        );
        assert_eq!(actual, vector.signature, "{}", vector.name);
        assert!(
            pc_snapbi::symmetric_verify(
                vector.secret.as_bytes(),
                &vector.method,
                &vector.url,
                &vector.token,
                vector.body.as_bytes(),
                &vector.timestamp,
                &vector.signature,
            ),
            "{}",
            vector.name
        );
    }
}
