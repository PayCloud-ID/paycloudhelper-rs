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

/// Regression vectors for the minified-body SNAP-BI layout.
///
/// Named `go_*` for history; these are **not** Go-captured. See the fixture's
/// `oracle` field — the minified-body signature could only have been produced by
/// hashing the compacted body, which Go's `SymmetricSignatureGen` does not do.
/// The genuinely Go-sourced vectors are in `go_algorithm_vectors.rs`.
#[test]
fn go_symmetric_signature_vectors_are_byte_exact() {
    let fixture: Fixture = serde_json::from_str(include_str!("vectors/symmetric_signatures.json"))
        .expect("valid fixture");
    // Pin the provenance label, not just the numbers. These were mislabelled as
    // Go-captured through a release; a signature nobody can trace to an oracle
    // is a regression test for whatever the code did that day, and it should say
    // so out loud rather than borrowing authority it does not have.
    assert!(
        fixture.oracle.contains("Rust self-consistency"),
        "these vectors are Rust-computed; do not relabel them as Go-sourced \
         without actually capturing them from Go"
    );
    assert!(fixture.oracle.contains("MINIFIED body hash"));
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
