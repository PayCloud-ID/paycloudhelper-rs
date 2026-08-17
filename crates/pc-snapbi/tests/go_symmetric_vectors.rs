//! Genuine Go oracle vectors for the SNAP-BI symmetric signature.
//!
//! These were produced by calling the real exported Go signer,
//! `helpers.SymmetricSignatureGen` (`helpers/signature.go:187-198`), from a program run inside the
//! `paycloud-be-qoinhubinterface-manager` module. The capture source is checked in beside the fixture at
//! `tests/vectors/capture/gen_symmetric_go_vectors.go` so this is reproducible without reconstructing it
//! from prose.
//!
//! # Why this file exists separately from `golden_vectors.rs`
//!
//! `symmetric_signatures.json` is **Rust self-consistency data** — it says so in its own `oracle` field.
//! It pins that this crate does not change, but it cannot detect this crate having been wrong from the
//! start. Only a vector produced by Go can do that, and until now the symmetric layout had none. That gap
//! is what let a mislabelled fixture claim Go provenance through a release.
//!
//! # The raw-vs-minified split, and why some vectors must NOT match
//!
//! Go has three copies of the string-to-sign and they disagree on the body hash: `SignatureService` and
//! `ValidateSignatureSnap` **minify**, `SymmetricSignatureGen` hashes **raw**. This crate always minifies,
//! which is right for the live paths — outbound bodies are built by `serde_json::to_vec` and minifying
//! compact JSON is the identity.
//!
//! Only `SymmetricSignatureGen` is both exported and raw-hashing, so it is the only one of the three a
//! generator can drive through a real Go entry point (`SignatureService` is an unexported method on a
//! private struct; reimplementing it in the generator would make the "oracle" an oracle of nothing).
//!
//! So for a body that is *not* already compact, the Go vector and this crate **must differ**. Asserting
//! equality there would be a lie; skipping those vectors would hide the divergence. Instead each vector's
//! expectation is derived from the rule itself — see [`rust_and_go_hash_the_same_bytes`] — so the test
//! states *why* it expects agreement or disagreement rather than hardcoding a verdict per vector.

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

/// The two implementations hash the same bytes exactly when minifying the body is a no-op.
///
/// Go hashes `body` raw. This crate hashes `json_minify(body).unwrap_or_default()` — and the
/// `unwrap_or_default` matters: Go's `JsonMinify` also returns `[]byte{}` on a compact error
/// (`helpers/string.go:77-92`), so an empty body agrees while invalid JSON does not.
fn rust_and_go_hash_the_same_bytes(body: &[u8]) -> bool {
    pc_core::json_minify(body).unwrap_or_default() == body
}

fn fixture() -> Fixture {
    serde_json::from_str(include_str!("vectors/symmetric_go_vectors.json")).expect("valid fixture")
}

#[test]
fn go_symmetric_vectors_pin_agreement_and_divergence() {
    let fixture = fixture();

    // Provenance is asserted, not decorative: these ARE Go-sourced, and the claim must not be
    // transplanted onto Rust-computed data the way it was for `symmetric_signatures.json`.
    assert!(
        fixture.oracle.contains("helpers.SymmetricSignatureGen"),
        "these vectors must come from the real Go signer"
    );
    assert!(fixture.oracle.contains("RAW body hash path"));
    assert_eq!(fixture.captured_at, "2026-08-17");
    assert_eq!(fixture.vectors.len(), 4, "all four cases must stay covered");

    let mut agreed = 0;
    let mut diverged = 0;

    for vector in &fixture.vectors {
        let body = vector.body.as_bytes();
        let ours = pc_snapbi::symmetric_sign(
            vector.secret.as_bytes(),
            &vector.method,
            &vector.url,
            &vector.token,
            body,
            &vector.timestamp,
        );

        if rust_and_go_hash_the_same_bytes(body) {
            agreed += 1;
            assert_eq!(
                ours, vector.signature,
                "[{}] minifying this body is a no-op, so this crate must reproduce Go byte-for-byte. \
                 A mismatch here means the live outbound signing path is broken.",
                vector.name
            );
        } else {
            diverged += 1;
            assert_ne!(
                ours, vector.signature,
                "[{}] Go hashes the raw body and this crate hashes the minified one, so these cannot \
                 match. If they now do, the minify was removed — which silently breaks the \
                 signature-service endpoint and inbound callback verification, both of which need it.",
                vector.name
            );
        }
    }

    // Guard the guard: if every vector landed on one side, the test has stopped covering the split it
    // exists to cover.
    assert!(
        agreed >= 2,
        "expected at least two agreeing vectors, got {agreed}"
    );
    assert!(
        diverged >= 2,
        "expected at least two diverging vectors, got {diverged}"
    );
}

/// The agreeing case in isolation, because it is the one that maps to production: every outbound caller
/// builds its body with `serde_json::to_vec`, whose output is already compact.
#[test]
fn compact_body_matches_go_byte_for_byte() {
    let fixture = fixture();
    let vector = fixture
        .vectors
        .iter()
        .find(|v| v.name == "compact-body")
        .expect("compact-body vector present");

    assert!(
        rust_and_go_hash_the_same_bytes(vector.body.as_bytes()),
        "the compact-body vector stopped being compact"
    );
    assert_eq!(
        pc_snapbi::symmetric_sign(
            vector.secret.as_bytes(),
            &vector.method,
            &vector.url,
            &vector.token,
            vector.body.as_bytes(),
            &vector.timestamp,
        ),
        vector.signature
    );
}

/// Verification must accept a signature Go produced over a compact body — this is the inbound direction,
/// and `symmetric_verify` is a separate code path from `symmetric_sign`.
#[test]
fn symmetric_verify_accepts_a_go_produced_signature() {
    let fixture = fixture();
    for vector in fixture
        .vectors
        .iter()
        .filter(|v| rust_and_go_hash_the_same_bytes(v.body.as_bytes()))
    {
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
            "[{}] a Go-produced signature over a compact body must verify",
            vector.name
        );
    }
}
