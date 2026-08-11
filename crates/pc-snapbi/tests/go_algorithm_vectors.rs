//! Grades the three SNAP-BI algorithms that had only roundtrip tests against
//! vectors captured from the real Go helper.
//!
//! A roundtrip proves the Rust signer and the Rust verifier agree with each
//! other, which they will even when both are wrong about Go. These do not.
//!
//! # Provenance
//!
//! `vectors/snapbi_go_vectors.json` was produced on 2026-08-11 by a Go program
//! that called `helpers.SignatureGenerate`, `helpers.EncryptAES` and
//! `golang-jwt/jwt/v5` in `paycloud-be-qoinhubinterface-manager` directly — the
//! production code path, not a reimplementation of it. That is what makes these
//! vectors an oracle rather than a second opinion from the same source.
//!
//! **The capture program deliberately does not live in this repo.** A Go module
//! here would sit outside `cargo-deny`/`cargo-audit` coverage, go uncompiled by
//! a Rust-only CI, and hard-code a relative path to one specific consumer
//! service — and this is a library many services consume. It belongs beside the
//! Go code it captures.
//!
//! To regenerate or extend: write a `main` in the Go service module that builds
//! the struct below, calls those three helpers, and writes JSON to a file
//! (a file, not stdout — the helper package's `init()` prints config warnings
//! there). The fixture's `oracle` field must keep naming the Go source, which
//! [`fixture`] asserts.

use serde::Deserialize;

#[derive(Deserialize)]
struct Fixture {
    oracle: String,
    private_key_pkcs1_pem: String,
    public_key_pkix_pem: String,
    rsa_pkcs1v15_sha256: Vec<RsaVector>,
    aes_256_gcm_decrypt: Vec<AesVector>,
    aes_non32_key_go_ciphertext: Vec<AesVector>,
    aes_key_size_policy: Vec<AesPolicy>,
    jwt_rs256: Vec<JwtVector>,
}

#[derive(Deserialize)]
struct RsaVector {
    name: String,
    client_key: String,
    timestamp: String,
    string_to_sign: String,
    signature: String,
}

#[derive(Deserialize)]
struct AesVector {
    name: String,
    key: String,
    plaintext: String,
    ciphertext: String,
}

#[derive(Deserialize)]
struct AesPolicy {
    name: String,
    key: String,
    key_len: usize,
    ok: bool,
}

#[derive(Deserialize)]
struct JwtVector {
    name: String,
    token: String,
}

fn fixture() -> Fixture {
    let raw = include_str!("vectors/snapbi_go_vectors.json");
    let fixture: Fixture = serde_json::from_str(raw).expect("valid fixture");
    assert!(
        fixture.oracle.contains("qoinhub Go helpers"),
        "the fixture must name the Go code it came from"
    );
    fixture
}

#[test]
fn go_rsa_signature_vectors_are_byte_exact() {
    let fixture = fixture();
    assert!(!fixture.rsa_pkcs1v15_sha256.is_empty());

    for vector in &fixture.rsa_pkcs1v15_sha256 {
        // PKCS#1 v1.5 is deterministic, so this is an exact match against Go's
        // `helpers.SignatureGenerate`, not a roundtrip.
        let signed = pc_snapbi::rsa_sign(
            &fixture.private_key_pkcs1_pem,
            &vector.client_key,
            &vector.timestamp,
        )
        .unwrap_or_else(|error| panic!("{}: {error}", vector.name));
        assert_eq!(signed, vector.signature, "{}", vector.name);

        // And the layout the signature was taken over is the one Go built.
        assert_eq!(
            format!("{}|{}", vector.client_key, vector.timestamp),
            vector.string_to_sign,
            "{}",
            vector.name
        );

        assert!(
            pc_snapbi::rsa_verify(
                &fixture.public_key_pkix_pem,
                &vector.client_key,
                &vector.timestamp,
                &vector.signature,
            )
            .unwrap_or_else(|error| panic!("{}: {error}", vector.name)),
            "{}",
            vector.name
        );
    }
}

#[test]
fn go_rsa_signatures_are_rejected_when_tampered() {
    let fixture = fixture();
    let vector = &fixture.rsa_pkcs1v15_sha256[0];

    // Same signature, different string-to-sign.
    assert!(!pc_snapbi::rsa_verify(
        &fixture.public_key_pkix_pem,
        &vector.client_key,
        "2000-01-01T00:00:00Z",
        &vector.signature,
    )
    .unwrap());
}

#[test]
fn go_written_ciphertext_is_readable_by_rust() {
    let fixture = fixture();
    assert!(!fixture.aes_256_gcm_decrypt.is_empty());

    for vector in &fixture.aes_256_gcm_decrypt {
        // Decrypt direction only: `EncryptAES` uses a random nonce, so Go's
        // ciphertext for a given plaintext is not reproducible.
        let decrypted = pc_snapbi::decrypt_aes(vector.key.as_bytes(), &vector.ciphertext)
            .unwrap_or_else(|error| panic!("{}: {error}", vector.name));
        assert_eq!(decrypted, vector.plaintext, "{}", vector.name);
    }
}

/// **Known divergence, pinned so it cannot change unnoticed.**
///
/// Go's `EncryptAES(plaintext, secret)` passes a non-empty `secret` to
/// `aes.NewCipher` **raw**: 16/24/32 bytes succeed, anything else fails with
/// `crypto/aes: invalid key size N`. The SHA-256 derivation only happens in
/// `getAESKey`, which serves the *env default* path (`secret == ""`).
///
/// `pc_snapbi` collapses both into one function that always derives, so it
/// accepts key lengths Go rejects. For a 16- or 24-byte key both sides succeed
/// and produce **different ciphertext** — Go uses the raw key at AES-128/192,
/// Rust uses SHA256(key) at AES-256. Neither errors; neither can read the
/// other.
///
/// Reachable through `helpers.DecryptAES(dmDynamicPrivKey, secretKey)` in
/// `services/BiAccessTokenB2b.go`, where the secret is merchant-supplied.
#[test]
fn aes_key_size_policy_divergence_from_go_is_pinned() {
    let fixture = fixture();

    for policy in &fixture.aes_key_size_policy {
        let rust_ok = pc_snapbi::encrypt_aes(policy.key.as_bytes(), "probe").is_ok();

        // Rust never rejects a key: every length is hashed into 32 bytes.
        assert!(rust_ok, "{}: Rust unexpectedly rejected a key", policy.name);

        if policy.key_len == 32 {
            // The one length where both sides use the same key material.
            assert!(policy.ok, "Go must accept a 32-byte key");
            continue;
        }

        if policy.ok {
            // 16 and 24: both sides succeed with DIFFERENT keys. This is the
            // silent-corruption case — assert it still exists so that fixing
            // `derive_aes_key` forces this test to be updated deliberately.
            assert!(
                matches!(policy.key_len, 16 | 24),
                "unexpected Go-accepted key length {}",
                policy.key_len
            );
        } else {
            // 5 and 48: Go errors, Rust silently succeeds.
            assert!(
                !matches!(policy.key_len, 16 | 24 | 32),
                "Go rejected a valid AES key length"
            );
        }
    }
}

/// The divergence above, demonstrated rather than argued: real ciphertext that
/// Go produced with a 16- and a 24-byte secret, which `pc_snapbi` cannot read.
///
/// Go encrypted at AES-128/192 with the raw secret; `decrypt_aes` hashes the
/// same secret to 32 bytes and attempts AES-256, so GCM authentication fails.
/// If `derive_aes_key` is ever changed to match Go, this test starts failing —
/// which is the point.
#[test]
fn rust_cannot_read_go_ciphertext_written_with_a_non_32_byte_key() {
    let fixture = fixture();
    assert_eq!(fixture.aes_non32_key_go_ciphertext.len(), 2);

    for vector in &fixture.aes_non32_key_go_ciphertext {
        let result = pc_snapbi::decrypt_aes(vector.key.as_bytes(), &vector.ciphertext);
        assert!(
            result.is_err(),
            "{}: pc-snapbi read ciphertext it should not be able to read — \
             derive_aes_key now matches Go, so update this test and the \
             divergence note on aes_key_size_policy_divergence_from_go_is_pinned",
            vector.name
        );
    }
}

#[test]
fn go_issued_callback_token_verifies() {
    let fixture = fixture();
    let vector = fixture
        .jwt_rs256
        .iter()
        .find(|vector| vector.name == "callback-token-rs256")
        .expect("RS256 vector present");

    let claims = pc_snapbi::verify_jwt_rs256(&fixture.public_key_pkix_pem, &vector.token)
        .expect("a Go-issued RS256 token must verify");
    assert_eq!(
        claims.extra.get("clientKey").and_then(|v| v.as_str()),
        Some("PAYCLOUD-CLIENT-KEY")
    );
}

#[test]
fn algorithm_confusion_token_is_rejected() {
    let fixture = fixture();
    let vector = fixture
        .jwt_rs256
        .iter()
        .find(|vector| vector.name == "algorithm-confusion-hs256-must-reject")
        .expect("HS256 vector present");

    // Signed HS256 using the RSA *public* key as the shared secret — the
    // classic confusion attack. A verifier that does not pin the algorithm
    // accepts it, because the public key is not secret.
    assert!(
        pc_snapbi::verify_jwt_rs256(&fixture.public_key_pkix_pem, &vector.token).is_err(),
        "an HS256 token must never satisfy an RS256 verifier"
    );
}
