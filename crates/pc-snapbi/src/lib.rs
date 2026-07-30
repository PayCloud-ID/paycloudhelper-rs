#![forbid(unsafe_code)]
//! `pc-snapbi` — the SNAP-BI crypto suite.
//!
//! Bit-for-bit Rust port of the Go signature/crypto helpers in
//! `paycloud-be-qoinhubinterface-manager/helpers` and
//! `github.com/PayCloud-ID/paycloudhelper`. Every string-to-sign layout,
//! hash and encoding mirrors the reference Go exactly — the outputs are
//! graded byte-for-byte against Go.
//!
//! Go symbols mirrored:
//! - [`symmetric_sign`] ⇔ `helpers.SymmetricSignatureGen`
//! - [`symmetric_verify`] ⇔ `helpers.ValidateSignatureSnap`
//! - [`rsa_sign`] ⇔ `helpers.SignatureGenerate` / `services.SignatureGenerate`
//! - [`rsa_verify`] ⇔ `helpers.ValidateSignature` (`rsaPublicKey.unsign`)
//! - [`public_key_from_pem`] ⇔ `adicrypto.PEMBytesToPublicKey` / `helpers.BytesToPublicKey`
//! - [`private_key_from_pem`] ⇔ `helpers.parsePrivateKey` / `helpers.ValidatePrivateKey`
//! - [`encrypt_aes`] ⇔ `helpers.EncryptAES` (+ `getAESKey`)
//! - [`decrypt_aes`] ⇔ `helpers.DecryptAES`
//! - [`verify_jwt_rs256`] ⇔ `middlewares.GetTokenClaims` / `paycloudhelper.RevokeToken`

use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256, Sha512};
use subtle::ConstantTimeEq;

use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};

use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs1v15::{Signature, SigningKey, VerifyingKey};
use rsa::pkcs8::DecodePublicKey;
use rsa::signature::{SignatureEncoding, Signer, Verifier};
use rsa::{RsaPrivateKey, RsaPublicKey};

use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};

type HmacSha512 = Hmac<Sha512>;

// ---------------------------------------------------------------------------
// Symmetric signature (HMAC-SHA512)
// ---------------------------------------------------------------------------

/// Assemble the SNAP-BI symmetric string-to-sign.
///
/// Mirrors the layout from Go `helpers.SymmetricSignatureGen` /
/// `helpers.ValidateSignatureSnap`:
///
/// ```text
/// METHOD:url:accessToken:lower(hex(sha256(json_minify(body)))):timestamp
/// ```
///
/// The body-hash segment is the lowercase hex of the SHA-256 of the
/// **minified** JSON body. Minification is delegated to
/// [`pc_core::json_minify`] (the byte-exact Go `json.Compact` twin); on a
/// minify error the body hashes to the empty input, mirroring Go's
/// `bb, _ := JsonMinify(...)` which discards the error.
fn symmetric_string_to_sign(method: &str, url: &str, token: &str, body: &[u8], ts: &str) -> String {
    let minified = pc_core::json_minify(body).unwrap_or_default();
    let digest = Sha256::digest(&minified);
    let blc = hex::encode(digest); // hex::encode is already lowercase
    format!("{method}:{url}:{token}:{blc}:{ts}")
}

/// Generate the SNAP-BI symmetric signature (HMAC-SHA512, base64 std).
///
/// Go: `helpers.SymmetricSignatureGen`. `secret` is the API secret key,
/// output is `base64.StdEncoding` of `HMAC_SHA512(secret, stringToSign)`.
pub fn symmetric_sign(
    secret: &[u8],
    method: &str,
    url: &str,
    token: &str,
    body: &[u8],
    ts: &str,
) -> String {
    let sts = symmetric_string_to_sign(method, url, token, body, ts);
    let mut mac =
        <HmacSha512 as Mac>::new_from_slice(secret).expect("HMAC accepts keys of any length");
    mac.update(sts.as_bytes());
    let out = mac.finalize().into_bytes();
    B64.encode(out)
}

/// Verify a SNAP-BI symmetric signature in constant time.
///
/// Go: `helpers.ValidateSignatureSnap` (which uses `hmac.Equal`). The
/// provided base64 signature is decoded and compared against the freshly
/// computed MAC using a constant-time comparison ([`subtle`]).
pub fn symmetric_verify(
    secret: &[u8],
    method: &str,
    url: &str,
    token: &str,
    body: &[u8],
    ts: &str,
    sig_b64: &str,
) -> bool {
    let sts = symmetric_string_to_sign(method, url, token, body, ts);
    let mut mac =
        <HmacSha512 as Mac>::new_from_slice(secret).expect("HMAC accepts keys of any length");
    mac.update(sts.as_bytes());
    let expected = mac.finalize().into_bytes();

    let Ok(provided) = B64.decode(sig_b64) else {
        return false;
    };
    // subtle's ConstantTimeEq for slices returns 0 on a length mismatch.
    expected.as_slice().ct_eq(provided.as_slice()).into()
}

// ---------------------------------------------------------------------------
// Asymmetric signature (RSA PKCS#1 v1.5 with SHA-256)
// ---------------------------------------------------------------------------

/// Build the SNAP-BI asymmetric string-to-sign: `X-CLIENT-KEY + "|" + X-TIMESTAMP`.
fn asymmetric_string_to_sign(client_key: &str, ts: &str) -> String {
    format!("{client_key}|{ts}")
}

/// Sign the SNAP-BI asymmetric string-to-sign with an RSA private key.
///
/// Go: `helpers.SignatureGenerate` / `services.SignatureGenerate`
/// (`rsaPrivateKey.Sign`). Private key is parsed as **PKCS#1**, the message
/// `client_key|ts` is hashed with SHA-256 and signed with RSA PKCS#1 v1.5;
/// the result is `base64.StdEncoding`.
pub fn rsa_sign(priv_pem: &str, client_key: &str, ts: &str) -> Result<String> {
    let key = private_key_from_pem(priv_pem)?;
    let signing_key = SigningKey::<Sha256>::new(key);
    let msg = asymmetric_string_to_sign(client_key, ts);
    let sig = signing_key.sign(msg.as_bytes());
    Ok(B64.encode(sig.to_bytes()))
}

/// Verify a SNAP-BI asymmetric signature with an RSA public key.
///
/// Go: `helpers.ValidateSignature` (`rsaPublicKey.unsign`). Public key is
/// parsed as **PKIX/SPKI**, message is `client_key|ts` hashed with SHA-256,
/// signature is base64-decoded and checked with RSA PKCS#1 v1.5.
pub fn rsa_verify(pub_pem: &str, client_key: &str, ts: &str, sig_b64: &str) -> Result<bool> {
    let key = public_key_from_pem(pub_pem)?;
    let verifying_key = VerifyingKey::<Sha256>::new(key);
    let msg = asymmetric_string_to_sign(client_key, ts);
    let raw = B64
        .decode(sig_b64)
        .context("asymmetric signature is not valid base64")?;
    let Ok(sig) = Signature::try_from(raw.as_slice()) else {
        return Ok(false);
    };
    Ok(verifying_key.verify(msg.as_bytes(), &sig).is_ok())
}

// ---------------------------------------------------------------------------
// PEM → RSA key parsing
// ---------------------------------------------------------------------------

/// Parse an RSA public key from a **PKIX/SPKI** PEM block (`-----BEGIN PUBLIC KEY-----`).
///
/// Go: `adicrypto.PEMBytesToPublicKey` → `x509.ParsePKIXPublicKey`, and the
/// `parsePublicKey` helpers which accept only the `PUBLIC KEY` block type.
/// The Go code has no PKCS#1 fallback, so neither does this.
pub fn public_key_from_pem(pem: &str) -> Result<RsaPublicKey> {
    RsaPublicKey::from_public_key_pem(pem).context("failed to parse PKIX/SPKI RSA public key")
}

/// Parse an RSA private key from a **PKCS#1** PEM block (`-----BEGIN RSA PRIVATE KEY-----`).
///
/// Go: `helpers.parsePrivateKey` / `helpers.ValidatePrivateKey` →
/// `x509.ParsePKCS1PrivateKey`, which accept only the `RSA PRIVATE KEY`
/// block type. The Go code has no PKCS#8 fallback, so neither does this.
pub fn private_key_from_pem(pem: &str) -> Result<RsaPrivateKey> {
    RsaPrivateKey::from_pkcs1_pem(pem).context("failed to parse PKCS#1 RSA private key")
}

// ---------------------------------------------------------------------------
// AES-256-GCM
// ---------------------------------------------------------------------------

/// Derive the AES-256 key: used directly if exactly 32 bytes, else `SHA256(key)`.
///
/// Go: `helpers.getAESKey`.
fn derive_aes_key(key: &[u8]) -> [u8; 32] {
    if key.len() == 32 {
        let mut k = [0u8; 32];
        k.copy_from_slice(key);
        k
    } else {
        Sha256::digest(key).into()
    }
}

/// Encrypt `plaintext` with AES-256-GCM and a random 12-byte nonce.
///
/// Go: `helpers.EncryptAES`. Output is `base64.StdEncoding(nonce ‖ ciphertext)`
/// where `ciphertext` already carries the GCM tag appended (as Go's `Seal` does).
pub fn encrypt_aes(key: &[u8], plaintext: &str) -> Result<String> {
    let derived = derive_aes_key(key);
    let cipher = Aes256Gcm::new_from_slice(&derived).map_err(|e| anyhow!("aes init: {e}"))?;

    let mut nonce_bytes = [0u8; 12];
    rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, plaintext.as_bytes())
        .map_err(|e| anyhow!("aes-gcm encrypt: {e}"))?;

    let mut out = Vec::with_capacity(nonce_bytes.len() + ciphertext.len());
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&ciphertext);
    Ok(B64.encode(out))
}

/// Decrypt `base64(nonce ‖ ciphertext)` produced by [`encrypt_aes`].
///
/// Go: `helpers.DecryptAES`. Faithful to the **PEM-passthrough branch**: if the
/// input is not valid base64 but *is* a valid PKCS#1 PEM RSA private key, the
/// input is returned unchanged (Go returns the raw key when base64 decoding
/// fails at byte 0 and `ValidatePrivateKey` succeeds).
pub fn decrypt_aes(key: &[u8], input: &str) -> Result<String> {
    let raw = match B64.decode(input) {
        Ok(raw) => raw,
        Err(err) => {
            // PEM-passthrough: a valid PKCS#1 private key PEM is returned as-is.
            if private_key_from_pem(input).is_ok() {
                return Ok(input.to_string());
            }
            return Err(err).context("decrypt_aes: input is neither base64 nor a PEM private key");
        }
    };

    let derived = derive_aes_key(key);
    let cipher = Aes256Gcm::new_from_slice(&derived).map_err(|e| anyhow!("aes init: {e}"))?;

    if raw.len() < 12 {
        return Err(anyhow!("ciphertext too short"));
    }
    let (nonce_bytes, ciphertext) = raw.split_at(12);
    let nonce = Nonce::from_slice(nonce_bytes);

    let plaintext = cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| anyhow!("aes-gcm decrypt: {e}"))?;
    String::from_utf8(plaintext).context("decrypted plaintext is not valid UTF-8")
}

// ---------------------------------------------------------------------------
// JWT (RS256 only, custom string `Expired` claim)
// ---------------------------------------------------------------------------

/// JWT claims carrying the custom string `Expired` field.
///
/// Go: the `jwt.MapClaims` consumed in `paycloudhelper.RevokeToken` /
/// `middlewares.GetTokenClaims`. `Expired` is a **string** formatted
/// `2006-01-02 15:04:05` (Go layout) / `%Y-%m-%d %H:%M:%S` (Rust), *not* a
/// numeric `exp`. Any other claims land in `extra`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    /// The `Expired` string claim (Go layout `2006-01-02 15:04:05`).
    #[serde(rename = "Expired", default)]
    pub expired: String,
    /// Any remaining claims (e.g. `MerchantId`).
    #[serde(flatten)]
    pub extra: std::collections::HashMap<String, serde_json::Value>,
}

/// Verify an **RS256** JWT and return its [`Claims`].
///
/// Go: `middlewares.GetTokenClaims` / `paycloudhelper.RevokeToken`. Only the
/// `RS256` algorithm is accepted (any other alg is rejected, mirroring the
/// `token.Method.(*jwt.SigningMethodRSA)` guard). The public key is a PKIX PEM
/// (`APP_PUBLIC_KEY` / vendor public key). Signature and alg are verified here;
/// the time-based `Expired`/revoke logic is left to the caller (as Go does it
/// outside the parse callback).
pub fn verify_jwt_rs256(pub_pem: &str, token: &str) -> Result<Claims> {
    let key = DecodingKey::from_rsa_pem(pub_pem.as_bytes())
        .context("failed to build RS256 decoding key from PEM")?;

    let mut validation = Validation::new(Algorithm::RS256);
    // Custom string `Expired` claim, no numeric `exp` — disable the built-in
    // exp requirement and validation so jsonwebtoken does not reject tokens.
    validation.validate_exp = false;
    validation.validate_aud = false;
    validation.required_spec_claims = std::collections::HashSet::new();

    let data = decode::<Claims>(token, &key, &validation)
        .context("JWT verification failed (signature or non-RS256 alg)")?;
    Ok(data.claims)
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, EncodingKey, Header};
    use rsa::pkcs1::EncodeRsaPrivateKey;
    use rsa::pkcs8::{EncodePublicKey, LineEnding};
    use rsa::rand_core::OsRng as RsaOsRng;

    fn test_keypair() -> (RsaPrivateKey, RsaPublicKey) {
        let priv_key = RsaPrivateKey::new(&mut RsaOsRng, 2048).expect("generate RSA key");
        let pub_key = RsaPublicKey::from(&priv_key);
        (priv_key, pub_key)
    }

    // -- Symmetric (HMAC-SHA512) --------------------------------------------

    #[test]
    fn symmetric_string_to_sign_layout_is_exact() {
        let secret = b"super-secret-api-key";
        let method = "POST";
        let url = "/v1.0/transfer";
        let token = "accessTok123";
        let body = br#"{  "b": 2,
            "a": 1 }"#;
        let ts = "2026-07-30T10:00:00+07:00";

        // Rebuild the documented layout by hand and compute the MAC with the
        // same primitives, proving the segment order.
        let minified = pc_core::json_minify(body).unwrap();
        let blc = hex::encode(Sha256::digest(&minified));
        let expected_sts = format!("{method}:{url}:{token}:{blc}:{ts}");
        assert_eq!(
            expected_sts,
            symmetric_string_to_sign(method, url, token, body, ts)
        );

        let mut mac = <HmacSha512 as Mac>::new_from_slice(secret).unwrap();
        mac.update(expected_sts.as_bytes());
        let expected_sig = B64.encode(mac.finalize().into_bytes());

        assert_eq!(
            expected_sig,
            symmetric_sign(secret, method, url, token, body, ts)
        );
    }

    #[test]
    fn symmetric_verify_accepts_own_signature_and_rejects_tampering() {
        let secret = b"secret";
        let (method, url, token, ts) = ("POST", "/pay", "tok", "2026-01-01T00:00:00Z");
        let body = br#"{"amount":1000,"currency":"IDR"}"#;

        let sig = symmetric_sign(secret, method, url, token, body, ts);
        assert!(symmetric_verify(secret, method, url, token, body, ts, &sig));

        // Tampered body.
        let bad_body = br#"{"amount":9999,"currency":"IDR"}"#;
        assert!(!symmetric_verify(
            secret, method, url, token, bad_body, ts, &sig
        ));

        // Tampered timestamp.
        let bad_ts = "2099-01-01T00:00:00Z";
        assert!(!symmetric_verify(
            secret, method, url, token, body, bad_ts, &sig
        ));

        // Garbage / non-base64 signature.
        assert!(!symmetric_verify(
            secret,
            method,
            url,
            token,
            body,
            ts,
            "not base64!!"
        ));
    }

    // -- Asymmetric (RSA PKCS#1 v1.5 / SHA-256) -----------------------------

    #[test]
    fn rsa_sign_verify_roundtrip() {
        let (priv_key, pub_key) = test_keypair();
        let priv_pem = priv_key.to_pkcs1_pem(LineEnding::LF).unwrap().to_string();
        let pub_pem = pub_key.to_public_key_pem(LineEnding::LF).unwrap();

        let client_key = "CLIENT-KEY-123";
        let ts = "2026-07-30T10:00:00+07:00";

        let sig = rsa_sign(&priv_pem, client_key, ts).unwrap();
        assert!(rsa_verify(&pub_pem, client_key, ts, &sig).unwrap());

        // Wrong client key must fail.
        assert!(!rsa_verify(&pub_pem, "WRONG-KEY", ts, &sig).unwrap());
        // Wrong timestamp must fail.
        assert!(!rsa_verify(&pub_pem, client_key, "2000-01-01T00:00:00Z", &sig).unwrap());
    }

    // -- PEM parsing --------------------------------------------------------

    #[test]
    fn pem_roundtrip_public_pkix_and_private_pkcs1() {
        let (priv_key, pub_key) = test_keypair();
        let priv_pem = priv_key.to_pkcs1_pem(LineEnding::LF).unwrap().to_string();
        let pub_pem = pub_key.to_public_key_pem(LineEnding::LF).unwrap();

        let parsed_priv = private_key_from_pem(&priv_pem).unwrap();
        let parsed_pub = public_key_from_pem(&pub_pem).unwrap();

        assert_eq!(parsed_priv, priv_key);
        assert_eq!(parsed_pub, pub_key);

        // A PKIX public PEM must not parse as a PKCS#1 private key.
        assert!(private_key_from_pem(&pub_pem).is_err());
    }

    // -- AES-256-GCM --------------------------------------------------------

    #[test]
    fn aes_roundtrip_32_byte_key() {
        let key = b"0123456789abcdef0123456789abcdef"; // exactly 32 bytes
        assert_eq!(key.len(), 32);
        let plaintext = "hello snap-bi";
        let enc = encrypt_aes(key, plaintext).unwrap();
        assert_eq!(decrypt_aes(key, &enc).unwrap(), plaintext);
    }

    #[test]
    fn aes_roundtrip_sha256_derived_key() {
        let key = b"short-key"; // not 32 bytes -> SHA256 path
        assert_ne!(key.len(), 32);
        let plaintext = "derive me";
        let enc = encrypt_aes(key, plaintext).unwrap();
        assert_eq!(decrypt_aes(key, &enc).unwrap(), plaintext);
    }

    #[test]
    fn decrypt_aes_pem_passthrough() {
        let (priv_key, _) = test_keypair();
        let priv_pem = priv_key.to_pkcs1_pem(LineEnding::LF).unwrap().to_string();

        // A PEM private key is not base64; it must be returned unchanged.
        let out = decrypt_aes(b"any-key", &priv_pem).unwrap();
        assert_eq!(out, priv_pem);
    }

    // -- JWT (RS256 only) ---------------------------------------------------

    #[test]
    fn jwt_rs256_verify_and_reject_wrong_alg() {
        #[derive(Serialize)]
        struct Mint {
            #[serde(rename = "Expired")]
            expired: String,
            #[serde(rename = "MerchantId")]
            merchant_id: u64,
        }

        let (priv_key, pub_key) = test_keypair();
        let priv_pkcs1 = priv_key.to_pkcs1_pem(LineEnding::LF).unwrap().to_string();
        let pub_pem = pub_key.to_public_key_pem(LineEnding::LF).unwrap();

        let mint = Mint {
            expired: "2099-12-31 23:59:59".to_string(),
            merchant_id: 42,
        };

        let enc_key = EncodingKey::from_rsa_pem(priv_pkcs1.as_bytes()).unwrap();
        let token = encode(&Header::new(Algorithm::RS256), &mint, &enc_key).unwrap();

        let claims = verify_jwt_rs256(&pub_pem, &token).unwrap();
        assert_eq!(claims.expired, "2099-12-31 23:59:59");
        assert_eq!(
            claims
                .extra
                .get("MerchantId")
                .and_then(serde_json::Value::as_u64),
            Some(42)
        );

        // A token minted with a non-RS256 alg (HS256) must be rejected.
        let hs_token = encode(
            &Header::new(Algorithm::HS256),
            &mint,
            &EncodingKey::from_secret(b"shared-secret"),
        )
        .unwrap();
        assert!(verify_jwt_rs256(&pub_pem, &hs_token).is_err());
    }
}
