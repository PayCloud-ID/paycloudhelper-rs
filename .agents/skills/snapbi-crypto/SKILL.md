---
name: snapbi-crypto
description: >
  Covers the SNAP-BI signature and crypto suite in pc-snapbi — string-to-sign
  layouts, PEM format constraints, the AES key-size divergence, and RS256 JWT
  claims. Invoke when touching signatures, key parsing, encryption, or tokens.
applyTo: 'crates/pc-snapbi/**/*.rs, crates/pc-auth/**/*.rs'
---

# SNAP-BI Crypto (`pc-snapbi`)

A bit-for-bit port of the Go signature/crypto helpers in
`paycloud-be-qoinhubinterface-manager/helpers` and `PayCloud-ID/paycloudhelper`. Every
string-to-sign layout, hash, and encoding mirrors the Go exactly, and the outputs are
graded byte-for-byte against captured Go vectors.

**Any change here is a wire-contract change.** Merchants sign requests with these
layouts; a one-character difference in a string-to-sign silently rejects every inbound
request. Do not refactor for elegance.

## The two string-to-sign layouts

```rust
// symmetric — HMAC-SHA512, base64 std
// blc = lowercase hex of SHA256(json_minify(body))
format!("{method}:{url}:{token}:{blc}:{ts}")

// asymmetric — RSA PKCS#1 v1.5 over SHA-256, base64 std
format!("{client_key}|{ts}")
```

The symmetric body hash runs through `pc_core::json_minify` — the byte-exact twin of Go's
`encoding/json.Compact`. That is why `pc-core`'s minifier has its own Go-oracle fixture:
it is a signature input, not a formatting nicety.

| Function | Go counterpart |
|---|---|
| `symmetric_sign` | `helpers.SymmetricSignatureGen` |
| `symmetric_verify` | `helpers.ValidateSignatureSnap` (`hmac.Equal`) |
| `rsa_sign` | `helpers.SignatureGenerate` / `services.SignatureGenerate` |
| `rsa_verify` | `helpers.ValidateSignature` (`rsaPublicKey.unsign`) |
| `public_key_from_pem` | `adicrypto.PEMBytesToPublicKey` / `helpers.BytesToPublicKey` |
| `private_key_from_pem` | `helpers.parsePrivateKey` / `helpers.ValidatePrivateKey` |
| `encrypt_aes` / `decrypt_aes` | `helpers.EncryptAES` (+ `getAESKey`) / `helpers.DecryptAES` |
| `verify_jwt_rs256` | `middlewares.GetTokenClaims` / `paycloudhelper.RevokeToken` |

## Comparisons are constant-time

`symmetric_verify` decodes the provided base64 and compares with `subtle`'s
`ConstantTimeEq` (which returns 0 on a length mismatch), mirroring Go's `hmac.Equal`.

```rust
// ❌ never
computed_b64 == provided_b64
```

## PEM formats are asymmetric on purpose

| Key | Accepted block | Rejected |
|---|---|---|
| public | **PKIX/SPKI** — `-----BEGIN PUBLIC KEY-----` | PKCS#1 |
| private | **PKCS#1** — `-----BEGIN RSA PRIVATE KEY-----` | PKCS#8 |

The Go code has no fallback in either direction, so neither does this. Adding a
"helpful" fallback would accept keys the Go service rejects — a parity break that only
shows up in production.

## The AES key-size divergence (known, deliberate)

`derive_aes_key` uses the key directly at exactly 32 bytes and applies `SHA256(key)`
otherwise. Go only derives that way on the **env-default** path (`secret == ""`, via
`getAESKey`); given an explicit non-empty secret it passes raw bytes to `aes.NewCipher`,
which accepts 16/24/32 and errors otherwise.

Consequences:

- **16- or 24-byte secret:** both sides succeed and produce **mutually unreadable
  ciphertext**.
- **Other lengths:** Go errors where Rust succeeds.

Reachable in Go via `helpers.DecryptAES(dmDynamicPrivKey, secretKey)` in
`services/BiAccessTokenB2b.go`, where the secret is merchant-supplied. Pinned by
`tests/go_algorithm_vectors.rs::rust_cannot_read_go_ciphertext_written_with_a_non_32_byte_key`
against captured Go ciphertext, and recorded in `CHANGELOG.md` under
`### Known divergences`.

**Do not "fix" this.** Aligning it changes behavior for consumers already pinned to
`v1.0.2`; the call belongs to the helper owner. Report it if it becomes relevant.

## Ciphertext framing and the PEM-passthrough branch

`encrypt_aes` emits `base64_std(nonce ‖ ciphertext)` with a random 12-byte nonce; the GCM
tag is already appended to the ciphertext, as Go's `Seal` does.

`decrypt_aes` reproduces a quirk worth knowing about: when the input is **not** valid
base64 but **is** a valid PKCS#1 private-key PEM, the input is returned unchanged. Go
returns the raw key when base64 decoding fails at byte 0 and `ValidatePrivateKey`
succeeds. It looks like a bug; it is load-bearing.

The random nonce means there is no encrypt-direction oracle. Only decrypt is gradable —
capture Go ciphertext and assert Rust reads it.

## JWT: RS256 only, with a string `Expired` claim

```rust
let mut validation = Validation::new(Algorithm::RS256);
validation.validate_exp = false;          // no numeric `exp` in these tokens
validation.validate_aud = false;
validation.required_spec_claims = HashSet::new();
```

Tokens carry a **string** `Expired` claim formatted `%Y-%m-%d %H:%M:%S` (Go layout
`2006-01-02 15:04:05`), not a numeric `exp`. Other claims (e.g. `MerchantId`) land in
`Claims::extra` via `#[serde(flatten)]`.

`verify_jwt_rs256` checks **signature and algorithm only**. Time-based expiry and
revocation stay with the caller, as Go does them outside the parse callback — `pc-auth`
treats statuses 3, 4, and 7 as revoked. Algorithm confusion is rejected (mirroring Go's
`token.Method.(*jwt.SigningMethodRSA)` guard) and there is a captured vector for it.
