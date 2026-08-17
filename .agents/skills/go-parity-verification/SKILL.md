---
name: go-parity-verification
description: >
  Guides capturing, storing, and asserting Go-oracle test vectors that prove a
  pc-* crate matches the Go paycloudhelper byte-for-byte. Invoke when adding or
  changing parity-bearing behavior, or when a roundtrip test is the only coverage.
applyTo: 'crates/*/tests/**/*.rs, crates/*/tests/vectors/*.json'
---

# Go Parity Verification

## When a roundtrip test is not enough

A roundtrip test signs with the Rust signer and verifies with the Rust verifier. Both
agree — including when both are wrong about Go. Any behavior that leaves the process
(bytes, signatures, key strings, JSON, log lines) needs an **oracle**: a vector produced
by running the Go code path itself.

Ask: *if my Rust implementation and the Go implementation disagreed, would this test
fail?* If not, it is a roundtrip and it does not pin parity.

## The four existing oracles

| Fixture | Oracle | Asserted by |
|---|---|---|
| `pc-core/tests/vectors/json_minify.json` | Go `encoding/json.Compact` | `pc-core/tests/golden_vectors.rs` |
| `pc-snapbi/tests/vectors/symmetric_signatures.json` | Go SNAP-BI signature helpers | `pc-snapbi/tests/golden_vectors.rs` |
| `pc-snapbi/tests/vectors/snapbi_go_vectors.json` | `helpers.SignatureGenerate`, `helpers.EncryptAES`, `golang-jwt/jwt/v5` | `pc-snapbi/tests/go_algorithm_vectors.rs` |
| `pc-audit/tests/vectors/audit_trx.json` | Go transaction-audit serialization | `pc-audit/tests/golden_vectors.rs` |

## Fixture shape

Every fixture carries provenance metadata alongside the vectors:

```json
{
  "oracle": "go encoding/json.Compact",
  "captured_at": "2026-07-30",
  "vectors": [
    { "name": "nested object whitespace", "input": "…", "output": "…" }
  ]
}
```

The test **asserts on the provenance**, so a fixture regenerated from a reimplementation
fails loudly instead of quietly grading Rust against Rust:

```rust
let fixture: Fixture =
    serde_json::from_str(include_str!("vectors/json_minify.json")).expect("valid fixture");
assert!(fixture.oracle.contains("encoding/json.Compact"));
assert_eq!(fixture.captured_at, "2026-07-30");

for vector in fixture.vectors {
    let actual = pc_core::json_minify(vector.input.as_bytes())
        .unwrap_or_else(|error| panic!("{}: {error}", vector.name));
    assert_eq!(actual, vector.output.as_bytes(), "{}", vector.name);
}
```

Every vector has a `name`, and every assertion passes it as the failure message — a red
suite must say *which* vector broke.

## Capturing new vectors

**The capture program does not live in this repo.** A Go module here would sit outside
`cargo-deny` / `cargo-audit` coverage, go uncompiled by a Rust-only CI, and hard-code a
relative path into one consumer service — and this is a library many services consume.
It belongs beside the Go code it captures.

Procedure:

1. In the **Go** service module that owns the helper (e.g.
   `paycloud-be-qoinhubinterface-manager`), write a `main` that builds the fixture struct
   and calls the production helpers directly — not a reimplementation of them. Calling
   `helpers.SignatureGenerate` is what makes the output an oracle rather than a second
   opinion from the same source.
2. Write the JSON **to a file, not stdout** — the Go helper package's `init()` prints
   config warnings to stdout and will corrupt a piped capture.
3. Set `oracle` to a string naming the Go source, and `captured_at` to the capture date.
4. Copy the file into `crates/<crate>/tests/vectors/`, add the assertions, and document
   the provenance and regeneration steps in the test file's `//!` module docs — see
   `pc-snapbi/tests/go_algorithm_vectors.rs` for the model.
5. Note the addition in `CHANGELOG.md` under `### Added`, naming which algorithms moved
   from roundtrip-only to oracle-graded.

## Directions that cannot be captured

Randomized outputs have no encrypt-direction vector. AES-256-GCM uses a random nonce, so
only the **decrypt** direction is gradable: capture Go ciphertext, assert Rust decrypts
it to the expected plaintext. Say so explicitly in the fixture field name
(`aes_256_gcm_decrypt`) rather than leaving a reader to wonder what is missing.

## When the vector proves a divergence

Sometimes the captured Go output shows Rust and Go genuinely differ. Do not delete the
vector and do not silently align the implementation — see
[`../../rules/go-parity.md`](../../rules/go-parity.md) §4. Instead:

- keep the captured Go bytes as a fixture,
- write a test whose **name states the divergence**, e.g.
  `rust_cannot_read_go_ciphertext_written_with_a_non_32_byte_key`,
- record it in `CHANGELOG.md` under `### Known divergences` with the reachable call path
  in Go and why it was not fixed.
