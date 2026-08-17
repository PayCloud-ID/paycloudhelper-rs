---
description: Audit a diff or crate for Go-parity violations before merge.
argument-hint: <crate name, file path, or "diff" for the working tree>
---

# Parity audit — `$ARGUMENTS`

Review the target for violations of the transport-identical-strangler rule. This repo's
whole value is that a Rust service on it is indistinguishable, on the wire and in the
logs, from the Go service it replaces — so this audit outranks style review.

Read [`.agents/rules/go-parity.md`](../rules/go-parity.md) first, then work through:

## 1. Provenance

- Does every **new or changed public item that reproduces Go behavior** carry a
  `mirrors:` marker naming the Go symbol, file, or struct field?
- Do the markers still match reality, or did the Rust drift away from the Go symbol they
  name?

## 2. Wire-visible surface

For each change, decide whether it alters bytes leaving the process:

- serialized field names, ordering, `omitempty`/`skip_serializing_if` behavior
- status strings, message text, casing, punctuation, log-level tags
- Redis key formats, AMQP routing keys and properties
- signature inputs (string-to-sign layouts, hashing, encoding)
- HTTP response envelopes and headers

Anything on that list changing is **breaking for every consumer pinned to a tag**, even
when the new behavior is more correct.

## 3. Corrections that are actually regressions

Flag any "cleanup" that normalizes a Go quirk this library deliberately preserves:

- the `"developement"` spelling in `AppEnv::parse`
- `APP_MODE` as an `APP_ENV` fallback; `RQ_*` alongside `RABBITMQ_*`
- `decrypt_aes`'s PEM-passthrough branch
- PKIX-only public keys / PKCS#1-only private keys with no fallback
- Go's exact warning strings in `pc-config::validate_configuration`

## 4. Test strength

- Is new parity-bearing behavior covered by a **Go-captured vector**, or only by a
  roundtrip that would pass even if both directions were wrong about Go?
- If a fixture was touched, does the test still assert its `oracle` and `captured_at`
  fields?
- Do live-service tests carry `#[ignore]` with the required env var named?

## 5. Structure

- Does any new internal dependency create a cycle, or pull a heavy crate into a layer
  that could return data instead? (`pc-config` deliberately does not depend on `pc-log`.)
- If a crate was added: all three umbrella wiring points present (optional dep, feature +
  `full`, `#[cfg]` re-export)?

## 6. Report

List findings as **breaking / risky / fine**, each naming the file, the Go counterpart,
and the consumer-visible consequence. For anything breaking, state explicitly that it
requires an owner decision and a `### Known divergences` or major-version entry — do not
apply the change yourself.
