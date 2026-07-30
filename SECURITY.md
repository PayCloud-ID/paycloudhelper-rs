# Security policy

Report suspected vulnerabilities privately to the PayCloud security and
platform owners. Do not open a public issue containing credentials, customer
data, exploit details, or unpublished vulnerabilities.

## Dependency advisory policy

`cargo-deny` and `cargo-audit` are release gates. An advisory may be ignored
only when no fixed dependency release exists, the compatibility requirement is
documented, and the exception is mirrored in `deny.toml` and
`.cargo/audit.toml`.

The following temporary exceptions were reviewed on 2026-07-30:

| Advisory | Dependency path | Reason and removal condition |
|---|---|---|
| RUSTSEC-2023-0071 | Direct SNAP-BI `rsa`; transitive `sqlx-mysql` | RustCrypto `rsa` has no patched release. Preserve the frozen SNAP-BI RSA contract, avoid exposing local signing timing, and remove the exception when a fixed release is available. |
| RUSTSEC-2024-0384 | `lapin` → `instant` | `instant` is unmaintained with no patched release. Remove when the AMQP stack no longer resolves it. |
| RUSTSEC-2025-0134 | `tonic`/`lapin` → `rustls-pemfile` | The parser is unmaintained with no patched release. Remove when both upstream stacks permit it. |

These exceptions are risk acceptances, not declarations that the dependencies
are safe. Each dependency refresh must rerun both scanners and try to remove
the entries.
