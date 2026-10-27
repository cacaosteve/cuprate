# Scoped Monero arithmetic security patch

`monero-ed25519` is copied from monero-oxide revision
`946ec5f00ff071b129758ee8cba5528539fccfe4` (the same revision as Cuprate's
other Monero dependencies).
The original MIT license and Monero test-vector license are retained.

Only the crypto-bigint dependency and the equivalent Montgomery-arithmetic API
calls in `Point::biased_hash` are changed, to use crypto-bigint 0.7.10 and address
RUSTSEC-2026-0387. The crate API and the remaining Monero dependency revisions
are unchanged. The local patch avoids depending on an unpublished fork commit.
Remove it once the matching upstream Monero dependency includes the fix.

Run `cargo test --locked -p monero-ed25519` for the original Monero vectors and
the fixed 4,610-input compatibility corpus, and run Cuprate's consensus tests
before updating this patch. Functional vectors check consensus compatibility;
they are not a proof of constant-time execution on every compiler or CPU.
