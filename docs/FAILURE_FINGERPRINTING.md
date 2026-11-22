# LYAPUNOV Failure Fingerprinting

## Purpose

Identify recurring delivery failure patterns using deterministic fingerprints.

## Implementation

- Root-cause extraction with circular-reference protection.
- UUID, timestamp and numeric identifier normalization.
- Common credential redaction.
- Bounded input and normalized message length.
- SHA-256 fingerprints based on exception type and normalized message.

## Security

The sanitizer is best-effort. It does not guarantee that arbitrary exception
messages are free of sensitive information. Applications should avoid putting
secrets in exception messages.

## Delivery semantics

Fingerprinting does not change delivery semantics. LYAPUNOV continues to
provide at-least-once delivery with consumer-side idempotency.

## Next integration

Connect fingerprints to existing event history and PostgreSQL failure
aggregation without introducing a parallel event store.