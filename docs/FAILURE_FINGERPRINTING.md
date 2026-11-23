# LYAPUNOV Failure Fingerprinting and Persistent Failure Intelligence

## Purpose

Identify recurring delivery failure patterns using deterministic fingerprints
and retain those observations in the existing PostgreSQL event-history stream.

## Fingerprint generation

- Root-cause extraction with circular-reference protection.
- UUID, timestamp, memory-address and large-number normalization.
- Best-effort redaction of common credential patterns.
- Bounded input and normalized-message lengths.
- SHA-256 digest derived from the root exception class and normalized message.

## Persistence and worker behavior

When publishing an outbox event fails, the relay worker generates a fingerprint
and writes a `FAILURE_FINGERPRINTED` action to `event_history`. The JSON details
contain the fingerprint, exception type and normalized message. Event identity
and worker identity use the existing event-history columns.

Fingerprint persistence is deliberately best-effort. If recording the
intelligence observation fails, the worker logs the failure and still executes
the existing retry/backoff transition. Failure intelligence must never prevent
delivery recovery.

The event-history stream now provides a durable foundation for later
aggregation by fingerprint, affected-event counts, recurring-failure analysis
and recovery recommendations. Those analytics are not claimed as implemented
by this integration.

## Security

Sanitization is best-effort and does not guarantee that arbitrary exception
messages are free of sensitive information. Applications should avoid putting
secrets in exception messages and should restrict access to event-history data.

## Delivery semantics

Fingerprinting does not change delivery semantics. LYAPUNOV continues to
provide at-least-once delivery with consumer-side idempotency. It does not claim
exactly-once delivery.