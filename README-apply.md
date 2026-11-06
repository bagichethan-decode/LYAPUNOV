LYAPUNOV dead-event recovery patch

Files in this archive are intended to be extracted over the matching repository root.

Changes:
- max retry threshold moves outbox events to DEAD (legacy FAILED rows remain supported for operator actions).
- DEAD events block later sequence numbers for the same aggregate.
- retryDeadEvent resets retry_count, clears claim/lease state, and returns an operator-reviewed dead event to PENDING.
- skipDeadEvent records SKIPPED, allowing later sequence numbers to proceed without incorrectly marking the skipped event as published.
- retry and skip actions, operator identity, and reason are recorded in event_history on the same transaction as the state change.
- four PostgreSQL/Testcontainers regression tests cover aggregate isolation, retry, skip, and invalid operator transitions.

Test command:
  .\mvnw.cmd "-Dtest=OutboxRepositoryDeadEventRecoveryTest,OutboxRepositoryLeaseFencingTest,OutboxRepositoryOrderingTest,EventHistoryRepositoryTest,OutboxRepositoryValidationTest" test

Docker Desktop must be running for these integration tests. Do not commit until the tests pass.
