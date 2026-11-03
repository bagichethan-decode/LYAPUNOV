# Automatic Worker Heartbeats

`OutboxRelayWorker` can now publish its own lifecycle and progress to the
existing `worker_heartbeats` table. The old constructors remain supported;
heartbeat reporting is opt-in until the application bootstrap supplies a
`WorkerHeartbeatStore`.

## Wiring

Create the JDBC repository using the application's existing `DataSource`, then
pass it to the extended worker constructor:

```java
WorkerHeartbeatRepository heartbeatRepository =
    new WorkerHeartbeatRepository(dataSource);

OutboxRelayWorker worker = new OutboxRelayWorker(
    outboxStore,
    eventPublisher,
    "orders-relay",
    100,
    8,
    Duration.ofSeconds(30),
    Duration.ofMillis(500),
    Duration.ofSeconds(1),
    Duration.ofMinutes(5),
    heartbeatRepository,
    UUID.randomUUID().toString(),
    Duration.ofSeconds(10)
);
```

Run the worker on its normal application-managed thread. `run()` writes an
initial heartbeat, then a periodic heartbeat on a dedicated daemon scheduler.
The scheduler is independent of the relay polling loop, so a slow poll or
publish does not suppress periodic liveness updates.

## Lifecycle and failure behavior

- Each process instance should use a unique `instanceId` (UUID recommended).
- Heartbeats include the worker's current published, failed, and stale-claim
  counters plus the latest delivery/cycle error.
- `stop()` requests loop termination. When `run()` exits, the worker attempts
  to mark its matching instance `STOPPED`.
- `close()` also stops heartbeat scheduling and attempts the STOPPED update.
- A heartbeat database error is logged but does not terminate event delivery.
- Existing constructors disable automatic heartbeat reporting, preserving
  existing callers and tests.

The existing heartbeat CLI and Prometheus metrics exporter can inspect these
records. This change does not add an HTTP endpoint or guarantee that a
STOPPED update succeeds during process termination or machine failure; stale
heartbeat detection remains necessary for crash detection.
