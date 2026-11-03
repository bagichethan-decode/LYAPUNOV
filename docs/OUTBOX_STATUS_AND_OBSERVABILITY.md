# Outbox status and queue observability

Run `OutboxStatusCli` with these environment variables:

- `LYAPUNOV_JDBC_URL`
- `LYAPUNOV_DB_USER`
- `LYAPUNOV_DB_PASSWORD`

The command prints a point-in-time aggregate of `outbox_events`: pending,
claimed, published, dead-lettered and skipped counts; accumulated retry attempts;
currently due retries; expired claims; and oldest pending/claimed timestamps.

## Queue signal semantics

- `HEALTHY`: no pending, claimed, dead-lettered, or expired-claim rows were observed.
- `DEGRADED`: pending or claimed work exists, with no dead-lettered or expired claim.
- `UNHEALTHY`: at least one dead-lettered event or expired claim exists.

These are intentionally conservative queue signals, not an SLO or a complete
service-health determination. A queue snapshot cannot prove that a worker process
is alive, that Kafka is reachable, or that consumers are processing messages.
Use independent process heartbeats and broker/consumer probes before making
those claims. Counts are a database snapshot and can change immediately after
the query completes.

## Exit codes

- `0`: the queue signal was `HEALTHY` or `DEGRADED`.
- `1`: the queue signal was `UNHEALTHY` or the database could not be queried.
- `2`: invalid invocation or required environment configuration is missing.