# Worker Heartbeats and Runtime Registry

LYAPUNOV stores worker identity and heartbeat state in PostgreSQL. The registry
records the worker ID, process instance ID, lifecycle status, last-seen time,
last successful publish time, counters, and most recent reported error.

## Schema

`docker/init.sql` creates `worker_heartbeats`. The `worker_id` is the logical
worker identity; `instance_id` distinguishes process incarnations. A new
instance taking over a worker ID updates the row and refreshes the heartbeat.
Use unique worker IDs when multiple independent replicas should be visible.

## Apply to an existing database

`docker/init.sql` includes the table for fresh databases. Existing PostgreSQL
volumes do not rerun initialization scripts, so apply the migration once:

```powershell
docker exec -i lyapunov-postgres psql -U lyapunov -d lyapunov < docker/migrations/V006__worker_heartbeats.sql
```

## CLI

Set `LYAPUNOV_JDBC_URL`, `LYAPUNOV_DB_USER`, and `LYAPUNOV_DB_PASSWORD`.

```powershell
# Register or refresh a worker heartbeat (counts are cumulative process totals)
java -cp "target/lyapunov-0.1.0-SNAPSHOT.jar;..." com.lyapunov.cli.WorkerHeartbeatCli heartbeat relay-1 instance-a 125 2 1 "broker timeout"

# List workers; RUNNING workers older than the threshold are marked stale
java -cp "target/lyapunov-0.1.0-SNAPSHOT.jar;..." com.lyapunov.cli.WorkerHeartbeatCli list 30

# Mark a matching process instance as stopped
java -cp "target/lyapunov-0.1.0-SNAPSHOT.jar;..." com.lyapunov.cli.WorkerHeartbeatCli stop relay-1 instance-a
```

`list` exits with status 1 if any worker is stale or explicitly STOPPED.
`STOPPED` is a lifecycle state, not a stale heartbeat. A `RUNNING` row is stale
when its `last_seen_at` is older than the configured threshold.

## Operational notes

- Heartbeats are persisted in PostgreSQL and therefore survive process restarts.
- `last_successful_publish_at` advances only when the cumulative published
  counter increases.
- `last_error` is the latest error explicitly supplied by the reporting process;
  null clears it.
- A heartbeat proves only that a process reported recently. It does not prove
  Kafka is healthy or that its counters are independently verified.
- This batch provides the persistence API and operator CLI. The next integration
  step is wiring the repository into the worker bootstrap so the process emits
  heartbeats automatically rather than relying on an external caller.
