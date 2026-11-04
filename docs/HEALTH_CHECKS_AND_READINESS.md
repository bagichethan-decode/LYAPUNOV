# Health Checks and Readiness

Batch 09 adds a command-line health probe for the LYAPUNOV runtime dependencies.

## Run

From the repository root, with PostgreSQL and Kafka running:

```powershell
.\mvnw.cmd -q -DskipTests package
$env:LYAPUNOV_JDBC_URL = "jdbc:postgresql://localhost:5432/lyapunov"
$env:LYAPUNOV_DB_USER = "postgres"
$env:LYAPUNOV_DB_PASSWORD = "postgres"
$env:LYAPUNOV_KAFKA_HOST = "localhost"
$env:LYAPUNOV_KAFKA_PORT = "9092"
java -cp "target/classes;target/dependency/*" com.lyapunov.cli.HealthCheckCli
```

The Maven runtime classpath must include the PostgreSQL JDBC driver. If the project does not copy runtime dependencies to `target/dependency`, run the CLI through the project's configured exec mechanism or add the driver's jar to the classpath.

## Probes

- `kafka`: opens a TCP connection to the configured broker host and port.
- `postgres`: opens a JDBC connection and executes `SELECT 1`.
- `outbox`: verifies that `outbox_events` is queryable and reports its row count.
- `worker-heartbeats`: verifies the heartbeat table and flags RUNNING workers whose `last_seen_at` is older than the configured threshold.

The Kafka probe confirms TCP reachability only. It does not perform Kafka protocol negotiation, authenticate, inspect topics, or prove that a produce/consume round trip works.

## Status and exit codes

- `HEALTHY`: all probes succeeded; exit code `0`.
- `DEGRADED`: no hard dependency failure, but at least one worker heartbeat is stale; exit code `0` for normal health mode and `1` with `--readiness`.
- `UNHEALTHY`: a required dependency or schema probe failed; exit code `2`.

Use `--readiness` when a deployment should receive a non-zero exit code for either DEGRADED or UNHEALTHY.

## Environment variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `LYAPUNOV_JDBC_URL` | `jdbc:postgresql://localhost:5432/lyapunov` | PostgreSQL JDBC URL |
| `LYAPUNOV_DB_USER` | `postgres` | Database user |
| `LYAPUNOV_DB_PASSWORD` | `postgres` | Database password |
| `LYAPUNOV_KAFKA_HOST` | `localhost` | Kafka broker host |
| `LYAPUNOV_KAFKA_PORT` | `9092` | Kafka broker port |
| `LYAPUNOV_HEALTH_TIMEOUT_MS` | `2000` | Kafka TCP connect timeout |
| `LYAPUNOV_HEARTBEAT_STALE_SECONDS` | `60` | Heartbeat staleness threshold |

Do not commit production credentials. Supply them through the deployment environment or secret manager.

## Operational notes

- The health CLI is a diagnostic/readiness process; it does not start or stop the relay worker.
- A missing heartbeat row is not considered stale. Worker lifecycle and registration remain the responsibility of the worker integration.
- Health probes are read-only.
- This batch does not change event delivery guarantees. LYAPUNOV remains at-least-once; consumers still need idempotent processing.
