# LYAPUNOV load testing and failure injection

## Scope

`InboxLoadBenchmark` measures concurrent PostgreSQL insert throughput and transaction latency against an isolated `lyapunov_benchmark_events` table. It does not claim to measure end-to-end Kafka delivery, relay throughput, or consumer acknowledgement latency. Those require the real Kafka consumer integration and will be measured separately once that path is wired.

The benchmark table is separate from the production outbox and consumer inbox tables. Each run receives a UUID `run_id`, allowing results to be inspected and removed independently.

## Prerequisites

- Docker Compose PostgreSQL service is running (`docker compose up -d postgres`, or your existing compose service name).
- PostgreSQL JDBC driver is present in the Maven runtime dependency tree.
- `.\mvnw.cmd clean verify` succeeds.

Set the connection variables in PowerShell (adjust credentials/port to match your local compose file):

```powershell
$env:LYAPUNOV_JDBC_URL = "jdbc:postgresql://localhost:5432/lyapunov"
$env:LYAPUNOV_DB_USER = "postgres"
$env:LYAPUNOV_DB_PASSWORD = "postgres"
```

Find the driver JAR and launch the benchmark after compiling:

```powershell
.\mvnw.cmd -q -DskipTests package
$driver = Get-ChildItem "$HOME\.m2\repository\org\postgresql\postgresql" -Filter "postgresql-*.jar" -Recurse |
    Sort-Object FullName -Descending | Select-Object -First 1
if (-not $driver) { throw "PostgreSQL JDBC driver not found in the local Maven repository." }
$cp = "target\classes;$($driver.FullName)"
java -cp $cp com.lyapunov.benchmark.InboxLoadBenchmark --events 10000 --concurrency 8 --duplicate-rate 0.10 --failure-rate 0
```

Run a baseline first with `--failure-rate 0`. Then vary one parameter at a time:

```powershell
java -cp $cp com.lyapunov.benchmark.InboxLoadBenchmark --events 50000 --concurrency 1 --duplicate-rate 0 --failure-rate 0
java -cp $cp com.lyapunov.benchmark.InboxLoadBenchmark --events 50000 --concurrency 4 --duplicate-rate 0 --failure-rate 0
java -cp $cp com.lyapunov.benchmark.InboxLoadBenchmark --events 50000 --concurrency 8 --duplicate-rate 0.10 --failure-rate 0
java -cp $cp com.lyapunov.benchmark.InboxLoadBenchmark --events 50000 --concurrency 8 --duplicate-rate 0.10 --failure-rate 0.02
```

## Metrics

- `throughput_ops_per_sec`: requested operations divided by wall-clock duration.
- `latency_p50_ms`, `latency_p95_ms`, `latency_p99_ms`: nearest-rank percentiles of measured per-attempt database transaction latency.
- `inserted`: newly persisted unique event keys.
- `duplicates`: operations ignored by the unique primary key.
- `injected_failures`: deliberate attempts skipped before SQL execution.
- `sql_or_worker_errors`: unexpected SQL or worker errors.

Injected failures are a deterministic category of skipped attempts, not simulated process crashes or database outages. Do not combine them with baseline throughput comparisons. The benchmark uses a per-run unique namespace, so repeated runs do not conflict with earlier runs.

## Cleanup

The program prints a run-specific cleanup statement. Review the UUID, then execute only that statement against the benchmark table:

```sql
DELETE FROM lyapunov_benchmark_events WHERE run_id = '<run-id-from-output>';
```

Do not truncate the table; that would remove other benchmark runs.

## Experimental discipline

1. Record machine, Java version, PostgreSQL version, Docker resource limits, event count, concurrency, duplicate rate, and failure rate.
2. Run each configuration at least three times; compare medians rather than selecting the best run.
3. Keep database/container settings constant when comparing commits.
4. Report p50/p95/p99 alongside throughput; throughput alone hides tail latency.
5. Do not claim exactly-once delivery or end-to-end performance from this database-only benchmark.
6. This benchmark does not currently simulate a worker crash, Kafka broker outage, or consumer offset replay. Those require integration-level fault tests.
