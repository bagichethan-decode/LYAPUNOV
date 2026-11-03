# Prometheus Worker Metrics Export

LYAPUNOV can export its persisted worker registry as Prometheus text exposition format 0.0.4.

## Run

Set `LYAPUNOV_JDBC_URL`, `LYAPUNOV_DB_USER`, and `LYAPUNOV_DB_PASSWORD`, then run:

```powershell
# Write metrics to stdout
java -cp "target/lyapunov-0.1.0-SNAPSHOT.jar;..." com.lyapunov.cli.WorkerMetricsCli

# Write an atomically replaced textfile for the Prometheus node_exporter textfile collector
java -cp "target/lyapunov-0.1.0-SNAPSHOT.jar;..." com.lyapunov.cli.WorkerMetricsCli 30 "C:\prometheus\textfile\lyapunov-workers.prom"
```

The first argument is the stale-heartbeat threshold in seconds (default 30). The optional second argument is an output file. File output is written to a temporary file in the destination directory and then atomically replaced where the filesystem supports atomic moves.

## Exported metrics

- `lyapunov_worker_up`: 1 only when the worker is RUNNING and its heartbeat is fresh.
- `lyapunov_worker_stale`: 1 when the persisted registry identifies a stale RUNNING worker.
- `lyapunov_worker_published_total`
- `lyapunov_worker_failed_total`
- `lyapunov_worker_stale_claims_total`
- `lyapunov_worker_last_seen_timestamp_seconds`
- `lyapunov_worker_last_successful_publish_timestamp_seconds` (0 means no successful publish has been reported)
- `lyapunov_worker_started_timestamp_seconds`

Every sample is labelled by `worker_id`, `instance_id`, and `status`. Label values are escaped according to Prometheus text format requirements.

## Example alert rules

```yaml
groups:
  - name: lyapunov-worker-health
    rules:
      - alert: LyapunovWorkerDown
        expr: lyapunov_worker_up == 0
        for: 1m
        labels:
          severity: critical
      - alert: LyapunovWorkerStale
        expr: lyapunov_worker_stale == 1
        for: 30s
        labels:
          severity: warning
      - alert: LyapunovWorkerPublishFailures
        expr: increase(lyapunov_worker_failed_total[5m]) > 0
        for: 0m
        labels:
          severity: warning
```

## Operational limits

This exporter reports the worker registry stored in PostgreSQL. It does not independently probe Kafka, prove that the worker's counters are correct, or measure end-to-end event delivery latency. A scrape or textfile collector must invoke the CLI periodically; this is not an embedded HTTP server. Use a unique worker ID for each independently monitored replica.
