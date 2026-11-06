[CmdletBinding()]
param(
    [int]$Events = 10000,
    [int]$Concurrency = 8,
    [double]$DuplicateRate = 0.10,
    [double]$FailureRate = 0.02
)

$ErrorActionPreference = "Stop"
$RepoRoot = (Get-Location).Path
$GitRoot = git rev-parse --show-toplevel 2>$null
if ($LASTEXITCODE -ne 0) { throw "Current directory is not inside a Git repository." }
$NormalizePath = { param([string]$Path) [System.IO.Path]::GetFullPath($Path).TrimEnd('\','/') }
if ((& $NormalizePath $GitRoot) -ine (& $NormalizePath $RepoRoot)) {
    throw "Run this script from the LYAPUNOV repository root. Git root: $GitRoot"
}
$Branch = git branch --show-current
if ($Branch -ne "main") { throw "Expected branch main; found '$Branch'. No files changed." }
$Head = (git rev-parse --short HEAD).Trim()
if ($Head -ne "09cdb0c") { throw "Expected HEAD 09cdb0c; found '$Head'. Review repository state before proceeding." }
git diff --quiet
if ($LASTEXITCODE -ne 0) { throw "Tracked working-tree changes exist. Commit/stash them first. No files changed." }
git diff --cached --quiet
if ($LASTEXITCODE -ne 0) { throw "Staged changes exist. Commit/stash them first. No files changed." }
if ($Events -lt 1 -or $Events -gt 1000000) { throw "Events must be between 1 and 1,000,000." }
if ($Concurrency -lt 1 -or $Concurrency -gt 128) { throw "Concurrency must be between 1 and 128." }
if ($DuplicateRate -lt 0 -or $DuplicateRate -gt 1) { throw "DuplicateRate must be between 0 and 1." }
if ($FailureRate -lt 0 -or $FailureRate -gt 0.5) { throw "FailureRate must be between 0 and 0.5." }

$Files = @(
    "src/main/java/com/lyapunov/benchmark/InboxLoadBenchmark.java",
    "src/test/java/com/lyapunov/benchmark/LatencyPercentilesTest.java",
    "docs/LOAD_TESTING_AND_FAILURE_INJECTION.md"
)
foreach ($f in $Files) {
    if (Test-Path $f) { throw "Refusing to overwrite existing file: $f" }
}

$JavaPath = $Files[0]
$JavaDir = Split-Path $JavaPath
$TestPath = $Files[1]
$TestDir = Split-Path $TestPath
$DocPath = $Files[2]
New-Item -ItemType Directory -Force -Path $JavaDir,$TestDir,(Split-Path $DocPath) | Out-Null

@'
package com.lyapunov.benchmark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Reproducible PostgreSQL persistence benchmark for LYAPUNOV.
 * Uses an isolated benchmark table; it does not alter outbox or inbox data.
 */
public final class InboxLoadBenchmark {
    private static final String CREATE_TABLE = """
        CREATE TABLE IF NOT EXISTS lyapunov_benchmark_events (
            run_id UUID NOT NULL,
            event_id UUID NOT NULL,
            consumer_name VARCHAR(120) NOT NULL,
            processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (run_id, event_id, consumer_name)
        )
        """;
    private static final String INSERT = """
        INSERT INTO lyapunov_benchmark_events(run_id, event_id, consumer_name)
        VALUES (?, ?, ?)
        ON CONFLICT (run_id, event_id, consumer_name) DO NOTHING
        """;

    private InboxLoadBenchmark() {}

    public static void main(String[] args) throws Exception {
        int events = integerArg(args, "--events", 10000);
        int concurrency = integerArg(args, "--concurrency", 8);
        double duplicateRate = doubleArg(args, "--duplicate-rate", 0.10);
        double failureRate = doubleArg(args, "--failure-rate", 0.02);
        String jdbcUrl = requiredEnv("LYAPUNOV_JDBC_URL");
        String user = System.getenv().getOrDefault("LYAPUNOV_DB_USER", "postgres");
        String password = System.getenv().getOrDefault("LYAPUNOV_DB_PASSWORD", "postgres");
        String consumer = "load-benchmark";

        if (events < 1 || events > 1_000_000 || concurrency < 1 || concurrency > 128
                || duplicateRate < 0 || duplicateRate > 1 || failureRate < 0 || failureRate > 0.5) {
            throw new IllegalArgumentException("Invalid benchmark parameters.");
        }

        UUID runId = UUID.randomUUID();
        try (Connection setup = DriverManager.getConnection(jdbcUrl, user, password);
             var statement = setup.createStatement()) {
            statement.execute(CREATE_TABLE);
        }

        int uniqueCount = Math.max(1, (int) Math.ceil(events * (1.0 - duplicateRate)));
        List<UUID> ids = new ArrayList<>(events);
        for (int i = 0; i < events; i++) {
            ids.add(i < uniqueCount ? UUID.randomUUID() : ids.get(i % uniqueCount));
        }
        Collections.shuffle(ids);
        AtomicInteger cursor = new AtomicInteger();
        LongAdder inserted = new LongAdder();
        LongAdder duplicates = new LongAdder();
        LongAdder injectedFailures = new LongAdder();
        LongAdder errors = new LongAdder();
        List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        CountDownLatch ready = new CountDownLatch(concurrency);
        CountDownLatch start = new CountDownLatch(1);
        Instant startedAt = Instant.now();
        long wallStart = System.nanoTime();

        List<Future<?>> tasks = new ArrayList<>();
        for (int worker = 0; worker < concurrency; worker++) {
            tasks.add(pool.submit(() -> {
                try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password);
                     PreparedStatement insert = connection.prepareStatement(INSERT)) {
                    connection.setAutoCommit(false);
                    ready.countDown();
                    start.await();
                    while (true) {
                        int index = cursor.getAndIncrement();
                        if (index >= events) break;
                        if (Math.random() < failureRate) {
                            injectedFailures.increment();
                            continue;
                        }
                        long t0 = System.nanoTime();
                        try {
                            insert.setObject(1, runId);
                            insert.setObject(2, ids.get(index));
                            insert.setString(3, consumer);
                            int changed = insert.executeUpdate();
                            connection.commit();
                            if (changed == 1) inserted.increment(); else duplicates.increment();
                        } catch (SQLException e) {
                            errors.increment();
                            try { connection.rollback(); } catch (SQLException ignored) { }
                        } finally {
                            latencies.add(System.nanoTime() - t0);
                        }
                    }
                } catch (Exception e) {
                    errors.increment();
                    throw new RuntimeException(e);
                }
            }));
        }

        ready.await();
        start.countDown();
        for (Future<?> task : tasks) task.get();
        pool.shutdown();
        long elapsedNanos = System.nanoTime() - wallStart;
        double seconds = elapsedNanos / 1_000_000_000.0;
        List<Long> sorted = new ArrayList<>(latencies);
        Collections.sort(sorted);

        System.out.println("LYAPUNOV_LOAD_BENCHMARK");
        System.out.println("run_id=" + runId);
        System.out.println("started_at=" + startedAt);
        System.out.println("requested_events=" + events);
        System.out.println("concurrency=" + concurrency);
        System.out.println("inserted=" + inserted.sum());
        System.out.println("duplicates=" + duplicates.sum());
        System.out.println("injected_failures=" + injectedFailures.sum());
        System.out.println("sql_or_worker_errors=" + errors.sum());
        System.out.printf(java.util.Locale.ROOT, "elapsed_seconds=%.3f%n", seconds);
        System.out.printf(java.util.Locale.ROOT, "throughput_ops_per_sec=%.2f%n", events / seconds);
        System.out.printf(java.util.Locale.ROOT, "latency_p50_ms=%.3f%n", percentile(sorted, 0.50) / 1_000_000.0);
        System.out.printf(java.util.Locale.ROOT, "latency_p95_ms=%.3f%n", percentile(sorted, 0.95) / 1_000_000.0);
        System.out.printf(java.util.Locale.ROOT, "latency_p99_ms=%.3f%n", percentile(sorted, 0.99) / 1_000_000.0);
        System.out.println("note=Injected failures are deliberate skipped attempts; rerun with failure-rate 0 for baseline.");
        System.out.println("cleanup_sql=DELETE FROM lyapunov_benchmark_events WHERE run_id = '" + runId + "';");
    }

    static long percentile(List<Long> sorted, double quantile) {
        if (sorted == null || sorted.isEmpty()) return 0L;
        if (quantile < 0 || quantile > 1) throw new IllegalArgumentException("quantile must be in [0,1]");
        int index = (int) Math.ceil(quantile * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }

    private static int integerArg(String[] args, String key, int fallback) {
        return (int) doubleArg(args, key, fallback);
    }
    private static double doubleArg(String[] args, String key, double fallback) {
        for (int i = 0; i < args.length - 1; i++) if (args[i].equals(key)) return Double.parseDouble(args[i + 1]);
        return fallback;
    }
    private static String requiredEnv(String key) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) throw new IllegalStateException("Set environment variable " + key);
        return value;
    }
}
'@ | Set-Content -Encoding UTF8 $JavaPath

@'
package com.lyapunov.benchmark;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class LatencyPercentilesTest {
    @Test void emptySamplesReturnZero() {
        assertEquals(0L, InboxLoadBenchmark.percentile(List.of(), 0.95));
    }
    @Test void percentileUsesNearestRank() {
        List<Long> values = List.of(10L, 20L, 30L, 40L, 50L);
        assertEquals(10L, InboxLoadBenchmark.percentile(values, 0.01));
        assertEquals(30L, InboxLoadBenchmark.percentile(values, 0.50));
        assertEquals(50L, InboxLoadBenchmark.percentile(values, 0.99));
    }
    @Test void invalidQuantileIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> InboxLoadBenchmark.percentile(List.of(1L), 1.1));
    }
}
'@ | Set-Content -Encoding UTF8 $TestPath

@'
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
'@ | Set-Content -Encoding UTF8 $DocPath

Write-Host "Batch 11 files created. Running Maven verification..."
& .\mvnw.cmd clean verify
if ($LASTEXITCODE -ne 0) { throw "Maven verification failed. Files remain for inspection; nothing was staged or committed." }

Write-Host "Verification passed. Review the diff before staging:"
git status --short
git diff -- $Files
Write-Host ""
Write-Host "If the diff is correct, this script can stage only the three Batch 11 files and commit/push."
$answer = Read-Host "Type COMMIT to stage these exact files, commit, and push; anything else stops here"
if ($answer -cne "COMMIT") { Write-Host "Stopped without staging or committing."; exit 0 }

git add -- $Files
if ($LASTEXITCODE -ne 0) { throw "git add failed." }
git diff --cached --check
if ($LASTEXITCODE -ne 0) { throw "Staged diff check failed. Review before committing." }
git diff --cached --stat
git diff --cached --
$confirm = Read-Host "Type PUSH to commit and push the reviewed Batch 11 changes"
if ($confirm -cne "PUSH") { Write-Host "Stopped. Batch 11 files remain staged; no commit or push was performed."; exit 0 }

git commit -m "feat: add PostgreSQL load benchmark and latency metrics"
if ($LASTEXITCODE -ne 0) { throw "Commit failed." }
git push origin main
if ($LASTEXITCODE -ne 0) { throw "Push failed. Local commit exists; inspect git status and push state." }
git status -sb
