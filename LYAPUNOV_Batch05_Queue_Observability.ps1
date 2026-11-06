# LYAPUNOV Batch 05 - Queue observability and operator status
# Windows PowerShell 5.1 compatible. Run from the LYAPUNOV repository root.
$ErrorActionPreference = "Stop"
Set-StrictMode -Version 2

$ExpectedHead = "0734a1b"
$Branch = "main"

function Write-Utf8File([string]$RelativePath, [string]$Content) {
    $full = Join-Path (Get-Location) $RelativePath
    $parent = Split-Path -Parent $full
    if (!(Test-Path $parent)) { New-Item -ItemType Directory -Path $parent -Force | Out-Null }
    $utf8 = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($full, $Content, $utf8)
}

function Assert-CleanTrackedTree {
    $status = git status --porcelain
    if ($LASTEXITCODE -ne 0) { throw "Could not inspect git status." }
    $trackedChanges = @()
    foreach ($line in $status) {
        if ($line.Length -ge 3 -and $line.Substring(0,2) -ne "??") { $trackedChanges += $line }
    }
    if ($trackedChanges.Count -gt 0) {
        $trackedChanges | ForEach-Object { Write-Host $_ -ForegroundColor Yellow }
        throw "Tracked changes exist. Commit/stash them before running this batch. Untracked files are left untouched."
    }
}

if (!(Test-Path ".git") -or !(Test-Path ".\mvnw.cmd")) {
    throw "Run this script from the LYAPUNOV repository root."
}
$branchNow = (git branch --show-current).Trim()
if ($branchNow -ne $Branch) { throw "Expected branch '$Branch', found '$branchNow'." }
$headNow = (git rev-parse --short HEAD).Trim()
if (!$headNow.StartsWith($ExpectedHead)) {
    throw "Expected Batch 04 base $ExpectedHead, found $headNow. Stop rather than applying against an unknown revision."
}
Assert-CleanTrackedTree

$stats = @'
package com.lyapunov.repository;

import java.time.Instant;

/**
 * Point-in-time aggregate view of the transactional outbox.
 * This describes persisted queue state, not Kafka or worker process liveness.
 */
public record OutboxQueueStats(
        long pending,
        long claimed,
        long published,
        long dead,
        long skipped,
        long totalRetries,
        long retriesDue,
        long expiredClaims,
        Instant oldestPendingAt,
        Instant oldestClaimedAt,
        Instant snapshotAt
) {
    public long totalEvents() {
        return pending + claimed + published + dead + skipped;
    }

    public String queueHealth() {
        if (dead > 0 || expiredClaims > 0) return "UNHEALTHY";
        if (pending > 0 || claimed > 0) return "DEGRADED";
        return "HEALTHY";
    }
}
'@

$repository = @'
package com.lyapunov.repository;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Objects;

/**
 * Reads a consistent, point-in-time summary of outbox queue state.
 * It intentionally does not infer worker or Kafka liveness from database rows.
 */
public final class OutboxQueueStatsRepository {
    private final DataSource dataSource;

    public OutboxQueueStatsRepository(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    public OutboxQueueStats snapshot() throws SQLException {
        String sql = """
                SELECT
                    COUNT(*) FILTER (WHERE status = 'PENDING') AS pending,
                    COUNT(*) FILTER (WHERE status = 'CLAIMED') AS claimed,
                    COUNT(*) FILTER (WHERE status = 'PUBLISHED') AS published,
                    COUNT(*) FILTER (WHERE status = 'DEAD') AS dead,
                    COUNT(*) FILTER (WHERE status = 'SKIPPED') AS skipped,
                    COALESCE(SUM(retry_count), 0) AS total_retries,
                    COUNT(*) FILTER (
                        WHERE status = 'PENDING'
                          AND (next_attempt_at IS NULL OR next_attempt_at <= clock_timestamp())
                          AND retry_count > 0
                    ) AS retries_due,
                    COUNT(*) FILTER (
                        WHERE status = 'CLAIMED'
                          AND lease_expires_at IS NOT NULL
                          AND lease_expires_at <= clock_timestamp()
                    ) AS expired_claims,
                    MIN(created_at) FILTER (WHERE status = 'PENDING') AS oldest_pending_at,
                    MIN(updated_at) FILTER (WHERE status = 'CLAIMED') AS oldest_claimed_at,
                    clock_timestamp() AS snapshot_at
                FROM outbox_events
                """;

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet result = statement.executeQuery()) {
            if (!result.next()) {
                throw new SQLException("Outbox statistics query returned no row");
            }
            return new OutboxQueueStats(
                    result.getLong("pending"),
                    result.getLong("claimed"),
                    result.getLong("published"),
                    result.getLong("dead"),
                    result.getLong("skipped"),
                    result.getLong("total_retries"),
                    result.getLong("retries_due"),
                    result.getLong("expired_claims"),
                    instant(result.getTimestamp("oldest_pending_at")),
                    instant(result.getTimestamp("oldest_claimed_at")),
                    instant(result.getTimestamp("snapshot_at"))
            );
        }
    }

    private static Instant instant(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant();
    }
}
'@

$cli = @'
package com.lyapunov.cli;

import com.lyapunov.repository.OutboxQueueStats;
import com.lyapunov.repository.OutboxQueueStatsRepository;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * Prints a database-backed outbox queue snapshot.
 *
 * This command does not probe Kafka or prove that a worker process is alive.
 * Required environment: LYAPUNOV_JDBC_URL, LYAPUNOV_DB_USER, LYAPUNOV_DB_PASSWORD.
 */
public final class OutboxStatusCli {
    private OutboxStatusCli() {}

    public static void main(String[] args) {
        if (args.length != 0) {
            usage();
            System.exit(2);
            return;
        }

        String jdbcUrl = requiredEnv("LYAPUNOV_JDBC_URL");
        String username = requiredEnv("LYAPUNOV_DB_USER");
        String password = requiredEnv("LYAPUNOV_DB_PASSWORD");
        if (jdbcUrl == null || username == null || password == null) {
            System.err.println("Missing required database environment variables.");
            System.exit(2);
            return;
        }

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(1);
        config.setPoolName("lyapunov-status-cli");

        try (HikariDataSource dataSource = new HikariDataSource(config)) {
            OutboxQueueStats stats = new OutboxQueueStatsRepository(dataSource).snapshot();
            print(stats);
            if ("UNHEALTHY".equals(stats.queueHealth())) {
                System.exit(1);
            }
        } catch (Exception exception) {
            System.err.println("Could not read outbox status: " + exception.getMessage());
            System.exit(1);
        }
    }

    private static void print(OutboxQueueStats s) {
        System.out.println("LYAPUNOV OUTBOX STATUS (database snapshot)");
        System.out.println("Queue signal:       " + s.queueHealth());
        System.out.println("Snapshot at:        " + s.snapshotAt());
        System.out.println("Total events:       " + s.totalEvents());
        System.out.println("Pending:            " + s.pending());
        System.out.println("Claimed:            " + s.claimed());
        System.out.println("Published:          " + s.published());
        System.out.println("Dead-lettered:      " + s.dead());
        System.out.println("Skipped:            " + s.skipped());
        System.out.println("Total retry attempts: " + s.totalRetries());
        System.out.println("Retries currently due: " + s.retriesDue());
        System.out.println("Expired claims:     " + s.expiredClaims());
        System.out.println("Oldest pending:     " + value(s.oldestPendingAt()));
        System.out.println("Oldest claimed update: " + value(s.oldestClaimedAt()));
        System.out.println();
        System.out.println("Note: this is persisted queue state; it does not establish Kafka connectivity or worker liveness.");
    }

    private static String value(Object value) {
        return value == null ? "none" : value.toString();
    }

    private static String requiredEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            System.err.println("Required environment variable is not set: " + name);
            return null;
        }
        return value;
    }

    private static void usage() {
        System.err.println("Usage: OutboxStatusCli");
        System.err.println("Set LYAPUNOV_JDBC_URL, LYAPUNOV_DB_USER, LYAPUNOV_DB_PASSWORD.");
    }
}
'@

$test = @'
package com.lyapunov.repository;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class OutboxQueueStatsRepositoryTest {
    private static PostgreSQLContainer<?> postgres;
    private static HikariDataSource dataSource;
    private OutboxQueueStatsRepository repository;

    @BeforeAll
    static void startDatabase() throws Exception {
        postgres = new PostgreSQLContainer<>("postgres:16-alpine");
        postgres.start();
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(postgres.getJdbcUrl());
        config.setUsername(postgres.getUsername());
        config.setPassword(postgres.getPassword());
        config.setMaximumPoolSize(3);
        dataSource = new HikariDataSource(config);
        String ddl = Files.readString(Path.of("docker", "init.sql"));
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(ddl);
        }
    }

    @AfterAll
    static void stopDatabase() {
        if (dataSource != null) dataSource.close();
        if (postgres != null) postgres.stop();
    }

    @BeforeEach
    void reset() throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE event_history, outbox_events");
        }
        repository = new OutboxQueueStatsRepository(dataSource);
    }

    @Test
    void returnsZeroCountsForAnEmptyQueue() throws Exception {
        OutboxQueueStats stats = repository.snapshot();
        assertEquals(0, stats.totalEvents());
        assertEquals(0, stats.pending());
        assertEquals(0, stats.claimed());
        assertEquals(0, stats.published());
        assertEquals(0, stats.dead());
        assertEquals(0, stats.skipped());
        assertEquals("HEALTHY", stats.queueHealth());
        assertNotNull(stats.snapshotAt());
        assertNull(stats.oldestPendingAt());
    }

    @Test
    void aggregatesEveryLifecycleStateAndRetrySignals() throws Exception {
        insert("PENDING", 2, null, Instant.now().minusSeconds(120));
        insert("CLAIMED", 1, Instant.now().minusSeconds(30), Instant.now().minusSeconds(60));
        insert("PUBLISHED", 3, null, Instant.now().minusSeconds(50));
        insert("DEAD", 4, null, Instant.now().minusSeconds(40));
        insert("SKIPPED", 0, null, Instant.now().minusSeconds(20));

        OutboxQueueStats stats = repository.snapshot();
        assertEquals(5, stats.totalEvents());
        assertEquals(1, stats.pending());
        assertEquals(1, stats.claimed());
        assertEquals(1, stats.published());
        assertEquals(1, stats.dead());
        assertEquals(1, stats.skipped());
        assertEquals(10, stats.totalRetries());
        assertEquals(1, stats.retriesDue());
        assertEquals(1, stats.expiredClaims());
        assertEquals("UNHEALTHY", stats.queueHealth());
        assertNotNull(stats.oldestPendingAt());
        assertNotNull(stats.oldestClaimedAt());
    }

    @Test
    void reportsDegradedWhenQueueHasWorkButNoTerminalOrExpiredSignals() throws Exception {
        insert("PENDING", 0, null, Instant.now());
        OutboxQueueStats stats = repository.snapshot();
        assertEquals("DEGRADED", stats.queueHealth());
        assertEquals(0, stats.dead());
        assertEquals(0, stats.expiredClaims());
    }

    private static void insert(String status, int retries, Instant leaseExpiry, Instant createdAt)
            throws Exception {
        String sql = """
                INSERT INTO outbox_events
                    (id, aggregate_type, aggregate_id, sequence_number, event_type, payload,
                     status, retry_count, next_attempt_at, lease_expires_at, created_at, updated_at)
                VALUES (?, 'TEST', ?, 1, 'TEST_EVENT', '{}'::jsonb,
                        ?, ?, NOW() - INTERVAL '1 second', ?, ?, ?)
                """;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, UUID.randomUUID());
            statement.setString(2, UUID.randomUUID().toString());
            statement.setString(3, status);
            statement.setInt(4, retries);
            statement.setTimestamp(5, leaseExpiry == null ? null : java.sql.Timestamp.from(leaseExpiry));
            statement.setTimestamp(6, java.sql.Timestamp.from(createdAt));
            statement.setTimestamp(7, java.sql.Timestamp.from(createdAt));
            statement.executeUpdate();
        }
    }
}
'@

$docs = @'
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
'@

Write-Utf8File "src\main\java\com\lyapunov\repository\OutboxQueueStats.java" $stats
Write-Utf8File "src\main\java\com\lyapunov\repository\OutboxQueueStatsRepository.java" $repository
Write-Utf8File "src\main\java\com\lyapunov\cli\OutboxStatusCli.java" $cli
Write-Utf8File "src\test\java\com\lyapunov\repository\OutboxQueueStatsRepositoryTest.java" $test
Write-Utf8File "docs\OUTBOX_STATUS_AND_OBSERVABILITY.md" $docs

Write-Host "`nBatch 05 files written. Running full Maven verification..." -ForegroundColor Cyan
& .\mvnw.cmd -B -ntp clean verify
if ($LASTEXITCODE -ne 0) {
    throw "Maven verification failed. Files are retained locally; nothing was staged, committed, or pushed."
}

$paths = @(
    "src/main/java/com/lyapunov/repository/OutboxQueueStats.java",
    "src/main/java/com/lyapunov/repository/OutboxQueueStatsRepository.java",
    "src/main/java/com/lyapunov/cli/OutboxStatusCli.java",
    "src/test/java/com/lyapunov/repository/OutboxQueueStatsRepositoryTest.java",
    "docs/OUTBOX_STATUS_AND_OBSERVABILITY.md"
)
git add -- $paths
if ($LASTEXITCODE -ne 0) { throw "Could not stage the explicit Batch 05 paths." }

git diff --cached --check
if ($LASTEXITCODE -ne 0) { throw "Staged diff check failed. Nothing committed." }

git commit -m "feat: add outbox queue observability and status CLI"
if ($LASTEXITCODE -ne 0) { throw "Commit failed. Review staged changes." }

git push origin main
if ($LASTEXITCODE -ne 0) { throw "Push failed. Local commit is retained; retry git push origin main." }

Write-Host "`nBatch 05 committed and pushed successfully." -ForegroundColor Green
git log -1 --oneline
git status -sb
