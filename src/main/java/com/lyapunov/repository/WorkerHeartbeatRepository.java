package com.lyapunov.repository;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public final class WorkerHeartbeatRepository {
    private final DataSource dataSource;

    public WorkerHeartbeatRepository(DataSource dataSource) {
        if (dataSource == null) throw new IllegalArgumentException("dataSource must not be null");
        this.dataSource = dataSource;
    }

    public void heartbeat(String workerId, String instanceId, long publishedCount,
                          long failedCount, long staleClaimCount, String lastError) throws Exception {
        requireText(workerId, "workerId");
        requireText(instanceId, "instanceId");
        if (publishedCount < 0 || failedCount < 0 || staleClaimCount < 0) {
            throw new IllegalArgumentException("worker counters must not be negative");
        }

        String sql = """
                INSERT INTO worker_heartbeats
                    (worker_id, instance_id, status, started_at, last_seen_at,
                     last_successful_publish_at, published_count, failed_count,
                     stale_claim_count, last_error)
                VALUES (?, ?, 'RUNNING', clock_timestamp(), clock_timestamp(),
                        CASE WHEN ? > 0 THEN clock_timestamp() ELSE NULL END, ?, ?, ?, ?)
                ON CONFLICT (worker_id) DO UPDATE SET
                    instance_id = EXCLUDED.instance_id,
                    status = 'RUNNING',
                    last_seen_at = clock_timestamp(),
                    last_successful_publish_at = CASE
                        WHEN EXCLUDED.published_count > worker_heartbeats.published_count
                        THEN clock_timestamp()
                        ELSE worker_heartbeats.last_successful_publish_at
                    END,
                    published_count = EXCLUDED.published_count,
                    failed_count = EXCLUDED.failed_count,
                    stale_claim_count = EXCLUDED.stale_claim_count,
                    last_error = EXCLUDED.last_error
                """;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, workerId);
            statement.setString(2, instanceId);
            statement.setLong(3, publishedCount);
            statement.setLong(4, publishedCount);
            statement.setLong(5, failedCount);
            statement.setLong(6, staleClaimCount);
            statement.setString(7, lastError);
            statement.executeUpdate();
        }
    }

    public boolean markStopped(String workerId, String instanceId) throws Exception {
        requireText(workerId, "workerId");
        requireText(instanceId, "instanceId");
        String sql = """
                UPDATE worker_heartbeats
                   SET status = 'STOPPED', last_seen_at = clock_timestamp()
                 WHERE worker_id = ? AND instance_id = ? AND status <> 'STOPPED'
                """;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, workerId);
            statement.setString(2, instanceId);
            return statement.executeUpdate() == 1;
        }
    }

    public List<WorkerHeartbeat> findAll(Duration staleAfter) throws Exception {
        if (staleAfter == null || staleAfter.isNegative() || staleAfter.isZero()) {
            throw new IllegalArgumentException("staleAfter must be positive");
        }
        String sql = """
                SELECT worker_id, instance_id, status, started_at, last_seen_at,
                       last_successful_publish_at, published_count, failed_count,
                       stale_claim_count, last_error,
                       (status = 'RUNNING' AND last_seen_at < clock_timestamp() - (?::double precision * INTERVAL '1 millisecond')) AS stale
                  FROM worker_heartbeats
                 ORDER BY last_seen_at DESC, worker_id
                """;
        List<WorkerHeartbeat> workers = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, staleAfter.toMillis());
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    Timestamp started = rs.getTimestamp("started_at");
                    Timestamp seen = rs.getTimestamp("last_seen_at");
                    Timestamp published = rs.getTimestamp("last_successful_publish_at");
                    workers.add(new WorkerHeartbeat(
                            rs.getString("worker_id"), rs.getString("instance_id"),
                            rs.getString("status"), started.toInstant(), seen.toInstant(),
                            published == null ? null : published.toInstant(),
                            rs.getLong("published_count"), rs.getLong("failed_count"),
                            rs.getLong("stale_claim_count"), rs.getString("last_error"),
                            rs.getBoolean("stale")));
                }
            }
        }
        return List.copyOf(workers);
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " must not be blank");
    }
}
