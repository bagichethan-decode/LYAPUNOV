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