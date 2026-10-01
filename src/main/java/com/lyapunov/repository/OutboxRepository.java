package com.lyapunov.repository;

import com.lyapunov.model.OutboxEvent;
import com.lyapunov.model.OutboxEvent.EventStatus;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Handles outbox polling and state transitions using PostgreSQL row locking.
 */
public class OutboxRepository {

    private final DataSource dataSource;

    public OutboxRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Claims a batch of pending or lease-expired events using SELECT ... FOR UPDATE SKIP LOCKED.
     */
    public List<OutboxEvent> claimPendingEvents(String workerId, int batchSize, Duration leaseDuration) throws SQLException {
        String selectSql = """
            SELECT id, aggregate_type, aggregate_id, sequence_number, event_type, 
                   payload, status, retry_count, claimed_by, lease_expires_at, created_at, updated_at
            FROM outbox_events
            WHERE status = 'PENDING' 
               OR (status = 'CLAIMED' AND lease_expires_at < NOW())
            ORDER BY created_at ASC
            LIMIT ?
            FOR UPDATE SKIP LOCKED
        """;

        String updateSql = """
            UPDATE outbox_events
            SET status = 'CLAIMED',
                claimed_by = ?,
                lease_expires_at = ?,
                updated_at = NOW()
            WHERE id = ?
        """;

        List<OutboxEvent> claimed = new ArrayList<>();
        Instant leaseExpiry = Instant.now().plus(leaseDuration);

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement selectStmt = conn.prepareStatement(selectSql);
                 PreparedStatement updateStmt = conn.prepareStatement(updateSql)) {

                selectStmt.setInt(1, batchSize);
                try (ResultSet rs = selectStmt.executeQuery()) {
                    while (rs.next()) {
                        UUID id = (UUID) rs.getObject("id");
                        OutboxEvent event = new OutboxEvent(
                            id,
                            rs.getString("aggregate_type"),
                            rs.getString("aggregate_id"),
                            rs.getLong("sequence_number"),
                            rs.getString("event_type"),
                            rs.getString("payload"),
                            EventStatus.CLAIMED,
                            rs.getInt("retry_count"),
                            workerId,
                            leaseExpiry,
                            rs.getTimestamp("created_at").toInstant(),
                            rs.getTimestamp("updated_at").toInstant()
                        );
                        claimed.add(event);

                        updateStmt.setString(1, workerId);
                        updateStmt.setTimestamp(2, Timestamp.from(leaseExpiry));
                        updateStmt.setObject(3, id);
                        updateStmt.addBatch();
                    }
                }

                if (!claimed.isEmpty()) {
                    updateStmt.executeBatch();
                }
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        }
        return claimed;
    }

    /**
     * Marks an event as successfully published after Kafka broker acknowledgement.
     */
    public void markPublished(UUID eventId) throws SQLException {
        String sql = """
            UPDATE outbox_events
            SET status = 'PUBLISHED',
                updated_at = NOW()
            WHERE id = ?
        """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setObject(1, eventId);
            stmt.executeUpdate();
        }
    }

    /**
     * Increments retry count and schedules next attempt or marks as failed.
     */
    public void recordFailure(UUID eventId, int maxRetries) throws SQLException {
        String sql = """
            UPDATE outbox_events
            SET retry_count = retry_count + 1,
                status = CASE WHEN retry_count + 1 >= ? THEN 'FAILED' ELSE 'PENDING' END,
                claimed_by = NULL,
                lease_expires_at = NULL,
                updated_at = NOW()
            WHERE id = ?
        """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, maxRetries);
            stmt.setObject(2, eventId);
            stmt.executeUpdate();
        }
    }
}
