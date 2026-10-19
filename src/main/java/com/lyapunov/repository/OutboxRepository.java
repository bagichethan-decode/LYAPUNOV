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
    private final EventHistoryRepository historyRepository;

    public OutboxRepository(DataSource dataSource) {
        if (dataSource == null) {
            throw new IllegalArgumentException("dataSource must not be null");
        }

        this.dataSource = dataSource;
        this.historyRepository = new EventHistoryRepository(dataSource);
    }

    /**
     * Claims a batch of pending or lease-expired events using SELECT ... FOR UPDATE SKIP LOCKED.
     */
    public List<OutboxEvent> claimPendingEvents(
        String workerId,
        int batchSize,
        Duration leaseDuration
    ) throws SQLException {

        if (workerId == null || workerId.isBlank()) {
            throw new IllegalArgumentException("workerId must not be null or blank");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        if (leaseDuration == null || leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException("leaseDuration must be positive");
        }

        String selectSql = """
            SELECT e.id, e.aggregate_type, e.aggregate_id, e.sequence_number, e.event_type,
                   e.payload, e.status, e.retry_count, e.claimed_by, e.lease_expires_at,
                   e.created_at, e.updated_at
            FROM outbox_events e
            WHERE (
                e.status = 'PENDING'
                OR (e.status = 'CLAIMED' AND e.lease_expires_at < NOW())
            )
            AND NOT EXISTS (
                SELECT 1
                FROM outbox_events earlier
                WHERE earlier.aggregate_type = e.aggregate_type
                  AND earlier.aggregate_id = e.aggregate_id
                  AND earlier.sequence_number < e.sequence_number
                  AND earlier.status <> 'PUBLISHED'
            )
            ORDER BY e.created_at ASC,
                     e.aggregate_type ASC,
                     e.aggregate_id ASC,
                     e.sequence_number ASC
            LIMIT ?
            FOR UPDATE OF e SKIP LOCKED
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

                    for (OutboxEvent event : claimed) {
                        historyRepository.record(
                            event,
                            "EVENT_CLAIMED",
                            workerId,
                            "{\"leaseDurationSeconds\":" + leaseDuration.toSeconds() + "}"
                        );
                    }
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

        if (eventId == null) {
            throw new IllegalArgumentException("eventId must not be null");
        }

        String selectSql = """
            SELECT id, aggregate_type, aggregate_id, sequence_number, event_type,
                   payload, status, retry_count, claimed_by, lease_expires_at, created_at, updated_at
            FROM outbox_events
            WHERE id = ?
            FOR UPDATE
        """;

        String updateSql = """
            UPDATE outbox_events
            SET status = 'PUBLISHED',
                updated_at = NOW()
            WHERE id = ?
        """;

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement selectStmt = conn.prepareStatement(selectSql);
                 PreparedStatement updateStmt = conn.prepareStatement(updateSql)) {

                selectStmt.setObject(1, eventId);

                try (ResultSet rs = selectStmt.executeQuery()) {
                    if (!rs.next()) {
                        throw new SQLException("Event not found: " + eventId);
                    }

                    OutboxEvent event = mapEvent(rs);

                    updateStmt.setObject(1, eventId);
                    updateStmt.executeUpdate();

                    historyRepository.record(
                        event,
                        "EVENT_PUBLISHED",
                        event.claimedBy(),
                        "{\"previousStatus\":\"" + event.status() + "\"}"
                    );
                }

                conn.commit();

            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }

    /**
     * Increments retry count and schedules next attempt or marks as failed.
     */
    public void recordFailure(UUID eventId, int maxRetries) throws SQLException {

        if (eventId == null) {
            throw new IllegalArgumentException("eventId must not be null");
        }
        if (maxRetries <= 0) {
            throw new IllegalArgumentException("maxRetries must be positive");
        }

        String selectSql = """
            SELECT id, aggregate_type, aggregate_id, sequence_number, event_type,
                   payload, status, retry_count, claimed_by, lease_expires_at, created_at, updated_at
            FROM outbox_events
            WHERE id = ?
            FOR UPDATE
        """;

        String updateSql = """
            UPDATE outbox_events
            SET retry_count = retry_count + 1,
                status = CASE WHEN retry_count + 1 >= ? THEN 'FAILED' ELSE 'PENDING' END,
                claimed_by = NULL,
                lease_expires_at = NULL,
                updated_at = NOW()
            WHERE id = ?
        """;

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement selectStmt = conn.prepareStatement(selectSql);
                 PreparedStatement updateStmt = conn.prepareStatement(updateSql)) {

                selectStmt.setObject(1, eventId);

                try (ResultSet rs = selectStmt.executeQuery()) {
                    if (!rs.next()) {
                        throw new SQLException("Event not found: " + eventId);
                    }

                    OutboxEvent event = mapEvent(rs);
                    int nextRetryCount = event.retryCount() + 1;
                    String nextStatus = nextRetryCount >= maxRetries
                        ? "FAILED"
                        : "PENDING";

                    updateStmt.setInt(1, maxRetries);
                    updateStmt.setObject(2, eventId);
                    updateStmt.executeUpdate();

                    historyRepository.record(
                        event,
                        "EVENT_FAILED",
                        event.claimedBy(),
                        "{\"retryCount\":" + nextRetryCount +
                            ",\"nextStatus\":\"" + nextStatus + "\"}"
                    );
                }

                conn.commit();

            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }

    private OutboxEvent mapEvent(ResultSet rs) throws SQLException {
        return new OutboxEvent(
            (UUID) rs.getObject("id"),
            rs.getString("aggregate_type"),
            rs.getString("aggregate_id"),
            rs.getLong("sequence_number"),
            rs.getString("event_type"),
            rs.getString("payload"),
            EventStatus.valueOf(rs.getString("status")),
            rs.getInt("retry_count"),
            rs.getString("claimed_by"),
            rs.getTimestamp("lease_expires_at") == null
                ? null
                : rs.getTimestamp("lease_expires_at").toInstant(),
            rs.getTimestamp("created_at").toInstant(),
            rs.getTimestamp("updated_at").toInstant()
        );
    }
}
