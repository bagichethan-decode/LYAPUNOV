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
import java.util.Objects;
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
     * Each claim receives a fresh token. Completion and failure operations must present that
     * token, which fences off stale workers after a lease has been reclaimed.
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
                   e.payload, e.status, e.retry_count, e.claimed_by, e.claim_token,
                   e.lease_expires_at, e.created_at, e.updated_at
            FROM outbox_events e
            WHERE (
                e.status = 'PENDING'
                OR (e.status = 'CLAIMED' AND e.lease_expires_at < clock_timestamp())
            )
            AND NOT EXISTS (
                SELECT 1
                FROM outbox_events earlier
                WHERE earlier.aggregate_type = e.aggregate_type
                  AND earlier.aggregate_id = e.aggregate_id
                  AND earlier.sequence_number < e.sequence_number
                  AND earlier.status NOT IN ('PUBLISHED', 'SKIPPED')
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
                claim_token = ?,
                lease_expires_at = ?,
                updated_at = clock_timestamp()
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
                        UUID claimToken = UUID.randomUUID();

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
                            claimToken,
                            leaseExpiry,
                            rs.getTimestamp("created_at").toInstant(),
                            rs.getTimestamp("updated_at").toInstant()
                        );

                        claimed.add(event);

                        updateStmt.setString(1, workerId);
                        updateStmt.setObject(2, claimToken);
                        updateStmt.setTimestamp(3, Timestamp.from(leaseExpiry));
                        updateStmt.setObject(4, id);
                        updateStmt.addBatch();
                    }
                }

                if (!claimed.isEmpty()) {
                    int[] updates = updateStmt.executeBatch();
                    for (int updated : updates) {
                        if (updated == 0 || updated == PreparedStatement.EXECUTE_FAILED) {
                            throw new SQLException("Failed to persist an outbox event claim");
                        }
                    }

                    for (OutboxEvent event : claimed) {
                        historyRepository.record(
                            conn,
                            event,
                            "EVENT_CLAIMED",
                            workerId,
                            "{\"leaseDurationMillis\":" + leaseDuration.toMillis() +
                                ",\"claimToken\":\"" + event.claimToken() + "\"}"
                        );
                    }
                }

                conn.commit();

            } catch (SQLException | RuntimeException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        }

        return claimed;
    }

    /**
     * Marks an event as published only if the caller still owns the unexpired claim.
     * Returns false when the claim is stale, expired, or owned by another worker.
     */
    public boolean markPublished(
        UUID eventId,
        String workerId,
        UUID claimToken
    ) throws SQLException {
        validateClaimArguments(eventId, workerId, claimToken);

        String selectSql = """
            SELECT id, aggregate_type, aggregate_id, sequence_number, event_type,
                   payload, status, retry_count, claimed_by, claim_token,
                   lease_expires_at, created_at, updated_at,
                   lease_expires_at > clock_timestamp() AS lease_valid
            FROM outbox_events
            WHERE id = ?
            FOR UPDATE
        """;

        String updateSql = """
            UPDATE outbox_events
            SET status = 'PUBLISHED',
                claimed_by = NULL,
                claim_token = NULL,
                lease_expires_at = NULL,
                updated_at = clock_timestamp()
            WHERE id = ?
              AND status = 'CLAIMED'
              AND claimed_by = ?
              AND claim_token = ?
              AND lease_expires_at > clock_timestamp()
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
                    if (!isCurrentClaim(event, workerId, claimToken) || !rs.getBoolean("lease_valid")) {
                        conn.commit();
                        return false;
                    }

                    updateStmt.setObject(1, eventId);
                    updateStmt.setString(2, workerId);
                    updateStmt.setObject(3, claimToken);
                    if (updateStmt.executeUpdate() != 1) {
                        conn.commit();
                        return false;
                    }

                    historyRepository.record(
                        conn,
                        event,
                        "EVENT_PUBLISHED",
                        workerId,
                        "{\"previousStatus\":\"" + event.status() + "\"}"
                    );
                }

                conn.commit();
                return true;

            } catch (SQLException | RuntimeException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }

    /**
     * Records a failed delivery only if the caller still owns the unexpired claim.
     * Returns false when the claim is stale, expired, or owned by another worker.
     */
    public boolean recordFailure(
        UUID eventId,
        String workerId,
        UUID claimToken,
        int maxRetries
    ) throws SQLException {
        validateClaimArguments(eventId, workerId, claimToken);
        if (maxRetries <= 0) {
            throw new IllegalArgumentException("maxRetries must be positive");
        }

        String selectSql = """
            SELECT id, aggregate_type, aggregate_id, sequence_number, event_type,
                   payload, status, retry_count, claimed_by, claim_token,
                   lease_expires_at, created_at, updated_at,
                   lease_expires_at > clock_timestamp() AS lease_valid
            FROM outbox_events
            WHERE id = ?
            FOR UPDATE
        """;

        String updateSql = """
            UPDATE outbox_events
            SET retry_count = retry_count + 1,
                status = CASE WHEN retry_count + 1 >= ? THEN 'DEAD' ELSE 'PENDING' END,
                claimed_by = NULL,
                claim_token = NULL,
                lease_expires_at = NULL,
                updated_at = clock_timestamp()
            WHERE id = ?
              AND status = 'CLAIMED'
              AND claimed_by = ?
              AND claim_token = ?
              AND lease_expires_at > clock_timestamp()
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
                    if (!isCurrentClaim(event, workerId, claimToken) || !rs.getBoolean("lease_valid")) {
                        conn.commit();
                        return false;
                    }

                    int nextRetryCount = event.retryCount() + 1;
                    String nextStatus = nextRetryCount >= maxRetries ? "DEAD" : "PENDING";

                    updateStmt.setInt(1, maxRetries);
                    updateStmt.setObject(2, eventId);
                    updateStmt.setString(3, workerId);
                    updateStmt.setObject(4, claimToken);
                    if (updateStmt.executeUpdate() != 1) {
                        conn.commit();
                        return false;
                    }

                    historyRepository.record(
                        conn,
                        event,
                        "EVENT_FAILED",
                        workerId,
                        "{\"retryCount\":" + nextRetryCount +
                            ",\"nextStatus\":\"" + nextStatus + "\"}"
                    );
                }

                conn.commit();
                return true;

            } catch (SQLException | RuntimeException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }

    /**
     * Requeues a terminally failed event after an operator reviews the failure.
     * The retry budget is reset and the operator action is persisted atomically
     * with the state change. Legacy FAILED rows are accepted for compatibility.
     */
    public void retryDeadEvent(UUID eventId, String operatorId, String reason) throws SQLException {
        transitionDeadEvent(eventId, operatorId, reason, "PENDING", "EVENT_RETRIED", true);
    }

    /**
     * Explicitly skips a terminally failed event. SKIPPED events no longer block
     * later sequence numbers for their aggregate, and the action is audited.
     */
    public void skipDeadEvent(UUID eventId, String operatorId, String reason) throws SQLException {
        transitionDeadEvent(eventId, operatorId, reason, "SKIPPED", "EVENT_SKIPPED", false);
    }

    private void transitionDeadEvent(
        UUID eventId,
        String operatorId,
        String reason,
        String targetStatus,
        String action,
        boolean resetRetryCount
    ) throws SQLException {
        validateOperatorActionArguments(eventId, operatorId, reason);

        String selectSql = """
            SELECT id, aggregate_type, aggregate_id, sequence_number, event_type,
                   payload, status, retry_count, claimed_by, claim_token,
                   lease_expires_at, created_at, updated_at
            FROM outbox_events
            WHERE id = ?
            FOR UPDATE
        """;

        String updateSql = """
            UPDATE outbox_events
            SET status = ?,
                retry_count = CASE WHEN ? THEN 0 ELSE retry_count END,
                claimed_by = NULL,
                claim_token = NULL,
                lease_expires_at = NULL,
                updated_at = clock_timestamp()
            WHERE id = ?
              AND status IN ('DEAD', 'FAILED')
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
                    if (event.status() != EventStatus.DEAD && event.status() != EventStatus.FAILED) {
                        throw new SQLException(
                            "Operator action requires a DEAD event; current status is " + event.status()
                        );
                    }

                    updateStmt.setString(1, targetStatus);
                    updateStmt.setBoolean(2, resetRetryCount);
                    updateStmt.setObject(3, eventId);
                    if (updateStmt.executeUpdate() != 1) {
                        throw new SQLException("Could not transition dead event: " + eventId);
                    }

                    String details = "{\"previousStatus\":\"" + event.status()
                        + "\",\"previousRetryCount\":" + event.retryCount()
                        + ",\"reason\":\"" + escapeJson(reason) + "\"}";
                    historyRepository.record(conn, event, action, operatorId, details);
                }

                conn.commit();

            } catch (SQLException | RuntimeException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }

    private static void validateOperatorActionArguments(UUID eventId, String operatorId, String reason) {
        if (eventId == null) {
            throw new IllegalArgumentException("eventId must not be null");
        }
        if (operatorId == null || operatorId.isBlank()) {
            throw new IllegalArgumentException("operatorId must not be null or blank");
        }
        if (reason == null || reason.isBlank()) {
            throw new IllegalArgumentException("reason must not be null or blank");
        }
    }

    private static String escapeJson(String value) {
        StringBuilder escaped = new StringBuilder(value.length() + 16);
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
                case '"' -> escaped.append("\\\"");
                case '\\' -> escaped.append("\\\\");
                case '\b' -> escaped.append("\\b");
                case '\f' -> escaped.append("\\f");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> {
                    if (ch < 0x20) {
                        escaped.append(String.format("\\u%04x", (int) ch));
                    } else {
                        escaped.append(ch);
                    }
                }
            }
        }
        return escaped.toString();
    }

    private static void validateClaimArguments(UUID eventId, String workerId, UUID claimToken) {
        if (eventId == null) {
            throw new IllegalArgumentException("eventId must not be null");
        }
        if (workerId == null || workerId.isBlank()) {
            throw new IllegalArgumentException("workerId must not be null or blank");
        }
        if (claimToken == null) {
            throw new IllegalArgumentException("claimToken must not be null");
        }
    }

    private static boolean isCurrentClaim(OutboxEvent event, String workerId, UUID claimToken) {
        return event.status() == EventStatus.CLAIMED
            && Objects.equals(event.claimedBy(), workerId)
            && Objects.equals(event.claimToken(), claimToken);
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
            (UUID) rs.getObject("claim_token"),
            rs.getTimestamp("lease_expires_at") == null
                ? null
                : rs.getTimestamp("lease_expires_at").toInstant(),
            rs.getTimestamp("created_at").toInstant(),
            rs.getTimestamp("updated_at").toInstant()
        );
    }
}
