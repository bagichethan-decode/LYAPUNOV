package com.lyapunov.repository;

import com.lyapunov.model.OutboxEvent;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;

/**
 * Persists the lifecycle of outbox events for reliability analysis and autopsy.
 */
public class EventHistoryRepository {

    private final DataSource dataSource;

    public EventHistoryRepository(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    /**
     * Records a lifecycle action using its own connection and transaction.
     */
    public void record(
        OutboxEvent event,
        String action,
        String workerId,
        String details
    ) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            record(conn, event, action, workerId, details);
        }
    }

    /**
     * Records a lifecycle action on the caller's connection so it can commit or roll back
     * atomically with the related outbox state transition.
     */
    public void record(
        Connection connection,
        OutboxEvent event,
        String action,
        String workerId,
        String details
    ) throws SQLException {
        Objects.requireNonNull(connection, "connection must not be null");
        Objects.requireNonNull(event, "event must not be null");
        Objects.requireNonNull(action, "action must not be null");

        String sql = """
            INSERT INTO event_history (
                event_id,
                aggregate_type,
                aggregate_id,
                sequence_number,
                event_type,
                action,
                worker_id,
                details
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb)
        """;

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setObject(1, event.id());
            stmt.setString(2, event.aggregateType());
            stmt.setString(3, event.aggregateId());
            stmt.setLong(4, event.sequenceNumber());
            stmt.setString(5, event.eventType());
            stmt.setString(6, action);

            if (workerId == null) {
                stmt.setNull(7, java.sql.Types.VARCHAR);
            } else {
                stmt.setString(7, workerId);
            }

            stmt.setString(8, details == null ? "{}" : details);
            stmt.executeUpdate();
        }
    }
}
