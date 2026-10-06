package com.lyapunov.repository;

import com.lyapunov.model.OutboxEvent;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

/**
 * Persists the lifecycle of outbox events for reliability analysis and autopsy.
 */
public class EventHistoryRepository {

    private final DataSource dataSource;

    public EventHistoryRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Records a lifecycle action for an outbox event.
     */
    public void record(
        OutboxEvent event,
        String action,
        String workerId,
        String details
    ) throws SQLException {

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

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

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

            if (details == null) {
                stmt.setString(8, "{}");
            } else {
                stmt.setString(8, details);
            }

            stmt.executeUpdate();
        }
    }
}
