package com.lyapunov.inbox;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;
import java.util.UUID;

/** PostgreSQL inbox: deduplication and business writes commit atomically. */
public final class JdbcInboxProcessor {
    private static final String INSERT_SQL = """
            INSERT INTO consumer_inbox (consumer_name, event_id, event_type, received_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (consumer_name, event_id) DO NOTHING
            """;
    private static final String MARK_SQL = """
            UPDATE consumer_inbox SET processed_at = CURRENT_TIMESTAMP
             WHERE consumer_name = ? AND event_id = ? AND processed_at IS NULL
            """;
    private final DataSource dataSource;

    public JdbcInboxProcessor(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    /** Returns DUPLICATE without running work when this consumer already committed the event. */
    public InboxProcessingResult process(String consumerName, UUID eventId, String eventType, InboxWork work) throws SQLException {
        InboxKey key = new InboxKey(consumerName, eventId);
        if (eventType == null || eventType.isBlank()) throw new IllegalArgumentException("eventType must not be blank");
        Objects.requireNonNull(work, "work must not be null");

        try (Connection connection = dataSource.getConnection()) {
            boolean originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                int inserted;
                try (PreparedStatement statement = connection.prepareStatement(INSERT_SQL)) {
                    statement.setString(1, key.consumerName());
                    statement.setObject(2, key.eventId());
                    statement.setString(3, eventType);
                    inserted = statement.executeUpdate();
                }
                if (inserted == 0) {
                    connection.commit();
                    return InboxProcessingResult.DUPLICATE;
                }

                work.execute(connection);
                try (PreparedStatement statement = connection.prepareStatement(MARK_SQL)) {
                    statement.setString(1, key.consumerName());
                    statement.setObject(2, key.eventId());
                    int updated = statement.executeUpdate();
                    if (updated != 1) throw new SQLException("Inbox row was not marked processed; updated=" + updated);
                }
                connection.commit();
                return InboxProcessingResult.PROCESSED;
            } catch (SQLException | RuntimeException failure) {
                try { connection.rollback(); } catch (SQLException rollbackFailure) { failure.addSuppressed(rollbackFailure); }
                throw failure;
            } finally {
                try { connection.setAutoCommit(originalAutoCommit); } catch (SQLException ignored) { /* connection closes below */ }
            }
        }
    }
}
