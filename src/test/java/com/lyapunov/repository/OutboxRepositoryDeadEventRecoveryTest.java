package com.lyapunov.repository;

import com.lyapunov.model.OutboxEvent;
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OutboxRepositoryDeadEventRecoveryTest {

    private static PostgreSQLContainer<?> postgres;
    private static HikariDataSource dataSource;
    private OutboxRepository repository;

    @BeforeAll
    static void startDatabase() throws Exception {
        postgres = new PostgreSQLContainer<>("postgres:16-alpine");
        postgres.start();

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(postgres.getJdbcUrl());
        config.setUsername(postgres.getUsername());
        config.setPassword(postgres.getPassword());
        config.setMaximumPoolSize(4);
        dataSource = new HikariDataSource(config);

        String ddl = Files.readString(Path.of("docker", "init.sql"));
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(ddl);
        }
    }

    @AfterAll
    static void stopDatabase() {
        if (dataSource != null) {
            dataSource.close();
        }
        if (postgres != null) {
            postgres.stop();
        }
    }

    @BeforeEach
    void cleanDatabase() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("TRUNCATE event_history, outbox_events");
        }
        repository = new OutboxRepository(dataSource);
    }

    private UUID insertEvent(String aggregateId, long sequence) throws Exception {
        String sql = """
            INSERT INTO outbox_events
                (aggregate_type, aggregate_id, sequence_number, event_type, payload)
            VALUES ('Order', ?, ?, 'ORDER_UPDATED', '{}'::jsonb)
            RETURNING id
            """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, aggregateId);
            stmt.setLong(2, sequence);
            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue(rs.next());
                return (UUID) rs.getObject(1);
            }
        }
    }

    private OutboxEvent claimOnly(String workerId) throws Exception {
        List<OutboxEvent> events = repository.claimPendingEvents(
            workerId, 1, Duration.ofSeconds(20)
        );
        assertEquals(1, events.size());
        return events.get(0);
    }

    @Test
    void maxRetriesMovesEventToDeadAndOnlyBlocksItsAggregate() throws Exception {
        UUID deadId = insertEvent("order-A", 1);
        insertEvent("order-A", 2);
        insertEvent("order-B", 1);

        OutboxEvent claim = claimOnly("worker-A");
        assertEquals(deadId, claim.id());
        assertTrue(repository.recordFailure(deadId, "worker-A", claim.claimToken(), 1));
        assertEquals("DEAD", status(deadId));

        List<OutboxEvent> available = repository.claimPendingEvents(
            "worker-B", 10, Duration.ofSeconds(20)
        );
        assertEquals(1, available.size(),
            "The dead event should block order-A sequence 2 but not order-B");
        assertEquals("order-B", available.get(0).aggregateId());
        assertEquals(1L, available.get(0).sequenceNumber());
    }

    @Test
    void operatorCanRetryDeadEventAndAuditReason() throws Exception {
        UUID eventId = insertEvent("order-retry", 1);
        OutboxEvent claim = claimOnly("worker-A");
        assertTrue(repository.recordFailure(eventId, "worker-A", claim.claimToken(), 1));

        repository.retryDeadEvent(eventId, "operator-7", "fixed downstream mapping");

        assertEquals("PENDING", status(eventId));
        assertEquals(0, retryCount(eventId), "An operator retry starts a fresh retry budget");
        assertEquals(1, historyCount("EVENT_RETRIED"));
        assertEquals("operator-7", historyWorker("EVENT_RETRIED"));
        assertTrue(historyDetails("EVENT_RETRIED").contains("fixed downstream mapping"));

        OutboxEvent retried = claimOnly("worker-B");
        assertEquals(eventId, retried.id());
    }

    @Test
    void operatorCanSkipDeadEventToUnblockLaterSequence() throws Exception {
        UUID firstId = insertEvent("order-skip", 1);
        insertEvent("order-skip", 2);

        OutboxEvent claim = claimOnly("worker-A");
        assertEquals(firstId, claim.id());
        assertTrue(repository.recordFailure(firstId, "worker-A", claim.claimToken(), 1));
        assertEquals("DEAD", status(firstId));
        assertTrue(repository.claimPendingEvents("worker-B", 5, Duration.ofSeconds(20)).isEmpty());

        repository.skipDeadEvent(firstId, "operator-9", "poison event accepted for skip");

        assertEquals("SKIPPED", status(firstId));
        assertEquals(1, historyCount("EVENT_SKIPPED"));
        assertEquals("operator-9", historyWorker("EVENT_SKIPPED"));
        assertTrue(historyDetails("EVENT_SKIPPED").contains("poison event accepted for skip"));

        List<OutboxEvent> next = repository.claimPendingEvents(
            "worker-B", 5, Duration.ofSeconds(20)
        );
        assertEquals(1, next.size());
        assertEquals(2L, next.get(0).sequenceNumber());
    }

    @Test
    void operatorCannotRetryOrSkipAnEventThatIsNotDead() throws Exception {
        UUID eventId = insertEvent("order-pending", 1);
        assertThrows(SQLException.class,
            () -> repository.retryDeadEvent(eventId, "operator-1", "reason"));
        assertThrows(SQLException.class,
            () -> repository.skipDeadEvent(eventId, "operator-1", "reason"));
        assertEquals("PENDING", status(eventId));
        assertEquals(0, historyCount("EVENT_RETRIED"));
        assertEquals(0, historyCount("EVENT_SKIPPED"));
    }

    private static String status(UUID eventId) throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                 "SELECT status FROM outbox_events WHERE id = ?")) {
            stmt.setObject(1, eventId);
            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue(rs.next());
                return rs.getString(1);
            }
        }
    }

    private static int retryCount(UUID eventId) throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                 "SELECT retry_count FROM outbox_events WHERE id = ?")) {
            stmt.setObject(1, eventId);
            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue(rs.next());
                return rs.getInt(1);
            }
        }
    }

    private static int historyCount(String action) throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                 "SELECT COUNT(*) FROM event_history WHERE action = ?")) {
            stmt.setString(1, action);
            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue(rs.next());
                return rs.getInt(1);
            }
        }
    }

    private static String historyWorker(String action) throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                 "SELECT worker_id FROM event_history WHERE action = ?")) {
            stmt.setString(1, action);
            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue(rs.next());
                return rs.getString(1);
            }
        }
    }

    private static String historyDetails(String action) throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                 "SELECT details::text FROM event_history WHERE action = ?")) {
            stmt.setString(1, action);
            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue(rs.next());
                return rs.getString(1);
            }
        }
    }
}
