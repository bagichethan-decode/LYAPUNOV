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
import java.sql.Statement;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OutboxRepositoryOrderingTest {

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
    void cleanTable() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("TRUNCATE outbox_events");
        }

        repository = new OutboxRepository(dataSource);
    }

    private void insertEvent(String aggregateId, long sequence)
            throws Exception {

        String sql = """
            INSERT INTO outbox_events
                (aggregate_type, aggregate_id, sequence_number,
                 event_type, payload, created_at)
            VALUES ('Order', ?, ?, 'ORDER_UPDATED',
                    '{}'::jsonb, clock_timestamp())
            """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, aggregateId);
            stmt.setLong(2, sequence);
            stmt.executeUpdate();
        }
    }

    private static List<Long> sequences(List<OutboxEvent> events) {
        return events.stream()
            .map(OutboxEvent::sequenceNumber)
            .toList();
    }

    @Test
    void secondWorkerMustNotClaimLaterEventsOfAnAggregateStillInFlight()
            throws Exception {

        for (long seq = 1; seq <= 6; seq++) {
            insertEvent("order-1", seq);
        }

        List<OutboxEvent> workerA = repository.claimPendingEvents(
            "worker-A",
            3,
            Duration.ofSeconds(30)
        );

        List<OutboxEvent> workerB = repository.claimPendingEvents(
            "worker-B",
            3,
            Duration.ofSeconds(30)
        );

        assertFalse(
            workerA.isEmpty(),
            "Worker A should claim at least one event"
        );

        assertEquals(
            1L,
            workerA.get(0).sequenceNumber(),
            "Worker A must start at sequence 1"
        );

        assertTrue(
            workerB.isEmpty(),
            "Worker B claimed sequences " + sequences(workerB)
                + " while earlier events of order-1 were still unpublished; "
                + "publishing them first would break per-aggregate ordering"
        );
    }

    @Test
    void workersCanClaimDifferentAggregatesIndependently()
            throws Exception {

        insertEvent("order-1", 1);
        insertEvent("order-1", 2);

        insertEvent("order-2", 1);
        insertEvent("order-2", 2);

        List<OutboxEvent> workerA = repository.claimPendingEvents(
            "worker-A",
            1,
            Duration.ofSeconds(30)
        );

        List<OutboxEvent> workerB = repository.claimPendingEvents(
            "worker-B",
            1,
            Duration.ofSeconds(30)
        );

        assertEquals(
            1,
            workerA.size(),
            "Worker A should claim one event"
        );

        assertEquals(
            1,
            workerB.size(),
            "Worker B should claim an event from another aggregate"
        );

        assertEquals(
            1L,
            workerA.get(0).sequenceNumber(),
            "Worker A must claim the first event of its aggregate"
        );

        assertEquals(
            1L,
            workerB.get(0).sequenceNumber(),
            "Worker B must claim the first event of its aggregate"
        );

        assertNotEquals(
            workerA.get(0).aggregateId(),
            workerB.get(0).aggregateId(),
            "Workers should claim different aggregates"
        );
    }
}
