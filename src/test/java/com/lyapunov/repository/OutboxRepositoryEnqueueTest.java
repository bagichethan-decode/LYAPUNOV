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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

class OutboxRepositoryEnqueueTest {

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
        config.setMaximumPoolSize(12);

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
            stmt.execute("TRUNCATE outbox_events, event_history");
        }

        repository = new OutboxRepository(dataSource);
    }

    @Test
    void allocatesIncreasingSequencesForSameAggregate() throws Exception {
        OutboxEvent first = repository.enqueue(
            "Order", "order-100", "ORDER_CREATED", "{\"amount\":100}"
        );

        OutboxEvent second = repository.enqueue(
            "Order", "order-100", "ORDER_UPDATED", "{\"amount\":150}"
        );

        assertEquals(1L, first.sequenceNumber());
        assertEquals(2L, second.sequenceNumber());
        assertEquals(OutboxEvent.EventStatus.PENDING, first.status());
        assertEquals(OutboxEvent.EventStatus.PENDING, second.status());
    }

    @Test
    void allocatesIndependentSequencesForDifferentAggregates() throws Exception {
        OutboxEvent first = repository.enqueue(
            "Order", "order-A", "ORDER_CREATED", "{}"
        );

        OutboxEvent second = repository.enqueue(
            "Order", "order-B", "ORDER_CREATED", "{}"
        );

        assertEquals(1L, first.sequenceNumber());
        assertEquals(1L, second.sequenceNumber());
    }

    @Test
    void recordsEnqueueHistoryAtomically() throws Exception {
        OutboxEvent event = repository.enqueue(
            "Order", "order-history", "ORDER_CREATED", "{\"valid\":true}"
        );

        try (Connection conn = dataSource.getConnection();
             var stmt = conn.prepareStatement(
                 "SELECT action FROM event_history WHERE event_id = ?"
             )) {

            stmt.setObject(1, event.id());

            try (var rs = stmt.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("EVENT_ENQUEUED", rs.getString("action"));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void callerTransactionCommitPersistsEventAndHistory() throws Exception {
        OutboxEvent event;

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);

            event = repository.enqueue(
                connection,
                "Order",
                "caller-commit-order",
                "ORDER_CREATED",
                "{}"
            );

            assertFalse(connection.getAutoCommit());
            assertFalse(connection.isClosed());

            connection.commit();
        }

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement("""
                 SELECT
                     (SELECT COUNT(*) FROM outbox_events WHERE id = ?) AS events,
                     (SELECT COUNT(*) FROM event_history WHERE event_id = ?) AS history
                 """)) {

            statement.setObject(1, event.id());
            statement.setObject(2, event.id());

            try (ResultSet rs = statement.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(1L, rs.getLong("events"));
                assertEquals(1L, rs.getLong("history"));
            }
        }
    }

    @Test
    void callerTransactionRollbackRemovesEventAndHistory() throws Exception {
        OutboxEvent event;

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);

            event = repository.enqueue(
                connection,
                "Order",
                "caller-rollback-order",
                "ORDER_CREATED",
                "{}"
            );

            assertFalse(connection.getAutoCommit());
            assertFalse(connection.isClosed());

            connection.rollback();
        }

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement("""
                 SELECT
                     (SELECT COUNT(*) FROM outbox_events WHERE id = ?) AS events,
                     (SELECT COUNT(*) FROM event_history WHERE event_id = ?) AS history
                 """)) {

            statement.setObject(1, event.id());
            statement.setObject(2, event.id());

            try (ResultSet rs = statement.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(0L, rs.getLong("events"));
                assertEquals(0L, rs.getLong("history"));
            }
        }
    }

    @Test
    void callerOwnedEnqueueRejectsAutoCommitConnectionWithoutClosingIt()
            throws Exception {

        try (Connection connection = dataSource.getConnection()) {
            assertTrue(connection.getAutoCommit());

            assertThrows(
                IllegalArgumentException.class,
                () -> repository.enqueue(
                    connection,
                    "Order",
                    "caller-autocommit-order",
                    "ORDER_CREATED",
                    "{}"
                )
            );

            assertFalse(connection.isClosed());
            assertTrue(connection.getAutoCommit());
        }
    }

    @Test
    void rejectsInvalidArguments() {
        assertThrows(
            IllegalArgumentException.class,
            () -> repository.enqueue(null, "order-1", "CREATED", "{}")
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> repository.enqueue("Order", " ", "CREATED", "{}")
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> repository.enqueue("Order", "order-1", null, "{}")
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> repository.enqueue("Order", "order-1", "CREATED", " ")
        );
    }

    @Test
    void serializesConcurrentSequenceAllocation() throws Exception {
        int eventCount = 12;
        var executor = Executors.newFixedThreadPool(8);

        try {
            List<Callable<OutboxEvent>> tasks = new ArrayList<>();

            for (int i = 0; i < eventCount; i++) {
                final int index = i;
                tasks.add(() -> repository.enqueue(
                    "Order",
                    "concurrent-order",
                    "ORDER_UPDATED",
                    "{\"index\":" + index + "}"
                ));
            }

            List<Future<OutboxEvent>> futures = executor.invokeAll(tasks);

            List<Long> sequences = new ArrayList<>();

            for (Future<OutboxEvent> future : futures) {
                sequences.add(future.get(15, TimeUnit.SECONDS).sequenceNumber());
            }

            Collections.sort(sequences);

            for (int i = 0; i < eventCount; i++) {
                assertEquals((long) i + 1, sequences.get(i));
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void rejectsInvalidJsonPayload() {
        assertThrows(
            java.sql.SQLException.class,
            () -> repository.enqueue(
                "Order", "invalid-json", "ORDER_CREATED", "{invalid-json}"
            )
        );
    }
}