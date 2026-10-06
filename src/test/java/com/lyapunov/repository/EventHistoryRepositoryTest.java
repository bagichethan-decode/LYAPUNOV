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
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EventHistoryRepositoryTest {

    private static PostgreSQLContainer<?> postgres;
    private static HikariDataSource dataSource;
    private EventHistoryRepository historyRepository;

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

        historyRepository = new EventHistoryRepository(dataSource);
    }

    @Test
    void recordsEventLifecycleAction() throws Exception {
        UUID eventId = UUID.randomUUID();

        OutboxEvent event = new OutboxEvent(
            eventId,
            "Order",
            "order-42",
            7L,
            "ORDER_UPDATED",
            "{}",
            OutboxEvent.EventStatus.CLAIMED,
            0,
            "worker-A",
            Instant.now().plusSeconds(30),
            Instant.now(),
            Instant.now()
        );

        historyRepository.record(
            event,
            "EVENT_CLAIMED",
            "worker-A",
            "{\"reason\":\"test\"}"
        );

        String sql = """
            SELECT event_id, aggregate_type, aggregate_id,
                   sequence_number, event_type, action,
                   worker_id, details::text
            FROM event_history
            WHERE event_id = ?
        """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setObject(1, eventId);

            try (ResultSet rs = stmt.executeQuery()) {
                org.junit.jupiter.api.Assertions.assertTrue(
                    rs.next(),
                    "event history record should exist"
                );

                assertEquals(eventId, rs.getObject("event_id"));
                assertEquals("Order", rs.getString("aggregate_type"));
                assertEquals("order-42", rs.getString("aggregate_id"));
                assertEquals(7L, rs.getLong("sequence_number"));
                assertEquals("ORDER_UPDATED", rs.getString("event_type"));
                assertEquals("EVENT_CLAIMED", rs.getString("action"));
                assertEquals("worker-A", rs.getString("worker_id"));
                assertEquals("{\"reason\": \"test\"}", rs.getString("details"));
            }
        }
    }
}
