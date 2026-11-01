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
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class OutboxRepositoryRetrySchedulingTest {
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
        dataSource = new HikariDataSource(config);
        String ddl = Files.readString(Path.of("docker", "init.sql"));
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute(ddl);
        }
    }

    @AfterAll
    static void stopDatabase() {
        if (dataSource != null) dataSource.close();
        if (postgres != null) postgres.stop();
    }

    @BeforeEach
    void cleanDatabase() throws Exception {
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("TRUNCATE event_history, outbox_events");
        }
        repository = new OutboxRepository(dataSource);
    }

    private void insertEvent() throws Exception {
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("""
                INSERT INTO outbox_events
                    (aggregate_type, aggregate_id, sequence_number, event_type, payload)
                VALUES ('Order', 'retry-order', 1, 'ORDER_CREATED', '{}'::jsonb)
                """);
        }
    }

    private Instant nextAttemptAt() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT next_attempt_at FROM outbox_events")) {
            assertTrue(rs.next());
            return rs.getTimestamp(1).toInstant();
        }
    }

    private int retryCount() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT retry_count FROM outbox_events")) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private void makeRetryDue() throws Exception {
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("UPDATE outbox_events SET next_attempt_at = clock_timestamp() - INTERVAL '1 second'");
        }
    }

    @Test
    void failedEventIsNotClaimedUntilPersistedRetryTime() throws Exception {
        insertEvent();
        OutboxEvent first = repository.claimPendingEvents("worker-A", 1, Duration.ofSeconds(30)).get(0);
        Instant beforeFailure = Instant.now();

        assertTrue(repository.recordFailure(first.id(), "worker-A", first.claimToken(), 5,
            Duration.ofSeconds(2), Duration.ofSeconds(10)));

        Instant due = nextAttemptAt();
        assertEquals(1, retryCount());
        assertTrue(due.isAfter(beforeFailure.plusMillis(1500)),
            "The retry timestamp should be persisted approximately two seconds in the future");
        assertTrue(repository.claimPendingEvents("worker-B", 1, Duration.ofSeconds(30)).isEmpty(),
            "A pending event must not be claimed before next_attempt_at");

        makeRetryDue();
        assertEquals(1, repository.claimPendingEvents("worker-B", 1, Duration.ofSeconds(30)).size());
    }

    @Test
    void retryDelayDoublesAndIsCapped() throws Exception {
        insertEvent();
        OutboxEvent first = repository.claimPendingEvents("worker-A", 1, Duration.ofSeconds(30)).get(0);
        assertTrue(repository.recordFailure(first.id(), "worker-A", first.claimToken(), 5,
            Duration.ofSeconds(2), Duration.ofSeconds(5)));
        long firstDelay = Duration.between(Instant.now(), nextAttemptAt()).toMillis();
        assertTrue(firstDelay >= 1500 && firstDelay <= 2500, "First retry should be about 2 seconds");

        makeRetryDue();
        OutboxEvent second = repository.claimPendingEvents("worker-A", 1, Duration.ofSeconds(30)).get(0);
        assertTrue(repository.recordFailure(second.id(), "worker-A", second.claimToken(), 5,
            Duration.ofSeconds(2), Duration.ofSeconds(5)));
        long secondDelay = Duration.between(Instant.now(), nextAttemptAt()).toMillis();
        assertTrue(secondDelay >= 3500 && secondDelay <= 5000,
            "Second retry should double to 4 seconds and remain below the 5 second cap");

        makeRetryDue();
        OutboxEvent third = repository.claimPendingEvents("worker-A", 1, Duration.ofSeconds(30)).get(0);
        assertTrue(repository.recordFailure(third.id(), "worker-A", third.claimToken(), 5,
            Duration.ofSeconds(2), Duration.ofSeconds(5)));
        long thirdDelay = Duration.between(Instant.now(), nextAttemptAt()).toMillis();
        assertTrue(thirdDelay >= 3500 && thirdDelay <= 5000,
            "Third retry should be capped at 5 seconds");
    }

    @Test
    void finalFailureMovesEventToDeadAndClearsRetryTimestamp() throws Exception {
        insertEvent();
        OutboxEvent claim = repository.claimPendingEvents("worker-A", 1, Duration.ofSeconds(30)).get(0);
        assertTrue(repository.recordFailure(claim.id(), "worker-A", claim.claimToken(), 1,
            Duration.ofSeconds(1), Duration.ofSeconds(5)));
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT status, next_attempt_at FROM outbox_events")) {
            assertTrue(rs.next());
            assertEquals("DEAD", rs.getString("status"));
            assertNull(rs.getTimestamp("next_attempt_at"));
        }
    }
}