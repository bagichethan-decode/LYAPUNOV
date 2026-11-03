package com.lyapunov.repository;

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
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class OutboxQueueStatsRepositoryTest {
    private static PostgreSQLContainer<?> postgres;
    private static HikariDataSource dataSource;
    private OutboxQueueStatsRepository repository;

    @BeforeAll
    static void startDatabase() throws Exception {
        postgres = new PostgreSQLContainer<>("postgres:16-alpine");
        postgres.start();
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(postgres.getJdbcUrl());
        config.setUsername(postgres.getUsername());
        config.setPassword(postgres.getPassword());
        config.setMaximumPoolSize(3);
        dataSource = new HikariDataSource(config);
        String ddl = Files.readString(Path.of("docker", "init.sql"));
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(ddl);
        }
    }

    @AfterAll
    static void stopDatabase() {
        if (dataSource != null) dataSource.close();
        if (postgres != null) postgres.stop();
    }

    @BeforeEach
    void reset() throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE event_history, outbox_events");
        }
        repository = new OutboxQueueStatsRepository(dataSource);
    }

    @Test
    void returnsZeroCountsForAnEmptyQueue() throws Exception {
        OutboxQueueStats stats = repository.snapshot();
        assertEquals(0, stats.totalEvents());
        assertEquals(0, stats.pending());
        assertEquals(0, stats.claimed());
        assertEquals(0, stats.published());
        assertEquals(0, stats.dead());
        assertEquals(0, stats.skipped());
        assertEquals("HEALTHY", stats.queueHealth());
        assertNotNull(stats.snapshotAt());
        assertNull(stats.oldestPendingAt());
    }

    @Test
    void aggregatesEveryLifecycleStateAndRetrySignals() throws Exception {
        insert("PENDING", 2, null, Instant.now().minusSeconds(120));
        insert("CLAIMED", 1, Instant.now().minusSeconds(30), Instant.now().minusSeconds(60));
        insert("PUBLISHED", 3, null, Instant.now().minusSeconds(50));
        insert("DEAD", 4, null, Instant.now().minusSeconds(40));
        insert("SKIPPED", 0, null, Instant.now().minusSeconds(20));

        OutboxQueueStats stats = repository.snapshot();
        assertEquals(5, stats.totalEvents());
        assertEquals(1, stats.pending());
        assertEquals(1, stats.claimed());
        assertEquals(1, stats.published());
        assertEquals(1, stats.dead());
        assertEquals(1, stats.skipped());
        assertEquals(10, stats.totalRetries());
        assertEquals(1, stats.retriesDue());
        assertEquals(1, stats.expiredClaims());
        assertEquals("UNHEALTHY", stats.queueHealth());
        assertNotNull(stats.oldestPendingAt());
        assertNotNull(stats.oldestClaimedAt());
    }

    @Test
    void reportsDegradedWhenQueueHasWorkButNoTerminalOrExpiredSignals() throws Exception {
        insert("PENDING", 0, null, Instant.now());
        OutboxQueueStats stats = repository.snapshot();
        assertEquals("DEGRADED", stats.queueHealth());
        assertEquals(0, stats.dead());
        assertEquals(0, stats.expiredClaims());
    }

    private static void insert(String status, int retries, Instant leaseExpiry, Instant createdAt)
            throws Exception {
        String sql = """
                INSERT INTO outbox_events
                    (id, aggregate_type, aggregate_id, sequence_number, event_type, payload,
                     status, retry_count, next_attempt_at, lease_expires_at, created_at, updated_at)
                VALUES (?, 'TEST', ?, 1, 'TEST_EVENT', '{}'::jsonb,
                        ?, ?, NOW() - INTERVAL '1 second', ?, ?, ?)
                """;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, UUID.randomUUID());
            statement.setString(2, UUID.randomUUID().toString());
            statement.setString(3, status);
            statement.setInt(4, retries);
            statement.setTimestamp(5, leaseExpiry == null ? null : java.sql.Timestamp.from(leaseExpiry));
            statement.setTimestamp(6, java.sql.Timestamp.from(createdAt));
            statement.setTimestamp(7, java.sql.Timestamp.from(createdAt));
            statement.executeUpdate();
        }
    }
}