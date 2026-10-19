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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OutboxRepositoryLeaseFencingTest {

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

    private void insertEvent(String aggregateId, long sequence) throws Exception {
        String sql = """
            INSERT INTO outbox_events
                (aggregate_type, aggregate_id, sequence_number, event_type, payload)
            VALUES ('Order', ?, ?, 'ORDER_UPDATED', '{}'::jsonb)
            """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, aggregateId);
            stmt.setLong(2, sequence);
            stmt.executeUpdate();
        }
    }

    private OutboxEvent onlyClaim(String workerId, Duration lease) throws Exception {
        List<OutboxEvent> events = repository.claimPendingEvents(workerId, 1, lease);
        assertEquals(1, events.size(), "Exactly one event should be claimed");
        assertNotNull(events.get(0).claimToken(), "Every claim must have a fencing token");
        return events.get(0);
    }

    private static int countHistory(String action) throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                 "SELECT COUNT(*) FROM event_history WHERE action = ?")) {
            stmt.setString(1, action);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    @Test
    void staleTokenCannotRecordFailureAfterSameWorkerReclaimsEvent() throws Exception {
        insertEvent("order-stale", 1);
        OutboxEvent oldClaim = onlyClaim("worker-A", Duration.ofMillis(100));

        Thread.sleep(300);
        OutboxEvent currentClaim = onlyClaim("worker-A", Duration.ofSeconds(10));

        assertEquals(oldClaim.id(), currentClaim.id());
        assertNotEquals(oldClaim.claimToken(), currentClaim.claimToken());

        assertFalse(repository.recordFailure(
            oldClaim.id(), "worker-A", oldClaim.claimToken(), 3
        ), "A stale token must be rejected even when the worker ID is reused");

        assertDatabaseState("CLAIMED", 0, "worker-A", currentClaim.claimToken());
        assertEquals(0, countHistory("EVENT_FAILED"),
            "A rejected stale update must not create failure history");

        assertTrue(repository.recordFailure(
            currentClaim.id(), "worker-A", currentClaim.claimToken(), 3
        ));
        assertDatabaseState("PENDING", 1, null, null);
        assertEquals(1, countHistory("EVENT_FAILED"));
    }

    @Test
    void expiredClaimCannotPublishOrRecordFailure() throws Exception {
        insertEvent("order-expired", 1);
        OutboxEvent claim = onlyClaim("worker-A", Duration.ofMillis(100));
        Thread.sleep(300);

        assertFalse(repository.markPublished(
            claim.id(), "worker-A", claim.claimToken()
        ));
        assertFalse(repository.recordFailure(
            claim.id(), "worker-A", claim.claimToken(), 3
        ));

        assertDatabaseState("CLAIMED", 0, "worker-A", claim.claimToken());
        assertEquals(0, countHistory("EVENT_PUBLISHED"));
        assertEquals(0, countHistory("EVENT_FAILED"));
    }

    @Test
    void currentClaimCanPublishAndClearsLeaseOwnership() throws Exception {
        insertEvent("order-publish", 1);
        OutboxEvent claim = onlyClaim("worker-A", Duration.ofSeconds(10));

        assertTrue(repository.markPublished(
            claim.id(), "worker-A", claim.claimToken()
        ));

        assertDatabaseState("PUBLISHED", 0, null, null);
        assertEquals(1, countHistory("EVENT_PUBLISHED"));
    }

    private static void assertDatabaseState(
        String expectedStatus,
        int expectedRetryCount,
        String expectedWorker,
        java.util.UUID expectedClaimToken
    ) throws Exception {
        String sql = """
            SELECT status, retry_count, claimed_by, claim_token, lease_expires_at
            FROM outbox_events
            """;
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next(), "Outbox row should exist");
            assertEquals(expectedStatus, rs.getString("status"));
            assertEquals(expectedRetryCount, rs.getInt("retry_count"));
            assertEquals(expectedWorker, rs.getString("claimed_by"));
            assertEquals(expectedClaimToken, rs.getObject("claim_token"));
            if (expectedStatus.equals("PUBLISHED") || expectedStatus.equals("PENDING")) {
                assertNull(rs.getTimestamp("lease_expires_at"));
            } else {
                assertNotNull(rs.getTimestamp("lease_expires_at"));
            }
            assertFalse(rs.next(), "Only one test outbox row should exist");
        }
    }
}
