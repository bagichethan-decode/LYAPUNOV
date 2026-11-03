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
import java.sql.Statement;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WorkerHeartbeatRepositoryTest {
    private static PostgreSQLContainer<?> postgres;
    private static HikariDataSource dataSource;
    private WorkerHeartbeatRepository repository;

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
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute(ddl);
        }
    }

    @AfterAll
    static void stopDatabase() {
        if (dataSource != null) dataSource.close();
        if (postgres != null) postgres.stop();
    }

    @BeforeEach
    void cleanDatabase() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE worker_heartbeats");
        }
        repository = new WorkerHeartbeatRepository(dataSource);
    }

    @Test
    void heartbeatRegistersWorkerAndTracksPublishProgress() throws Exception {
        repository.heartbeat("relay-a", "instance-1", 3, 1, 0, null);
        List<WorkerHeartbeat> workers = repository.findAll(Duration.ofSeconds(30));
        assertEquals(1, workers.size());
        WorkerHeartbeat worker = workers.get(0);
        assertEquals("RUNNING", worker.status());
        assertEquals("instance-1", worker.instanceId());
        assertEquals(3, worker.publishedCount());
        assertEquals(1, worker.failedCount());
        assertNotNull(worker.lastSuccessfulPublishAt());
        assertFalse(worker.stale());
    }

    @Test
    void heartbeatUpsertPreservesOriginalStartAndPublishTimestampWhenCountDoesNotAdvance() throws Exception {
        repository.heartbeat("relay-a", "instance-1", 2, 0, 0, null);
        WorkerHeartbeat first = repository.findAll(Duration.ofSeconds(30)).get(0);
        repository.heartbeat("relay-a", "instance-2", 2, 1, 1, "temporary broker error");
        WorkerHeartbeat second = repository.findAll(Duration.ofSeconds(30)).get(0);
        assertEquals(first.startedAt(), second.startedAt());
        assertEquals(first.lastSuccessfulPublishAt(), second.lastSuccessfulPublishAt());
        assertEquals("instance-2", second.instanceId());
        assertEquals("temporary broker error", second.lastError());
    }

    @Test
    void stoppedWorkerIsMarkedAndCannotBeMistakenForRunning() throws Exception {
        repository.heartbeat("relay-a", "instance-1", 0, 0, 0, null);
        assertTrue(repository.markStopped("relay-a", "instance-1"));
        assertFalse(repository.markStopped("relay-a", "instance-1"));
        WorkerHeartbeat worker = repository.findAll(Duration.ofSeconds(30)).get(0);
        assertEquals("STOPPED", worker.status());
        assertFalse(worker.stale());
    }

    @Test
    void staleRunningWorkerIsDetected() throws Exception {
        repository.heartbeat("relay-a", "instance-1", 0, 0, 0, null);
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("UPDATE worker_heartbeats SET last_seen_at = clock_timestamp() - INTERVAL '5 minutes'");
        }
        WorkerHeartbeat worker = repository.findAll(Duration.ofSeconds(30)).get(0);
        assertTrue(worker.stale());
    }

    @Test
    void rejectsInvalidHeartbeatArguments() {
        assertThrows(IllegalArgumentException.class,
                () -> repository.heartbeat(" ", "instance-1", 0, 0, 0, null));
        assertThrows(IllegalArgumentException.class,
                () -> repository.heartbeat("relay-a", "instance-1", -1, 0, 0, null));
        assertThrows(IllegalArgumentException.class,
                () -> repository.findAll(Duration.ZERO));
    }
}
