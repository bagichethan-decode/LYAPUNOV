package com.lyapunov.repository;

import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;

class OutboxRepositoryValidationTest {

    @Test
    void rejectsNullDataSource() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new OutboxRepository(null)
        );
    }

    @Test
    void rejectsNullOrBlankWorkerIdBeforeDatabaseAccess() {
        OutboxRepository repository = repositoryWithoutDatabaseAccess();

        assertThrows(
            IllegalArgumentException.class,
            () -> repository.claimPendingEvents(null, 1, Duration.ofSeconds(10))
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> repository.claimPendingEvents("  ", 1, Duration.ofSeconds(10))
        );
    }

    @Test
    void rejectsNonPositiveBatchSizeBeforeDatabaseAccess() {
        OutboxRepository repository = repositoryWithoutDatabaseAccess();

        assertThrows(
            IllegalArgumentException.class,
            () -> repository.claimPendingEvents("worker-A", 0, Duration.ofSeconds(10))
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> repository.claimPendingEvents("worker-A", -1, Duration.ofSeconds(10))
        );
    }

    @Test
    void rejectsNullOrNonPositiveLeaseDurationBeforeDatabaseAccess() {
        OutboxRepository repository = repositoryWithoutDatabaseAccess();

        assertThrows(
            IllegalArgumentException.class,
            () -> repository.claimPendingEvents("worker-A", 1, null)
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> repository.claimPendingEvents("worker-A", 1, Duration.ZERO)
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> repository.claimPendingEvents("worker-A", 1, Duration.ofSeconds(-1))
        );
    }

    @Test
    void rejectsNullEventIdBeforeDatabaseAccess() {
        OutboxRepository repository = repositoryWithoutDatabaseAccess();

        assertThrows(
            IllegalArgumentException.class,
            () -> repository.markPublished(null)
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> repository.recordFailure(null, 3)
        );
    }

    @Test
    void rejectsNonPositiveRetryLimitBeforeDatabaseAccess() {
        OutboxRepository repository = repositoryWithoutDatabaseAccess();

        assertThrows(
            IllegalArgumentException.class,
            () -> repository.recordFailure(UUID.randomUUID(), 0)
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> repository.recordFailure(UUID.randomUUID(), -1)
        );
    }

    private static OutboxRepository repositoryWithoutDatabaseAccess() {
        DataSource dataSource = (DataSource) Proxy.newProxyInstance(
            DataSource.class.getClassLoader(),
            new Class<?>[] { DataSource.class },
            (proxy, method, args) -> {
                throw new AssertionError(
                    "Database access should not occur for invalid arguments"
                );
            }
        );

        return new OutboxRepository(dataSource);
    }
}
