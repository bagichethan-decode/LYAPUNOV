package com.lyapunov.inbox;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class JdbcInboxProcessorTest {

    private static final PostgreSQLContainer<?> POSTGRES =
        new PostgreSQLContainer<>("postgres:16-alpine");

    private static DataSource dataSource;
    private JdbcInboxProcessor processor;

    @BeforeAll
    static void startDatabase() throws Exception {
        POSTGRES.start();

        PGSimpleDataSource source = new PGSimpleDataSource();
        source.setURL(POSTGRES.getJdbcUrl());
        source.setUser(POSTGRES.getUsername());
        source.setPassword(POSTGRES.getPassword());
        dataSource = source;

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {

            try (InputStream migration =
                     JdbcInboxProcessorTest.class.getResourceAsStream(
                         "/db/migration/V007__consumer_inbox.sql")) {

                assertNotNull(migration, "V007 inbox migration must exist");

                String sql = new String(
                    migration.readAllBytes(),
                    StandardCharsets.UTF_8
                );

                statement.execute(sql);
            }

            statement.execute("""
                CREATE TABLE IF NOT EXISTS inbox_test_effects (
                    id BIGSERIAL PRIMARY KEY,
                    event_id UUID NOT NULL,
                    consumer_name VARCHAR(200) NOT NULL
                )
                """);
        }
    }

    @AfterAll
    static void stopDatabase() {
        POSTGRES.stop();
    }

    @BeforeEach
    void setUp() throws SQLException {
        processor = new JdbcInboxProcessor(dataSource);

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("""
                TRUNCATE TABLE inbox_test_effects, consumer_inbox
                """);
        }
    }

    @Test
    void processesEventAndCommitsInboxRecord() throws Exception {
        UUID eventId = UUID.randomUUID();

        InboxProcessingResult result = processor.process(
            "orders",
            eventId,
            "ORDER_CREATED",
            connection -> insertEffect(connection, "orders", eventId)
        );

        assertEquals(InboxProcessingResult.PROCESSED, result);
        assertEquals(1, effectCount(eventId));
        assertEquals(1, inboxCount("orders", eventId));
        assertTrue(isProcessed("orders", eventId));
    }

    @Test
    void duplicateDeliveryDoesNotExecuteBusinessWork() throws Exception {
        UUID eventId = UUID.randomUUID();
        AtomicInteger executions = new AtomicInteger();

        InboxWork work = connection -> {
            executions.incrementAndGet();
            insertEffect(connection, "orders", eventId);
        };

        assertEquals(
            InboxProcessingResult.PROCESSED,
            processor.process("orders", eventId, "ORDER_CREATED", work)
        );

        assertEquals(
            InboxProcessingResult.DUPLICATE,
            processor.process("orders", eventId, "ORDER_CREATED", work)
        );

        assertEquals(1, executions.get());
        assertEquals(1, effectCount(eventId));
    }

    @Test
    void failedBusinessWorkRollsBackInboxAndBusinessChanges() throws Exception {
        UUID eventId = UUID.randomUUID();

        assertThrows(SQLException.class, () ->
            processor.process(
                "orders",
                eventId,
                "ORDER_CREATED",
                connection -> {
                    insertEffect(connection, "orders", eventId);
                    throw new SQLException("Simulated business failure");
                }
            )
        );

        assertEquals(0, inboxCount("orders", eventId));
        assertEquals(0, effectCount(eventId));

        assertEquals(
            InboxProcessingResult.PROCESSED,
            processor.process(
                "orders",
                eventId,
                "ORDER_CREATED",
                connection -> insertEffect(connection, "orders", eventId)
            )
        );

        assertEquals(1, inboxCount("orders", eventId));
        assertEquals(1, effectCount(eventId));
    }

    @Test
    void differentConsumersCanProcessSameEvent() throws Exception {
        UUID eventId = UUID.randomUUID();

        InboxWork ordersWork =
            connection -> insertEffect(connection, "orders", eventId);

        InboxWork analyticsWork =
            connection -> insertEffect(connection, "analytics", eventId);

        assertEquals(
            InboxProcessingResult.PROCESSED,
            processor.process("orders", eventId, "ORDER_CREATED", ordersWork)
        );

        assertEquals(
            InboxProcessingResult.PROCESSED,
            processor.process("analytics", eventId, "ORDER_CREATED", analyticsWork)
        );

        assertEquals(2, effectCount(eventId));
        assertEquals(1, inboxCount("orders", eventId));
        assertEquals(1, inboxCount("analytics", eventId));
    }

    @Test
    void concurrentDuplicateDeliveriesExecuteBusinessWorkOnlyOnce()
        throws Exception {

        UUID eventId = UUID.randomUUID();
        AtomicInteger executions = new AtomicInteger();

        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);

        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            InboxWork work = connection -> {
                executions.incrementAndGet();
                insertEffect(connection, "orders", eventId);
            };

            Future<InboxProcessingResult> first = executor.submit(() -> {
                ready.countDown();
                if (!start.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Start timeout");
                }
                return processor.process(
                    "orders", eventId, "ORDER_CREATED", work
                );
            });

            Future<InboxProcessingResult> second = executor.submit(() -> {
                ready.countDown();
                if (!start.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Start timeout");
                }
                return processor.process(
                    "orders", eventId, "ORDER_CREATED", work
                );
            });

            assertTrue(ready.await(10, TimeUnit.SECONDS));
            start.countDown();

            InboxProcessingResult firstResult =
                first.get(20, TimeUnit.SECONDS);

            InboxProcessingResult secondResult =
                second.get(20, TimeUnit.SECONDS);

            assertNotEquals(firstResult, secondResult);
            assertTrue(
                firstResult == InboxProcessingResult.PROCESSED
                    || secondResult == InboxProcessingResult.PROCESSED
            );
            assertTrue(
                firstResult == InboxProcessingResult.DUPLICATE
                    || secondResult == InboxProcessingResult.DUPLICATE
            );

            assertEquals(1, executions.get());
            assertEquals(1, effectCount(eventId));
            assertEquals(1, inboxCount("orders", eventId));
            assertTrue(isProcessed("orders", eventId));

        } finally {
            executor.shutdownNow();
        }
    }

    private static void insertEffect(
        Connection connection,
        String consumer,
        UUID eventId
    ) throws SQLException {

        try (PreparedStatement statement = connection.prepareStatement("""
            INSERT INTO inbox_test_effects (event_id, consumer_name)
            VALUES (?, ?)
            """)) {
            statement.setObject(1, eventId);
            statement.setString(2, consumer);
            statement.executeUpdate();
        }
    }

    private static int effectCount(UUID eventId) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement("""
                 SELECT COUNT(*) FROM inbox_test_effects WHERE event_id = ?
                 """)) {

            statement.setObject(1, eventId);

            try (ResultSet result = statement.executeQuery()) {
                result.next();
                return result.getInt(1);
            }
        }
    }

    private static int inboxCount(
        String consumer,
        UUID eventId
    ) throws SQLException {

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement("""
                 SELECT COUNT(*) FROM consumer_inbox
                 WHERE consumer_name = ? AND event_id = ?
                 """)) {

            statement.setString(1, consumer);
            statement.setObject(2, eventId);

            try (ResultSet result = statement.executeQuery()) {
                result.next();
                return result.getInt(1);
            }
        }
    }

    private static boolean isProcessed(
        String consumer,
        UUID eventId
    ) throws SQLException {

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement("""
                 SELECT processed_at IS NOT NULL
                 FROM consumer_inbox
                 WHERE consumer_name = ? AND event_id = ?
                 """)) {

            statement.setString(1, consumer);
            statement.setObject(2, eventId);

            try (ResultSet result = statement.executeQuery()) {
                return result.next() && result.getBoolean(1);
            }
        }
    }
}
