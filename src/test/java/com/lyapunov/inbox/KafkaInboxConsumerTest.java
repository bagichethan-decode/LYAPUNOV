package com.lyapunov.inbox;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import java.util.concurrent.atomic.AtomicBoolean;
import java.lang.reflect.InvocationTargetException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringSerializer;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

class KafkaInboxConsumerTest {

    private static final PostgreSQLContainer<?> POSTGRES =
        new PostgreSQLContainer<>("postgres:16-alpine");

    private static final KafkaContainer KAFKA =
        new KafkaContainer(
            DockerImageName.parse("apache/kafka:3.7.0")
        );

    private static DataSource dataSource;
    private static JdbcInboxProcessor processor;

    @BeforeAll
    static void startInfrastructure() throws Exception {
        POSTGRES.start();
        KAFKA.start();

        PGSimpleDataSource source = new PGSimpleDataSource();
        source.setURL(POSTGRES.getJdbcUrl());
        source.setUser(POSTGRES.getUsername());
        source.setPassword(POSTGRES.getPassword());

        dataSource = source;
        processor = new JdbcInboxProcessor(dataSource);

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             InputStream migration =
                 KafkaInboxConsumerTest.class.getResourceAsStream(
                     "/db/migration/V007__consumer_inbox.sql"
                 )) {

            assertNotNull(migration, "Inbox migration must exist");

            statement.execute(
                new String(
                    migration.readAllBytes(),
                    StandardCharsets.UTF_8
                )
            );

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
    static void stopInfrastructure() {
        KAFKA.stop();
        POSTGRES.stop();
    }

    @BeforeEach
    void cleanDatabase() throws SQLException {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(
                "TRUNCATE TABLE inbox_test_effects, consumer_inbox"
            );
        }
    }

    @Test
    void processesKafkaEventAndCommitsInboxRecord() throws Exception {
        String topic = createTopic();
        UUID eventId = UUID.randomUUID();

        sendEvent(topic, eventId);

        try (KafkaInboxConsumer consumer = newConsumer(
            topic,
            "orders",
            "group-" + UUID.randomUUID(),
            (connection, record) ->
                insertEffect(connection, "orders", eventId)
        )) {
            assertEquals(1, awaitProcessed(consumer, 1));

            assertEquals(1, effectCount(eventId));
            assertEquals(1, inboxCount("orders", eventId));
            assertTrue(isProcessed("orders", eventId));
        }
    }

    @Test
    void duplicateKafkaDeliveryExecutesBusinessWorkOnlyOnce()
        throws Exception {

        String topic = createTopic();
        UUID eventId = UUID.randomUUID();

        sendEvent(topic, eventId);
        sendEvent(topic, eventId);

        try (KafkaInboxConsumer consumer = newConsumer(
            topic,
            "orders",
            "group-" + UUID.randomUUID(),
            (connection, record) ->
                insertEffect(connection, "orders", eventId)
        )) {
            assertEquals(2, awaitProcessed(consumer, 2));

            assertEquals(1, effectCount(eventId));
            assertEquals(1, inboxCount("orders", eventId));
            assertTrue(isProcessed("orders", eventId));
        }
    }

    @Test
    void failedBusinessWorkRollsBackAndSameConsumerCanRetryRecord()
        throws Exception {

        String topic = createTopic();
        UUID eventId = UUID.randomUUID();
        String groupId = "retry-" + UUID.randomUUID();
        int[] attempts = {0};

        sendEvent(topic, eventId);

        try (KafkaInboxConsumer consumer = newConsumer(
            topic,
            "orders",
            groupId,
            (connection, record) -> {
                attempts[0]++;
                insertEffect(connection, "orders", eventId);

                if (attempts[0] == 1) {
                    throw new SQLException("Simulated business failure");
                }
            }
        )) {
            awaitFailure(consumer);

            assertEquals(1, attempts[0]);
            assertEquals(0, effectCount(eventId));
            assertEquals(0, inboxCount("orders", eventId));

            assertEquals(1, awaitProcessed(consumer, 1));

            assertEquals(2, attempts[0]);
            assertEquals(1, effectCount(eventId));
            assertEquals(1, inboxCount("orders", eventId));
            assertTrue(isProcessed("orders", eventId));
        }
    }

    @Test
    void independentConsumerGroupsCanProcessSameKafkaEvent()
        throws Exception {

        String topic = createTopic();
        UUID eventId = UUID.randomUUID();

        sendEvent(topic, eventId);

        try (KafkaInboxConsumer ordersConsumer = newConsumer(
                 topic,
                 "orders",
                 "orders-" + UUID.randomUUID(),
                 (connection, record) ->
                     insertEffect(connection, "orders", eventId)
             );
             KafkaInboxConsumer analyticsConsumer = newConsumer(
                 topic,
                 "analytics",
                 "analytics-" + UUID.randomUUID(),
                 (connection, record) ->
                     insertEffect(connection, "analytics", eventId)
             )) {

            assertEquals(1, awaitProcessed(ordersConsumer, 1));
            assertEquals(1, awaitProcessed(analyticsConsumer, 1));

            assertEquals(2, effectCount(eventId));
            assertEquals(1, inboxCount("orders", eventId));
            assertEquals(1, inboxCount("analytics", eventId));

            assertTrue(isProcessed("orders", eventId));
            assertTrue(isProcessed("analytics", eventId));
        }
    }

    @Test
    void rewindsEveryUncommittedPartitionWhenFirstRecordFails()
        throws Exception {

        String topic = "lyapunov-mock-" + UUID.randomUUID();

        TopicPartition partition0 = new TopicPartition(topic, 0);
        TopicPartition partition1 = new TopicPartition(topic, 1);

        MockConsumer<String, String> mock =
            new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        mock.updateBeginningOffsets(
            Map.of(
                partition0, 0L,
                partition1, 0L
            )
        );

        int[] attempts = {0};

        String consumerName = "multi-partition-" + UUID.randomUUID();

        try (KafkaInboxConsumer consumer = new KafkaInboxConsumer(
            mock,
            topic,
            consumerName,
            processor,
            (connection, record) -> {
                attempts[0]++;

                if (attempts[0] == 1) {
                    throw new SQLException("Simulated first-record failure");
                }
            }
        )) {

            mock.rebalance(List.of(partition0, partition1));

            UUID event0 = UUID.randomUUID();
            UUID event1 = UUID.randomUUID();

            ConsumerRecord<String, String> record0 =
                new ConsumerRecord<>(topic, 0, 0L, "key-0", "{}");

            record0.headers().add(
                "lyapunov-event-id",
                event0.toString().getBytes(StandardCharsets.UTF_8)
            );

            record0.headers().add(
                "lyapunov-event-type",
                "ORDER_CREATED".getBytes(StandardCharsets.UTF_8)
            );

            ConsumerRecord<String, String> record1 =
                new ConsumerRecord<>(topic, 1, 0L, "key-1", "{}");

            record1.headers().add(
                "lyapunov-event-id",
                event1.toString().getBytes(StandardCharsets.UTF_8)
            );

            record1.headers().add(
                "lyapunov-event-type",
                "ORDER_CREATED".getBytes(StandardCharsets.UTF_8)
            );

            mock.addRecord(record0);
            mock.addRecord(record1);

            SQLException failure = assertThrows(
                SQLException.class,
                () -> consumer.processAvailable(Duration.ZERO)
            );

            assertEquals(
                "Simulated first-record failure",
                failure.getMessage()
            );

            assertEquals(1, attempts[0]);

            assertEquals(
                0L,
                mock.position(partition0),
                "Partition 0 must rewind to its uncommitted record"
            );

            assertEquals(
                0L,
                mock.position(partition1),
                "Partition 1 must rewind to its unprocessed record"
            );
        }
    }

    @Test
    void redeliveredEventAfterOffsetCommitFailureDoesNotRepeatBusinessWork()
        throws Exception {

        String topic = "lyapunov-commit-failure-" + UUID.randomUUID();
        String consumerName = "commit-recovery-" + UUID.randomUUID();
        UUID eventId = UUID.randomUUID();

        Properties adminProperties = new Properties();
        adminProperties.put(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
            KAFKA.getBootstrapServers()
        );

        try (AdminClient admin = AdminClient.create(adminProperties)) {
            admin.createTopics(
                List.of(new NewTopic(topic, 1, (short) 1))
            ).all().get(30, TimeUnit.SECONDS);
        }

        Properties producerProperties = new Properties();
        producerProperties.put(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            KAFKA.getBootstrapServers()
        );
        producerProperties.put(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName()
        );
        producerProperties.put(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName()
        );

        try (KafkaProducer<String, String> producer =
                 new KafkaProducer<>(producerProperties)) {

            ProducerRecord<String, String> record =
                new ProducerRecord<>(topic, "key-1", "{}");

            record.headers().add(
                "lyapunov-event-id",
                eventId.toString().getBytes(StandardCharsets.UTF_8)
            );

            record.headers().add(
                "lyapunov-event-type",
                "ORDER_CREATED".getBytes(StandardCharsets.UTF_8)
            );

            producer.send(record).get(30, TimeUnit.SECONDS);
        }

        Properties consumerProperties = new Properties();
        consumerProperties.put(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            KAFKA.getBootstrapServers()
        );
        consumerProperties.put(
            ConsumerConfig.GROUP_ID_CONFIG,
            consumerName
        );
        consumerProperties.put(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.StringDeserializer.class.getName()
        );
        consumerProperties.put(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.StringDeserializer.class.getName()
        );
        consumerProperties.put(
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
            "false"
        );
        consumerProperties.put(
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            "earliest"
        );

        KafkaConsumer<String, String> delegate =
            new KafkaConsumer<>(consumerProperties);

        AtomicBoolean failFirstCommit = new AtomicBoolean(true);

        @SuppressWarnings("unchecked")
        Consumer<String, String> failingConsumer =
            (Consumer<String, String>) java.lang.reflect.Proxy.newProxyInstance(
                Consumer.class.getClassLoader(),
                new Class<?>[] { Consumer.class },
                (proxy, method, args) -> {

                    if ("commitSync".equals(method.getName())
                        && failFirstCommit.compareAndSet(true, false)) {

                        throw new KafkaException(
                            "Simulated Kafka offset commit failure"
                        );
                    }

                    try {
                        return method.invoke(delegate, args);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                }
            );

        AtomicBoolean businessWorkExecuted = new AtomicBoolean(false);

        try (KafkaInboxConsumer consumer = new KafkaInboxConsumer(
            failingConsumer,
            topic,
            consumerName,
            processor,
            (connection, kafkaRecord) -> {

                businessWorkExecuted.set(true);

                try (PreparedStatement statement =
                         connection.prepareStatement("""
                             INSERT INTO inbox_test_effects
                                 (event_id, consumer_name)
                             VALUES (?, ?)
                             """)) {

                    statement.setObject(1, eventId);
                    statement.setString(2, consumerName);
                    statement.executeUpdate();
                }
            }
        )) {

            long deadline =
                System.nanoTime() + TimeUnit.SECONDS.toNanos(20);

            boolean commitFailureObserved = false;

            while (System.nanoTime() < deadline) {
                try {
                    consumer.processAvailable(Duration.ofMillis(250));
                } catch (KafkaException failure) {

                    assertEquals(
                        "Simulated Kafka offset commit failure",
                        failure.getMessage()
                    );

                    commitFailureObserved = true;
                    break;
                }
            }

            assertTrue(
                commitFailureObserved,
                "Expected the injected offset commit failure"
            );

            assertTrue(
                businessWorkExecuted.get(),
                "Business work should have completed before offset commit"
            );

            assertEquals(
                1,
                effectCount(eventId),
                "The database transaction must have committed"
            );

            assertEquals(
                1,
                inboxCount(consumerName, eventId),
                "The inbox entry must have committed"
            );

            assertEquals(
                1,
                consumer.processAvailable(Duration.ofSeconds(10)),
                "The redelivered duplicate should be recognized and committed"
            );

            assertEquals(
                1,
                effectCount(eventId),
                "Redelivery must not repeat business work"
            );

            assertEquals(
                1,
                inboxCount(consumerName, eventId),
                "Redelivery must not create another inbox entry"
            );
        }
    }
    private static KafkaInboxConsumer newConsumer(
        String topic,
        String consumerName,
        String groupId,
        KafkaInboxWork work
    ) {
        Properties properties = new Properties();

        properties.put(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            KAFKA.getBootstrapServers()
        );

        properties.put(
            ConsumerConfig.GROUP_ID_CONFIG,
            groupId
        );

        properties.put(
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            "earliest"
        );

        properties.put(
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
            "false"
        );

        return new KafkaInboxConsumer(
            properties,
            topic,
            consumerName,
            processor,
            work
        );
    }

    private static String createTopic() throws Exception {
        String topic = "lyapunov-inbox-" + UUID.randomUUID();

        Properties properties = new Properties();
        properties.put(
            "bootstrap.servers",
            KAFKA.getBootstrapServers()
        );

        try (AdminClient admin = AdminClient.create(properties)) {
            admin.createTopics(
                List.of(new NewTopic(topic, 1, (short) 1))
            ).all().get(20, TimeUnit.SECONDS);
        }

        return topic;
    }

    private static void sendEvent(
        String topic,
        UUID eventId
    ) throws Exception {

        Properties properties = new Properties();

        properties.put(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            KAFKA.getBootstrapServers()
        );

        properties.put(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName()
        );

        properties.put(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName()
        );

        properties.put(
            ProducerConfig.ACKS_CONFIG,
            "all"
        );

        try (KafkaProducer<String, String> producer =
                 new KafkaProducer<>(properties)) {

            ProducerRecord<String, String> record =
                new ProducerRecord<>(
                    topic,
                    eventId.toString(),
                    "{\"orderId\":\"" + eventId + "\"}"
                );

            Headers headers = record.headers();

            headers.add(
                "lyapunov-event-id",
                eventId.toString().getBytes(StandardCharsets.UTF_8)
            );

            headers.add(
                "lyapunov-event-type",
                "ORDER_CREATED".getBytes(StandardCharsets.UTF_8)
            );

            producer.send(record).get(20, TimeUnit.SECONDS);
            producer.flush();
        }
    }

    private static int awaitProcessed(
        KafkaInboxConsumer consumer,
        int expected
    ) throws Exception {

        long deadline = System.nanoTime()
            + TimeUnit.SECONDS.toNanos(30);

        int processed = 0;

        while (processed < expected && System.nanoTime() < deadline) {
            processed += consumer.processAvailable(
                Duration.ofMillis(250)
            );
        }

        assertEquals(
            expected,
            processed,
            "Kafka consumer did not process the expected record count"
        );

        return processed;
    }

    private static void awaitFailure(
        KafkaInboxConsumer consumer
    ) throws Exception {

        long deadline = System.nanoTime()
            + TimeUnit.SECONDS.toNanos(30);

        while (System.nanoTime() < deadline) {
            try {
                consumer.processAvailable(Duration.ofMillis(250));
            } catch (SQLException expected) {
                return;
            }
        }

        fail("Expected business-processing SQLException was not raised");
    }

    private static void insertEffect(
        Connection connection,
        String consumer,
        UUID eventId
    ) throws SQLException {

        try (PreparedStatement statement =
                 connection.prepareStatement("""
                     INSERT INTO inbox_test_effects
                         (event_id, consumer_name)
                     VALUES (?, ?)
                     """)) {

            statement.setObject(1, eventId);
            statement.setString(2, consumer);
            statement.executeUpdate();
        }
    }

    private static int effectCount(UUID eventId)
        throws SQLException {

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement =
                 connection.prepareStatement("""
                     SELECT COUNT(*)
                     FROM inbox_test_effects
                     WHERE event_id = ?
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
             PreparedStatement statement =
                 connection.prepareStatement("""
                     SELECT COUNT(*)
                     FROM consumer_inbox
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
             PreparedStatement statement =
                 connection.prepareStatement("""
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