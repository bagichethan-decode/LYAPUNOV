package com.lyapunov.cli;

import com.lyapunov.repository.OutboxRepository;
import com.lyapunov.repository.WorkerHeartbeatRepository;
import com.lyapunov.worker.EventPublisher;
import com.lyapunov.worker.JdbcOutboxStore;
import com.lyapunov.worker.KafkaEventPublisher;
import com.lyapunov.worker.OutboxRelayWorker;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;

public final class OutboxRelayWorkerCli {

    private OutboxRelayWorkerCli() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 0) {
            throw new IllegalArgumentException(
                "Usage: OutboxRelayWorkerCli (configured through environment variables)"
            );
        }

        String jdbcUrl = required("LYAPUNOV_JDBC_URL");
        String dbUser = required("LYAPUNOV_DB_USER");
        String dbPassword = required("LYAPUNOV_DB_PASSWORD");

        String workerId = env("LYAPUNOV_WORKER_ID", "lyapunov-relay");
        String instanceId = UUID.randomUUID().toString();

        int batchSize = integer("LYAPUNOV_WORKER_BATCH_SIZE", 100);
        int maxRetries = integer("LYAPUNOV_WORKER_MAX_RETRIES", 8);

        long leaseSeconds = positiveLong("LYAPUNOV_WORKER_LEASE_SECONDS", 30);
        long pollMillis = positiveLong("LYAPUNOV_WORKER_POLL_MILLIS", 500);
        long baseRetryMillis = nonNegativeLong(
            "LYAPUNOV_WORKER_BASE_RETRY_DELAY_MILLIS", 1000
        );
        long maxRetrySeconds = nonNegativeLong(
            "LYAPUNOV_WORKER_MAX_RETRY_DELAY_SECONDS", 300
        );
        long heartbeatSeconds = positiveLong(
            "LYAPUNOV_WORKER_HEARTBEAT_SECONDS", 10
        );

        String bootstrapServers = env(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        );
        String topic = env(
            "LYAPUNOV_KAFKA_TOPIC", "lyapunov.events"
        );

        HikariConfig databaseConfig = new HikariConfig();
        databaseConfig.setJdbcUrl(jdbcUrl);
        databaseConfig.setUsername(dbUser);
        databaseConfig.setPassword(dbPassword);
        databaseConfig.setMaximumPoolSize(5);
        databaseConfig.setPoolName("lyapunov-relay-pool");

        System.out.println("========================================");
        System.out.println("LYAPUNOV OUTBOX RELAY WORKER");
        System.out.println("========================================");
        System.out.println("Worker ID: " + workerId);
        System.out.println("Instance ID: " + instanceId);
        System.out.println("Database: " + jdbcUrl);
        System.out.println("Kafka: " + bootstrapServers);
        System.out.println("Topic: " + topic);
        System.out.println("Batch size: " + batchSize);
        System.out.println("Max retries: " + maxRetries);
        System.out.println("========================================");

        try (HikariDataSource dataSource =
                 new HikariDataSource(databaseConfig)) {

            OutboxRepository outboxRepository =
                new OutboxRepository(dataSource);

            WorkerHeartbeatRepository heartbeatRepository =
                new WorkerHeartbeatRepository(dataSource);

            Properties kafkaProperties = new Properties();
            kafkaProperties.put("bootstrap.servers", bootstrapServers);
            kafkaProperties.put("client.id", workerId + "-" + instanceId);
            kafkaProperties.put("acks", "all");
            kafkaProperties.put("enable.idempotence", "true");
            kafkaProperties.put(
                "max.in.flight.requests.per.connection", "5"
            );

            EventPublisher publisher =
                new KafkaEventPublisher(kafkaProperties, topic);

            OutboxRelayWorker worker = new OutboxRelayWorker(
                new JdbcOutboxStore(outboxRepository),
                publisher,
                workerId,
                batchSize,
                maxRetries,
                Duration.ofSeconds(leaseSeconds),
                Duration.ofMillis(pollMillis),
                Duration.ofMillis(baseRetryMillis),
                Duration.ofSeconds(maxRetrySeconds),
                heartbeatRepository,
                instanceId,
                Duration.ofSeconds(heartbeatSeconds)
            );

            Thread runner = Thread.currentThread();

            Thread shutdownHook = new Thread(() -> {
                System.out.println(
                    "\nShutdown requested. Stopping relay worker..."
                );
                worker.stop();
                runner.interrupt();
            }, "lyapunov-relay-shutdown");

            Runtime.getRuntime().addShutdownHook(shutdownHook);

            try {
                System.out.println("Relay worker started. Press Ctrl+C to stop.");
                worker.run();
            } finally {
                try {
                    Runtime.getRuntime().removeShutdownHook(shutdownHook);
                } catch (IllegalStateException ignored) {
                    // JVM shutdown already in progress.
                }

                worker.close();
                System.out.println("Relay worker stopped.");
            }
        }
    }

    private static String required(String key) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                "Missing required environment variable: " + key
            );
        }
        return value;
    }

    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static int integer(String key, int defaultValue) {
        int value = Integer.parseInt(env(key, Integer.toString(defaultValue)));
        if (value <= 0) {
            throw new IllegalArgumentException(key + " must be positive");
        }
        return value;
    }

    private static long positiveLong(String key, long defaultValue) {
        long value = Long.parseLong(env(key, Long.toString(defaultValue)));
        if (value <= 0) {
            throw new IllegalArgumentException(key + " must be positive");
        }
        return value;
    }

    private static long nonNegativeLong(String key, long defaultValue) {
        long value = Long.parseLong(env(key, Long.toString(defaultValue)));
        if (value < 0) {
            throw new IllegalArgumentException(
                key + " must not be negative"
            );
        }
        return value;
    }
}