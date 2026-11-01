package com.lyapunov.cli;

import com.lyapunov.model.OutboxEvent;
import com.lyapunov.repository.OutboxRepository;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.util.List;
import java.util.UUID;

/**
 * Operator entry point for inspecting and resolving terminally failed outbox events.
 *
 * Required environment:
 * LYAPUNOV_JDBC_URL, LYAPUNOV_DB_USER, LYAPUNOV_DB_PASSWORD
 */
public final class DeadLetterQueueCli {
    private DeadLetterQueueCli() {}

    public static void main(String[] args) {
        if (args.length == 0 || !List.of("list", "retry", "skip").contains(args[0])) {
            usage();
            System.exit(2);
            return;
        }

        String jdbcUrl = requiredEnv("LYAPUNOV_JDBC_URL");
        String username = requiredEnv("LYAPUNOV_DB_USER");
        String password = requiredEnv("LYAPUNOV_DB_PASSWORD");
        if (jdbcUrl == null || username == null || password == null) {
            System.err.println("Missing required database environment variables.");
            System.exit(2);
            return;
        }

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(2);
        config.setPoolName("lyapunov-dlq-cli");

        try (HikariDataSource dataSource = new HikariDataSource(config)) {
            OutboxRepository repository = new OutboxRepository(dataSource);
            switch (args[0]) {
                case "list" -> list(repository, args);
                case "retry" -> transition(repository, args, true);
                case "skip" -> transition(repository, args, false);
                default -> usage();
            }
        } catch (Exception exception) {
            System.err.println("DLQ operation failed: " + exception.getMessage());
            System.exit(1);
        }
    }

    private static void list(OutboxRepository repository, String[] args) throws Exception {
        if (args.length > 3) {
            usage();
            System.exit(2);
            return;
        }
        int limit = args.length >= 2 ? Integer.parseInt(args[1]) : 50;
        int offset = args.length == 3 ? Integer.parseInt(args[2]) : 0;
        List<OutboxEvent> events = repository.listDeadEvents(limit, offset);

        System.out.printf("Dead-letter events (limit=%d, offset=%d): %d%n",
            limit, offset, events.size());
        System.out.println("ID | STATUS | RETRIES | AGGREGATE | SEQUENCE | EVENT TYPE | CREATED");
        for (OutboxEvent event : events) {
            System.out.printf("%s | %s | %d | %s:%s | %d | %s | %s%n",
                event.id(), event.status(), event.retryCount(),
                event.aggregateType(), event.aggregateId(), event.sequenceNumber(),
                event.eventType(), event.createdAt());
        }
        if (events.isEmpty()) {
            System.out.println("No terminally failed events on this page.");
        }
    }

    private static void transition(OutboxRepository repository, String[] args,
                                   boolean retry) throws Exception {
        if (args.length < 4) {
            usage();
            System.exit(2);
            return;
        }
        UUID eventId = UUID.fromString(args[1]);
        String operatorId = args[2];
        String reason = String.join(" ", java.util.Arrays.copyOfRange(args, 3, args.length));

        if (retry) {
            repository.retryDeadEvent(eventId, operatorId, reason);
            System.out.println("Event requeued for delivery: " + eventId);
        } else {
            repository.skipDeadEvent(eventId, operatorId, reason);
            System.out.println("Event marked SKIPPED: " + eventId);
        }
    }

    private static String requiredEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            System.err.println("Required environment variable is not set: " + name);
            return null;
        }
        return value;
    }

    private static void usage() {
        System.err.println("""
            Usage:
              DeadLetterQueueCli list [limit] [offset]
              DeadLetterQueueCli retry <event-id> <operator-id> <reason...>
              DeadLetterQueueCli skip  <event-id> <operator-id> <reason...>

            Database environment:
              LYAPUNOV_JDBC_URL
              LYAPUNOV_DB_USER
              LYAPUNOV_DB_PASSWORD
            """);
    }
}