package com.lyapunov.cli;

import com.lyapunov.repository.OutboxQueueStats;
import com.lyapunov.repository.OutboxQueueStatsRepository;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * Prints a database-backed outbox queue snapshot.
 *
 * This command does not probe Kafka or prove that a worker process is alive.
 * Required environment: LYAPUNOV_JDBC_URL, LYAPUNOV_DB_USER, LYAPUNOV_DB_PASSWORD.
 */
public final class OutboxStatusCli {
    private OutboxStatusCli() {}

    public static void main(String[] args) {
        if (args.length != 0) {
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
        config.setMaximumPoolSize(1);
        config.setPoolName("lyapunov-status-cli");

        try (HikariDataSource dataSource = new HikariDataSource(config)) {
            OutboxQueueStats stats = new OutboxQueueStatsRepository(dataSource).snapshot();
            print(stats);
            if ("UNHEALTHY".equals(stats.queueHealth())) {
                System.exit(1);
            }
        } catch (Exception exception) {
            System.err.println("Could not read outbox status: " + exception.getMessage());
            System.exit(1);
        }
    }

    private static void print(OutboxQueueStats s) {
        System.out.println("LYAPUNOV OUTBOX STATUS (database snapshot)");
        System.out.println("Queue signal:       " + s.queueHealth());
        System.out.println("Snapshot at:        " + s.snapshotAt());
        System.out.println("Total events:       " + s.totalEvents());
        System.out.println("Pending:            " + s.pending());
        System.out.println("Claimed:            " + s.claimed());
        System.out.println("Published:          " + s.published());
        System.out.println("Dead-lettered:      " + s.dead());
        System.out.println("Skipped:            " + s.skipped());
        System.out.println("Total retry attempts: " + s.totalRetries());
        System.out.println("Retries currently due: " + s.retriesDue());
        System.out.println("Expired claims:     " + s.expiredClaims());
        System.out.println("Oldest pending:     " + value(s.oldestPendingAt()));
        System.out.println("Oldest claimed update: " + value(s.oldestClaimedAt()));
        System.out.println();
        System.out.println("Note: this is persisted queue state; it does not establish Kafka connectivity or worker liveness.");
    }

    private static String value(Object value) {
        return value == null ? "none" : value.toString();
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
        System.err.println("Usage: OutboxStatusCli");
        System.err.println("Set LYAPUNOV_JDBC_URL, LYAPUNOV_DB_USER, LYAPUNOV_DB_PASSWORD.");
    }
}