package com.lyapunov.health;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

public final class HealthCheckProbes {
    private HealthCheckProbes() {
    }

    public static List<HealthProbe> create(HealthCheckConfig config) {
        return List.of(
                () -> checkKafka(config),
                () -> checkPostgres(config),
                () -> checkOutboxSchema(config),
                () -> checkWorkerHeartbeats(config));
    }

    private static HealthCheckResult checkKafka(HealthCheckConfig config) {
        try (Socket socket = new Socket()) {
            socket.connect(
                    new InetSocketAddress(config.kafkaHost(), config.kafkaPort()),
                    config.timeoutMillis());

            return HealthCheckResult.healthy(
                    "kafka", "TCP connection established to "
                            + config.kafkaHost() + ":" + config.kafkaPort());
        } catch (Exception exception) {
            return HealthCheckResult.unhealthy("kafka", concise(exception));
        }
    }

    private static HealthCheckResult checkPostgres(HealthCheckConfig config) {
        try (Connection connection = connect(config);
             PreparedStatement statement = connection.prepareStatement("SELECT 1");
             ResultSet result = statement.executeQuery()) {

            if (result.next() && result.getInt(1) == 1) {
                return HealthCheckResult.healthy(
                        "postgres", "Database connection and SELECT 1 succeeded");
            }

            return HealthCheckResult.unhealthy("postgres", "SELECT 1 returned no result");
        } catch (Exception exception) {
            return HealthCheckResult.unhealthy("postgres", concise(exception));
        }
    }

    private static HealthCheckResult checkOutboxSchema(HealthCheckConfig config) {
        try (Connection connection = connect(config);
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT COUNT(*) FROM outbox_events");
             ResultSet result = statement.executeQuery()) {

            result.next();

            return HealthCheckResult.healthy(
                    "outbox",
                    "outbox_events is queryable; rows=" + result.getLong(1));
        } catch (Exception exception) {
            return HealthCheckResult.unhealthy("outbox", concise(exception));
        }
    }

    private static HealthCheckResult checkWorkerHeartbeats(HealthCheckConfig config) {
        Instant staleBefore = Instant.now()
                .minus(Duration.ofSeconds(config.staleAfterSeconds()));

        String sql = """
                SELECT COUNT(*)
                FROM worker_heartbeats
                WHERE status = 'RUNNING'
                  AND last_seen_at < ?
                """;

        try (Connection connection = connect(config);
             PreparedStatement statement = connection.prepareStatement(sql)) {

            statement.setTimestamp(1, Timestamp.from(staleBefore));

            try (ResultSet result = statement.executeQuery()) {
                result.next();

                long stale = result.getLong(1);

                if (stale > 0) {
                    return HealthCheckResult.degraded(
                            "worker-heartbeats",
                            stale + " RUNNING worker heartbeat(s) older than "
                                    + config.staleAfterSeconds() + " seconds");
                }

                return HealthCheckResult.healthy(
                        "worker-heartbeats",
                        "No stale RUNNING worker heartbeats detected");
            }
        } catch (Exception exception) {
            return HealthCheckResult.unhealthy(
                    "worker-heartbeats", concise(exception));
        }
    }

    private static Connection connect(HealthCheckConfig config) throws Exception {
        return DriverManager.getConnection(
                config.jdbcUrl(),
                config.dbUser(),
                config.dbPassword());
    }

    private static String concise(Exception exception) {
        String message = exception.getMessage();

        return exception.getClass().getSimpleName() + ": "
                + (message == null
                ? "no detail"
                : message.replaceAll("[\\r\\n]+", " "));
    }
}