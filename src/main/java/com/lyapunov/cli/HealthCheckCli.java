package com.lyapunov.cli;

import com.lyapunov.health.HealthCheckReport;
import com.lyapunov.health.HealthCheckResult;
import com.lyapunov.health.HealthCheckService;
import com.lyapunov.health.HealthProbe;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public final class HealthCheckCli {
    private HealthCheckCli() {
    }

    public static void main(String[] args) {
        boolean readiness = args.length > 0 && "--readiness".equals(args[0]);
        Config config = Config.fromEnvironment();

        List<HealthProbe> probes = new ArrayList<>();
        probes.add(() -> checkKafka(config));
        probes.add(() -> checkPostgres(config));
        probes.add(() -> checkOutboxSchema(config));
        probes.add(() -> checkWorkerHeartbeats(config));

        HealthCheckReport report = new HealthCheckService(probes).run();
        System.out.println("LYAPUNOV HEALTH: " + report.status());
        System.out.println("checkedAt=" + report.checkedAt());
        report.checks().forEach(check -> System.out.printf(
                "%s %-10s %s%n", check.name(), check.status(), check.message()));

        if (report.status().name().equals("UNHEALTHY")
                || (readiness && !report.status().name().equals("HEALTHY"))) {
            System.exit(report.status().name().equals("UNHEALTHY") ? 2 : 1);
        }
    }

    private static HealthCheckResult checkKafka(Config config) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(config.kafkaHost(), config.kafkaPort()),
                    config.timeoutMillis());
            return HealthCheckResult.healthy("kafka", "TCP connection established to "
                    + config.kafkaHost() + ":" + config.kafkaPort());
        } catch (Exception exception) {
            return HealthCheckResult.unhealthy("kafka", concise(exception));
        }
    }

    private static HealthCheckResult checkPostgres(Config config) {
        try (Connection connection = connect(config);
             PreparedStatement statement = connection.prepareStatement("SELECT 1");
             ResultSet result = statement.executeQuery()) {
            if (result.next() && result.getInt(1) == 1) {
                return HealthCheckResult.healthy("postgres", "database connection and SELECT 1 succeeded");
            }
            return HealthCheckResult.unhealthy("postgres", "SELECT 1 returned no result");
        } catch (Exception exception) {
            return HealthCheckResult.unhealthy("postgres", concise(exception));
        }
    }

    private static HealthCheckResult checkOutboxSchema(Config config) {
        try (Connection connection = connect(config);
             PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM outbox_events");
             ResultSet result = statement.executeQuery()) {
            result.next();
            return HealthCheckResult.healthy("outbox", "outbox_events is queryable; rows=" + result.getLong(1));
        } catch (Exception exception) {
            return HealthCheckResult.unhealthy("outbox", concise(exception));
        }
    }

    private static HealthCheckResult checkWorkerHeartbeats(Config config) {
        Instant staleBefore = Instant.now().minus(Duration.ofSeconds(config.staleAfterSeconds()));
        String sql = "SELECT COUNT(*) FROM worker_heartbeats WHERE status = 'RUNNING' AND last_seen_at < ?";
        try (Connection connection = connect(config);
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, java.sql.Timestamp.from(staleBefore));
            try (ResultSet result = statement.executeQuery()) {
                result.next();
                long stale = result.getLong(1);
                if (stale > 0) {
                    return HealthCheckResult.degraded("worker-heartbeats",
                            stale + " RUNNING worker heartbeat(s) older than "
                                    + config.staleAfterSeconds() + " seconds");
                }
                return HealthCheckResult.healthy("worker-heartbeats",
                        "no stale RUNNING worker heartbeats detected");
            }
        } catch (Exception exception) {
            return HealthCheckResult.unhealthy("worker-heartbeats", concise(exception));
        }
    }

    private static Connection connect(Config config) throws Exception {
        return DriverManager.getConnection(config.jdbcUrl(), config.dbUser(), config.dbPassword());
    }

    private static String concise(Exception exception) {
        String message = exception.getMessage();
        return exception.getClass().getSimpleName() + ": "
                + (message == null ? "no detail" : message.replaceAll("[\\r\\n]+", " "));
    }

    private record Config(
            String jdbcUrl,
            String dbUser,
            String dbPassword,
            String kafkaHost,
            int kafkaPort,
            int timeoutMillis,
            long staleAfterSeconds) {

        private static Config fromEnvironment() {
            return new Config(
                    env("LYAPUNOV_JDBC_URL", "jdbc:postgresql://localhost:5432/lyapunov"),
                    env("LYAPUNOV_DB_USER", "postgres"),
                    env("LYAPUNOV_DB_PASSWORD", "postgres"),
                    env("LYAPUNOV_KAFKA_HOST", "localhost"),
                    integerEnv("LYAPUNOV_KAFKA_PORT", 9092),
                    integerEnv("LYAPUNOV_HEALTH_TIMEOUT_MS", 2000),
                    longEnv("LYAPUNOV_HEARTBEAT_STALE_SECONDS", 60));
        }

        private static String env(String name, String fallback) {
            String value = System.getenv(name);
            return value == null || value.isBlank() ? fallback : value;
        }

        private static int integerEnv(String name, int fallback) {
            return Integer.parseInt(env(name, Integer.toString(fallback)));
        }

        private static long longEnv(String name, long fallback) {
            return Long.parseLong(env(name, Long.toString(fallback)));
        }
    }
}
