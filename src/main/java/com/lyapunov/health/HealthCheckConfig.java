package com.lyapunov.health;

public record HealthCheckConfig(
        String jdbcUrl,
        String dbUser,
        String dbPassword,
        String kafkaHost,
        int kafkaPort,
        int timeoutMillis,
        long staleAfterSeconds) {

    public HealthCheckConfig {
        requireText(jdbcUrl, "jdbcUrl");
        requireText(dbUser, "dbUser");
        requireText(kafkaHost, "kafkaHost");

        if (kafkaPort < 1 || kafkaPort > 65535) {
            throw new IllegalArgumentException("Kafka port must be between 1 and 65535");
        }
        if (timeoutMillis < 1) {
            throw new IllegalArgumentException("Health timeout must be positive");
        }
        if (staleAfterSeconds < 1) {
            throw new IllegalArgumentException("Heartbeat stale threshold must be positive");
        }
    }

    public static HealthCheckConfig fromEnvironment() {
        return new HealthCheckConfig(
                env("LYAPUNOV_JDBC_URL", "jdbc:postgresql://localhost:5432/lyapunov"),
                env("LYAPUNOV_DB_USER", "postgres"),
                env("LYAPUNOV_DB_PASSWORD", "postgres"),
                env("LYAPUNOV_KAFKA_HOST", "localhost"),
                integerEnv("LYAPUNOV_KAFKA_PORT", 9092),
                integerEnv("LYAPUNOV_HEALTH_TIMEOUT_MS", 2000),
                longEnv("LYAPUNOV_HEARTBEAT_STALE_SECONDS", 60));
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static int integerEnv(String name, int fallback) {
        try {
            return Integer.parseInt(env(name, Integer.toString(fallback)));
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Invalid integer environment variable: " + name);
        }
    }

    private static long longEnv(String name, long fallback) {
        try {
            return Long.parseLong(env(name, Long.toString(fallback)));
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Invalid long environment variable: " + name);
        }
    }

    @Override
    public String toString() {
        return "HealthCheckConfig[jdbcUrl=" + jdbcUrl
                + ", dbUser=" + dbUser
                + ", dbPassword=[REDACTED]"
                + ", kafkaHost=" + kafkaHost
                + ", kafkaPort=" + kafkaPort
                + ", timeoutMillis=" + timeoutMillis
                + ", staleAfterSeconds=" + staleAfterSeconds + "]";
    }
}