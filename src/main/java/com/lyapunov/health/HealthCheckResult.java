package com.lyapunov.health;

import java.time.Instant;
import java.util.Objects;

public record HealthCheckResult(
        String name,
        HealthStatus status,
        String message,
        Instant checkedAt) {

    public HealthCheckResult {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(checkedAt, "checkedAt");
    }

    public static HealthCheckResult healthy(String name, String message) {
        return new HealthCheckResult(name, HealthStatus.HEALTHY, message, Instant.now());
    }

    public static HealthCheckResult degraded(String name, String message) {
        return new HealthCheckResult(name, HealthStatus.DEGRADED, message, Instant.now());
    }

    public static HealthCheckResult unhealthy(String name, String message) {
        return new HealthCheckResult(name, HealthStatus.UNHEALTHY, message, Instant.now());
    }
}
