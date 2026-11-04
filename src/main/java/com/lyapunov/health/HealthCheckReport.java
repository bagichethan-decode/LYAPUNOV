package com.lyapunov.health;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public record HealthCheckReport(
        HealthStatus status,
        List<HealthCheckResult> checks,
        Instant checkedAt) {

    public HealthCheckReport {
        Objects.requireNonNull(status, "status");
        checks = List.copyOf(Objects.requireNonNull(checks, "checks"));
        Objects.requireNonNull(checkedAt, "checkedAt");
        if (checks.isEmpty()) {
            throw new IllegalArgumentException("At least one health check is required");
        }
    }

    public static HealthCheckReport from(List<HealthCheckResult> checks) {
        Objects.requireNonNull(checks, "checks");
        if (checks.isEmpty()) {
            throw new IllegalArgumentException("At least one health check is required");
        }

        HealthStatus overall = HealthStatus.HEALTHY;
        for (HealthCheckResult check : checks) {
            if (check.status() == HealthStatus.UNHEALTHY) {
                overall = HealthStatus.UNHEALTHY;
                break;
            }
            if (check.status() == HealthStatus.DEGRADED) {
                overall = HealthStatus.DEGRADED;
            }
        }
        return new HealthCheckReport(overall, checks, Instant.now());
    }
}
