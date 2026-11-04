package com.lyapunov.health;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class HealthCheckReportTest {
    @Test
    void allHealthyChecksProduceHealthyReport() {
        HealthCheckReport report = HealthCheckReport.from(List.of(
                HealthCheckResult.healthy("db", "ok"),
                HealthCheckResult.healthy("broker", "ok")));

        assertEquals(HealthStatus.HEALTHY, report.status());
        assertEquals(2, report.checks().size());
    }

    @Test
    void degradedCheckProducesDegradedReport() {
        HealthCheckReport report = HealthCheckReport.from(List.of(
                HealthCheckResult.healthy("db", "ok"),
                HealthCheckResult.degraded("worker", "stale")));

        assertEquals(HealthStatus.DEGRADED, report.status());
    }

    @Test
    void unhealthyTakesPrecedenceOverDegraded() {
        HealthCheckReport report = HealthCheckReport.from(List.of(
                HealthCheckResult.degraded("worker", "stale"),
                HealthCheckResult.unhealthy("db", "down")));

        assertEquals(HealthStatus.UNHEALTHY, report.status());
    }

    @Test
    void emptyReportIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> HealthCheckReport.from(List.of()));
    }
}
