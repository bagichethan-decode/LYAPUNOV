package com.lyapunov.health;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class HealthCheckServiceTest {
    @Test
    void executesAllProbesAndAggregatesTheirResults() {
        HealthCheckService service = new HealthCheckService(List.of(
                () -> HealthCheckResult.healthy("postgres", "ok"),
                () -> HealthCheckResult.degraded("worker", "stale")));

        HealthCheckReport report = service.run();

        assertEquals(HealthStatus.DEGRADED, report.status());
        assertEquals(2, report.checks().size());
    }

    @Test
    void convertsUnexpectedProbeExceptionIntoUnhealthyResult() {
        HealthCheckService service = new HealthCheckService(List.of(
                () -> { throw new IllegalStateException("probe exploded"); }));

        HealthCheckReport report = service.run();

        assertEquals(HealthStatus.UNHEALTHY, report.status());
        assertTrue(report.checks().get(0).message().contains("probe exploded"));
    }

    @Test
    void rejectsEmptyProbeList() {
        assertThrows(IllegalArgumentException.class, () -> new HealthCheckService(List.of()));
    }
}
