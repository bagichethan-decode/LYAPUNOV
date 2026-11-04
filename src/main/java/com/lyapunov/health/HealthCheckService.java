package com.lyapunov.health;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class HealthCheckService {
    private final List<HealthProbe> probes;

    public HealthCheckService(List<HealthProbe> probes) {
        this.probes = List.copyOf(Objects.requireNonNull(probes, "probes"));
        if (this.probes.isEmpty()) {
            throw new IllegalArgumentException("At least one probe is required");
        }
    }

    public HealthCheckReport run() {
        List<HealthCheckResult> results = new ArrayList<>();
        for (HealthProbe probe : probes) {
            try {
                results.add(Objects.requireNonNull(probe.check(), "probe result"));
            } catch (RuntimeException exception) {
                results.add(HealthCheckResult.unhealthy(
                        "probe-execution",
                        exception.getClass().getSimpleName() + ": " + safeMessage(exception)));
            }
        }
        return HealthCheckReport.from(results);
    }

    private static String safeMessage(RuntimeException exception) {
        return exception.getMessage() == null ? "no detail" : exception.getMessage();
    }
}
