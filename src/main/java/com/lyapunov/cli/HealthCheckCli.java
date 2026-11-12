package com.lyapunov.cli;

import com.lyapunov.health.HealthCheckConfig;
import com.lyapunov.health.HealthCheckProbes;
import com.lyapunov.health.HealthCheckReport;
import com.lyapunov.health.HealthCheckService;
import com.lyapunov.health.HealthProbe;
import com.lyapunov.health.HealthStatus;

import java.util.List;

public final class HealthCheckCli {
    private HealthCheckCli() {
    }

    public static void main(String[] args) {
        boolean readiness = args.length > 0
                && "--readiness".equals(args[0]);

        HealthCheckConfig config = HealthCheckConfig.fromEnvironment();
        List<HealthProbe> probes = HealthCheckProbes.create(config);

        HealthCheckReport report = new HealthCheckService(probes).run();

        System.out.println("LYAPUNOV HEALTH: " + report.status());
        System.out.println("checkedAt=" + report.checkedAt());

        report.checks().forEach(check -> System.out.printf(
                "%s %-10s %s%n",
                check.name(),
                check.status(),
                check.message()));

        if (report.status() == HealthStatus.UNHEALTHY) {
            System.exit(2);
        }

        if (readiness && report.status() != HealthStatus.HEALTHY) {
            System.exit(1);
        }
    }
}