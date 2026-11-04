package com.lyapunov.health;

@FunctionalInterface
public interface HealthProbe {
    HealthCheckResult check();
}
