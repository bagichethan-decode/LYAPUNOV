package com.lyapunov.cli;

import com.lyapunov.health.HealthApiServer;
import com.lyapunov.health.HealthCheckConfig;
import com.lyapunov.health.HealthCheckProbes;
import com.lyapunov.health.HealthCheckService;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

public final class HealthApiCli {
    private HealthApiCli() {
    }

    public static void main(String[] args) throws Exception {
        String host = env("LYAPUNOV_API_HOST", "0.0.0.0");
        int port = integerEnv("LYAPUNOV_API_PORT", 8080);

        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("API port must be between 1 and 65535");
        }

        HealthCheckConfig config = HealthCheckConfig.fromEnvironment();

        HealthCheckService healthService = new HealthCheckService(
                HealthCheckProbes.create(config));

        HealthApiServer server = new HealthApiServer(
                new InetSocketAddress(host, port),
                healthService::run);

        Runtime.getRuntime().addShutdownHook(
                new Thread(server::close, "lyapunov-api-shutdown"));

        server.start();

        System.out.println("LYAPUNOV Health API started on "
                + host + ":" + server.port());

        System.out.println("Health: http://localhost:"
                + server.port() + "/api/v1/health");

        new CountDownLatch(1).await();
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static int integerEnv(String name, int fallback) {
        try {
            return Integer.parseInt(env(name, Integer.toString(fallback)));
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Invalid API port configuration");
        }
    }
}