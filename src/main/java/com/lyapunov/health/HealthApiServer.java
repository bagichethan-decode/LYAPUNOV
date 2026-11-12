package com.lyapunov.health;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public final class HealthApiServer implements AutoCloseable {
    private static final String BASE_PATH = "/api/v1/health";

    private final HttpServer server;
    private final ExecutorService executor;
    private final Supplier<HealthCheckReport> healthSupplier;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public HealthApiServer(
            InetSocketAddress address,
            Supplier<HealthCheckReport> healthSupplier) throws IOException {

        this.healthSupplier = healthSupplier;

        this.server = HttpServer.create(address, 64);

        AtomicInteger threadNumber = new AtomicInteger();

        ThreadFactory threadFactory = task -> {
            Thread thread = new Thread(
                    task,
                    "lyapunov-health-api-" + threadNumber.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };

        this.executor = Executors.newFixedThreadPool(4, threadFactory);
        this.server.setExecutor(executor);
        this.server.createContext(BASE_PATH, this::handle);
    }

    public void start() {
        server.start();
    }

    public int port() {
        return server.getAddress().getPort();
    }

    private void handle(HttpExchange exchange) throws IOException {
        try {
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Allow", "GET");
                sendJson(exchange, 405, Map.of(
                        "status", "ERROR",
                        "error", "Method not allowed"));
                return;
            }

            String path = exchange.getRequestURI().getPath();

            switch (path) {
                case BASE_PATH, BASE_PATH + "/" ->
                        sendHealth(exchange);

                case BASE_PATH + "/ready" ->
                        sendHealth(exchange);

                case BASE_PATH + "/live" ->
                        sendJson(exchange, 200, Map.of("status", "UP"));

                default ->
                        sendJson(exchange, 404, Map.of(
                                "status", "NOT_FOUND",
                                "error", "Endpoint not found"));
            }
        } finally {
            exchange.close();
        }
    }

    private void sendHealth(HttpExchange exchange) throws IOException {
        try {
            HealthCheckReport report = healthSupplier.get();

            boolean healthy = report.status() == HealthStatus.HEALTHY;

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("status", report.status().name());
            body.put("checkedAt", report.checkedAt().toString());
            body.put("checks", report.checks().stream()
                    .map(check -> Map.of(
                            "name", check.name(),
                            "status", check.status().name(),
                            "message", publicMessage(check.status())))
                    .toList());

            sendJson(exchange, healthy ? 200 : 503, body);
        } catch (RuntimeException exception) {
            sendJson(exchange, 503, Map.of(
                    "status", "UNHEALTHY",
                    "error", "Health checks are temporarily unavailable"));
        }
    }

    private static String publicMessage(HealthStatus status) {
        return switch (status) {
            case HEALTHY -> "Check passed";
            case DEGRADED -> "Check requires attention";
            case UNHEALTHY -> "Check failed";
        };
    }

    private void sendJson(
            HttpExchange exchange,
            int statusCode,
            Object body) throws IOException {

        byte[] response = objectMapper.writeValueAsBytes(body);

        exchange.getResponseHeaders().set(
                "Content-Type", "application/json; charset=utf-8");
        exchange.getResponseHeaders().set("Cache-Control", "no-store");
        exchange.getResponseHeaders().set("X-Content-Type-Options", "nosniff");

        exchange.sendResponseHeaders(statusCode, response.length);

        try (OutputStream output = exchange.getResponseBody()) {
            output.write(response);
        }
    }

    @Override
    public void close() {
        server.stop(0);
        executor.shutdownNow();
    }
}