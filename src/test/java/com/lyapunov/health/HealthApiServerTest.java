package com.lyapunov.health;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class HealthApiServerTest {
    private final HttpClient client = HttpClient.newHttpClient();

    @Test
    void healthyEndpointReturns200AndStructuredJson() throws Exception {
        HealthCheckReport report = HealthCheckReport.from(List.of(
                HealthCheckResult.healthy("postgres", "database ok"),
                HealthCheckResult.healthy("kafka", "broker ok")));

        try (HealthApiServer server = start(report)) {
            HttpResponse<String> response = get(server, "/api/v1/health");

            assertEquals(200, response.statusCode());
            assertTrue(response.body().contains("\"status\":\"HEALTHY\""));
            assertTrue(response.body().contains("\"checkedAt\""));
            assertTrue(response.body().contains("\"checks\""));
            assertTrue(response.body().contains("postgres"));
            assertFalse(response.body().contains("database ok"));
        }
    }

    @Test
    void degradedEndpointReturns503() throws Exception {
        HealthCheckReport report = HealthCheckReport.from(List.of(
                HealthCheckResult.healthy("postgres", "ok"),
                HealthCheckResult.degraded("worker", "stale")));

        try (HealthApiServer server = start(report)) {
            HttpResponse<String> response = get(server, "/api/v1/health/ready");

            assertEquals(503, response.statusCode());
            assertTrue(response.body().contains("\"status\":\"DEGRADED\""));
        }
    }

    @Test
    void unhealthyEndpointReturns503() throws Exception {
        HealthCheckReport report = HealthCheckReport.from(List.of(
                HealthCheckResult.unhealthy("postgres", "connection failed")));

        try (HealthApiServer server = start(report)) {
            HttpResponse<String> response = get(server, "/api/v1/health");

            assertEquals(503, response.statusCode());
            assertTrue(response.body().contains("\"status\":\"UNHEALTHY\""));
            assertFalse(response.body().contains("connection failed"));
        }
    }

    @Test
    void livenessDoesNotExecuteDependencyChecks() throws Exception {
        AtomicInteger calls = new AtomicInteger();

        try (HealthApiServer server = new HealthApiServer(
                new InetSocketAddress("127.0.0.1", 0),
                () -> {
                    calls.incrementAndGet();
                    return HealthCheckReport.from(List.of(
                            HealthCheckResult.healthy("test", "ok")));
                })) {

            server.start();

            HttpResponse<String> response = get(server, "/api/v1/health/live");

            assertEquals(200, response.statusCode());
            assertTrue(response.body().contains("\"status\":\"UP\""));
            assertEquals(0, calls.get());
        }
    }

    @Test
    void unknownEndpointReturns404() throws Exception {
        HealthCheckReport report = HealthCheckReport.from(List.of(
                HealthCheckResult.healthy("test", "ok")));

        try (HealthApiServer server = start(report)) {
            HttpResponse<String> response = get(server, "/api/v1/health/unknown");

            assertEquals(404, response.statusCode());
        }
    }

    @Test
    void unsupportedMethodReturns405() throws Exception {
        HealthCheckReport report = HealthCheckReport.from(List.of(
                HealthCheckResult.healthy("test", "ok")));

        try (HealthApiServer server = start(report)) {
            HttpRequest request = HttpRequest.newBuilder(
                    URI.create(url(server, "/api/v1/health")))
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build();

            HttpResponse<String> response = client.send(
                    request, HttpResponse.BodyHandlers.ofString());

            assertEquals(405, response.statusCode());
            assertEquals("GET", response.headers()
                    .firstValue("Allow").orElse(""));
        }
    }

    @Test
    void healthSupplierFailureReturnsSafe503Response() throws Exception {
        try (HealthApiServer server = new HealthApiServer(
                new InetSocketAddress("127.0.0.1", 0),
                () -> {
                    throw new IllegalStateException("sensitive internal detail");
                })) {

            server.start();

            HttpResponse<String> response = get(server, "/api/v1/health");

            assertEquals(503, response.statusCode());
            assertTrue(response.body().contains("temporarily unavailable"));
            assertFalse(response.body().contains("sensitive internal detail"));
        }
    }

    private HealthApiServer start(HealthCheckReport report) throws Exception {
        HealthApiServer server = new HealthApiServer(
                new InetSocketAddress("127.0.0.1", 0),
                () -> report);

        server.start();
        return server;
    }

    private HttpResponse<String> get(
            HealthApiServer server,
            String path) throws Exception {

        HttpRequest request = HttpRequest.newBuilder(
                URI.create(url(server, path)))
                .GET()
                .build();

        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private String url(HealthApiServer server, String path) {
        return "http://127.0.0.1:" + server.port() + path;
    }
}