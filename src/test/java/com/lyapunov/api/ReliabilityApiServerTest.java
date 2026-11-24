package com.lyapunov.api;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class ReliabilityApiServerTest {

    private static PostgreSQLContainer<?> postgres;
    private static HikariDataSource dataSource;
    private static ReliabilityApiServer api;
    private static HttpClient client;

    private static String baseUrl() {
        return "http://127.0.0.1:" + api.port();
    }

    @BeforeAll
    static void startInfrastructure() throws Exception {
        postgres = new PostgreSQLContainer<>("postgres:16-alpine");
        postgres.start();

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(postgres.getJdbcUrl());
        config.setUsername(postgres.getUsername());
        config.setPassword(postgres.getPassword());
        config.setMaximumPoolSize(5);

        dataSource = new HikariDataSource(config);

        String ddl = Files.readString(Path.of("docker", "init.sql"));

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(ddl);
        }

        api = new ReliabilityApiServer("127.0.0.1", 0, dataSource);
        api.start();

        client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    }

    @AfterAll
    static void stopInfrastructure() {
        if (api != null) {
            api.close();
        }
        if (dataSource != null) {
            dataSource.close();
        }
        if (postgres != null) {
            postgres.stop();
        }
    }

    @BeforeEach
    void cleanDatabase() throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(
                "TRUNCATE TABLE outbox_events, event_history, worker_heartbeats"
            );
        }
    }

    private HttpResponse<String> get(String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(baseUrl() + path))
            .GET()
            .build();

        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> post(String path, String body, String contentType)
        throws Exception {

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(baseUrl() + path))
            .header("Content-Type", contentType)
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();

        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void healthEndpointReportsDatabaseAvailability() throws Exception {
        HttpResponse<String> response = get("/health");

        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"status\":\"UP\""));
        assertTrue(response.body().contains("\"database\":\"UP\""));
    }

    @Test
    void createsAndPersistsEvent() throws Exception {
        String body = """
            {
              "aggregateType": "ORDER",
              "aggregateId": "order-1001",
              "eventType": "ORDER_CREATED",
              "payload": {"orderId": "order-1001", "amount": 1250}
            }
            """;

        HttpResponse<String> response =
            post("/api/v1/events", body, "application/json");

        assertEquals(201, response.statusCode());
        assertTrue(response.body().contains("\"status\":\"PENDING\""));
        assertTrue(response.body().contains("\"sequenceNumber\":1"));
        assertTrue(response.body().contains("\"id\""));
    }

    @Test
    void listsDeadLetterEvents() throws Exception {
        HttpResponse<String> response =
            get("/api/v1/events/dead?limit=20&offset=0");

        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"events\":[]"));
        assertTrue(response.body().contains("\"count\":0"));
    }

    @Test
    void listsWorkerHeartbeats() throws Exception {
        HttpResponse<String> response = get("/api/v1/workers");

        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"workers\":[]"));
    }

    @Test
    void malformedJsonReturnsBadRequest() throws Exception {
        HttpResponse<String> response =
            post("/api/v1/events", "{invalid-json", "application/json");

        assertEquals(400, response.statusCode());
        assertTrue(response.body().contains("INVALID_REQUEST"));
    }

    @Test
    void rejectsUnsupportedContentType() throws Exception {
        HttpResponse<String> response =
            post("/api/v1/events", "{}", "text/plain");

        assertEquals(400, response.statusCode());
        assertTrue(response.body().contains("INVALID_REQUEST"));
    }

    @Test
    void rejectsUnsupportedHttpMethod() throws Exception {
        HttpResponse<String> response = get("/api/v1/events");

        assertEquals(405, response.statusCode());
        assertTrue(response.headers().firstValue("Allow").orElse("").contains("POST"));
    }

    @Test
    void rejectsInvalidPaginationParameters() throws Exception {
        HttpResponse<String> response =
            get("/api/v1/events/dead?limit=9999");

        assertEquals(400, response.statusCode());
    }
}