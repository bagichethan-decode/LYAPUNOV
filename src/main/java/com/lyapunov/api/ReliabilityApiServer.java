package com.lyapunov.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.lyapunov.model.OutboxEvent;
import com.lyapunov.repository.OutboxRepository;
import com.lyapunov.repository.WorkerHeartbeat;
import com.lyapunov.repository.WorkerHeartbeatRepository;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public final class ReliabilityApiServer implements AutoCloseable {

    private static final int MAX_BODY_BYTES = 1_048_576;
    private static final ObjectMapper JSON = new ObjectMapper();

    private final HttpServer server;
    private final ExecutorService executor;
    private final DataSource dataSource;
    private final OutboxRepository outboxRepository;
    private final WorkerHeartbeatRepository heartbeatRepository;

    public ReliabilityApiServer(
        String host,
        int port,
        DataSource dataSource
    ) throws IOException {

        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("Host must not be blank");
        }
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }
        if (dataSource == null) {
            throw new IllegalArgumentException("DataSource is required");
        }

        this.dataSource = dataSource;
        this.outboxRepository = new OutboxRepository(dataSource);
        this.heartbeatRepository = new WorkerHeartbeatRepository(dataSource);

        this.server = HttpServer.create(
            new InetSocketAddress(host, port), 64
        );

        ThreadFactory factory = task -> {
            Thread thread = new Thread(task, "lyapunov-api");
            thread.setDaemon(false);
            return thread;
        };

        this.executor = Executors.newFixedThreadPool(8, factory);
        server.setExecutor(executor);
        server.createContext("/health", this::handleHealth);
        server.createContext("/api/v1/events", this::handleEvents);
        server.createContext("/api/v1/workers", this::handleWorkers);
    }

    public void start() {
        server.start();
    }

    public int port() {
        return server.getAddress().getPort();
    }

    private void handleHealth(HttpExchange exchange) throws IOException {
        try {
            if (!"GET".equals(exchange.getRequestMethod())) {
                sendMethodNotAllowed(exchange, "GET");
                return;
            }

            try (Connection connection = dataSource.getConnection()) {
                boolean healthy = connection.isValid(3);

                ObjectNode response = JSON.createObjectNode();
                response.put("service", "LYAPUNOV");
                response.put("status", healthy ? "UP" : "DEGRADED");
                response.put("database", healthy ? "UP" : "DOWN");

                sendJson(exchange, healthy ? 200 : 503, response);
            }
        } catch (Exception exception) {
            sendError(exchange, 503, "DATABASE_UNAVAILABLE",
                "Database health check failed");
        }
    }

    private void handleEvents(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();

        try {
            if ("/api/v1/events".equals(path)) {
                if (!"POST".equals(exchange.getRequestMethod())) {
                    sendMethodNotAllowed(exchange, "POST");
                    return;
                }

                createEvent(exchange);
                return;
            }

            if ("/api/v1/events/dead".equals(path)) {
                if (!"GET".equals(exchange.getRequestMethod())) {
                    sendMethodNotAllowed(exchange, "GET");
                    return;
                }

                listDeadEvents(exchange);
                return;
            }

            sendError(exchange, 404, "NOT_FOUND",
                "The requested endpoint does not exist");

        } catch (IllegalArgumentException exception) {
            sendError(exchange, 400, "INVALID_REQUEST",
                safeMessage(exception));
        } catch (Exception exception) {
            sendError(exchange, 500, "INTERNAL_ERROR",
                "The request could not be completed");
        }
    }

    private void createEvent(HttpExchange exchange) throws Exception {
        JsonNode body = readJson(exchange);

        String aggregateType = requiredText(body, "aggregateType", 255);
        String aggregateId = requiredText(body, "aggregateId", 255);
        String eventType = requiredText(body, "eventType", 255);

        JsonNode payloadNode = body.get("payload");

        if (payloadNode == null || payloadNode.isNull()) {
            throw new IllegalArgumentException("payload is required");
        }

        String payload = payloadNode.isTextual()
            ? payloadNode.textValue()
            : JSON.writeValueAsString(payloadNode);

        if (payload.isBlank()) {
            throw new IllegalArgumentException("payload must not be blank");
        }

        if (payload.length() > MAX_BODY_BYTES) {
            throw new IllegalArgumentException("payload is too large");
        }

        OutboxEvent event = outboxRepository.enqueue(
            aggregateType,
            aggregateId,
            eventType,
            payload
        );

        ObjectNode response = JSON.createObjectNode();
        response.put("id", event.id().toString());
        response.put("aggregateType", event.aggregateType());
        response.put("aggregateId", event.aggregateId());
        response.put("sequenceNumber", event.sequenceNumber());
        response.put("eventType", event.eventType());
        response.put("status", event.status().name());
        response.put("createdAt", event.createdAt().toString());

        sendJson(exchange, 201, response);
    }

    private void listDeadEvents(HttpExchange exchange) throws Exception {
        Map<String, String> query = parseQuery(
            exchange.getRequestURI().getRawQuery()
        );

        int limit = parseInteger(query.getOrDefault("limit", "50"), "limit");
        int offset = parseInteger(query.getOrDefault("offset", "0"), "offset");

        if (limit < 1 || limit > 500) {
            throw new IllegalArgumentException("limit must be between 1 and 500");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset must not be negative");
        }

        List<OutboxEvent> events =
            outboxRepository.listDeadEvents(limit, offset);

        ObjectNode response = JSON.createObjectNode();
        response.put("limit", limit);
        response.put("offset", offset);
        response.put("count", events.size());

        response.set("events", JSON.valueToTree(events));

        sendJson(exchange, 200, response);
    }

    private void handleWorkers(HttpExchange exchange) throws IOException {
        try {
            if (!"GET".equals(exchange.getRequestMethod())) {
                sendMethodNotAllowed(exchange, "GET");
                return;
            }

            List<WorkerHeartbeat> workers =
                heartbeatRepository.findAll(Duration.ofSeconds(30));

            ObjectNode response = JSON.createObjectNode();
            response.put("count", workers.size());
            response.set("workers", JSON.valueToTree(workers));

            sendJson(exchange, 200, response);

        } catch (Exception exception) {
            sendError(exchange, 500, "WORKER_STATUS_UNAVAILABLE",
                "Could not retrieve worker status");
        }
    }

    private static JsonNode readJson(HttpExchange exchange) throws IOException {
        String contentType = exchange.getRequestHeaders()
            .getFirst("Content-Type");

        if (contentType == null ||
            !contentType.split(";", 2)[0].trim()
                .equalsIgnoreCase("application/json")) {
            throw new IllegalArgumentException(
                "Content-Type must be application/json"
            );
        }

        byte[] bytes;

        try (InputStream input = exchange.getRequestBody()) {
            bytes = input.readNBytes(MAX_BODY_BYTES + 1);
        }

        if (bytes.length == 0) {
            throw new IllegalArgumentException("Request body is required");
        }

        if (bytes.length > MAX_BODY_BYTES) {
            throw new IllegalArgumentException("Request body is too large");
        }

        JsonNode body;

        try {
            body = JSON.readTree(bytes);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Request body contains invalid JSON");
        }

        if (body == null || !body.isObject()) {
            throw new IllegalArgumentException(
                "Request body must be a JSON object"
            );
        }

        return body;
    }
    private static String requiredText(
        JsonNode body,
        String field,
        int maxLength
    ) {
        JsonNode value = body.get(field);

        if (value == null || !value.isTextual()) {
            throw new IllegalArgumentException(
                field + " must be a string"
            );
        }

        String text = value.textValue().trim();

        if (text.isBlank()) {
            throw new IllegalArgumentException(
                field + " must not be blank"
            );
        }

        if (text.length() > maxLength) {
            throw new IllegalArgumentException(
                field + " exceeds " + maxLength + " characters"
            );
        }

        return text;
    }

    private static int parseInteger(String value, String field) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                field + " must be a valid integer"
            );
        }
    }

    private static Map<String, String> parseQuery(String rawQuery) {
        if (rawQuery == null || rawQuery.isBlank()) {
            return Map.of();
        }

        java.util.HashMap<String, String> values =
            new java.util.HashMap<>();

        for (String part : rawQuery.split("&")) {
            String[] pair = part.split("=", 2);

            String key = URLDecoder.decode(
                pair[0], StandardCharsets.UTF_8
            );

            String value = pair.length > 1
                ? URLDecoder.decode(pair[1], StandardCharsets.UTF_8)
                : "";

            if (values.putIfAbsent(key, value) != null) {
                throw new IllegalArgumentException(
                    "Duplicate query parameter: " + key
                );
            }
        }

        return values;
    }

    private static String safeMessage(Exception exception) {
        String message = exception.getMessage();
        return message == null ? "Invalid request" : message;
    }

    private static void sendMethodNotAllowed(
        HttpExchange exchange,
        String allowed
    ) throws IOException {
        exchange.getResponseHeaders().set("Allow", allowed);
        sendError(exchange, 405, "METHOD_NOT_ALLOWED",
            "Allowed method: " + allowed);
    }

    private static void sendError(
        HttpExchange exchange,
        int status,
        String code,
        String message
    ) throws IOException {
        ObjectNode response = JSON.createObjectNode();
        response.put("error", code);
        response.put("message", message);
        sendJson(exchange, status, response);
    }

    private static void sendJson(
        HttpExchange exchange,
        int status,
        Object body
    ) throws IOException {
        byte[] bytes = JSON.writeValueAsBytes(body);

        exchange.getResponseHeaders().set(
            "Content-Type", "application/json; charset=utf-8"
        );
        exchange.getResponseHeaders().set(
            "X-Content-Type-Options", "nosniff"
        );
        exchange.getResponseHeaders().set(
            "Cache-Control", "no-store"
        );

        exchange.sendResponseHeaders(status, bytes.length);

        try (var output = exchange.getResponseBody()) {
            output.write(bytes);
        }
    }

    @Override
    public void close() {
        server.stop(1);
        executor.shutdownNow();
    }

    public static void main(String[] args) throws Exception {
        String host = System.getenv().getOrDefault(
            "LYAPUNOV_API_HOST", "127.0.0.1"
        );

        int port = Integer.parseInt(
            System.getenv().getOrDefault("LYAPUNOV_API_PORT", "8081")
        );

        String jdbcUrl = requiredEnvironment("LYAPUNOV_DB_URL");
        String dbUser = requiredEnvironment("LYAPUNOV_DB_USER");
        String dbPassword = requiredEnvironment("LYAPUNOV_DB_PASSWORD");

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(dbUser);
        config.setPassword(dbPassword);
        config.setMaximumPoolSize(5);
        config.setPoolName("lyapunov-api-pool");
        config.setConnectionTimeout(5000);

        HikariDataSource dataSource = new HikariDataSource(config);

        ReliabilityApiServer api = null;

        try {
            api = new ReliabilityApiServer(host, port, dataSource);
            api.start();

            System.out.println(
                "LYAPUNOV REST API listening on http://" +
                host + ":" + api.port()
            );

            System.out.println(
                "Unauthenticated local development API. " +
                "Do not expose it to public networks."
            );

            ReliabilityApiServer runningApi = api;

            Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    runningApi.close();
                    dataSource.close();
                })
            );

            new CountDownLatch(1).await();

        } catch (Exception exception) {
            if (api != null) {
                api.close();
            }
            dataSource.close();
            throw exception;
        }
    }

    private static String requiredEnvironment(String name) {
        String value = System.getenv(name);

        if (value == null || value.isBlank()) {
            throw new IllegalStateException(
                "Required environment variable is missing: " + name
            );
        }

        return value;
    }
}