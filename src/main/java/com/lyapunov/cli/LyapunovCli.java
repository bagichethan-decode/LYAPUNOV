package com.lyapunov.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.Callable;

@Command(
    name = "lyapunov",
    description = "LYAPUNOV distributed event reliability CLI",
    mixinStandardHelpOptions = true,
    version = "LYAPUNOV CLI 1.0.0",
    subcommands = {
        LyapunovCli.StatusCommand.class,
        LyapunovCli.EventsCommand.class
    }
)
public class LyapunovCli implements Runnable {

    @Option(
        names = "--api-url",
        description = "LYAPUNOV API base URL",
        defaultValue = "${env:LYAPUNOV_API_URL:-http://127.0.0.1:8081}"
    )
    private String apiUrl;

    private final ApiClient client = new ApiClient();

    public static void main(String[] args) {
        int exitCode = new CommandLine(new LyapunovCli()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public void run() {
        CommandLine.usage(this, System.out);
    }

    private ApiClient client() {
        return client;
    }

    private String apiUrl() {
        return apiUrl;
    }

    @Command(name = "status", description = "Check LYAPUNOV API health")
    static class StatusCommand implements Callable<Integer> {

        @ParentCommand
        private LyapunovCli parent;

        @Override
        public Integer call() {
            return parent.client().get(parent.apiUrl(), "/health");
        }
    }

    @Command(name = "events", description = "Manage reliability events",
        subcommands = {
            DeadEventsCommand.class,
            CreateEventCommand.class
        })
    static class EventsCommand implements Runnable {

        @ParentCommand
        private LyapunovCli parent;

        private ApiClient client() {
            return parent.client();
        }

        private String apiUrl() {
            return parent.apiUrl();
        }

        @Override
        public void run() {
            CommandLine.usage(this, System.out);
        }
    }

    @Command(name = "dead", description = "List dead-letter events")
    static class DeadEventsCommand implements Callable<Integer> {

        @ParentCommand
        private EventsCommand parent;

        @Option(names = "--limit", defaultValue = "50",
            description = "Maximum number of events (1-500)")
        private int limit;

        @Option(names = "--offset", defaultValue = "0",
            description = "Pagination offset")
        private int offset;

        @Override
        public Integer call() {
            if (limit < 1 || limit > 500 || offset < 0) {
                System.err.println("Invalid pagination: limit must be 1-500 and offset must be non-negative.");
                return 2;
            }

            String path = "/api/v1/events/dead?limit=" + limit + "&offset=" + offset;
            return parent.client().get(parent.apiUrl(), path);
        }
    }

    @Command(name = "create", description = "Create a reliability event")
    static class CreateEventCommand implements Callable<Integer> {

        @ParentCommand
        private EventsCommand parent;

        @Option(names = "--aggregate-type", required = true)
        private String aggregateType;

        @Option(names = "--aggregate-id", required = true)
        private String aggregateId;

        @Option(names = "--event-type", required = true)
        private String eventType;

        @Option(names = "--payload", required = true,
            description = "JSON payload for the event")
        private String payload;

        @Override
        public Integer call() {
            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode payloadNode = mapper.readTree(payload);

                if (payloadNode == null || payloadNode.isNull()) {
                    System.err.println("Payload must be valid, non-null JSON.");
                    return 2;
                }

                var body = mapper.createObjectNode();
                body.put("aggregateType", aggregateType);
                body.put("aggregateId", aggregateId);
                body.put("eventType", eventType);
                body.set("payload", payloadNode);

                return parent.client().post(
                    parent.apiUrl(),
                    "/api/v1/events",
                    mapper.writeValueAsString(body)
                );
            } catch (Exception exception) {
                System.err.println("Invalid JSON payload: " + exception.getMessage());
                return 2;
            }
        }
    }

    static class ApiClient {

        private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

        private final ObjectMapper mapper = new ObjectMapper();

        int get(String baseUrl, String path) {
            return send(baseUrl, path, null);
        }

        int post(String baseUrl, String path, String body) {
            return send(baseUrl, path, body);
        }

        private int send(String baseUrl, String path, String body) {
            try {
                String normalizedBase = baseUrl.replaceAll("/+$", "");
                URI uri = URI.create(normalizedBase + path);

                HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .uri(uri)
                    .timeout(Duration.ofSeconds(15))
                    .header("Accept", "application/json");

                if (body == null) {
                    builder.GET();
                } else {
                    builder.header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body));
                }

                HttpResponse<String> response = httpClient.send(
                    builder.build(),
                    HttpResponse.BodyHandlers.ofString()
                );

                System.out.println("HTTP " + response.statusCode());

                try {
                    JsonNode json = mapper.readTree(response.body());
                    System.out.println(mapper.writerWithDefaultPrettyPrinter()
                        .writeValueAsString(json));
                } catch (Exception ignored) {
                    System.out.println(response.body());
                }

                return response.statusCode() >= 200
                    && response.statusCode() < 300 ? 0 : 1;

            } catch (Exception exception) {
                System.err.println("API request failed: " + exception.getMessage());
                return 1;
            }
        }
    }
}
