package com.lyapunov.cli;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public final class ApiHealthcheckCli {

    private ApiHealthcheckCli() {
    }

    public static void main(String[] args) {
        String port = System.getenv().getOrDefault(
            "LYAPUNOV_API_PORT",
            "8080"
        );

        try {
            URI endpoint = URI.create(
                "http://127.0.0.1:" + port + "/api/v1/health/ready"
            );

            HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .build();

            HttpRequest request = HttpRequest.newBuilder()
                .uri(endpoint)
                .timeout(Duration.ofSeconds(3))
                .GET()
                .build();

            HttpResponse<Void> response = client.send(
                request,
                HttpResponse.BodyHandlers.discarding()
            );

            if (response.statusCode() != 200) {
                System.err.println(
                    "LYAPUNOV API readiness check failed: HTTP "
                        + response.statusCode()
                );
                System.exit(1);
            }

            System.out.println("LYAPUNOV API readiness check passed.");

        } catch (Exception failure) {
            System.err.println(
                "LYAPUNOV API readiness check failed."
            );
            System.exit(1);
        }
    }
}