package com.lyapunov.cli;

import com.lyapunov.metrics.WorkerMetricsFormatter;
import com.lyapunov.repository.WorkerHeartbeat;
import com.lyapunov.repository.WorkerHeartbeatRepository;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.List;

public final class WorkerMetricsCli {
    private WorkerMetricsCli() { }

    public static void main(String[] args) throws Exception {
        if (args == null || args.length > 2) {
            usage();
            System.exit(2);
        }
        long staleSeconds = args.length >= 1 ? positiveLong(args[0]) : 30;
        Path output = args.length == 2 ? Path.of(args[1]).toAbsolutePath() : null;

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(requiredEnv("LYAPUNOV_JDBC_URL"));
        config.setUsername(requiredEnv("LYAPUNOV_DB_USER"));
        config.setPassword(requiredEnv("LYAPUNOV_DB_PASSWORD"));
        config.setMaximumPoolSize(2);

        String exposition;
        try (HikariDataSource dataSource = new HikariDataSource(config)) {
            List<WorkerHeartbeat> workers = new WorkerHeartbeatRepository(dataSource)
                    .findAll(Duration.ofSeconds(staleSeconds));
            exposition = new WorkerMetricsFormatter().format(workers);
        }

        if (output == null) {
            System.out.print(exposition);
        } else {
            writeAtomically(output, exposition);
            System.out.println("Prometheus metrics written to " + output);
        }
    }

    private static void writeAtomically(Path output, String content) throws Exception {
        Path parent = output.getParent();
        if (parent != null) Files.createDirectories(parent);
        Path directory = parent == null ? Path.of(".").toAbsolutePath() : parent;
        Path temporary = Files.createTempFile(directory, output.getFileName().toString(), ".tmp");
        try {
            Files.writeString(temporary, content, StandardCharsets.UTF_8);
            try {
                Files.move(temporary, output, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException ex) {
                Files.move(temporary, output, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private static String requiredEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) throw new IllegalStateException("Missing environment variable: " + name);
        return value;
    }

    private static long positiveLong(String value) {
        try {
            long parsed = Long.parseLong(value);
            if (parsed <= 0) throw new NumberFormatException();
            return parsed;
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("stale-seconds must be a positive integer");
        }
    }

    private static void usage() {
        System.err.println("""
                Usage:
                  WorkerMetricsCli [stale-seconds] [output-file]
                If output-file is omitted, Prometheus text exposition is written to stdout.
                Environment: LYAPUNOV_JDBC_URL, LYAPUNOV_DB_USER, LYAPUNOV_DB_PASSWORD
                """);
    }
}
