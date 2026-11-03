package com.lyapunov.cli;

import com.lyapunov.repository.WorkerHeartbeat;
import com.lyapunov.repository.WorkerHeartbeatRepository;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.time.Duration;
import java.util.List;

public final class WorkerHeartbeatCli {
    private WorkerHeartbeatCli() { }

    public static void main(String[] args) throws Exception {
        if (args == null || args.length == 0) {
            usage();
            System.exit(2);
        }
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(requiredEnv("LYAPUNOV_JDBC_URL"));
        config.setUsername(requiredEnv("LYAPUNOV_DB_USER"));
        config.setPassword(requiredEnv("LYAPUNOV_DB_PASSWORD"));
        config.setMaximumPoolSize(2);
        try (HikariDataSource dataSource = new HikariDataSource(config)) {
            WorkerHeartbeatRepository repository = new WorkerHeartbeatRepository(dataSource);
            switch (args[0].toLowerCase()) {
                case "heartbeat" -> {
                    if (args.length < 3 || args.length > 7) { usage(); System.exit(2); }
                    repository.heartbeat(args[1], args[2],
                            args.length > 3 ? nonNegativeLong(args[3], "published-count") : 0,
                            args.length > 4 ? nonNegativeLong(args[4], "failed-count") : 0,
                            args.length > 5 ? nonNegativeLong(args[5], "stale-claim-count") : 0,
                            args.length > 6 ? args[6] : null);
                    System.out.println("Heartbeat recorded: worker=" + args[1] + ", instance=" + args[2]);
                }
                case "stop" -> {
                    if (args.length != 3) { usage(); System.exit(2); }
                    System.out.println(repository.markStopped(args[1], args[2])
                            ? "Worker marked STOPPED." : "No matching active worker instance found.");
                }
                case "list" -> {
                    if (args.length > 2) { usage(); System.exit(2); }
                    long staleSeconds = args.length == 2 ? positiveLong(args[1], "stale-seconds") : 30;
                    List<WorkerHeartbeat> workers = repository.findAll(Duration.ofSeconds(staleSeconds));
                    if (workers.isEmpty()) {
                        System.out.println("No registered workers.");
                    }
                    for (WorkerHeartbeat worker : workers) {
                        System.out.printf("%s | %s | %s | lastSeen=%s | published=%d | failed=%d | staleClaims=%d | stale=%s%n",
                                worker.workerId(), worker.instanceId(), worker.status(), worker.lastSeenAt(),
                                worker.publishedCount(), worker.failedCount(), worker.staleClaimCount(), worker.stale());
                        if (worker.lastError() != null && !worker.lastError().isBlank()) {
                            System.out.println("  lastError=" + worker.lastError());
                        }
                    }
                    boolean unhealthy = workers.stream().anyMatch(w -> w.stale() || "STOPPED".equals(w.status()));
                    if (unhealthy) System.exit(1);
                }
                default -> { usage(); System.exit(2); }
            }
        }
    }

    private static String requiredEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) throw new IllegalStateException("Missing environment variable: " + name);
        return value;
    }

    private static long nonNegativeLong(String value, String name) {
        try {
            long parsed = Long.parseLong(value);
            if (parsed < 0) throw new NumberFormatException();
            return parsed;
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(name + " must be a non-negative integer");
        }
    }

    private static long positiveLong(String value, String name) {
        long parsed = nonNegativeLong(value, name);
        if (parsed == 0) throw new IllegalArgumentException(name + " must be positive");
        return parsed;
    }

    private static void usage() {
        System.err.println("""
                Usage:
                  WorkerHeartbeatCli heartbeat <worker-id> <instance-id> [published-count] [failed-count] [stale-claim-count] [last-error]
                  WorkerHeartbeatCli stop <worker-id> <instance-id>
                  WorkerHeartbeatCli list [stale-seconds]
                Environment: LYAPUNOV_JDBC_URL, LYAPUNOV_DB_USER, LYAPUNOV_DB_PASSWORD
                """);
    }
}
