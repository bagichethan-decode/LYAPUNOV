package com.lyapunov.benchmark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Reproducible PostgreSQL persistence benchmark for LYAPUNOV.
 * Uses an isolated benchmark table; it does not alter outbox or inbox data.
 */
public final class InboxLoadBenchmark {
    private static final String CREATE_TABLE = """
        CREATE TABLE IF NOT EXISTS lyapunov_benchmark_events (
            run_id UUID NOT NULL,
            event_id UUID NOT NULL,
            consumer_name VARCHAR(120) NOT NULL,
            processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (run_id, event_id, consumer_name)
        )
        """;
    private static final String INSERT = """
        INSERT INTO lyapunov_benchmark_events(run_id, event_id, consumer_name)
        VALUES (?, ?, ?)
        ON CONFLICT (run_id, event_id, consumer_name) DO NOTHING
        """;

    private InboxLoadBenchmark() {}

    public static void main(String[] args) throws Exception {
        int events = integerArg(args, "--events", 10000);
        int concurrency = integerArg(args, "--concurrency", 8);
        double duplicateRate = doubleArg(args, "--duplicate-rate", 0.10);
        double failureRate = doubleArg(args, "--failure-rate", 0.02);
        String jdbcUrl = requiredEnv("LYAPUNOV_JDBC_URL");
        String user = System.getenv().getOrDefault("LYAPUNOV_DB_USER", "postgres");
        String password = System.getenv().getOrDefault("LYAPUNOV_DB_PASSWORD", "postgres");
        String consumer = "load-benchmark";

        if (events < 1 || events > 1_000_000 || concurrency < 1 || concurrency > 128
                || duplicateRate < 0 || duplicateRate > 1 || failureRate < 0 || failureRate > 0.5) {
            throw new IllegalArgumentException("Invalid benchmark parameters.");
        }

        UUID runId = UUID.randomUUID();
        try (Connection setup = DriverManager.getConnection(jdbcUrl, user, password);
             var statement = setup.createStatement()) {
            statement.execute(CREATE_TABLE);
        }

        int uniqueCount = Math.max(1, (int) Math.ceil(events * (1.0 - duplicateRate)));
        List<UUID> ids = new ArrayList<>(events);
        for (int i = 0; i < events; i++) {
            ids.add(i < uniqueCount ? UUID.randomUUID() : ids.get(i % uniqueCount));
        }
        Collections.shuffle(ids);
        AtomicInteger cursor = new AtomicInteger();
        LongAdder inserted = new LongAdder();
        LongAdder duplicates = new LongAdder();
        LongAdder injectedFailures = new LongAdder();
        LongAdder errors = new LongAdder();
        List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        CountDownLatch ready = new CountDownLatch(concurrency);
        CountDownLatch start = new CountDownLatch(1);
        Instant startedAt = Instant.now();
        long wallStart = System.nanoTime();

        List<Future<?>> tasks = new ArrayList<>();
        for (int worker = 0; worker < concurrency; worker++) {
            tasks.add(pool.submit(() -> {
                try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password);
                     PreparedStatement insert = connection.prepareStatement(INSERT)) {
                    connection.setAutoCommit(false);
                    ready.countDown();
                    start.await();
                    while (true) {
                        int index = cursor.getAndIncrement();
                        if (index >= events) break;
                        if (Math.random() < failureRate) {
                            injectedFailures.increment();
                            continue;
                        }
                        long t0 = System.nanoTime();
                        try {
                            insert.setObject(1, runId);
                            insert.setObject(2, ids.get(index));
                            insert.setString(3, consumer);
                            int changed = insert.executeUpdate();
                            connection.commit();
                            if (changed == 1) inserted.increment(); else duplicates.increment();
                        } catch (SQLException e) {
                            errors.increment();
                            try { connection.rollback(); } catch (SQLException ignored) { }
                        } finally {
                            latencies.add(System.nanoTime() - t0);
                        }
                    }
                } catch (Exception e) {
                    errors.increment();
                    throw new RuntimeException(e);
                }
            }));
        }

        ready.await();
        start.countDown();
        for (Future<?> task : tasks) task.get();
        pool.shutdown();
        long elapsedNanos = System.nanoTime() - wallStart;
        double seconds = elapsedNanos / 1_000_000_000.0;
        List<Long> sorted = new ArrayList<>(latencies);
        Collections.sort(sorted);

        System.out.println("LYAPUNOV_LOAD_BENCHMARK");
        System.out.println("run_id=" + runId);
        System.out.println("started_at=" + startedAt);
        System.out.println("requested_events=" + events);
        System.out.println("concurrency=" + concurrency);
        System.out.println("inserted=" + inserted.sum());
        System.out.println("duplicates=" + duplicates.sum());
        System.out.println("injected_failures=" + injectedFailures.sum());
        System.out.println("sql_or_worker_errors=" + errors.sum());
        System.out.printf(java.util.Locale.ROOT, "elapsed_seconds=%.3f%n", seconds);
        System.out.printf(java.util.Locale.ROOT, "throughput_ops_per_sec=%.2f%n", events / seconds);
        System.out.printf(java.util.Locale.ROOT, "latency_p50_ms=%.3f%n", percentile(sorted, 0.50) / 1_000_000.0);
        System.out.printf(java.util.Locale.ROOT, "latency_p95_ms=%.3f%n", percentile(sorted, 0.95) / 1_000_000.0);
        System.out.printf(java.util.Locale.ROOT, "latency_p99_ms=%.3f%n", percentile(sorted, 0.99) / 1_000_000.0);
        System.out.println("note=Injected failures are deliberate skipped attempts; rerun with failure-rate 0 for baseline.");
        System.out.println("cleanup_sql=DELETE FROM lyapunov_benchmark_events WHERE run_id = '" + runId + "';");
    }

    static long percentile(List<Long> sorted, double quantile) {
        if (sorted == null || sorted.isEmpty()) return 0L;
        if (quantile < 0 || quantile > 1) throw new IllegalArgumentException("quantile must be in [0,1]");
        int index = (int) Math.ceil(quantile * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }

    private static int integerArg(String[] args, String key, int fallback) {
        return (int) doubleArg(args, key, fallback);
    }
    private static double doubleArg(String[] args, String key, double fallback) {
        for (int i = 0; i < args.length - 1; i++) if (args[i].equals(key)) return Double.parseDouble(args[i + 1]);
        return fallback;
    }
    private static String requiredEnv(String key) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) throw new IllegalStateException("Set environment variable " + key);
        return value;
    }
}
