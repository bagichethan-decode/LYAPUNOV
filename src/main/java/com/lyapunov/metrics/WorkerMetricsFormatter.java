package com.lyapunov.metrics;

import com.lyapunov.repository.WorkerHeartbeat;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Renders the persisted worker registry as Prometheus text exposition format 0.0.4.
 */
public final class WorkerMetricsFormatter {
    public String format(List<WorkerHeartbeat> workers) {
        Objects.requireNonNull(workers, "workers must not be null");
        for (WorkerHeartbeat worker : workers) {
            Objects.requireNonNull(worker, "workers must not contain null");
            if (worker.publishedCount() < 0 || worker.failedCount() < 0 || worker.staleClaimCount() < 0) {
                throw new IllegalArgumentException("worker counters must not be negative");
            }
        }

        StringBuilder out = new StringBuilder(512);
        appendFamily(out, workers, "lyapunov_worker_up",
                "Whether the worker is running and its heartbeat is fresh.", "gauge",
                worker -> "RUNNING".equals(worker.status()) && !worker.stale() ? "1" : "0");
        appendFamily(out, workers, "lyapunov_worker_stale",
                "Whether a RUNNING worker heartbeat is stale.", "gauge",
                worker -> worker.stale() ? "1" : "0");
        appendFamily(out, workers, "lyapunov_worker_published_total",
                "Cumulative events reported as published by the worker.", "counter",
                worker -> Long.toString(worker.publishedCount()));
        appendFamily(out, workers, "lyapunov_worker_failed_total",
                "Cumulative publish failures reported by the worker.", "counter",
                worker -> Long.toString(worker.failedCount()));
        appendFamily(out, workers, "lyapunov_worker_stale_claims_total",
                "Cumulative stale claim observations reported by the worker.", "counter",
                worker -> Long.toString(worker.staleClaimCount()));
        appendFamily(out, workers, "lyapunov_worker_last_seen_timestamp_seconds",
                "Unix timestamp of the last worker heartbeat.", "gauge",
                worker -> seconds(worker.lastSeenAt()));
        appendFamily(out, workers, "lyapunov_worker_last_successful_publish_timestamp_seconds",
                "Unix timestamp of the last successful publish, or zero if none.", "gauge",
                worker -> worker.lastSuccessfulPublishAt() == null ? "0" : seconds(worker.lastSuccessfulPublishAt()));
        appendFamily(out, workers, "lyapunov_worker_started_timestamp_seconds",
                "Unix timestamp when the current worker instance started.", "gauge",
                worker -> seconds(worker.startedAt()));
        return out.toString();
    }

    private static void appendFamily(StringBuilder out, List<WorkerHeartbeat> workers,
                                     String name, String help, String type,
                                     java.util.function.Function<WorkerHeartbeat, String> value) {
        metricHeader(out, name, help, type);
        for (WorkerHeartbeat worker : workers) {
            sample(out, name, labels(worker), value.apply(worker));
        }
    }

    private static String labels(WorkerHeartbeat worker) {
        return "{worker_id=\"" + escape(worker.workerId()) + "\",instance_id=\""
                + escape(worker.instanceId()) + "\",status=\"" + escape(worker.status()) + "\"}";
    }

    private static String escape(String value) {
        if (value == null) return "";
        return value.replace("\\", "\\\\").replace("\n", "\\n").replace("\"", "\\\"");
    }

    private static String seconds(java.time.Instant instant) {
        return String.format(Locale.ROOT, "%.3f", instant.toEpochMilli() / 1000.0d);
    }

    private static void metricHeader(StringBuilder out, String name, String help, String type) {
        out.append("# HELP ").append(name).append(' ').append(help).append('\n');
        out.append("# TYPE ").append(name).append(' ').append(type).append('\n');
    }

    private static void sample(StringBuilder out, String name, String labels, String value) {
        out.append(name).append(labels).append(' ').append(value).append('\n');
    }
}
