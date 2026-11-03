package com.lyapunov.metrics;

import com.lyapunov.repository.WorkerHeartbeat;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerMetricsFormatterTest {
    private final WorkerMetricsFormatter formatter = new WorkerMetricsFormatter();

    @Test
    void exportsWorkerStateAndCountersInPrometheusFormat() {
        WorkerHeartbeat worker = worker("relay-a", "instance-1", "RUNNING", false, 12, 2, 1);
        String metrics = formatter.format(List.of(worker));

        assertTrue(metrics.contains("# TYPE lyapunov_worker_published_total counter"));
        assertTrue(metrics.contains("lyapunov_worker_up{worker_id=\"relay-a\",instance_id=\"instance-1\",status=\"RUNNING\"} 1"));
        assertTrue(metrics.contains("lyapunov_worker_published_total{worker_id=\"relay-a\",instance_id=\"instance-1\",status=\"RUNNING\"} 12"));
        assertTrue(metrics.contains("lyapunov_worker_failed_total{worker_id=\"relay-a\",instance_id=\"instance-1\",status=\"RUNNING\"} 2"));
        assertTrue(metrics.endsWith("\n"));
    }

    @Test
    void staleRunningWorkerIsExportedAsDownAndStale() {
        String metrics = formatter.format(List.of(worker("relay-a", "instance-1", "RUNNING", true, 0, 0, 0)));
        assertTrue(metrics.contains("lyapunov_worker_up{worker_id=\"relay-a\",instance_id=\"instance-1\",status=\"RUNNING\"} 0"));
        assertTrue(metrics.contains("lyapunov_worker_stale{worker_id=\"relay-a\",instance_id=\"instance-1\",status=\"RUNNING\"} 1"));
    }

    @Test
    void stoppedWorkerIsExportedAsDown() {
        String metrics = formatter.format(List.of(worker("relay-a", "instance-1", "STOPPED", false, 0, 0, 0)));
        assertTrue(metrics.contains("lyapunov_worker_up{worker_id=\"relay-a\",instance_id=\"instance-1\",status=\"STOPPED\"} 0"));
    }

    @Test
    void escapesPrometheusLabelValues() {
        String metrics = formatter.format(List.of(worker("relay\"a", "line\\one\nnext", "RUNNING", false, 0, 0, 0)));
        assertTrue(metrics.contains("worker_id=\"relay\\\"a\""));
        assertTrue(metrics.contains("instance_id=\"line\\\\one\\nnext\""));
    }

    @Test
    void exportsZeroWhenNoSuccessfulPublishHasBeenReported() {
        String metrics = formatter.format(List.of(worker("relay-a", "instance-1", "RUNNING", false, 0, 0, 0)));
        assertTrue(metrics.contains("lyapunov_worker_last_successful_publish_timestamp_seconds{worker_id=\"relay-a\",instance_id=\"instance-1\",status=\"RUNNING\"} 0"));
    }

    @Test
    void rejectsNullCollectionAndNegativeCounters() {
        assertThrows(NullPointerException.class, () -> formatter.format(null));
        WorkerHeartbeat invalid = new WorkerHeartbeat("relay-a", "instance-1", "RUNNING",
                Instant.EPOCH, Instant.EPOCH, null, -1, 0, 0, null, false);
        assertThrows(IllegalArgumentException.class, () -> formatter.format(List.of(invalid)));
    }

    @Test
    void emptyRegistryStillEmitsMetricMetadata() {
        String metrics = formatter.format(List.of());
        assertTrue(metrics.contains("# HELP lyapunov_worker_up"));
        assertEquals(8, metrics.lines().filter(line -> line.startsWith("# TYPE ")).count());
    }

    private static WorkerHeartbeat worker(String id, String instance, String status, boolean stale,
                                           long published, long failed, long staleClaims) {
        return new WorkerHeartbeat(id, instance, status,
                Instant.parse("2026-11-03T10:00:00Z"),
                Instant.parse("2026-11-03T10:01:00Z"),
                published == 0 ? null : Instant.parse("2026-11-03T10:00:30Z"),
                published, failed, staleClaims, null, stale);
    }
}
