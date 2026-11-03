package com.lyapunov.repository;

import java.time.Instant;

/**
 * Point-in-time aggregate view of the transactional outbox.
 * This describes persisted queue state, not Kafka or worker process liveness.
 */
public record OutboxQueueStats(
        long pending,
        long claimed,
        long published,
        long dead,
        long skipped,
        long totalRetries,
        long retriesDue,
        long expiredClaims,
        Instant oldestPendingAt,
        Instant oldestClaimedAt,
        Instant snapshotAt
) {
    public long totalEvents() {
        return pending + claimed + published + dead + skipped;
    }

    public String queueHealth() {
        if (dead > 0 || expiredClaims > 0) return "UNHEALTHY";
        if (pending > 0 || claimed > 0) return "DEGRADED";
        return "HEALTHY";
    }
}