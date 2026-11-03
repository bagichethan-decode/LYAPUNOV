package com.lyapunov.repository;

import java.time.Instant;

public record WorkerHeartbeat(
        String workerId,
        String instanceId,
        String status,
        Instant startedAt,
        Instant lastSeenAt,
        Instant lastSuccessfulPublishAt,
        long publishedCount,
        long failedCount,
        long staleClaimCount,
        String lastError,
        boolean stale
) { }
