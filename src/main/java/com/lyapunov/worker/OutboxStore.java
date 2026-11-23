package com.lyapunov.worker;

import com.lyapunov.model.OutboxEvent;
import com.lyapunov.failure.FailureFingerprint;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

public interface OutboxStore {
    List<OutboxEvent> claim(String workerId, int batchSize, Duration leaseDuration) throws Exception;
    boolean published(UUID eventId, String workerId, UUID claimToken) throws Exception;
    boolean failed(UUID eventId, String workerId, UUID claimToken, int maxRetries) throws Exception;

    default boolean failed(UUID eventId, String workerId, UUID claimToken, int maxRetries,
                           Duration baseRetryDelay, Duration maxRetryDelay) throws Exception {
        return failed(eventId, workerId, claimToken, maxRetries);
    }

    /**
     * Persists sanitized failure intelligence independently of delivery state.
     * Implementations may ignore this capability when persistence is unavailable.
     */
    default void recordFailureFingerprint(
        OutboxEvent event,
        String workerId,
        FailureFingerprint fingerprint
    ) throws Exception {
        // Optional capability for stores without failure-history persistence.
    }
}