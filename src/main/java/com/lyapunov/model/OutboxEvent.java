package com.lyapunov.model;

import java.time.Instant;
import java.util.UUID;

public record OutboxEvent(
    UUID id,
    String aggregateType,
    String aggregateId,
    long sequenceNumber,
    String eventType,
    String payload,
    EventStatus status,
    int retryCount,
    String claimedBy,
    Instant leaseExpiresAt,
    Instant createdAt,
    Instant updatedAt
) {
    public enum EventStatus {
        PENDING,
        CLAIMED,
        PUBLISHED,
        FAILED
    }
}
