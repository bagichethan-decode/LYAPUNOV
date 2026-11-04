package com.lyapunov.inbox;

import java.util.Objects;
import java.util.UUID;

/** Stable deduplication identity for one consumer and one event. */
public record InboxKey(String consumerName, UUID eventId) {
    public InboxKey {
        if (consumerName == null || consumerName.isBlank()) throw new IllegalArgumentException("consumerName must not be blank");
        Objects.requireNonNull(eventId, "eventId must not be null");
    }
}
