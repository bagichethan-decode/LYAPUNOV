package com.lyapunov.worker;

import com.lyapunov.model.OutboxEvent;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

public interface OutboxStore {
    List<OutboxEvent> claim(String workerId, int batchSize, Duration leaseDuration) throws Exception;
    boolean published(UUID eventId, String workerId, UUID claimToken) throws Exception;
    boolean failed(UUID eventId, String workerId, UUID claimToken, int maxRetries) throws Exception;
}