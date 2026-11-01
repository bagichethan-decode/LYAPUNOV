package com.lyapunov.worker;

import com.lyapunov.model.OutboxEvent;
import com.lyapunov.repository.OutboxRepository;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

public final class JdbcOutboxStore implements OutboxStore {
    private final OutboxRepository repository;

    public JdbcOutboxStore(OutboxRepository repository) {
        if (repository == null) throw new IllegalArgumentException("repository must not be null");
        this.repository = repository;
    }

    @Override
    public List<OutboxEvent> claim(String workerId, int batchSize, Duration leaseDuration) throws Exception {
        return repository.claimPendingEvents(workerId, batchSize, leaseDuration);
    }

    @Override
    public boolean published(UUID eventId, String workerId, UUID claimToken) throws Exception {
        return repository.markPublished(eventId, workerId, claimToken);
    }

    @Override
    public boolean failed(UUID eventId, String workerId, UUID claimToken, int maxRetries) throws Exception {
        return repository.recordFailure(eventId, workerId, claimToken, maxRetries);
    }
}