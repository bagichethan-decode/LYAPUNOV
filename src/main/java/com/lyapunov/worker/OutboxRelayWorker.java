package com.lyapunov.worker;

import com.lyapunov.model.OutboxEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

public final class OutboxRelayWorker implements Runnable, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(OutboxRelayWorker.class);

    private final OutboxStore store;
    private final EventPublisher publisher;
    private final String workerId;
    private final int batchSize;
    private final int maxRetries;
    private final Duration leaseDuration;
    private final Duration pollInterval;
    private final Duration baseRetryDelay;
    private final Duration maxRetryDelay;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final LongAdder publishedCount = new LongAdder();
    private final LongAdder failedCount = new LongAdder();
    private final LongAdder staleClaimCount = new LongAdder();

    public OutboxRelayWorker(OutboxStore store, EventPublisher publisher, String workerId,
                             int batchSize, int maxRetries, Duration leaseDuration,
                             Duration pollInterval) {
        this(store, publisher, workerId, batchSize, maxRetries, leaseDuration, pollInterval,
            Duration.ofSeconds(1), Duration.ofMinutes(5));
    }

    public OutboxRelayWorker(OutboxStore store, EventPublisher publisher, String workerId,
                             int batchSize, int maxRetries, Duration leaseDuration,
                             Duration pollInterval, Duration baseRetryDelay, Duration maxRetryDelay) {
        if (store == null || publisher == null) throw new IllegalArgumentException("store and publisher are required");
        if (workerId == null || workerId.isBlank()) throw new IllegalArgumentException("workerId must not be blank");
        if (batchSize <= 0 || maxRetries <= 0) throw new IllegalArgumentException("batchSize and maxRetries must be positive");
        if (leaseDuration == null || leaseDuration.isNegative() || leaseDuration.isZero()) throw new IllegalArgumentException("leaseDuration must be positive");
        if (pollInterval == null || pollInterval.isNegative() || pollInterval.isZero()) throw new IllegalArgumentException("pollInterval must be positive");
        if (baseRetryDelay == null || baseRetryDelay.isNegative()
            || maxRetryDelay == null || maxRetryDelay.isNegative()
            || maxRetryDelay.compareTo(baseRetryDelay) < 0) {
            throw new IllegalArgumentException("retry delays must be non-negative and maxRetryDelay >= baseRetryDelay");
        }
        this.store = store;
        this.publisher = publisher;
        this.workerId = workerId;
        this.batchSize = batchSize;
        this.maxRetries = maxRetries;
        this.leaseDuration = leaseDuration;
        this.pollInterval = pollInterval;
        this.baseRetryDelay = baseRetryDelay;
        this.maxRetryDelay = maxRetryDelay;
    }

    public int runOneCycle() throws Exception {
        List<OutboxEvent> events = store.claim(workerId, batchSize, leaseDuration);
        int processed = 0;
        for (OutboxEvent event : events) {
            try {
                publisher.publish(event);
                if (store.published(event.id(), workerId, event.claimToken())) {
                    publishedCount.increment();
                } else {
                    staleClaimCount.increment();
                    log.warn("Kafka acknowledged event but claim was stale; eventId={}", event.id());
                }
            } catch (Exception deliveryFailure) {
                failedCount.increment();
                log.error("Outbox delivery failed; eventId={}, retryCount={}",
                    event.id(), event.retryCount(), deliveryFailure);
                try {
                    store.failed(event.id(), workerId, event.claimToken(), maxRetries,
                        baseRetryDelay, maxRetryDelay);
                } catch (Exception persistenceFailure) {
                    deliveryFailure.addSuppressed(persistenceFailure);
                    log.error("Could not persist delivery failure; eventId={}", event.id(), persistenceFailure);
                }
            }
            processed++;
        }
        return processed;
    }

    @Override
    public void run() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                int processed = runOneCycle();
                if (processed == 0) Thread.sleep(pollInterval.toMillis());
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception cycleFailure) {
                log.error("Outbox relay cycle failed; workerId={}", workerId, cycleFailure);
                try {
                    Thread.sleep(pollInterval.toMillis());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    public long publishedCount() { return publishedCount.sum(); }
    public long failedCount() { return failedCount.sum(); }
    public long staleClaimCount() { return staleClaimCount.sum(); }

    public void stop() { running.set(false); }

    @Override
    public void close() throws Exception {
        stop();
        publisher.close();
    }
}