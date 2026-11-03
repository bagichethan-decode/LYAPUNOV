package com.lyapunov.worker;

import com.lyapunov.model.OutboxEvent;
import com.lyapunov.repository.WorkerHeartbeatStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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
    private final WorkerHeartbeatStore heartbeatStore;
    private final String instanceId;
    private final Duration heartbeatInterval;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean heartbeatStarted = new AtomicBoolean(false);
    private final AtomicBoolean heartbeatStopped = new AtomicBoolean(false);
    private final AtomicReference<String> lastError = new AtomicReference<>();
    private final LongAdder publishedCount = new LongAdder();
    private final LongAdder failedCount = new LongAdder();
    private final LongAdder staleClaimCount = new LongAdder();
    private volatile ScheduledExecutorService heartbeatExecutor;

    public OutboxRelayWorker(OutboxStore store, EventPublisher publisher, String workerId,
                             int batchSize, int maxRetries, Duration leaseDuration,
                             Duration pollInterval) {
        this(store, publisher, workerId, batchSize, maxRetries, leaseDuration, pollInterval,
            Duration.ofSeconds(1), Duration.ofMinutes(5));
    }

    public OutboxRelayWorker(OutboxStore store, EventPublisher publisher, String workerId,
                             int batchSize, int maxRetries, Duration leaseDuration,
                             Duration pollInterval, Duration baseRetryDelay, Duration maxRetryDelay) {
        this(store, publisher, workerId, batchSize, maxRetries, leaseDuration, pollInterval,
            baseRetryDelay, maxRetryDelay, null, UUID.randomUUID().toString(), Duration.ofSeconds(10));
    }

    public OutboxRelayWorker(OutboxStore store, EventPublisher publisher, String workerId,
                             int batchSize, int maxRetries, Duration leaseDuration,
                             Duration pollInterval, Duration baseRetryDelay, Duration maxRetryDelay,
                             WorkerHeartbeatStore heartbeatStore, String instanceId,
                             Duration heartbeatInterval) {
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
        if (heartbeatStore != null && (instanceId == null || instanceId.isBlank())) {
            throw new IllegalArgumentException("instanceId must not be blank when heartbeat reporting is enabled");
        }
        if (heartbeatStore != null && (heartbeatInterval == null || heartbeatInterval.isNegative()
            || heartbeatInterval.isZero())) {
            throw new IllegalArgumentException("heartbeatInterval must be positive when heartbeat reporting is enabled");
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
        this.heartbeatStore = heartbeatStore;
        this.instanceId = instanceId;
        this.heartbeatInterval = heartbeatInterval;
    }

    public int runOneCycle() throws Exception {
        List<OutboxEvent> events = store.claim(workerId, batchSize, leaseDuration);
        int processed = 0;
        for (OutboxEvent event : events) {
            try {
                publisher.publish(event);
                if (store.published(event.id(), workerId, event.claimToken())) {
                    publishedCount.increment();
                    lastError.set(null);
                } else {
                    staleClaimCount.increment();
                    log.warn("Kafka acknowledged event but claim was stale; eventId={}", event.id());
                }
            } catch (Exception deliveryFailure) {
                failedCount.increment();
                lastError.set(deliveryFailure.getClass().getSimpleName() + ": " + String.valueOf(deliveryFailure.getMessage()));
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
        startHeartbeatReporting();
        try {
            while (running.get() && !Thread.currentThread().isInterrupted()) {
                try {
                    int processed = runOneCycle();
                    if (processed == 0) Thread.sleep(pollInterval.toMillis());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception cycleFailure) {
                    lastError.set(cycleFailure.getClass().getSimpleName() + ": " + String.valueOf(cycleFailure.getMessage()));
                    log.error("Outbox relay cycle failed; workerId={}", workerId, cycleFailure);
                    try {
                        Thread.sleep(pollInterval.toMillis());
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        } finally {
            stopHeartbeatReporting();
        }
    }

    private void startHeartbeatReporting() {
        if (heartbeatStore == null || !heartbeatStarted.compareAndSet(false, true)) return;
        recordHeartbeat();
        ThreadFactory factory = task -> {
            Thread thread = new Thread(task, "lyapunov-heartbeat-" + workerId);
            thread.setDaemon(true);
            return thread;
        };
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(factory);
        heartbeatExecutor.scheduleWithFixedDelay(this::recordHeartbeat,
            heartbeatInterval.toMillis(), heartbeatInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void recordHeartbeat() {
        try {
            heartbeatStore.heartbeat(workerId, instanceId, publishedCount(), failedCount(),
                staleClaimCount(), lastError.get());
        } catch (Exception heartbeatFailure) {
            // Monitoring must never take down the event-delivery path.
            log.warn("Could not persist worker heartbeat; workerId={}, instanceId={}",
                workerId, instanceId, heartbeatFailure);
        }
    }

    private void stopHeartbeatReporting() {
        if (heartbeatStore == null || !heartbeatStarted.get()
            || !heartbeatStopped.compareAndSet(false, true)) return;
        ScheduledExecutorService executor = heartbeatExecutor;
        if (executor != null) executor.shutdownNow();
        try {
            heartbeatStore.markStopped(workerId, instanceId);
        } catch (Exception stopFailure) {
            log.warn("Could not mark worker STOPPED; workerId={}, instanceId={}",
                workerId, instanceId, stopFailure);
        }
    }

    public long publishedCount() { return publishedCount.sum(); }
    public long failedCount() { return failedCount.sum(); }
    public long staleClaimCount() { return staleClaimCount.sum(); }
    public String instanceId() { return instanceId; }

    public void stop() { running.set(false); }

    @Override
    public void close() throws Exception {
        stop();
        stopHeartbeatReporting();
        publisher.close();
    }
}
