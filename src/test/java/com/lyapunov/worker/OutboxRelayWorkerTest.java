package com.lyapunov.worker;

import com.lyapunov.model.OutboxEvent;
import com.lyapunov.model.OutboxEvent.EventStatus;
import com.lyapunov.repository.WorkerHeartbeatStore;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class OutboxRelayWorkerTest {
    @Test
    void publishesClaimedEventsAndMarksThemPublished() throws Exception {
        FakeStore store = new FakeStore();
        OutboxEvent event = event();
        store.events = List.of(event);
        List<UUID> sent = new ArrayList<>();
        EventPublisher publisher = new EventPublisher() {
            public void publish(OutboxEvent value) { sent.add(value.id()); }
            public void close() {}
        };
        OutboxRelayWorker worker = worker(store, publisher);

        assertEquals(1, worker.runOneCycle());
        assertEquals(List.of(event.id()), sent);
        assertEquals(1, store.published.size());
        assertTrue(store.failed.isEmpty());
        assertEquals(1, worker.publishedCount());
    }

    @Test
    void recordsFailureWhenPublisherThrows() throws Exception {
        FakeStore store = new FakeStore();
        OutboxEvent event = event();
        store.events = List.of(event);
        EventPublisher publisher = new EventPublisher() {
            public void publish(OutboxEvent value) throws Exception { throw new Exception("broker unavailable"); }
            public void close() {}
        };
        OutboxRelayWorker worker = worker(store, publisher);

        assertEquals(1, worker.runOneCycle());
        assertEquals(1, store.failed.size());
        assertTrue(store.published.isEmpty());
        assertEquals(1, worker.failedCount());
    }

    @Test
    void countsStaleClaimWhenDatabaseRejectsCompletion() throws Exception {
        FakeStore store = new FakeStore();
        store.events = List.of(event());
        store.acceptPublished = false;
        EventPublisher publisher = new EventPublisher() {
            public void publish(OutboxEvent value) {}
            public void close() {}
        };
        OutboxRelayWorker worker = worker(store, publisher);

        worker.runOneCycle();
        assertEquals(1, worker.staleClaimCount());
    }

    @Test
    void reportsStartupPeriodicProgressAndStoppedLifecycle() throws Exception {
        FakeStore store = new FakeStore();
        FakeHeartbeatStore heartbeats = new FakeHeartbeatStore();
        OutboxRelayWorker worker = new OutboxRelayWorker(store, noOpPublisher(), "heartbeat-test",
            10, 5, Duration.ofSeconds(30), Duration.ofMillis(5),
            Duration.ofSeconds(1), Duration.ofMinutes(5),
            heartbeats, "instance-123", Duration.ofMillis(20));

        Thread thread = new Thread(worker, "test-relay-worker");
        thread.start();
        assertTrue(heartbeats.firstHeartbeat.await(2, TimeUnit.SECONDS));
        assertTrue(heartbeats.secondHeartbeat.await(2, TimeUnit.SECONDS));
        worker.stop();
        thread.join(2_000);

        assertFalse(thread.isAlive(), "worker should stop promptly");
        assertEquals(1, heartbeats.stopped.get());
        assertEquals("heartbeat-test", heartbeats.workerId);
        assertEquals("instance-123", heartbeats.instanceId);
    }

    @Test
    void heartbeatPersistenceFailureDoesNotStopWorker() throws Exception {
        FakeStore store = new FakeStore();
        FakeHeartbeatStore heartbeats = new FakeHeartbeatStore();
        heartbeats.failWrites = true;
        OutboxRelayWorker worker = new OutboxRelayWorker(store, noOpPublisher(), "heartbeat-resilience",
            10, 5, Duration.ofSeconds(30), Duration.ofMillis(5),
            Duration.ofSeconds(1), Duration.ofMinutes(5),
            heartbeats, "instance-456", Duration.ofMillis(20));

        Thread thread = new Thread(worker, "test-relay-worker-resilience");
        thread.start();
        assertTrue(heartbeats.attempted.await(2, TimeUnit.SECONDS));
        assertTrue(thread.isAlive(), "heartbeat database failure must not terminate relay");
        worker.stop();
        thread.join(2_000);
        assertFalse(thread.isAlive());
        assertEquals(1, heartbeats.stopAttempts.get());
    }

    private static OutboxRelayWorker worker(FakeStore store, EventPublisher publisher) {
        return new OutboxRelayWorker(store, publisher, "test-worker", 10, 5,
            Duration.ofSeconds(30), Duration.ofMillis(10));
    }

    private static EventPublisher noOpPublisher() {
        return new EventPublisher() {
            public void publish(OutboxEvent value) {}
            public void close() {}
        };
    }

    private static OutboxEvent event() {
        return new OutboxEvent(UUID.randomUUID(), "ORDER", "order-42", 1, "ORDER_CREATED",
            "{\"orderId\":\"order-42\"}", EventStatus.CLAIMED, 0, "test-worker",
            UUID.randomUUID(), Instant.now().plusSeconds(30), Instant.now(), Instant.now());
    }

    private static final class FakeHeartbeatStore implements WorkerHeartbeatStore {
        final CountDownLatch firstHeartbeat = new CountDownLatch(1);
        final CountDownLatch secondHeartbeat = new CountDownLatch(1);
        final CountDownLatch attempted = new CountDownLatch(1);
        final AtomicInteger heartbeatCalls = new AtomicInteger();
        final AtomicInteger stopped = new AtomicInteger();
        final AtomicInteger stopAttempts = new AtomicInteger();
        volatile String workerId;
        volatile String instanceId;
        volatile boolean failWrites;

        public void heartbeat(String workerId, String instanceId, long published,
                              long failed, long stale, String lastError) throws Exception {
            this.workerId = workerId;
            this.instanceId = instanceId;
            attempted.countDown();
            if (failWrites) throw new Exception("heartbeat database unavailable");
            int call = heartbeatCalls.incrementAndGet();
            if (call == 1) firstHeartbeat.countDown();
            if (call >= 2) secondHeartbeat.countDown();
        }

        public boolean markStopped(String workerId, String instanceId) {
            stopAttempts.incrementAndGet();
            stopped.incrementAndGet();
            return true;
        }
    }

    private static final class FakeStore implements OutboxStore {
        List<OutboxEvent> events = List.of();
        List<UUID> published = new ArrayList<>();
        List<UUID> failed = new ArrayList<>();
        boolean acceptPublished = true;

        public List<OutboxEvent> claim(String workerId, int batchSize, Duration leaseDuration) { return events; }
        public boolean published(UUID id, String workerId, UUID token) {
            if (acceptPublished) published.add(id);
            return acceptPublished;
        }
        public boolean failed(UUID id, String workerId, UUID token, int maxRetries) {
            failed.add(id);
            return true;
        }
    }
}
