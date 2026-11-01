package com.lyapunov.worker;

import com.lyapunov.model.OutboxEvent;
import com.lyapunov.model.OutboxEvent.EventStatus;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

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

    private static OutboxRelayWorker worker(FakeStore store, EventPublisher publisher) {
        return new OutboxRelayWorker(store, publisher, "test-worker", 10, 5,
            Duration.ofSeconds(30), Duration.ofMillis(10));
    }

    private static OutboxEvent event() {
        return new OutboxEvent(UUID.randomUUID(), "ORDER", "order-42", 1, "ORDER_CREATED",
            "{\"orderId\":\"order-42\"}", EventStatus.CLAIMED, 0, "test-worker",
            UUID.randomUUID(), Instant.now().plusSeconds(30), Instant.now(), Instant.now());
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