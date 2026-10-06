package com.lyapunov.verifier;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class EventVerifierTest {

    private final EventVerifier verifier = new EventVerifier();

    @Test
    void detectsMissingDuplicateAndOutOfOrderEvents() {

        List<EventVerifier.ExpectedEvent> expected = List.of(
            new EventVerifier.ExpectedEvent("e1", "Order", "order-1", 1),
            new EventVerifier.ExpectedEvent("e2", "Order", "order-1", 2),
            new EventVerifier.ExpectedEvent("e3", "Order", "order-1", 3),
            new EventVerifier.ExpectedEvent("e4", "Order", "order-1", 4)
        );

        List<EventVerifier.ObservedEvent> observed = List.of(
            new EventVerifier.ObservedEvent("e1", "Order", "order-1", 1),
            new EventVerifier.ObservedEvent("e3", "Order", "order-1", 3),
            new EventVerifier.ObservedEvent("e3", "Order", "order-1", 3),
            new EventVerifier.ObservedEvent("e2", "Order", "order-1", 2)
        );

        ReliabilityReport report = verifier.verify(expected, observed);

        assertEquals(4, report.expectedCount());
        assertEquals(4, report.observedCount());

        assertEquals(List.of("e4"), report.missingEventIds());
        assertEquals(List.of("e3"), report.duplicateEventIds());

        assertTrue(report.orderingViolations().isEmpty(),
            "This sequence is not actually out of order");

        assertFalse(report.isReliable());
    }

    @Test
    void detectsPerAggregateOrderingViolation() {

        List<EventVerifier.ExpectedEvent> expected = List.of(
            new EventVerifier.ExpectedEvent("e1", "Order", "order-1", 1),
            new EventVerifier.ExpectedEvent("e2", "Order", "order-1", 2),
            new EventVerifier.ExpectedEvent("e3", "Order", "order-1", 3)
        );

        List<EventVerifier.ObservedEvent> observed = List.of(
            new EventVerifier.ObservedEvent("e1", "Order", "order-1", 1),
            new EventVerifier.ObservedEvent("e3", "Order", "order-1", 3),
            new EventVerifier.ObservedEvent("e2", "Order", "order-1", 2)
        );

        ReliabilityReport report = verifier.verify(expected, observed);

        assertEquals(1, report.orderingViolations().size());
        assertTrue(report.orderingViolations().get(0).contains("order-1"));
        assertFalse(report.isReliable());
    }

    @Test
    void passesWhenStreamIsReliable() {

        List<EventVerifier.ExpectedEvent> expected = List.of(
            new EventVerifier.ExpectedEvent("e1", "Order", "order-1", 1),
            new EventVerifier.ExpectedEvent("e2", "Order", "order-1", 2),
            new EventVerifier.ExpectedEvent("e3", "Order", "order-1", 3)
        );

        List<EventVerifier.ObservedEvent> observed = List.of(
            new EventVerifier.ObservedEvent("e1", "Order", "order-1", 1),
            new EventVerifier.ObservedEvent("e2", "Order", "order-1", 2),
            new EventVerifier.ObservedEvent("e3", "Order", "order-1", 3)
        );

        ReliabilityReport report = verifier.verify(expected, observed);

        assertTrue(report.missingEventIds().isEmpty());
        assertTrue(report.duplicateEventIds().isEmpty());
        assertTrue(report.orderingViolations().isEmpty());
        assertTrue(report.isReliable());
    }
}
