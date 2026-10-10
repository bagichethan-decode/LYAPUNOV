package com.lyapunov.verifier;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FaultInjectionExperimentTest {

    private final FaultInjector injector = new FaultInjector();
    private final EventVerifier verifier = new EventVerifier();

    private List<EventVerifier.ExpectedEvent> expectedEvents() {
        return List.of(
                new EventVerifier.ExpectedEvent(
                        "e1", "ORDER", "order-1", 1),
                new EventVerifier.ExpectedEvent(
                        "e2", "ORDER", "order-1", 2),
                new EventVerifier.ExpectedEvent(
                        "e3", "ORDER", "order-1", 3),
                new EventVerifier.ExpectedEvent(
                        "e4", "ORDER", "order-1", 4)
        );
    }

    private List<EventVerifier.ObservedEvent> observedEvents() {
        return List.of(
                new EventVerifier.ObservedEvent(
                        "e1", "ORDER", "order-1", 1),
                new EventVerifier.ObservedEvent(
                        "e2", "ORDER", "order-1", 2),
                new EventVerifier.ObservedEvent(
                        "e3", "ORDER", "order-1", 3),
                new EventVerifier.ObservedEvent(
                        "e4", "ORDER", "order-1", 4)
        );
    }

    @Test
    void detectsDroppedEvent() {
        var fault = injector.inject(
                observedEvents(),
                FaultInjector.FaultType.DROP,
                42L
        );

        var report = verifier.verify(
                expectedEvents(),
                fault.events()
        );

        assertFalse(report.missingEventIds().isEmpty());
        assertFalse(report.isReliable());
    }

    @Test
    void detectsDuplicatedEvent() {
        var fault = injector.inject(
                observedEvents(),
                FaultInjector.FaultType.DUPLICATE,
                42L
        );

        var report = verifier.verify(
                expectedEvents(),
                fault.events()
        );

        assertFalse(report.duplicateEventIds().isEmpty());
        assertFalse(report.isReliable());
    }

    @Test
    void detectsReorderedEvents() {
        var fault = injector.inject(
                observedEvents(),
                FaultInjector.FaultType.REORDER,
                42L
        );

        var report = verifier.verify(
                expectedEvents(),
                fault.events()
        );

        assertFalse(report.orderingViolations().isEmpty());
        assertFalse(report.isReliable());
    }

    @Test
    void cleanStreamIsReliable() {
        var report = verifier.verify(
                expectedEvents(),
                observedEvents()
        );

        assertTrue(report.isReliable());
    }
}