package com.lyapunov.verifier;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FaultInjectorTest {

    private final FaultInjector injector = new FaultInjector();

    private List<EventVerifier.ObservedEvent> events() {
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
    void sameSeedProducesSameDropResult() {
        var first = injector.inject(
                events(), FaultInjector.FaultType.DROP, 42L);

        var second = injector.inject(
                events(), FaultInjector.FaultType.DROP, 42L);

        assertEquals(first.events(), second.events());
        assertEquals(3, first.events().size());
    }

    @Test
    void duplicateAddsExactlyOneEvent() {
        var result = injector.inject(
                events(), FaultInjector.FaultType.DUPLICATE, 42L);

        assertEquals(5, result.events().size());
    }

    @Test
    void reorderPreservesEventsButChangesTheirOrder() {
        var result = injector.inject(
                events(), FaultInjector.FaultType.REORDER, 42L);

        assertEquals(4, result.events().size());

        assertNotEquals(
                events(),
                result.events(),
                "Reordering should change the event sequence"
        );
    }

    @Test
    void rejectsReorderingSingleEvent() {
        var singleEvent = List.of(events().get(0));

        assertThrows(
                IllegalArgumentException.class,
                () -> injector.inject(
                        singleEvent,
                        FaultInjector.FaultType.REORDER,
                        42L
                )
        );
    }
}