package com.lyapunov.verifier;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class UnexpectedEventDetectorTest {

    @Test
    void returnsOnlyObservedIdsThatWereNotExpected() {
        Set<String> result = UnexpectedEventDetector.findUnexpectedIds(
                List.of("event-1", "event-2"),
                List.of("event-1", "rogue-event", "event-2", "another-rogue")
        );

        assertEquals(Set.of("rogue-event", "another-rogue"), result);
    }

    @Test
    void returnsEmptySetWhenEveryObservedIdWasExpected() {
        assertTrue(UnexpectedEventDetector.findUnexpectedIds(
                List.of("event-1", "event-2"),
                List.of("event-2", "event-1")
        ).isEmpty());
    }

    @Test
    void deduplicatesUnexpectedIdsWithoutChangingTheirMeaning() {
        assertEquals(Set.of("rogue"), UnexpectedEventDetector.findUnexpectedIds(
                List.of("expected"),
                List.of("rogue", "rogue", "expected")
        ));
    }

    @Test
    void handlesEmptyExpectedAndObservedCollections() {
        assertTrue(UnexpectedEventDetector.findUnexpectedIds(
                List.of(), List.of()
        ).isEmpty());

        assertEquals(Set.of("first", "second"),
                UnexpectedEventDetector.findUnexpectedIds(
                        List.of(), List.of("first", "second", "first")
                ));
    }

    @Test
    void rejectsNullCollections() {
        assertThrows(NullPointerException.class,
                () -> UnexpectedEventDetector.findUnexpectedIds(null, List.of()));
        assertThrows(NullPointerException.class,
                () -> UnexpectedEventDetector.findUnexpectedIds(List.of(), null));
    }
}
