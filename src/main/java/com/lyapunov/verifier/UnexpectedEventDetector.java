package com.lyapunov.verifier;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Identifies observed event IDs that were not present in the expected event set.
 * Each unexpected ID is returned once; duplicate delivery is a separate concern.
 */
public final class UnexpectedEventDetector {

    private UnexpectedEventDetector() {
    }

    public static Set<String> findUnexpectedIds(
            Collection<String> expectedEventIds,
            Collection<String> observedEventIds
    ) {
        Objects.requireNonNull(expectedEventIds, "expectedEventIds is required");
        Objects.requireNonNull(observedEventIds, "observedEventIds is required");

        Set<String> expected = new LinkedHashSet<>(expectedEventIds);
        Set<String> unexpected = new LinkedHashSet<>();

        for (String observedId : observedEventIds) {
            if (!expected.contains(observedId)) {
                unexpected.add(observedId);
            }
        }

        return Set.copyOf(unexpected);
    }
}
