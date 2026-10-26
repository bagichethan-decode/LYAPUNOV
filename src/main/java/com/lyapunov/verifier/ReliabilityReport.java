package com.lyapunov.verifier;

import java.util.List;

public record ReliabilityReport(
    int expectedCount,
    int observedCount,
    List<String> missingEventIds,
    List<String> duplicateEventIds,
    List<String> orderingViolations,
    List<String> unexpectedEventIds
) {
    public ReliabilityReport {
        missingEventIds = List.copyOf(missingEventIds);
        duplicateEventIds = List.copyOf(duplicateEventIds);
        orderingViolations = List.copyOf(orderingViolations);
        unexpectedEventIds = List.copyOf(unexpectedEventIds);
    }

    // Preserve compatibility with existing five-argument callers.
    public ReliabilityReport(
        int expectedCount,
        int observedCount,
        List<String> missingEventIds,
        List<String> duplicateEventIds,
        List<String> orderingViolations
    ) {
        this(
            expectedCount,
            observedCount,
            missingEventIds,
            duplicateEventIds,
            orderingViolations,
            List.of()
        );
    }

    public boolean isReliable() {
        return missingEventIds.isEmpty()
            && duplicateEventIds.isEmpty()
            && orderingViolations.isEmpty()
            && unexpectedEventIds.isEmpty();
    }
}
