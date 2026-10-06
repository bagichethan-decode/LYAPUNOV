package com.lyapunov.verifier;

import java.util.List;

public record ReliabilityReport(
    int expectedCount,
    int observedCount,
    List<String> missingEventIds,
    List<String> duplicateEventIds,
    List<String> orderingViolations
) {
    public boolean isReliable() {
        return missingEventIds.isEmpty()
            && duplicateEventIds.isEmpty()
            && orderingViolations.isEmpty();
    }
}
