package com.lyapunov.verifier;

public record ExperimentComparison(
        String baselineReport,
        String candidateReport,
        int missingEventDelta,
        int duplicateEventDelta,
        int orderingViolationDelta,
        int unexpectedEventDelta,
        String outcome
) {
    // Preserve compatibility with existing callers.
    public ExperimentComparison(
            String baselineReport,
            String candidateReport,
            int missingEventDelta,
            int duplicateEventDelta,
            int orderingViolationDelta,
            String outcome
    ) {
        this(
                baselineReport,
                candidateReport,
                missingEventDelta,
                duplicateEventDelta,
                orderingViolationDelta,
                0,
                outcome
        );
    }
}