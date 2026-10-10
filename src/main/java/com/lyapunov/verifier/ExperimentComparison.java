package com.lyapunov.verifier;

public record ExperimentComparison(
        String baselineReport,
        String candidateReport,
        int missingEventDelta,
        int duplicateEventDelta,
        int orderingViolationDelta,
        String outcome
) {
}
