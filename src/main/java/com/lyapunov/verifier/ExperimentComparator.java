package com.lyapunov.verifier;

public class ExperimentComparator {

    public ExperimentComparison compare(
            ExperimentRunner.ExperimentResult baseline,
            ExperimentRunner.ExperimentResult candidate,
            String baselineName,
            String candidateName
    ) {
        if (baseline == null || candidate == null) {
            throw new IllegalArgumentException(
                    "Baseline and candidate experiments are required"
            );
        }

        int missingDelta =
                candidate.missingEventIds().size()
                - baseline.missingEventIds().size();

        int duplicateDelta =
                candidate.duplicateEventIds().size()
                - baseline.duplicateEventIds().size();

        int orderingDelta =
                candidate.orderingViolations().size()
                - baseline.orderingViolations().size();

        boolean improved =
                missingDelta < 0
                || duplicateDelta < 0
                || orderingDelta < 0;

        boolean regressed =
                missingDelta > 0
                || duplicateDelta > 0
                || orderingDelta > 0;

        String outcome;

        if (improved && regressed) {
            outcome = "MIXED";
        } else if (improved) {
            outcome = "IMPROVED";
        } else if (regressed) {
            outcome = "REGRESSED";
        } else {
            outcome = "UNCHANGED";
        }

        return new ExperimentComparison(
                baselineName,
                candidateName,
                missingDelta,
                duplicateDelta,
                orderingDelta,
                outcome
        );
    }
}
