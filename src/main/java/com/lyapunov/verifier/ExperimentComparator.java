package com.lyapunov.verifier;

import java.util.ArrayList;
import java.util.List;

public class ExperimentComparator {

    public ExperimentComparison compare(
            ExperimentRunner.ExperimentResult baseline,
            ExperimentRunner.ExperimentResult candidate,
            String baselineName,
            String candidateName
    ) {
        validateComparableExperiments(baseline, candidate);

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

    private void validateComparableExperiments(
            ExperimentRunner.ExperimentResult baseline,
            ExperimentRunner.ExperimentResult candidate
    ) {
        if (baseline == null || candidate == null) {
            throw new IllegalArgumentException(
                    "Baseline and candidate experiments are required"
            );
        }

        List<String> mismatches = new ArrayList<>();

        if (baseline.seed() != candidate.seed()) {
            mismatches.add("seed");
        }

        if (baseline.faultType() != candidate.faultType()) {
            mismatches.add("faultType");
        }

        if (baseline.requestedEventCount()
                != candidate.requestedEventCount()) {
            mismatches.add("requestedEventCount");
        }

        if (!mismatches.isEmpty()) {
            throw new IllegalArgumentException(
                    "Cannot compare experiments with different parameters: "
                    + String.join(", ", mismatches)
            );
        }
    }
}