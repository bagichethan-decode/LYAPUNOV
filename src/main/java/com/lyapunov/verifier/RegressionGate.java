package com.lyapunov.verifier;

import java.util.ArrayList;
import java.util.List;

public class RegressionGate {

private final int maxMissingIncrease;
private final int maxDuplicateIncrease;
private final int maxOrderingIncrease;
private final int maxUnexpectedIncrease;

public RegressionGate(
        int maxMissingIncrease,
        int maxDuplicateIncrease,
        int maxOrderingIncrease
) {
    this(
            maxMissingIncrease,
            maxDuplicateIncrease,
            maxOrderingIncrease,
            0
    );
}

public RegressionGate(
        int maxMissingIncrease,
        int maxDuplicateIncrease,
        int maxOrderingIncrease,
        int maxUnexpectedIncrease
) {
    if (maxMissingIncrease < 0
            || maxDuplicateIncrease < 0
            || maxOrderingIncrease < 0
            || maxUnexpectedIncrease < 0) {
        throw new IllegalArgumentException(
                "Regression thresholds cannot be negative"
        );
    }

    this.maxMissingIncrease = maxMissingIncrease;
    this.maxDuplicateIncrease = maxDuplicateIncrease;
    this.maxOrderingIncrease = maxOrderingIncrease;
    this.maxUnexpectedIncrease = maxUnexpectedIncrease;
}

public RegressionDecision evaluate(
        ExperimentComparison comparison
) {
    if (comparison == null) {
        throw new IllegalArgumentException(
                "Experiment comparison is required"
        );
    }

    List<String> violations = new ArrayList<>();

    if (comparison.missingEventDelta() > maxMissingIncrease) {
        violations.add(
                "Missing events increased by "
                + comparison.missingEventDelta()
                + "; allowed increase: "
                + maxMissingIncrease
        );
    }

    if (comparison.duplicateEventDelta() > maxDuplicateIncrease) {
        violations.add(
                "Duplicate events increased by "
                + comparison.duplicateEventDelta()
                + "; allowed increase: "
                + maxDuplicateIncrease
        );
    }

    if (comparison.orderingViolationDelta() > maxOrderingIncrease) {
        violations.add(
                "Ordering violations increased by "
                + comparison.orderingViolationDelta()
                + "; allowed increase: "
                + maxOrderingIncrease
        );
    }

    if (comparison.unexpectedEventDelta() > maxUnexpectedIncrease) {
        violations.add(
                "Unexpected events increased by "
                + comparison.unexpectedEventDelta()
                + "; allowed increase: "
                + maxUnexpectedIncrease
        );
    }

    return new RegressionDecision(
            violations.isEmpty(),
            violations
    );
}

}
