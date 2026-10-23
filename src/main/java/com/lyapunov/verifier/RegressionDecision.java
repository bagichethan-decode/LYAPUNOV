package com.lyapunov.verifier;

import java.util.List;

public record RegressionDecision(
        boolean passed,
        List<String> violations
) {
    public RegressionDecision {
        violations = List.copyOf(violations);

        if (passed != violations.isEmpty()) {
            throw new IllegalArgumentException(
                    "A passed decision must have no violations, "
                    + "and a failed decision must have at least one violation"
            );
        }
    }
}