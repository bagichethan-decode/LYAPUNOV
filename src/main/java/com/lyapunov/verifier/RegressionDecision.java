package com.lyapunov.verifier;

import java.util.List;

public record RegressionDecision(
        boolean passed,
        List<String> violations
) {
    public RegressionDecision {
        violations = List.copyOf(violations);
    }
}
