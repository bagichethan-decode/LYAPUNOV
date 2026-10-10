package com.lyapunov.verifier;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RegressionGateTest {

    private final RegressionGate strictGate =
            new RegressionGate(0, 0, 0);

    private ExperimentComparison comparison(
            int missingDelta,
            int duplicateDelta,
            int orderingDelta
    ) {
        return new ExperimentComparison(
                "baseline.json",
                "candidate.json",
                missingDelta,
                duplicateDelta,
                orderingDelta,
                "MIXED"
        );
    }

    @Test
    void passesWhenNoMetricRegresses() {
        var decision = strictGate.evaluate(
                comparison(-1, 0, 0)
        );

        assertTrue(decision.passed());
        assertTrue(decision.violations().isEmpty());
    }

    @Test
    void failsWhenOrderingRegresses() {
        var decision = strictGate.evaluate(
                comparison(-1, 0, 1)
        );

        assertFalse(decision.passed());
        assertEquals(1, decision.violations().size());
        assertTrue(
                decision.violations().get(0)
                        .contains("Ordering violations increased")
        );
    }

    @Test
    void reportsMultipleRegressions() {
        var decision = strictGate.evaluate(
                comparison(1, 2, 1)
        );

        assertFalse(decision.passed());
        assertEquals(3, decision.violations().size());
    }

    @Test
    void permitsConfiguredIncreases() {
        var gate = new RegressionGate(1, 2, 0);

        var decision = gate.evaluate(
                comparison(1, 2, 0)
        );

        assertTrue(decision.passed());
        assertTrue(decision.violations().isEmpty());
    }

    @Test
    void rejectsNegativeThresholds() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new RegressionGate(-1, 0, 0)
        );
    }

    @Test
    void rejectsNullComparison() {
        assertThrows(
                IllegalArgumentException.class,
                () -> strictGate.evaluate(null)
        );
    }
}
