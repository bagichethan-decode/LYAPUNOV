package com.lyapunov.verifier;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ExperimentComparatorTest {

    private final ExperimentComparator comparator =
            new ExperimentComparator();

    private ExperimentRunner.ExperimentResult result(
            List<String> missing,
            List<String> duplicates,
            List<String> ordering
    ) {
        return new ExperimentRunner.ExperimentResult(
                "LYAPUNOV",
                42L,
                FaultInjector.FaultType.REORDER,
                10,
                "2026-10-10T10:00:00Z",
                10,
                10,
                missing,
                duplicates,
                ordering,
                missing.isEmpty()
                        && duplicates.isEmpty()
                        && ordering.isEmpty()
        );
    }

    @Test
    void detectsImprovement() {
        var baseline = result(
                List.of("e1"),
                List.of("e2"),
                List.of("e3")
        );

        var candidate = result(
                List.of(),
                List.of(),
                List.of()
        );

        var comparison = comparator.compare(
                baseline, candidate, "baseline", "candidate"
        );

        assertEquals("IMPROVED", comparison.outcome());
        assertEquals(-1, comparison.missingEventDelta());
        assertEquals(-1, comparison.duplicateEventDelta());
        assertEquals(-1, comparison.orderingViolationDelta());
    }

    @Test
    void detectsRegression() {
        var baseline = result(
                List.of(),
                List.of(),
                List.of()
        );

        var candidate = result(
                List.of("e1"),
                List.of(),
                List.of("e2")
        );

        var comparison = comparator.compare(
                baseline, candidate, "baseline", "candidate"
        );

        assertEquals("REGRESSED", comparison.outcome());
    }

    @Test
    void detectsMixedChanges() {
        var baseline = result(
                List.of("e1"),
                List.of(),
                List.of()
        );

        var candidate = result(
                List.of(),
                List.of(),
                List.of("e2")
        );

        var comparison = comparator.compare(
                baseline, candidate, "baseline", "candidate"
        );

        assertEquals("MIXED", comparison.outcome());
        assertEquals(-1, comparison.missingEventDelta());
        assertEquals(1, comparison.orderingViolationDelta());
    }

    @Test
    void detectsUnchangedResults() {
        var baseline = result(
                List.of("e1"),
                List.of("e2"),
                List.of()
        );

        var candidate = result(
                List.of("e3"),
                List.of("e4"),
                List.of()
        );

        var comparison = comparator.compare(
                baseline, candidate, "baseline", "candidate"
        );

        assertEquals("UNCHANGED", comparison.outcome());
    }
}
