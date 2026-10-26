package com.lyapunov.verifier;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ExperimentComparatorTest {

    private final ExperimentComparator comparator =
            new ExperimentComparator();

    private ExperimentRunner.ExperimentResult report(
            List<String> missing,
            List<String> duplicates,
            List<String> ordering,
            List<String> unexpected
    ) {
        return new ExperimentRunner.ExperimentResult(
                "LYAPUNOV",
                2,
                "test-revision",
                42L,
                FaultInjector.FaultType.DROP,
                10,
                "2026-10-26T00:00:00Z",
                10,
                10,
                missing,
                duplicates,
                ordering,
                unexpected,
                false
        );
    }

    @Test
    void unexpectedEventsIncreasingIsRegression() {
        var baseline = report(
                List.of(), List.of(), List.of(), List.of()
        );

        var candidate = report(
                List.of(), List.of(), List.of(),
                List.of("unexpected-001")
        );

        var comparison = comparator.compare(
                baseline, candidate, "baseline", "candidate"
        );

        assertEquals(1, comparison.unexpectedEventDelta());
        assertEquals("REGRESSED", comparison.outcome());
    }

    @Test
    void unexpectedEventsDecreasingIsImprovement() {
        var baseline = report(
                List.of(), List.of(), List.of(),
                List.of("unexpected-001", "unexpected-002")
        );

        var candidate = report(
                List.of(), List.of(), List.of(),
                List.of("unexpected-001")
        );

        var comparison = comparator.compare(
                baseline, candidate, "baseline", "candidate"
        );

        assertEquals(-1, comparison.unexpectedEventDelta());
        assertEquals("IMPROVED", comparison.outcome());
    }

    @Test
    void unchangedUnexpectedEventCountProducesUnchangedOutcome() {
        var baseline = report(
                List.of(), List.of(), List.of(),
                List.of("unexpected-001")
        );

        var candidate = report(
                List.of(), List.of(), List.of(),
                List.of("unexpected-002")
        );

        var comparison = comparator.compare(
                baseline, candidate, "baseline", "candidate"
        );

        assertEquals(0, comparison.unexpectedEventDelta());
        assertEquals("UNCHANGED", comparison.outcome());
    }

    @Test
    void improvementAndRegressionProduceMixedOutcome() {
        var baseline = report(
                List.of("missing-001"),
                List.of(),
                List.of(),
                List.of()
        );

        var candidate = report(
                List.of(),
                List.of(),
                List.of(),
                List.of("unexpected-001")
        );

        var comparison = comparator.compare(
                baseline, candidate, "baseline", "candidate"
        );

        assertEquals(-1, comparison.missingEventDelta());
        assertEquals(1, comparison.unexpectedEventDelta());
        assertEquals("MIXED", comparison.outcome());
    }
}