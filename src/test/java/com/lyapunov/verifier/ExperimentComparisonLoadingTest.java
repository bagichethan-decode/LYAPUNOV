package com.lyapunov.verifier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ExperimentComparisonLoadingTest {

    @TempDir
    Path tempDir;

    private final ExperimentRunner runner = new ExperimentRunner();
    private final ExperimentComparisonCli cli =
            new ExperimentComparisonCli();

    @Test
    void comparesRealJsonExperimentReports() throws Exception {
        var baseline = runner.run(
                42L, FaultInjector.FaultType.REORDER, 10
        );
        var candidate = runner.run(
                42L, FaultInjector.FaultType.REORDER, 10
        );

        Path baselinePath = runner.writeJsonReport(
                baseline, tempDir.resolve("baseline")
        );
        Path candidatePath = runner.writeJsonReport(
                candidate, tempDir.resolve("candidate")
        );

        var comparison = cli.compareReports(
                baselinePath, candidatePath
        );

        assertEquals("UNCHANGED", comparison.outcome());
        assertEquals(0, comparison.missingEventDelta());
        assertEquals(0, comparison.duplicateEventDelta());
        assertEquals(0, comparison.orderingViolationDelta());
    }

    @Test
    void rejectsMissingBaselineReport() {
        Path missingBaseline = tempDir.resolve("missing.json");
        Path candidate = tempDir.resolve("candidate.json");

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> cli.compareReports(missingBaseline, candidate)
        );

        assertTrue(exception.getMessage().contains("Baseline report not found"));
    }

    @Test
    void rejectsMalformedCandidateJson() throws Exception {
        var baseline = runner.run(
                42L, FaultInjector.FaultType.DROP, 10
        );

        Path baselinePath = runner.writeJsonReport(
                baseline, tempDir.resolve("baseline")
        );
        Path candidatePath = tempDir.resolve("malformed.json");

        Files.writeString(candidatePath, "{ invalid json");

        assertThrows(
                IOException.class,
                () -> cli.compareReports(baselinePath, candidatePath)
        );
    }

    @Test
    void rejectsExperimentsWithDifferentParameters() throws Exception {
        var baseline = runner.run(
                42L, FaultInjector.FaultType.DROP, 10
        );
        var candidate = runner.run(
                43L, FaultInjector.FaultType.DROP, 10
        );

        Path baselinePath = runner.writeJsonReport(
                baseline, tempDir.resolve("baseline")
        );
        Path candidatePath = runner.writeJsonReport(
                candidate, tempDir.resolve("candidate")
        );

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> cli.compareReports(baselinePath, candidatePath)
        );

        assertTrue(exception.getMessage().contains("seed"));
    }
}
