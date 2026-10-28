package com.lyapunov.verifier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ExperimentComparisonPathValidationTest {

    @TempDir
    Path tempDir;

    private final ExperimentComparisonCli cli =
            new ExperimentComparisonCli();

    @Test
    void rejectsNullBaselinePath() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> cli.compareReports(
                        null,
                        tempDir.resolve("candidate.json")
                )
        );

        assertEquals(
                "Baseline report path cannot be null or blank",
                exception.getMessage()
        );
    }

    @Test
    void rejectsNullCandidatePath() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> cli.compareReports(
                        tempDir.resolve("baseline.json"),
                        null
                )
        );

        assertEquals(
                "Candidate report path cannot be null or blank",
                exception.getMessage()
        );
    }
}
