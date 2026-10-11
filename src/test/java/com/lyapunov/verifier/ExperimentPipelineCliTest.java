package com.lyapunov.verifier;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ExperimentPipelineCliTest {

    @TempDir
    Path tempDir;

    private Path createBaseline(
            long seed,
            FaultInjector.FaultType faultType,
            int eventCount
    ) throws Exception {
        ExperimentRunner runner = new ExperimentRunner();

        ExperimentRunner.ExperimentResult baseline =
                runner.run(seed, faultType, eventCount);

        Path baselinePath = tempDir.resolve("baseline.json");

        new ObjectMapper().writeValue(
                baselinePath.toFile(),
                baseline
        );

        return baselinePath;
    }

    private String runCapturingOutput(String[] args) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        PrintStream originalErr = System.err;

        try (PrintStream captured = new PrintStream(
                output, true, StandardCharsets.UTF_8)) {
            System.setOut(captured);
            System.setErr(captured);

            int exitCode = ExperimentPipelineCli.run(args);

            captured.println("TEST_EXIT_CODE=" + exitCode);
        } finally {
            System.setOut(originalOut);
            System.setErr(originalErr);
        }

        return output.toString(StandardCharsets.UTF_8);
    }

    @Test
    void runsPipelineAndPassesWhenMetricsAreUnchanged()
            throws Exception {
        Path baseline = createBaseline(
                42L, FaultInjector.FaultType.DROP, 10
        );

        Path reportDirectory = tempDir.resolve("candidate-reports");

        String output = runCapturingOutput(new String[]{
                "42",
                "DROP",
                "10",
                baseline.toString(),
                reportDirectory.toString()
        });

        assertTrue(output.contains("Comparison outcome    : UNCHANGED"));
        assertTrue(output.contains("Regression gate       : PASS"));
        assertTrue(output.contains("TEST_EXIT_CODE=0"));

        try (var files = Files.list(reportDirectory)) {
            assertTrue(files.anyMatch(path ->
                    path.getFileName().toString().endsWith(".json")));
        }
    }

    @Test
    void rejectsMissingBaseline() {
        Path missingBaseline = tempDir.resolve("missing.json");

        String output = runCapturingOutput(new String[]{
                "42",
                "DROP",
                "10",
                missingBaseline.toString(),
                tempDir.resolve("candidate-reports").toString()
        });

        assertTrue(output.contains("Baseline report not found"));
        assertTrue(output.contains("TEST_EXIT_CODE=2"));
    }

    @Test
    void rejectsInvalidArguments() {
        String output = runCapturingOutput(new String[]{
                "42",
                "DROP"
        });

        assertTrue(output.contains("Usage: ExperimentPipelineCli"));
        assertTrue(output.contains("TEST_EXIT_CODE=2"));
    }

    @Test
    void rejectsUnsupportedFaultType() throws Exception {
        Path baseline = createBaseline(
                42L, FaultInjector.FaultType.DROP, 10
        );

        String output = runCapturingOutput(new String[]{
                "42",
                "UNKNOWN",
                "10",
                baseline.toString(),
                tempDir.resolve("candidate-reports").toString()
        });

        assertTrue(output.contains("Pipeline failed"));
        assertTrue(output.contains("TEST_EXIT_CODE=2"));
    }
    @Test
    void failsRegressionGateWhenCandidateHasMoreMissingEvents()
            throws Exception {
        ExperimentRunner runner = new ExperimentRunner();

        ExperimentRunner.ExperimentResult baselineResult =
                runner.run(
                        42L,
                        FaultInjector.FaultType.DUPLICATE,
                        10
                );

        Path baseline = tempDir.resolve("baseline-regression.json");
        new ObjectMapper().writeValue(
                baseline.toFile(),
                baselineResult
        );

        String output = runCapturingOutput(new String[]{
                "42",
                "DROP",
                "10",
                baseline.toString(),
                tempDir.resolve("regression-candidate-reports").toString()
        });

        assertTrue(output.contains("Missing event delta   : 1"), output);
        assertTrue(output.contains("Regression gate       : FAIL"), output);
        assertTrue(output.contains("TEST_EXIT_CODE=1"), output);
        assertTrue(output.contains("Missing events increased by 1"), output);
    }
}