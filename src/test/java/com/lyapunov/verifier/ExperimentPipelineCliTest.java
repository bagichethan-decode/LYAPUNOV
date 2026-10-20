package com.lyapunov.verifier;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

        ObjectMapper mapper = new ObjectMapper();

        mapper.writeValue(
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
                output,
                true,
                StandardCharsets.UTF_8
        )) {

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
                42L,
                FaultInjector.FaultType.DROP,
                10
        );

        Path reportDirectory =
                tempDir.resolve("candidate-reports");

        String output = runCapturingOutput(new String[]{
                "42",
                "DROP",
                "10",
                baseline.toString(),
                reportDirectory.toString()
        });

        assertTrue(
                output.contains("Comparison outcome    : UNCHANGED"),
                output
        );

        assertTrue(
                output.contains("Regression gate       : PASS"),
                output
        );

        assertTrue(
                output.contains("TEST_EXIT_CODE=0"),
                output
        );

        try (var files = Files.list(reportDirectory)) {

            assertTrue(
                    files.anyMatch(path ->
                            path.getFileName()
                                    .toString()
                                    .endsWith(".json")
                    )
            );
        }
    }

    @Test
    void rejectsMissingBaseline() {

        Path missingBaseline =
                tempDir.resolve("missing.json");

        String output = runCapturingOutput(new String[]{
                "42",
                "DROP",
                "10",
                missingBaseline.toString(),
                tempDir.resolve("candidate-reports").toString()
        });

        assertTrue(
                output.contains("Baseline report not found"),
                output
        );

        assertTrue(
                output.contains("TEST_EXIT_CODE=2"),
                output
        );
    }

    @Test
    void rejectsInvalidArguments() {

        String output = runCapturingOutput(new String[]{
                "42",
                "DROP"
        });

        assertTrue(
                output.contains("Usage: ExperimentPipelineCli"),
                output
        );

        assertTrue(
                output.contains("TEST_EXIT_CODE=2"),
                output
        );
    }

    @Test
    void rejectsUnsupportedFaultType() throws Exception {

        Path baseline = createBaseline(
                42L,
                FaultInjector.FaultType.DROP,
                10
        );

        String output = runCapturingOutput(new String[]{
                "42",
                "UNKNOWN",
                "10",
                baseline.toString(),
                tempDir.resolve("candidate-reports").toString()
        });

        assertTrue(
                output.contains("Pipeline failed"),
                output
        );

        assertTrue(
                output.contains("TEST_EXIT_CODE=2"),
                output
        );
    }

    @Test
    void failsRegressionGateWhenCandidateHasMoreMissingEvents()
            throws Exception {

        /*
         * Use matching experiment parameters for both reports:
         * seed = 42
         * fault type = DROP
         * event count = 10
         */
        Path baseline = createBaseline(
                42L,
                FaultInjector.FaultType.DROP,
                10
        );

        /*
         * Prepare a baseline report with no missing events.
         *
         * The candidate will run the same DROP experiment and should
         * report one missing event. This simulates a regression without
         * changing the experiment parameters.
         */
        ObjectMapper mapper = new ObjectMapper();

        ObjectNode baselineJson = (ObjectNode)
                mapper.readTree(baseline.toFile());

        baselineJson.set(
                "missingEventIds",
                mapper.createArrayNode()
        );

        baselineJson.put(
                "observedCount",
                10
        );

        baselineJson.put(
                "reliable",
                true
        );

        mapper.writeValue(
                baseline.toFile(),
                baselineJson
        );

        Path candidateReportDirectory =
                tempDir.resolve("regression-candidate-reports");

        String output = runCapturingOutput(new String[]{
                "42",
                "DROP",
                "10",
                baseline.toString(),
                candidateReportDirectory.toString()
        });

        assertTrue(
                output.contains("Missing event delta   : 1"),
                output
        );

        assertTrue(
                output.contains("Regression gate       : FAIL"),
                output
        );

        assertTrue(
                output.contains("TEST_EXIT_CODE=1"),
                output
        );

        assertTrue(
                output.contains("Missing events increased by 1"),
                output
        );
    }
}