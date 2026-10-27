package com.lyapunov.verifier;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ExperimentComparisonReportTest {

    @TempDir
    Path tempDir;

    private final ObjectMapper mapper = new ObjectMapper();

    private ExperimentComparisonCli.ComparisonReport sampleReport(
            boolean passed
    ) {
        var comparison = new ExperimentComparison(
                "baseline.json",
                "candidate.json",
                1,
                0,
                0,
                "MIXED"
        );

        return new ExperimentComparisonCli.ComparisonReport(
                1,
                Instant.parse("2026-01-01T00:00:00Z").toString(),
                comparison,
                true,
                new ExperimentComparisonCli.RegressionThresholds(
                        0, 0, 0
                ),
                passed,
                passed ? List.of() : List.of("Missing events increased by 1")
        );
    }

    @Test
    void writesMachineReadableJsonReport() throws Exception {
        Path output = tempDir.resolve("nested/comparison.json");

        var cli = new ExperimentComparisonCli();
        cli.writeReportAtomically(output, sampleReport(false));

        assertTrue(Files.isRegularFile(output));

        var json = mapper.readTree(output.toFile());

        assertEquals(1, json.get("schemaVersion").asInt());
        assertTrue(json.get("regressionGateEnabled").asBoolean());
        assertFalse(json.get("regressionPassed").asBoolean());
        assertEquals(
                1,
                json.get("comparison")
                        .get("missingEventDelta").asInt()
        );
        assertEquals(
                1,
                json.get("violations").size()
        );
                assertEquals(
                0,
                json.get("thresholds")
                        .get("maxUnexpectedIncrease").asInt()
        );
    }

    @Test
    void replacesExistingReportAtomically() throws Exception {
        Path output = tempDir.resolve("comparison.json");
        var cli = new ExperimentComparisonCli();

        cli.writeReportAtomically(output, sampleReport(false));
        cli.writeReportAtomically(output, sampleReport(true));

        var json = mapper.readTree(output.toFile());

        assertTrue(json.get("regressionPassed").asBoolean());
        assertEquals(0, json.get("violations").size());
    }

    @Test
    void writesNullDecisionWhenGateIsDisabled() throws Exception {
        Path output = tempDir.resolve("comparison.json");

        var report = new ExperimentComparisonCli.ComparisonReport(
                1,
                Instant.parse("2026-01-01T00:00:00Z").toString(),
                new ExperimentComparison(
                        "baseline.json",
                        "candidate.json",
                        0,
                        0,
                        0,
                        "MIXED"
                ),
                false,
                new ExperimentComparisonCli.RegressionThresholds(
                        0, 0, 0
                ),
                null,
                List.of()
        );

        new ExperimentComparisonCli()
                .writeReportAtomically(output, report);

        var json = mapper.readTree(output.toFile());

        assertFalse(json.get("regressionGateEnabled").asBoolean());
        assertTrue(json.get("regressionPassed").isNull());
        assertEquals(0, json.get("violations").size());
    }
}
