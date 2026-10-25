package com.lyapunov.verifier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ExperimentSummaryCliTest {

    @TempDir
    Path tempDir;

    private void writeReport(String filename, String json) throws Exception {
        Files.writeString(
                tempDir.resolve(filename),
                json,
                StandardCharsets.UTF_8
        );
    }

    private String validReport() {
        return """
                {
                  "project": "LYAPUNOV",
                  "seed": 42,
                  "faultType": "DROP",
                  "requestedEventCount": 10,
                  "executedAt": "2026-10-11T10:00:00Z",
                  "expectedCount": 10,
                  "observedCount": 9,
                  "missingEventIds": ["event-1"],
                  "duplicateEventIds": [],
                  "orderingViolations": [],
                  "reliable": false
                }
                """;
    }

    private String runCli() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;

        try {
            System.setOut(new PrintStream(
                    output, true, StandardCharsets.UTF_8
            ));
            ExperimentSummaryCli.main(new String[]{tempDir.toString()});
        } finally {
            System.setOut(originalOut);
        }

        return output.toString(StandardCharsets.UTF_8);
    }

    @Test
    void summarizesValidReport() throws Exception {
        writeReport("valid.json", validReport());

        String text = runCli();

        assertTrue(text.contains("Valid reports        : 1"));
        assertTrue(text.contains("Unreliable experiments: 1"));
        assertTrue(text.contains("Missing event entries: 1"));
        assertTrue(text.contains("DROP: experiments=1"));
    }

    @Test
    void reportsMalformedJsonAndContinues() throws Exception {
        writeReport("valid.json", validReport());
        writeReport("broken.json", "{ invalid json");

        String text = runCli();

        assertTrue(text.contains("Valid reports        : 1"));
        assertTrue(text.contains("Invalid reports      : 1"));
        assertTrue(text.contains("broken.json"));
    }

    @Test
    void rejectsMissingRequiredFields() throws Exception {
        writeReport("incomplete.json", """
                {
                  "faultType": "DROP",
                  "reliable": false
                }
                """);

        String text = runCli();

        assertTrue(text.contains("Invalid reports      : 1"));
        assertTrue(text.contains("incomplete.json"));
    }

    @Test
    void handlesEmptyDirectory() {
        String text = runCli();

        assertTrue(text.contains("JSON files found     : 0"));
        assertTrue(text.contains("No valid reports to summarize."));
    }

    @Test
    void rejectsUnsupportedFaultType() throws Exception {
        writeReport(
                "unsupported.json",
                validReport().replace("\"DROP\"", "\"UNKNOWN\"")
        );

        String text = runCli();

        assertTrue(text.contains("Invalid reports      : 1"));
        assertTrue(text.contains("Unsupported faultType: UNKNOWN"));
    }

    @Test
    void rejectsReportWhoseReliableFlagContradictsItsViolations()
            throws Exception {
        String inconsistent = validReport()
                .replace("\"reliable\": false", "\"reliable\": true");

        writeReport("inconsistent.json", inconsistent);

        String text = runCli();

        assertTrue(text.contains("Invalid reports      : 1"));
        assertTrue(text.contains("reliable does not match"));
    }
    
    @Test
    void rejectsNegativeEventCounts() throws Exception {
        String invalid = validReport()
                .replace("\"expectedCount\": 10", "\"expectedCount\": -1");

        writeReport("negative-count.json", invalid);

        String text = runCli();

        assertTrue(text.contains("Invalid reports      : 1"));
        assertTrue(text.contains("negative-count.json"));
    }

    @Test
    void rejectsDecimalValuesForIntegerFields() throws Exception {
        String invalid = validReport()
                .replace("\"expectedCount\": 10", "\"expectedCount\": 10.5");

        writeReport("decimal-count.json", invalid);

        String text = runCli();

        assertTrue(text.contains("Invalid reports      : 1"));
        assertTrue(text.contains("decimal-count.json"));
    }

    @Test
    void rejectsStringValuesForIntegerFields() throws Exception {
        String invalid = validReport()
                .replace("\"expectedCount\": 10", "\"expectedCount\": \"10\"");

        writeReport("string-count.json", invalid);

        String text = runCli();

        assertTrue(text.contains("Invalid reports      : 1"));
        assertTrue(text.contains("string-count.json"));
    }

    @Test
    void rejectsIntegerValuesOutsideIntRange() throws Exception {
        String invalid = validReport()
                .replace("\"expectedCount\": 10",
                        "\"expectedCount\": 2147483648");

        writeReport("out-of-range-count.json", invalid);

        String text = runCli();

        assertTrue(text.contains("Invalid reports      : 1"));
        assertTrue(text.contains("out-of-range-count.json"));
    }

    @Test
    void rejectsMissingNumericFields() throws Exception {
        String invalid = validReport()
                .replace("\"expectedCount\": 10,\n", "");

        writeReport("missing-count.json", invalid);

        String text = runCli();

        assertTrue(text.contains("Invalid reports      : 1"));
        assertTrue(text.contains("missing-count.json"));
    }
}