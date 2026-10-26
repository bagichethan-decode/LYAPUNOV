package com.lyapunov.verifier;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExperimentRunnerReportTest {

@TempDir
Path tempDir;

private final ExperimentRunner runner = new ExperimentRunner();

@Test
void writesCompleteReadableJsonWithoutLeavingTemporaryFiles()
        throws Exception {

    ExperimentRunner.ExperimentResult expected = runner.run(
            42L,
            FaultInjector.FaultType.DROP,
            10
    );

    Path report = runner.writeJsonReport(expected, tempDir);

    assertTrue(Files.isRegularFile(report));
    assertTrue(report.getFileName().toString().endsWith(".json"));

    ObjectMapper mapper = new ObjectMapper();

    ExperimentRunner.ExperimentResult actual = mapper.readValue(
            report.toFile(),
            ExperimentRunner.ExperimentResult.class
    );

    assertEquals(expected, actual);

    try (var files = Files.list(tempDir)) {
        List<Path> contents = files.toList();

        assertEquals(1, contents.size());
        assertEquals(report, contents.get(0));
    }
}

@Test
void repeatedWritesProduceDifferentReportPaths() throws Exception {

    ExperimentRunner.ExperimentResult result = runner.run(
            42L,
            FaultInjector.FaultType.REORDER,
            10
    );

    Path first = runner.writeJsonReport(result, tempDir);
    Path second = runner.writeJsonReport(result, tempDir);

    assertNotEquals(first, second);
    assertTrue(Files.isRegularFile(first));
    assertTrue(Files.isRegularFile(second));

    try (var files = Files.list(tempDir)) {
        assertEquals(2, files.count());
    }
}

@Test
void rejectsNullResultWithoutCreatingOutputDirectory() {

    Path outputDirectory = tempDir.resolve("not-created");

    assertThrows(
            IllegalArgumentException.class,
            () -> runner.writeJsonReport(null, outputDirectory)
    );

    assertFalse(Files.exists(outputDirectory));
}

@Test
void rejectsNullOutputDirectory() {

    ExperimentRunner.ExperimentResult result = runner.run(
            42L,
            FaultInjector.FaultType.DROP,
            10
    );

    assertThrows(
            IllegalArgumentException.class,
            () -> runner.writeJsonReport(result, null)
    );
}

@Test
void preservesUnexpectedEventIdsInJsonReport() throws Exception {

    ExperimentRunner.ExperimentResult original = runner.run(
            42L,
            FaultInjector.FaultType.DROP,
            10
    );

    ObjectMapper mapper = new ObjectMapper();

    ObjectNode json = mapper.valueToTree(original);

    json.set(
            "unexpectedEventIds",
            mapper.valueToTree(
                    List.of("unexpected-001", "unexpected-002")
            )
    );

    ExperimentRunner.ExperimentResult result = mapper.treeToValue(
            json,
            ExperimentRunner.ExperimentResult.class
    );

    Path report = runner.writeJsonReport(result, tempDir);

    ExperimentRunner.ExperimentResult restored = mapper.readValue(
            report.toFile(),
            ExperimentRunner.ExperimentResult.class
    );

    assertEquals(
            List.of("unexpected-001", "unexpected-002"),
            restored.unexpectedEventIds()
    );

    assertEquals(result, restored);
}

}
