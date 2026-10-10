package com.lyapunov.verifier;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Locale;

public class ExperimentReplay {

    private final ObjectMapper mapper = new ObjectMapper();

    public ExperimentRunner.ExperimentResult replay(Path reportPath)
            throws IOException {

        ExperimentRunner.ExperimentResult previous =
                mapper.readValue(
                        reportPath.toFile(),
                        ExperimentRunner.ExperimentResult.class
                );

        return new ExperimentRunner().run(
                previous.seed(),
                previous.faultType(),
                previous.requestedEventCount()
        );
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println(
                    "Usage: ExperimentReplay <experiment-report.json>"
            );
            System.exit(2);
        }

        try {
            Path reportPath = Path.of(args[0]);

            ExperimentReplay replay = new ExperimentReplay();
            ExperimentRunner.ExperimentResult result =
                    replay.replay(reportPath);

            System.out.println("=== LYAPUNOV EXPERIMENT REPLAY ===");
            System.out.println("Seed              : " + result.seed());
            System.out.println("Fault             : " + result.faultType());
            System.out.println("Expected events   : " + result.expectedCount());
            System.out.println("Observed events   : " + result.observedCount());
            System.out.println("Missing events    : " + result.missingEventIds());
            System.out.println("Duplicate events  : " + result.duplicateEventIds());
            System.out.println("Ordering issues   : " + result.orderingViolations());
            System.out.println("Reliable          : " + result.reliable());

        } catch (IOException | IllegalArgumentException e) {
            System.err.println("Replay failed: " + e.getMessage());
            System.exit(1);
        }
    }
}