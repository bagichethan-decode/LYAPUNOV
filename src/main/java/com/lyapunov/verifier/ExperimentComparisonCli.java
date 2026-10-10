package com.lyapunov.verifier;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class ExperimentComparisonCli {

    private final ObjectMapper mapper = new ObjectMapper();
    private final ExperimentComparator comparator =
            new ExperimentComparator();

    public ExperimentComparison compareReports(
            Path baselinePath,
            Path candidatePath
    ) throws IOException {

        if (!Files.isRegularFile(baselinePath)) {
            throw new IllegalArgumentException(
                    "Baseline report not found: " + baselinePath
            );
        }

        if (!Files.isRegularFile(candidatePath)) {
            throw new IllegalArgumentException(
                    "Candidate report not found: " + candidatePath
            );
        }

        ExperimentRunner.ExperimentResult baseline =
                mapper.readValue(
                        baselinePath.toFile(),
                        ExperimentRunner.ExperimentResult.class
                );

        ExperimentRunner.ExperimentResult candidate =
                mapper.readValue(
                        candidatePath.toFile(),
                        ExperimentRunner.ExperimentResult.class
                );

        return comparator.compare(
                baseline,
                candidate,
                baselinePath.toString(),
                candidatePath.toString()
        );
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println(
                    "Usage: ExperimentComparisonCli "
                    + "<baseline-report.json> <candidate-report.json>"
            );
            System.exit(2);
        }

        try {
            ExperimentComparisonCli cli =
                    new ExperimentComparisonCli();

            ExperimentComparison comparison =
                    cli.compareReports(
                            Path.of(args[0]),
                            Path.of(args[1])
                    );

            System.out.println("=== LYAPUNOV EXPERIMENT COMPARISON ===");
            System.out.println(
                    "Baseline report       : "
                    + comparison.baselineReport()
            );
            System.out.println(
                    "Candidate report      : "
                    + comparison.candidateReport()
            );
            System.out.println(
                    "Missing event delta   : "
                    + comparison.missingEventDelta()
            );
            System.out.println(
                    "Duplicate event delta : "
                    + comparison.duplicateEventDelta()
            );
            System.out.println(
                    "Ordering issue delta  : "
                    + comparison.orderingViolationDelta()
            );
            System.out.println(
                    "Outcome               : "
                    + comparison.outcome()
            );

        } catch (IOException | IllegalArgumentException e) {
            System.err.println(
                    "Comparison failed: " + e.getMessage()
            );
            System.exit(1);
        }
    }
}
