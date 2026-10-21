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
        if (args.length < 2) {
            printUsage();
            System.exit(2);
            return;
        }

        int maxMissing = 0;
        int maxDuplicates = 0;
        int maxOrdering = 0;
        boolean failOnRegression = false;

        try {
            for (int i = 2; i < args.length; i++) {
                switch (args[i]) {
                    case "--fail-on-regression" ->
                            failOnRegression = true;

                    case "--max-missing" ->
                            maxMissing = parseThreshold(args, ++i, "--max-missing");

                    case "--max-duplicates" ->
                            maxDuplicates = parseThreshold(args, ++i, "--max-duplicates");

                    case "--max-ordering" ->
                            maxOrdering = parseThreshold(args, ++i, "--max-ordering");

                    default -> throw new IllegalArgumentException(
                            "Unknown option: " + args[i]
                    );
                }
            }

            if ((maxMissing > 0 || maxDuplicates > 0 || maxOrdering > 0)
                    && !failOnRegression) {
                throw new IllegalArgumentException(
                        "Threshold options require --fail-on-regression"
                );
            }

            ExperimentComparisonCli cli =
                    new ExperimentComparisonCli();

            ExperimentComparison comparison =
                    cli.compareReports(
                            Path.of(args[0]),
                            Path.of(args[1])
                    );

            System.out.println("=== LYAPUNOV EXPERIMENT COMPARISON ===");
            System.out.println("Baseline report       : "
                    + comparison.baselineReport());
            System.out.println("Candidate report      : "
                    + comparison.candidateReport());
            System.out.println("Missing event delta   : "
                    + comparison.missingEventDelta());
            System.out.println("Duplicate event delta : "
                    + comparison.duplicateEventDelta());
            System.out.println("Ordering issue delta  : "
                    + comparison.orderingViolationDelta());
            System.out.println("Outcome               : "
                    + comparison.outcome());

            if (failOnRegression) {
                RegressionGate gate = new RegressionGate(
                        maxMissing,
                        maxDuplicates,
                        maxOrdering
                );

                RegressionDecision decision =
                        gate.evaluate(comparison);

                System.out.println("Maximum missing increase   : " + maxMissing);
                System.out.println("Maximum duplicate increase : " + maxDuplicates);
                System.out.println("Maximum ordering increase  : " + maxOrdering);
                System.out.println("Regression gate            : "
                        + (decision.passed() ? "PASS" : "FAIL"));

                for (String violation : decision.violations()) {
                    System.out.println("Violation                  : "
                            + violation);
                }

                if (!decision.passed()) {
                    System.exit(1);
                }
            }

        } catch (IOException | IllegalArgumentException e) {
            System.err.println(
                    "Comparison failed: " + e.getMessage()
            );
            printUsage();
            System.exit(2);
        }
    }

    static int parseThreshold(
            String[] args,
            int index,
            String option
    ) {
        if (index >= args.length) {
            throw new IllegalArgumentException(
                    "Missing integer value for " + option
            );
        }

        final int value;

        try {
            value = Integer.parseInt(args[index]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Threshold for " + option + " must be an integer"
            );
        }

        if (value < 0) {
            throw new IllegalArgumentException(
                    "Threshold for " + option + " cannot be negative"
            );
        }

        return value;
    }

    private static void printUsage() {
        System.err.println(
                "Usage: ExperimentComparisonCli "
                + "<baseline-report.json> "
                + "<candidate-report.json> "
                + "[--fail-on-regression] "
                + "[--max-missing N] "
                + "[--max-duplicates N] "
                + "[--max-ordering N]"
        );
    }
}