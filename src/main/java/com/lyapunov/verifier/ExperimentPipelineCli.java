package com.lyapunov.verifier;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

public class ExperimentPipelineCli {

    public static int run(String[] args) {
        if (args.length < 4 || args.length > 5) {
            System.err.println(
                    "Usage: ExperimentPipelineCli <seed> "
                    + "<DROP|DUPLICATE|REORDER> <event-count> "
                    + "<baseline-report.json> [report-directory]"
            );
            return 2;
        }

        try {
            long seed = Long.parseLong(args[0]);

            FaultInjector.FaultType faultType =
                    FaultInjector.FaultType.valueOf(
                            args[1].toUpperCase(Locale.ROOT)
                    );

            int eventCount = Integer.parseInt(args[2]);
            Path baselinePath = Path.of(args[3]);

            Path reportDirectory = args.length == 5
                    ? Path.of(args[4])
                    : Path.of("reports", "experiments");

            if (eventCount < 2) {
                throw new IllegalArgumentException(
                        "At least two events are required"
                );
            }

            if (!Files.isRegularFile(baselinePath)) {
                throw new IllegalArgumentException(
                        "Baseline report not found: " + baselinePath
                );
            }

            ExperimentRunner runner = new ExperimentRunner();

            ExperimentRunner.ExperimentResult candidate =
                    runner.run(seed, faultType, eventCount);

            Path candidatePath =
                    runner.writeJsonReport(candidate, reportDirectory);

            ExperimentComparisonCli comparisonCli =
                    new ExperimentComparisonCli();

            ExperimentComparison comparison =
                    comparisonCli.compareReports(
                            baselinePath,
                            candidatePath
                    );

            RegressionGate gate = new RegressionGate(0, 0, 0);
            RegressionDecision decision = gate.evaluate(comparison);

            System.out.println("=== LYAPUNOV EXPERIMENT PIPELINE ===");
            System.out.println("Baseline report       : " + baselinePath);
            System.out.println("Candidate report      : " + candidatePath);
            System.out.println("Fault                 : " + candidate.faultType());
            System.out.println("Seed                  : " + candidate.seed());
            System.out.println("Missing event delta   : "
                    + comparison.missingEventDelta());
            System.out.println("Duplicate event delta : "
                    + comparison.duplicateEventDelta());
            System.out.println("Ordering issue delta  : "
                    + comparison.orderingViolationDelta());
            System.out.println("Comparison outcome    : " + comparison.outcome());
            System.out.println("Regression gate       : "
                    + (decision.passed() ? "PASS" : "FAIL"));

            for (String violation : decision.violations()) {
                System.out.println("Violation             : " + violation);
            }

            return decision.passed() ? 0 : 1;

        } catch (IOException | IllegalArgumentException e) {
            System.err.println("Pipeline failed: " + e.getMessage());
            return 2;
        }
    }

    public static void main(String[] args) {
        int exitCode = run(args);

        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }
}