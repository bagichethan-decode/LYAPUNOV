package com.lyapunov.verifier;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class ExperimentRunner {

    private static final int REPORT_SCHEMA_VERSION = 1;

    private final EventVerifier verifier = new EventVerifier();
    private final FaultInjector injector = new FaultInjector();

    public record ExperimentResult(
            String project,
            int schemaVersion,
            String sourceRevision,
            long seed,
            FaultInjector.FaultType faultType,
            int requestedEventCount,
            String executedAt,
            int expectedCount,
            int observedCount,
            List<String> missingEventIds,
            List<String> duplicateEventIds,
            List<String> orderingViolations,
            boolean reliable
    ) {}

    public ExperimentResult run(
            long seed,
            FaultInjector.FaultType faultType,
            int eventCount
    ) {
        if (faultType == null) {
            throw new IllegalArgumentException(
                    "faultType must not be null"
            );
        }

        if (eventCount < 2) {
            throw new IllegalArgumentException(
                    "At least two events are required"
            );
        }

        List<EventVerifier.ExpectedEvent> expected =
                IntStream.rangeClosed(1, eventCount)
                        .mapToObj(i -> new EventVerifier.ExpectedEvent(
                                "event-" + i,
                                "ORDER",
                                "order-1",
                                i
                        ))
                        .toList();

        List<EventVerifier.ObservedEvent> observed =
                expected.stream()
                        .map(event -> new EventVerifier.ObservedEvent(
                                event.eventId(),
                                event.aggregateType(),
                                event.aggregateId(),
                                event.sequenceNumber()
                        ))
                        .toList();

        FaultInjector.FaultResult injected =
                injector.inject(observed, faultType, seed);

        ReliabilityReport report =
                verifier.verify(expected, injected.events());

        return new ExperimentResult(
                "LYAPUNOV",
                REPORT_SCHEMA_VERSION,
                resolveSourceRevision(),
                seed,
                faultType,
                eventCount,
                Instant.now().toString(),
                report.expectedCount(),
                report.observedCount(),
                report.missingEventIds(),
                report.duplicateEventIds(),
                report.orderingViolations(),
                report.isReliable()
        );
    }

    /**
     * Identifies the source revision used for this experiment.
     *
     * Resolution order:
     * 1. GitHub Actions commit SHA.
     * 2. Explicit lyapunov.sourceRevision system property.
     * 3. Current Git checkout.
     *
     * Returns "unknown" when the revision cannot be determined.
     */
    static String resolveSourceRevision() {
        String githubSha = System.getenv("GITHUB_SHA");

        if (isUsableRevision(githubSha)) {
            return githubSha.trim();
        }

        String configuredRevision =
                System.getProperty("lyapunov.sourceRevision");

        if (isUsableRevision(configuredRevision)) {
            return configuredRevision.trim();
        }

        Process process = null;

        try {
            process = new ProcessBuilder(
                    "git",
                    "rev-parse",
                    "--verify",
                    "HEAD"
            )
                    .redirectErrorStream(true)
                    .start();

            boolean completed = process.waitFor(
                    2,
                    TimeUnit.SECONDS
            );

            if (!completed) {
                process.destroyForcibly();
                return "unknown";
            }

            String output = new String(
                    process.getInputStream().readAllBytes(),
                    StandardCharsets.UTF_8
            ).trim();

            if (process.exitValue() == 0
                    && isUsableRevision(output)) {
                return output;
            }

        } catch (IOException e) {
            // Git may not be installed or the process may not be
            // running inside a Git checkout.
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            if (process != null) {
                process.destroyForcibly();
            }

            return "unknown";
        }

        return "unknown";
    }

    private static boolean isUsableRevision(String revision) {
        return revision != null
                && !revision.isBlank()
                && !"unknown".equalsIgnoreCase(revision.trim());
    }

    public Path writeJsonReport(
            ExperimentResult result,
            Path outputDirectory
    ) throws IOException {

        if (result == null) {
            throw new IllegalArgumentException(
                    "result must not be null"
            );
        }

        if (outputDirectory == null) {
            throw new IllegalArgumentException(
                    "outputDirectory must not be null"
            );
        }

        Files.createDirectories(outputDirectory);

        String filename = String.format(
                Locale.ROOT,
                "experiment-%s-seed-%d-%d-%s.json",
                result.faultType().name().toLowerCase(Locale.ROOT),
                result.seed(),
                System.currentTimeMillis(),
                UUID.randomUUID()
        );

        Path outputFile = outputDirectory.resolve(filename);

        Path temporaryFile = Files.createTempFile(
                outputDirectory,
                ".experiment-report-",
                ".tmp"
        );

        ObjectMapper mapper = new ObjectMapper()
                .enable(SerializationFeature.INDENT_OUTPUT);

        boolean published = false;

        try {
            // Write to a temporary file rather than exposing partial JSON.
            mapper.writeValue(temporaryFile.toFile(), result);

            // Parse the report back and verify the complete result.
            ExperimentResult validated = mapper.readValue(
                    temporaryFile.toFile(),
                    ExperimentResult.class
            );

            if (!result.equals(validated)) {
                throw new IOException(
                        "Written experiment report failed "
                                + "round-trip validation"
                );
            }

            // Publish only after validation succeeds.
            try {
                Files.move(
                        temporaryFile,
                        outputFile,
                        StandardCopyOption.ATOMIC_MOVE
                );
            } catch (AtomicMoveNotSupportedException exception) {
                Files.move(temporaryFile, outputFile);
            }

            published = true;

            return outputFile;

        } finally {
            // Clean up incomplete reports if publication fails.
            if (!published) {
                Files.deleteIfExists(temporaryFile);
            }
        }
    }

    public static void main(String[] args) {
        if (args.length < 3 || args.length > 4) {
            System.err.println(
                    "Usage: ExperimentRunner <seed> "
                            + "<DROP|DUPLICATE|REORDER> <event-count> "
                            + "[report-directory]"
            );

            System.exit(2);
        }

        try {
            long seed = Long.parseLong(args[0]);

            FaultInjector.FaultType faultType =
                    FaultInjector.FaultType.valueOf(
                            args[1].toUpperCase(Locale.ROOT)
                    );

            int eventCount = Integer.parseInt(args[2]);

            Path reportDirectory = args.length == 4
                    ? Path.of(args[3])
                    : Path.of("reports", "experiments");

            ExperimentRunner runner = new ExperimentRunner();

            ExperimentResult result =
                    runner.run(seed, faultType, eventCount);

            System.out.println("=== LYAPUNOV EXPERIMENT ===");
            System.out.println(
                    "Schema version    : " + result.schemaVersion()
            );
            System.out.println(
                    "Source revision   : " + result.sourceRevision()
            );
            System.out.println("Seed              : " + result.seed());
            System.out.println("Fault             : " + result.faultType());
            System.out.println(
                    "Expected events   : " + result.expectedCount()
            );
            System.out.println(
                    "Observed events   : " + result.observedCount()
            );
            System.out.println(
                    "Missing events    : " + result.missingEventIds()
            );
            System.out.println(
                    "Duplicate events  : " + result.duplicateEventIds()
            );
            System.out.println(
                    "Ordering issues   : " + result.orderingViolations()
            );
            System.out.println(
                    "Reliable          : " + result.reliable()
            );

            Path reportFile =
                    runner.writeJsonReport(result, reportDirectory);

            System.out.println(
                    "JSON report       : "
                            + reportFile.toAbsolutePath()
            );

            if (result.reliable()) {
                System.err.println(
                        "ERROR: injected fault was not detected"
                );

                System.exit(1);
            }

        } catch (IllegalArgumentException e) {
            System.err.println(
                    "Invalid experiment: " + e.getMessage()
            );

            System.exit(2);

        } catch (IOException e) {
            System.err.println(
                    "Could not write experiment report: "
                            + e.getMessage()
            );

            System.exit(1);
        }
    }
}