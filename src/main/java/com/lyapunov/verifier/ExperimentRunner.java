package com.lyapunov.verifier;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.stream.IntStream;

public class ExperimentRunner {

    private final EventVerifier verifier = new EventVerifier();
    private final FaultInjector injector = new FaultInjector();

    public record ExperimentResult(
            String project,
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

    public Path writeJsonReport(
            ExperimentResult result,
            Path outputDirectory
    ) throws IOException {

        Files.createDirectories(outputDirectory);

        String filename = String.format(
                Locale.ROOT,
                "experiment-%s-seed-%d-%d.json",
                result.faultType().name().toLowerCase(Locale.ROOT),
                result.seed(),
                System.currentTimeMillis()
        );

        Path outputFile = outputDirectory.resolve(filename);

        ObjectMapper mapper = new ObjectMapper()
                .enable(SerializationFeature.INDENT_OUTPUT);

        mapper.writeValue(outputFile.toFile(), result);

        return outputFile;
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
            System.out.println("Seed              : " + result.seed());
            System.out.println("Fault             : " + result.faultType());
            System.out.println("Expected events   : " + result.expectedCount());
            System.out.println("Observed events   : " + result.observedCount());
            System.out.println("Missing events    : " + result.missingEventIds());
            System.out.println("Duplicate events  : " + result.duplicateEventIds());
            System.out.println("Ordering issues   : " + result.orderingViolations());
            System.out.println("Reliable          : " + result.reliable());

            Path reportFile =
                    runner.writeJsonReport(result, reportDirectory);

            System.out.println(
                    "JSON report       : " + reportFile.toAbsolutePath()
            );

            if (result.reliable()) {
                System.err.println(
                        "ERROR: injected fault was not detected"
                );
                System.exit(1);
            }

        } catch (IllegalArgumentException e) {
            System.err.println("Invalid experiment: " + e.getMessage());
            System.exit(2);

        } catch (IOException e) {
            System.err.println(
                    "Could not write experiment report: " + e.getMessage()
            );
            System.exit(1);
        }
    }
}