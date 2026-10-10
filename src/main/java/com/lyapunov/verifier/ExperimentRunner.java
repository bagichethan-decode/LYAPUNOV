package com.lyapunov.verifier;

import java.util.List;

public class ExperimentRunner {

    private final EventVerifier verifier = new EventVerifier();
    private final FaultInjector injector = new FaultInjector();

    public record ExperimentResult(
            long seed,
            FaultInjector.FaultType faultType,
            ReliabilityReport report
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
                java.util.stream.IntStream.rangeClosed(1, eventCount)
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

        return new ExperimentResult(seed, faultType, report);
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println(
                    "Usage: ExperimentRunner <seed> <DROP|DUPLICATE|REORDER> <event-count>"
            );
            System.exit(2);
        }

        try {
            long seed = Long.parseLong(args[0]);
            FaultInjector.FaultType faultType =
                    FaultInjector.FaultType.valueOf(
                            args[1].toUpperCase(java.util.Locale.ROOT)
                    );
            int eventCount = Integer.parseInt(args[2]);

            ExperimentResult result =
                    new ExperimentRunner().run(seed, faultType, eventCount);

            ReliabilityReport report = result.report();

            System.out.println("=== LYAPUNOV EXPERIMENT ===");
            System.out.println("Seed              : " + result.seed());
            System.out.println("Fault              : " + result.faultType());
            System.out.println("Expected events    : " + report.expectedCount());
            System.out.println("Observed events    : " + report.observedCount());
            System.out.println("Missing events     : " + report.missingEventIds());
            System.out.println("Duplicate events   : " + report.duplicateEventIds());
            System.out.println("Ordering violations: " + report.orderingViolations());
            System.out.println("Reliable           : " + report.isReliable());

            if (report.isReliable()) {
                System.err.println(
                        "ERROR: injected fault was not detected"
                );
                System.exit(1);
            }
        } catch (IllegalArgumentException e) {
            System.err.println("Invalid experiment: " + e.getMessage());
            System.exit(2);
        }
    }
}
