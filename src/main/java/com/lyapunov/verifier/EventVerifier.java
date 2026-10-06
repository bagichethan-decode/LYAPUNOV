package com.lyapunov.verifier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EventVerifier {

    public ReliabilityReport verify(
        List<ExpectedEvent> expected,
        List<ObservedEvent> observed
    ) {
        Map<String, ExpectedEvent> expectedById = new HashMap<>();

        for (ExpectedEvent event : expected) {
            expectedById.put(event.eventId(), event);
        }

        Set<String> observedIds = new HashSet<>();
        List<String> missing = new ArrayList<>();
        List<String> duplicates = new ArrayList<>();

        for (ObservedEvent event : observed) {
            if (!observedIds.add(event.eventId())) {
                duplicates.add(event.eventId());
            }
        }

        for (ExpectedEvent event : expected) {
            if (!observedIds.contains(event.eventId())) {
                missing.add(event.eventId());
            }
        }

        List<String> orderingViolations = detectOrderingViolations(observed);

        return new ReliabilityReport(
            expected.size(),
            observed.size(),
            missing,
            duplicates,
            orderingViolations
        );
    }

    private List<String> detectOrderingViolations(
        List<ObservedEvent> observed
    ) {
        List<String> violations = new ArrayList<>();
        Map<String, Long> lastSequenceByAggregate = new HashMap<>();

        for (ObservedEvent event : observed) {
            String aggregateKey =
                event.aggregateType() + ":" + event.aggregateId();

            Long previous = lastSequenceByAggregate.get(aggregateKey);

            if (previous != null && event.sequenceNumber() < previous) {
                violations.add(
                    aggregateKey
                        + " expected sequence after "
                        + previous
                        + " but observed "
                        + event.sequenceNumber()
                );
            }

            lastSequenceByAggregate.put(
                aggregateKey,
                event.sequenceNumber()
            );
        }

        return violations;
    }

    public record ExpectedEvent(
        String eventId,
        String aggregateType,
        String aggregateId,
        long sequenceNumber
    ) {}

    public record ObservedEvent(
        String eventId,
        String aggregateType,
        String aggregateId,
        long sequenceNumber
    ) {}
}
