package com.lyapunov.verifier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EventVerifier {

    public ReliabilityReport verify(
        List<ExpectedEvent> expected,
        List<ObservedEvent> observed
    ) {
        Set<String> observedIds = new HashSet<>();
        Set<String> expectedIds = new HashSet<>();

        List<String> missing = new ArrayList<>();
        List<String> duplicates = new ArrayList<>();

        // Collect all expected event IDs.
        for (ExpectedEvent event : expected) {
            expectedIds.add(event.eventId());
        }

        // Detect unexpected events while preserving their encounter order.
        Set<String> unexpected = new LinkedHashSet<>();

        for (ObservedEvent event : observed) {
            if (!expectedIds.contains(event.eventId())) {
                unexpected.add(event.eventId());
            }
        }

        // Detect duplicate deliveries by event ID.
        for (ObservedEvent event : observed) {
            if (!observedIds.add(event.eventId())) {
                duplicates.add(event.eventId());
            }
        }

        // Detect expected events that were never observed.
        for (ExpectedEvent event : expected) {
            if (!observedIds.contains(event.eventId())) {
                missing.add(event.eventId());
            }
        }

        // Detect ordering regressions and conflicting sequence numbers.
        List<String> orderingViolations =
            detectOrderingViolations(observed);

        return new ReliabilityReport(
            expected.size(),
            observed.size(),
            missing,
            duplicates,
            orderingViolations,
            new ArrayList<>(unexpected)
        );
    }

    private List<String> detectOrderingViolations(
        List<ObservedEvent> observed
    ) {
        List<String> violations = new ArrayList<>();

        // Track the latest sequence number for each aggregate.
        Map<AggregateKey, Long> lastSequenceByAggregate =
            new HashMap<>();

        // Track the event ID assigned to each aggregate sequence.
        Map<AggregateKey, Map<Long, String>> eventIdByAggregateSequence =
            new HashMap<>();

        for (ObservedEvent event : observed) {

            AggregateKey aggregateKey = new AggregateKey(
                event.aggregateType(),
                event.aggregateId()
            );

            long sequenceNumber = event.sequenceNumber();

            // Detect different events occupying the same sequence.
            Map<Long, String> eventsBySequence =
                eventIdByAggregateSequence.computeIfAbsent(
                    aggregateKey,
                    key -> new HashMap<>()
                );

            String existingEventId =
                eventsBySequence.putIfAbsent(
                    sequenceNumber,
                    event.eventId()
                );

            if (
                existingEventId != null
                    && !existingEventId.equals(event.eventId())
            ) {
                violations.add(
                    aggregateKey
                        + " has conflicting events "
                        + existingEventId
                        + " and "
                        + event.eventId()
                        + " at sequence "
                        + sequenceNumber
                );
            }

            // Detect sequence numbers moving backwards.
            Long previousSequence =
                lastSequenceByAggregate.get(aggregateKey);

            if (
                previousSequence != null
                    && sequenceNumber < previousSequence
            ) {
                violations.add(
                    aggregateKey
                        + " expected sequence after "
                        + previousSequence
                        + " but observed "
                        + sequenceNumber
                );
            }

            // Preserve the highest sequence observed.
            // A regression must not lower the stored high-water mark.
            if (
                previousSequence == null
                    || sequenceNumber > previousSequence
            ) {
                lastSequenceByAggregate.put(
                    aggregateKey,
                    sequenceNumber
                );
            }
        }

        return violations;
    }

    // Structured keys prevent collisions between aggregate types and IDs.
    private record AggregateKey(
        String aggregateType,
        String aggregateId
    ) {}

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
