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
        Set<String> observedIds = new HashSet<>();
        List<String> missing = new ArrayList<>();
        List<String> duplicates = new ArrayList<>();

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
            orderingViolations
        );
    }

    private List<String> detectOrderingViolations(
        List<ObservedEvent> observed
    ) {
        List<String> violations = new ArrayList<>();

        // Track the latest sequence number observed for each aggregate.
        Map<String, Long> lastSequenceByAggregate = new HashMap<>();

        // Track which event ID first occupied each aggregate sequence.
        Map<String, Map<Long, String>> eventIdByAggregateSequence =
            new HashMap<>();

        for (ObservedEvent event : observed) {
            String aggregateKey =
                event.aggregateType() + ":" + event.aggregateId();

            long sequenceNumber = event.sequenceNumber();

            // Check whether this sequence number already belongs
            // to a different event for the same aggregate.
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

            // Check whether the event sequence moves backwards.
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

            // Preserve the highest sequence observed so that a
            // regression cannot hide a later ordering violation.
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