package com.lyapunov.verifier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class FaultInjector {

    public enum FaultType {
        DROP,
        DUPLICATE,
        REORDER
    }

    public record FaultResult(
            List<EventVerifier.ObservedEvent> events,
            FaultType faultType,
            long seed
    ) {}

    public FaultResult inject(
            List<EventVerifier.ObservedEvent> input,
            FaultType faultType,
            long seed
    ) {
        List<EventVerifier.ObservedEvent> events =
                new ArrayList<>(input);

        Random random = new Random(seed);

        switch (faultType) {
            case DROP -> {
                if (events.isEmpty()) {
                    throw new IllegalArgumentException(
                            "Cannot drop an event from an empty stream"
                    );
                }

                events.remove(random.nextInt(events.size()));
            }

            case DUPLICATE -> {
                if (events.isEmpty()) {
                    throw new IllegalArgumentException(
                            "Cannot duplicate an event from an empty stream"
                    );
                }

                int index = random.nextInt(events.size());
                events.add(index, events.get(index));
            }

            case REORDER -> {
                if (events.size() < 2) {
                    throw new IllegalArgumentException(
                            "At least two events are required to reorder"
                    );
                }

                int index = random.nextInt(events.size() - 1);

                Collections.swap(
                        events,
                        index,
                        index + 1
                );
            }
        }

        return new FaultResult(
                List.copyOf(events),
                faultType,
                seed
        );
    }
}