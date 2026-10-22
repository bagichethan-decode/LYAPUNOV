package com.lyapunov.verifier;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RegressionDecisionTest {

    @Test
    void defensivelyCopiesAndExposesAnImmutableViolationsList() {
        var source = new ArrayList<>(List.of("Ordering violations increased"));
        var decision = new RegressionDecision(false, source);

        source.add("Duplicate events increased");

        assertEquals(
                List.of("Ordering violations increased"),
                decision.violations()
        );

        assertThrows(
                UnsupportedOperationException.class,
                () -> decision.violations().add("External mutation")
        );
    }

    @Test
    void rejectsNullViolationsList() {
        assertThrows(
                NullPointerException.class,
                () -> new RegressionDecision(true, null)
        );
    }
}
