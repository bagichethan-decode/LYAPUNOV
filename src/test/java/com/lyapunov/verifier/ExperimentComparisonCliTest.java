package com.lyapunov.verifier;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ExperimentComparisonCliTest {

    @Test
    void rejectsMissingThresholdValue() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ExperimentComparisonCli.parseThreshold(
                        new String[]{"--max-missing"},
                        1,
                        "--max-missing"
                )
        );
    }

    @Test
    void rejectsNegativeThreshold() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ExperimentComparisonCli.parseThreshold(
                        new String[]{"--max-missing", "-1"},
                        1,
                        "--max-missing"
                )
        );
    }

    @Test
    void rejectsNonIntegerThreshold() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ExperimentComparisonCli.parseThreshold(
                        new String[]{"--max-missing", "abc"},
                        1,
                        "--max-missing"
                )
        );
    }

    @Test
    void acceptsZeroThreshold() {
        assertEquals(
                0,
                ExperimentComparisonCli.parseThreshold(
                        new String[]{"--max-missing", "0"},
                        1,
                        "--max-missing"
                )
        );
    }

    @Test
    void acceptsPositiveThreshold() {
        assertEquals(
                5,
                ExperimentComparisonCli.parseThreshold(
                        new String[]{"--max-missing", "5"},
                        1,
                        "--max-missing"
                )
        );
    }

    @Test
    void acceptsUnexpectedEventThreshold() {
        assertEquals(
                3,
                ExperimentComparisonCli.parseThreshold(
                        new String[]{"--max-unexpected", "3"},
                        1,
                        "--max-unexpected"
                )
        );
    }

    @Test
    void rejectsNegativeUnexpectedEventThreshold() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ExperimentComparisonCli.parseThreshold(
                        new String[]{"--max-unexpected", "-1"},
                        1,
                        "--max-unexpected"
                )
        );
    }
}