package com.lyapunov.benchmark;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class LatencyPercentilesTest {
    @Test void emptySamplesReturnZero() {
        assertEquals(0L, InboxLoadBenchmark.percentile(List.of(), 0.95));
    }
    @Test void percentileUsesNearestRank() {
        List<Long> values = List.of(10L, 20L, 30L, 40L, 50L);
        assertEquals(10L, InboxLoadBenchmark.percentile(values, 0.01));
        assertEquals(30L, InboxLoadBenchmark.percentile(values, 0.50));
        assertEquals(50L, InboxLoadBenchmark.percentile(values, 0.99));
    }
    @Test void invalidQuantileIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> InboxLoadBenchmark.percentile(List.of(1L), 1.1));
    }
}
