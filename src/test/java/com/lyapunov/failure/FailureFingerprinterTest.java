package com.lyapunov.failure;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FailureFingerprinterTest {

    @Test
    void groupsFailuresWithDifferentDynamicIdentifiers() {
        var first = FailureFingerprinter.fingerprint(
            new RuntimeException(
                "Connection to order 123456 failed for " +
                "550e8400-e29b-41d4-a716-446655440000"
            )
        );

        var second = FailureFingerprinter.fingerprint(
            new RuntimeException(
                "Connection to order 987654 failed for " +
                "123e4567-e89b-12d3-a456-426614174000"
            )
        );

        assertEquals(first.fingerprint(), second.fingerprint());
    }

    @Test
    void distinguishesExceptionTypes() {
        var first = FailureFingerprinter.fingerprint(
            new IllegalStateException("database unavailable")
        );

        var second = FailureFingerprinter.fingerprint(
            new IllegalArgumentException("database unavailable")
        );

        assertNotEquals(first.fingerprint(), second.fingerprint());
    }

    @Test
    void extractsRootCause() {
        var failure = new RuntimeException(
            "publisher failed",
            new IllegalStateException("broker unavailable")
        );

        var result = FailureFingerprinter.fingerprint(failure);

        assertEquals("IllegalStateException", result.exceptionType());
        assertEquals("broker unavailable", result.normalizedMessage());
    }

    @Test
    void redactsCredentials() {
        var result = FailureFingerprinter.fingerprint(
            new RuntimeException(
                "Request failed password=hunter2 token=abc123"
            )
        );

        assertFalse(result.normalizedMessage().contains("hunter2"));
        assertFalse(result.normalizedMessage().contains("abc123"));
        assertTrue(result.normalizedMessage().contains("<redacted>"));
    }

    @Test
    void redactsBearerCredentials() {
        var result = FailureFingerprinter.fingerprint(
            new RuntimeException("Authentication failed Bearer very-secret-value")
        );

        assertFalse(result.normalizedMessage().contains("very-secret-value"));
    }

    @Test
    void normalizesCaseAndWhitespace() {
        var first = FailureFingerprinter.fingerprint(
            new RuntimeException(" BROKER   UNAVAILABLE ")
        );

        var second = FailureFingerprinter.fingerprint(
            new RuntimeException("broker unavailable")
        );

        assertEquals(first.fingerprint(), second.fingerprint());
    }

    @Test
    void generatesSha256Digest() {
        var result = FailureFingerprinter.fingerprint(
            new RuntimeException("test failure")
        );

        assertEquals(64, result.fingerprint().length());
        assertTrue(result.fingerprint().matches("[0-9a-f]{64}"));
    }

    @Test
    void handlesMissingMessage() {
        var result = FailureFingerprinter.fingerprint(
            new RuntimeException()
        );

        assertEquals("<no-message>", result.normalizedMessage());
    }

    @Test
    void rejectsNullFailure() {
        assertThrows(
            IllegalArgumentException.class,
            () -> FailureFingerprinter.fingerprint(null)
        );
    }

    @Test
    void boundsNormalizedMessageLength() {
        var result = FailureFingerprinter.fingerprint(
            new RuntimeException("failure " + "x".repeat(5000))
        );

        assertTrue(result.normalizedMessage().length() <= 1024);
    }
}