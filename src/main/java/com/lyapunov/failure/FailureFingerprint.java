package com.lyapunov.failure;

public record FailureFingerprint(
    String fingerprint,
    String exceptionType,
    String normalizedMessage
) {
    public FailureFingerprint {
        if (fingerprint == null || !fingerprint.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("fingerprint must be a SHA-256 hex digest");
        }
        if (exceptionType == null || exceptionType.isBlank()) {
            throw new IllegalArgumentException("exceptionType must not be blank");
        }
        if (normalizedMessage == null || normalizedMessage.isBlank()) {
            throw new IllegalArgumentException("normalizedMessage must not be blank");
        }
    }
}