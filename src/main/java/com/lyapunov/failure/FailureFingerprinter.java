package com.lyapunov.failure;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

public final class FailureFingerprinter {

    private static final int MAX_INPUT_LENGTH = 4096;
    private static final int MAX_NORMALIZED_LENGTH = 1024;
    private static final int MAX_CAUSE_DEPTH = 16;

    private static final Pattern SECRET_PATTERN = Pattern.compile(
        "(?i)\\b(password|passwd|token|secret|api[_-]?key|authorization)" +
        "\\s*[:=]\\s*(?:Bearer\\s+)?(\"[^\"]*\"|'[^']*'|[^\\s,;]+)"
    );

    private static final Pattern BEARER_PATTERN = Pattern.compile(
        "(?i)\\bBearer\\s+[^\\s,;]+"
    );

    private static final Pattern UUID_PATTERN = Pattern.compile(
        "(?i)\\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\b"
    );

    private static final Pattern TIMESTAMP_PATTERN = Pattern.compile(
        "\\b\\d{4}-\\d{2}-\\d{2}[tT ]\\d{2}:\\d{2}:\\d{2}" +
        "(?:\\.\\d+)?(?:Z|[+-]\\d{2}:?\\d{2})?\\b"
    );

    private static final Pattern HEX_ADDRESS_PATTERN = Pattern.compile(
        "(?i)\\b0x[0-9a-f]{6,}\\b"
    );

    private static final Pattern LARGE_NUMBER_PATTERN = Pattern.compile(
        "\\b\\d{4,}\\b"
    );

    private FailureFingerprinter() {
    }

    public static FailureFingerprint fingerprint(Throwable failure) {
        if (failure == null) {
            throw new IllegalArgumentException("failure must not be null");
        }

        Throwable root = findRootCause(failure);

        String exceptionType = root.getClass().getSimpleName();
        String canonicalType = root.getClass().getName();
        String normalized = normalize(root.getMessage());

        return new FailureFingerprint(
            sha256(canonicalType + "\n" + normalized),
            exceptionType,
            normalized
        );
    }

    static String normalize(String message) {
        if (message == null || message.isBlank()) {
            return "<no-message>";
        }

        String value = message.length() > MAX_INPUT_LENGTH
            ? message.substring(0, MAX_INPUT_LENGTH)
            : message;

        value = BEARER_PATTERN.matcher(value)
            .replaceAll("Bearer <redacted>");

        value = SECRET_PATTERN.matcher(value)
            .replaceAll("$1=<redacted>");

        value = UUID_PATTERN.matcher(value)
            .replaceAll("<uuid>");

        value = TIMESTAMP_PATTERN.matcher(value)
            .replaceAll("<timestamp>");

        value = HEX_ADDRESS_PATTERN.matcher(value)
            .replaceAll("<hex-address>");

        value = LARGE_NUMBER_PATTERN.matcher(value)
            .replaceAll("<number>");

        value = value.replaceAll("\\s+", " ")
            .trim()
            .toLowerCase(Locale.ROOT);

        if (value.isEmpty()) {
            return "<no-message>";
        }

        return value.length() > MAX_NORMALIZED_LENGTH
            ? value.substring(0, MAX_NORMALIZED_LENGTH)
            : value;
    }

    private static Throwable findRootCause(Throwable failure) {
        Throwable current = failure;

        Set<Throwable> visited = Collections.newSetFromMap(
            new IdentityHashMap<>()
        );

        for (int depth = 0;
             depth < MAX_CAUSE_DEPTH && visited.add(current);
             depth++) {

            Throwable cause = current.getCause();

            if (cause == null || cause == current || visited.contains(cause)) {
                break;
            }

            current = cause;
        }

        return current;
    }

    private static String sha256(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");

            byte[] hash = digest.digest(
                input.getBytes(StandardCharsets.UTF_8)
            );

            return java.util.HexFormat.of().formatHex(hash);

        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 unavailable", exception);
        }
    }
}