package com.lyapunov.verifier;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

public class ExperimentSummaryCli {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private record ReportSummary(
            String faultType,
            boolean reliable,
            int missing,
            int duplicates,
            int orderingViolations
    ) {}

    private record InvalidReport(Path path, String reason) {}

    private ExperimentSummaryCli() {}

    public static void main(String[] args) {
        if (args.length < 1 || args.length > 2
                || (args.length == 2 && !"--strict".equals(args[1]))) {
            System.err.println(
                    "Usage: ExperimentSummaryCli <report-directory> [--strict]"
            );
            System.exit(2);
            return;
        }

        Path directory = Path.of(args[0]);
        boolean strict = args.length == 2;

        if (!Files.isDirectory(directory)) {
            System.err.println("Report directory does not exist: " + directory);
            System.exit(2);
            return;
        }

        List<Path> files;

        try (Stream<Path> stream = Files.list(directory)) {
            files = stream
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString()
                            .toLowerCase().endsWith(".json"))
                    .sorted(Comparator.comparing(
                            path -> path.getFileName().toString()
                    ))
                    .toList();
        } catch (IOException exception) {
            System.err.println("Cannot read report directory: "
                    + exception.getMessage());
            System.exit(2);
            return;
        }

        List<ReportSummary> validReports = new ArrayList<>();
        List<InvalidReport> invalidReports = new ArrayList<>();

        for (Path file : files) {
            try {
                validReports.add(readReport(file));
            } catch (IOException | IllegalArgumentException exception) {
                invalidReports.add(new InvalidReport(
                        file, exception.getMessage()
                ));
            }
        }

        printSummary(directory, files.size(), validReports, invalidReports);

        if (!invalidReports.isEmpty() && strict) {
            System.err.println(
                    "\nSTRICT MODE: invalid reports found; exiting with status 1."
            );
            System.exit(1);
        }
    }

    private static ReportSummary readReport(Path file) throws IOException {
        JsonNode root = MAPPER.readTree(file.toFile());

        if (root == null || !root.isObject()) {
            throw new IllegalArgumentException(
                    "Report must contain a JSON object"
            );
        }

        String faultType = requiredText(root, "faultType");
        if (!List.of("DROP", "DUPLICATE", "REORDER").contains(faultType)) {
            throw new IllegalArgumentException(
                    "Unsupported faultType: " + faultType
            );
        }

        JsonNode reliableNode = root.get("reliable");
        if (reliableNode == null || !reliableNode.isBoolean()) {
            throw new IllegalArgumentException(
                    "Missing or invalid boolean field: reliable"
            );
        }

        int missing = requiredArraySize(root, "missingEventIds");
        int duplicates = requiredArraySize(root, "duplicateEventIds");
        int ordering = requiredArraySize(root, "orderingViolations");

        requiredInteger(root, "seed");
        requiredInteger(root, "requestedEventCount");
        requiredInteger(root, "expectedCount");
        requiredInteger(root, "observedCount");
        requiredText(root, "project");
        requiredText(root, "executedAt");

        boolean calculatedReliable =
                missing == 0 && duplicates == 0 && ordering == 0;

        if (reliableNode.booleanValue() != calculatedReliable) {
            throw new IllegalArgumentException(
                    "reliable does not match the reported event violations"
            );
        }

        return new ReportSummary(
                faultType,
                calculatedReliable,
                missing,
                duplicates,
                ordering
        );
    }

    private static String requiredText(JsonNode root, String field) {
        JsonNode value = root.get(field);
        if (value == null || !value.isTextual()
                || value.asText().isBlank()) {
            throw new IllegalArgumentException(
                    "Missing or invalid text field: " + field
            );
        }
        return value.asText();
    }

    private static int requiredArraySize(JsonNode root, String field) {
        JsonNode value = root.get(field);
        if (value == null || !value.isArray()) {
            throw new IllegalArgumentException(
                    "Missing or invalid array field: " + field
            );
        }
        return value.size();
    }

    private static void requiredInteger(JsonNode root, String field) {
        JsonNode value = root.get(field);
        if (value == null || !value.isIntegralNumber()
                || !value.canConvertToInt() && !"seed".equals(field)) {
            throw new IllegalArgumentException(
                    "Missing or invalid integer field: " + field
            );
        }
    }

    private static void printSummary(
            Path directory,
            int totalFiles,
            List<ReportSummary> reports,
            List<InvalidReport> invalid
    ) {
        long reliable = reports.stream()
                .filter(ReportSummary::reliable).count();

        long unreliable = reports.size() - reliable;

        long missing = reports.stream()
                .mapToLong(ReportSummary::missing).sum();

        long duplicates = reports.stream()
                .mapToLong(ReportSummary::duplicates).sum();

        long ordering = reports.stream()
                .mapToLong(ReportSummary::orderingViolations).sum();

        System.out.println("=== LYAPUNOV EXPERIMENT SUMMARY ===");
        System.out.println("Directory            : " + directory);
        System.out.println("JSON files found     : " + totalFiles);
        System.out.println("Valid reports        : " + reports.size());
        System.out.println("Invalid reports      : " + invalid.size());
        System.out.println("Reliable experiments : " + reliable);
        System.out.println("Unreliable experiments: " + unreliable);
        System.out.println("Missing event entries: " + missing);
        System.out.println("Duplicate event entries: " + duplicates);
        System.out.println("Ordering violations  : " + ordering);

        Map<String, List<ReportSummary>> byFault = new TreeMap<>();
        for (ReportSummary report : reports) {
            byFault.computeIfAbsent(report.faultType(), key -> new ArrayList<>())
                    .add(report);
        }

        System.out.println("\n=== BY FAULT TYPE ===");
        if (byFault.isEmpty()) {
            System.out.println("No valid reports to summarize.");
        } else {
            byFault.forEach((fault, group) -> {
                long groupReliable = group.stream()
                        .filter(ReportSummary::reliable).count();

                long groupMissing = group.stream()
                        .mapToLong(ReportSummary::missing).sum();

                long groupDuplicates = group.stream()
                        .mapToLong(ReportSummary::duplicates).sum();

                long groupOrdering = group.stream()
                        .mapToLong(ReportSummary::orderingViolations).sum();

                System.out.printf(
                        "%s: experiments=%d, reliable=%d, unreliable=%d, "
                                + "missing=%d, duplicates=%d, ordering=%d%n",
                        fault,
                        group.size(),
                        groupReliable,
                        group.size() - groupReliable,
                        groupMissing,
                        groupDuplicates,
                        groupOrdering
                );
            });
        }

        if (!invalid.isEmpty()) {
            System.out.println("\n=== INVALID REPORTS ===");
            for (InvalidReport report : invalid) {
                System.out.println("- " + report.path() + ": "
                        + report.reason());
            }
        }
    }
}
