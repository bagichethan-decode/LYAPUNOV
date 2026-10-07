package com.lyapunov.verifier;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.time.Duration;
import java.util.List;

public class ReliabilityVerifier {

    public static void main(String[] args) throws Exception {

        String jdbcUrl = "jdbc:postgresql://localhost:5432/lyapunov";
        String username = "postgres";
        String password = "postgres";

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);

        try (HikariDataSource dataSource = new HikariDataSource(config);
             KafkaEventObserver observer = new KafkaEventObserver(
                 "localhost:9092",
                 "lyapunov-verifier",
                 "lyapunov.events"
             )) {

            ExpectedEventRepository repository =
                new ExpectedEventRepository(dataSource);

            List<EventVerifier.ExpectedEvent> expected =
                repository.findAll();

            List<EventVerifier.ObservedEvent> observed =
                observer.poll(Duration.ofSeconds(10));

            ReliabilityReport report =
                new EventVerifier().verify(expected, observed);

            System.out.println("=== LYAPUNOV RELIABILITY REPORT ===");
            System.out.println("Expected events : " + report.expectedCount());
            System.out.println("Observed events : " + report.observedCount());
            System.out.println("Missing         : " + report.missingEventIds());
            System.out.println("Duplicates      : " + report.duplicateEventIds());
            System.out.println("Ordering issues : " + report.orderingViolations());
            System.out.println("RELIABLE        : " + report.isReliable());
        }
    }
}
