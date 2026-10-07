package com.lyapunov.verifier;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ExpectedEventRepository {

    private final DataSource dataSource;

    public ExpectedEventRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public List<EventVerifier.ExpectedEvent> findAll() throws SQLException {
        String sql = """
            SELECT id, aggregate_type, aggregate_id, sequence_number
            FROM outbox_events
            ORDER BY aggregate_type, aggregate_id, sequence_number
        """;

        List<EventVerifier.ExpectedEvent> events = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                events.add(
                    new EventVerifier.ExpectedEvent(
                        rs.getString("id"),
                        rs.getString("aggregate_type"),
                        rs.getString("aggregate_id"),
                        rs.getLong("sequence_number")
                    )
                );
            }
        }

        return events;
    }
}
