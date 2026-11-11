package com.lyapunov.inbox;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Business processing for a Kafka record.
 *
 * All database writes must use the supplied connection
 * so they participate in the inbox transaction.
 */
@FunctionalInterface
public interface KafkaInboxWork {

    void execute(
        Connection connection,
        ConsumerRecord<String, String> record
    ) throws SQLException;
}