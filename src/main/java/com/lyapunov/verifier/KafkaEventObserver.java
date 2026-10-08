package com.lyapunov.verifier;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaEventObserver implements AutoCloseable {

    private final KafkaConsumer<String, String> consumer;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public KafkaEventObserver(
            String bootstrapServers,
            String groupId,
            String topic) {

        Properties properties = new Properties();

        properties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers
        );

        properties.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                groupId
        );

        properties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );

        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );

        properties.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        properties.put(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                "false"
        );

        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of(topic));
    }

    public List<EventVerifier.ObservedEvent> poll(
            int expectedCount,
            Duration timeout) {

        List<EventVerifier.ObservedEvent> events = new ArrayList<>();

        long deadline =
                System.nanoTime() + timeout.toNanos();

        while (events.size() < expectedCount
                && System.nanoTime() < deadline) {

            Duration remaining = Duration.ofNanos(
                    Math.max(
                            1,
                            deadline - System.nanoTime()
                    )
            );

            Duration pollTimeout =
                    remaining.compareTo(Duration.ofSeconds(1)) > 0
                            ? Duration.ofSeconds(1)
                            : remaining;

            for (ConsumerRecord<String, String> record
                    : consumer.poll(pollTimeout)) {

                events.add(parse(record.value()));

                if (events.size() >= expectedCount) {
                    break;
                }
            }
        }

        return events;
    }

    private EventVerifier.ObservedEvent parse(String payload) {

        try {
            JsonNode json = objectMapper.readTree(payload);

            return new EventVerifier.ObservedEvent(
                    json.get("eventId").asText(),
                    json.get("aggregateType").asText(),
                    json.get("aggregateId").asText(),
                    json.get("sequenceNumber").asLong()
            );

        } catch (Exception e) {

            throw new IllegalArgumentException(
                    "Invalid LYAPUNOV Kafka event: " + payload,
                    e
            );
        }
    }

    @Override
    public void close() {
        consumer.close();
    }
}