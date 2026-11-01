package com.lyapunov.worker;

import com.lyapunov.model.OutboxEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public final class KafkaEventPublisher implements EventPublisher {
    private final Producer<String, String> producer;
    private final String topic;

    public KafkaEventPublisher(Properties properties, String topic) {
        this(new KafkaProducer<>(validated(properties)), topic);
    }

    KafkaEventPublisher(Producer<String, String> producer, String topic) {
        if (producer == null) throw new IllegalArgumentException("producer must not be null");
        if (topic == null || topic.isBlank()) throw new IllegalArgumentException("topic must not be blank");
        this.producer = producer;
        this.topic = topic;
    }

    private static Properties validated(Properties source) {
        if (source == null) throw new IllegalArgumentException("properties must not be null");
        Properties copy = new Properties();
        copy.putAll(source);
        copy.putIfAbsent("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        copy.putIfAbsent("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        copy.putIfAbsent("acks", "all");
        copy.putIfAbsent("enable.idempotence", "true");
        copy.putIfAbsent("max.in.flight.requests.per.connection", "5");
        if (!"all".equals(copy.getProperty("acks"))) {
            throw new IllegalArgumentException("Kafka producer acks must be all");
        }
        if (!Boolean.parseBoolean(copy.getProperty("enable.idempotence"))) {
            throw new IllegalArgumentException("Kafka producer idempotence must be enabled");
        }
        return copy;
    }

    @Override
    public void publish(OutboxEvent event) throws Exception {
        if (event == null) throw new IllegalArgumentException("event must not be null");
        String key = event.aggregateType() + ":" + event.aggregateId();
        ProducerRecord<String, String> record =
            new ProducerRecord<>(topic, null, key, event.payload());
        addHeader(record, "lyapunov-event-id", event.id().toString());
        addHeader(record, "lyapunov-event-type", event.eventType());
        addHeader(record, "lyapunov-aggregate-type", event.aggregateType());
        addHeader(record, "lyapunov-aggregate-id", event.aggregateId());
        addHeader(record, "lyapunov-sequence", Long.toString(event.sequenceNumber()));
        producer.send(record).get(30, TimeUnit.SECONDS);
    }

    private static void addHeader(ProducerRecord<String, String> record, String name, String value) {
        record.headers().add(new RecordHeader(name, value.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }
}