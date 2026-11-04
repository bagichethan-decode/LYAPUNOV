# Consumer Inbox and Idempotency

## Transaction protocol

`JdbcInboxProcessor` executes these steps in one PostgreSQL transaction:

1. Insert `(consumer_name, event_id)` with `ON CONFLICT DO NOTHING`.
2. If no row was inserted, return `DUPLICATE` without invoking business work.
3. Execute business SQL using the exact same JDBC connection.
4. Set `processed_at` and commit.

If the callback fails, the inbox insert and business writes roll back together. A later broker redelivery can retry. If the database commits but the process crashes before acknowledging Kafka, the redelivery is identified as a duplicate. The primary key arbitrates concurrent deliveries of the same event for the same consumer.

## Usage

```java
InboxProcessingResult result = inboxProcessor.process(
    "orders-projection", eventId, "OrderCreated", connection -> {
        try (var statement = connection.prepareStatement(
                "INSERT INTO order_projection(order_id, customer_id) VALUES (?, ?)")) {
            statement.setObject(1, orderId);
            statement.setObject(2, customerId);
            statement.executeUpdate();
        }
    });
// Acknowledge the Kafka record only after process() returns successfully.
// Both PROCESSED and DUPLICATE are safe to acknowledge.
```

Construct the processor with the application's configured `DataSource`. Do not open another connection, call `commit()`, or perform external side effects inside `InboxWork`. Use a separate idempotency key or outbox handoff for HTTP calls, email, and other external effects.

## Migration and retention

Apply `src/main/resources/db/migration/V007__consumer_inbox.sql` to the consumer's PostgreSQL database before enabling the processor. The consumer name is part of the key; keep it stable across deployments. Retain inbox rows for at least the maximum broker retention, replay, and redelivery horizon. Deleting them sooner allows old events to execute again.

## Delivery semantics and scope

This provides effectively-once database business processing per `(consumer_name, event_id)`, not end-to-end exactly-once delivery. Kafka remains at-least-once. This batch adds a reusable JDBC primitive and migration; it does not silently alter an existing Kafka consumer or its offset-acknowledgement flow. The integration step must acknowledge offsets only after this transaction completes.
