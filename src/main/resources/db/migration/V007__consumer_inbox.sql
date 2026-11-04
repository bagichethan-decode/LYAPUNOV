-- Durable deduplication identity per logical consumer and event.
CREATE TABLE IF NOT EXISTS consumer_inbox (
    consumer_name VARCHAR(200) NOT NULL,
    event_id UUID NOT NULL,
    event_type VARCHAR(200) NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMPTZ,
    CONSTRAINT pk_consumer_inbox PRIMARY KEY (consumer_name, event_id)
);
CREATE INDEX IF NOT EXISTS idx_consumer_inbox_received_at ON consumer_inbox (received_at);
CREATE INDEX IF NOT EXISTS idx_consumer_inbox_unprocessed ON consumer_inbox (received_at) WHERE processed_at IS NULL;
