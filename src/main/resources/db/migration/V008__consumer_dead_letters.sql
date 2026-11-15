CREATE TABLE IF NOT EXISTS consumer_dead_letters (
    id BIGSERIAL PRIMARY KEY,
    consumer_name VARCHAR(200) NOT NULL,
    topic VARCHAR(255) NOT NULL,
    partition_id INTEGER NOT NULL,
    message_offset BIGINT NOT NULL,
    record_key TEXT,
    record_value TEXT,
    error_type VARCHAR(500) NOT NULL,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_consumer_dead_letter_offset
        UNIQUE (consumer_name, topic, partition_id, message_offset)
);

CREATE INDEX IF NOT EXISTS idx_consumer_dead_letters_created_at
    ON consumer_dead_letters (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_consumer_dead_letters_consumer
    ON consumer_dead_letters (consumer_name, created_at DESC);
