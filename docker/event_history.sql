CREATE TABLE IF NOT EXISTS event_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id UUID NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    aggregate_id VARCHAR(255) NOT NULL,
    sequence_number BIGINT NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    action VARCHAR(50) NOT NULL,
    worker_id VARCHAR(255),
    details JSONB,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_event_history_event
    ON event_history(event_id, occurred_at);

CREATE INDEX IF NOT EXISTS idx_event_history_aggregate
    ON event_history(aggregate_type, aggregate_id, sequence_number, occurred_at);
