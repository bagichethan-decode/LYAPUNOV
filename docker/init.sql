CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type VARCHAR(255) NOT NULL,
    aggregate_id VARCHAR(255) NOT NULL,
    sequence_number BIGINT NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    retry_count INT NOT NULL DEFAULT 0,
    claimed_by VARCHAR(255),
    lease_expires_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_aggregate_sequence UNIQUE (aggregate_type, aggregate_id, sequence_number)
);

CREATE INDEX idx_outbox_polling 
ON outbox_events (created_at) 
WHERE status = 'PENDING';

CREATE INDEX idx_outbox_aggregate 
ON outbox_events (aggregate_type, aggregate_id);
