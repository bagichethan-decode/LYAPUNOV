-- LYAPUNOV V006: durable worker heartbeat registry
CREATE TABLE IF NOT EXISTS worker_heartbeats (
    worker_id VARCHAR(255) PRIMARY KEY,
    instance_id VARCHAR(255) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'RUNNING',
    started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT clock_timestamp(),
    last_seen_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT clock_timestamp(),
    last_successful_publish_at TIMESTAMP WITH TIME ZONE,
    published_count BIGINT NOT NULL DEFAULT 0 CHECK (published_count >= 0),
    failed_count BIGINT NOT NULL DEFAULT 0 CHECK (failed_count >= 0),
    stale_claim_count BIGINT NOT NULL DEFAULT 0 CHECK (stale_claim_count >= 0),
    last_error TEXT,
    CONSTRAINT chk_worker_heartbeat_status CHECK (status IN ('RUNNING', 'STOPPED'))
);

CREATE INDEX IF NOT EXISTS idx_worker_heartbeats_last_seen
    ON worker_heartbeats (last_seen_at DESC);
