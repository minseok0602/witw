-- Receipt, domain writes and APPLIED transition share one transaction.
-- Keep event IDs to suppress duplicates across Kafka offset resets/re-publishing.
CREATE TABLE pipeline_event_inbox (
    event_id VARCHAR(255) PRIMARY KEY,
    event_type VARCHAR(32) NOT NULL,
    event_json TEXT NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'APPLIED')),
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_attempt_at TIMESTAMPTZ,
    applied_at TIMESTAMPTZ
);
CREATE INDEX idx_pipeline_event_pending
    ON pipeline_event_inbox(last_attempt_at NULLS FIRST, received_at, event_id) WHERE status = 'PENDING';
