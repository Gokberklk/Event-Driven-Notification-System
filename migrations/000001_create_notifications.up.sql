CREATE TABLE notifications (
    id                  UUID PRIMARY KEY,
    batch_id            UUID,
    idempotency_key     TEXT,
    recipient           TEXT        NOT NULL,
    channel             TEXT        NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
    content             TEXT        NOT NULL,
    priority            TEXT        NOT NULL DEFAULT 'normal' CHECK (priority IN ('high', 'normal', 'low')),
    status              TEXT        NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'scheduled', 'processing', 'sent', 'failed', 'cancelled')),
    attempts            INT         NOT NULL DEFAULT 0,
    max_attempts        INT         NOT NULL DEFAULT 5,
    last_error          TEXT,
    provider_message_id TEXT,
    correlation_id      TEXT,
    scheduled_at        TIMESTAMPTZ,
    next_attempt_at     TIMESTAMPTZ,
    sent_at             TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX notifications_idempotency_key_uq ON notifications (idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX notifications_batch_id_idx ON notifications (batch_id) WHERE batch_id IS NOT NULL;
CREATE INDEX notifications_status_channel_created_idx ON notifications (status, channel, created_at DESC);
CREATE INDEX notifications_created_at_idx ON notifications (created_at DESC, id DESC);
