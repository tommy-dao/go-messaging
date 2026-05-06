package message

import "fmt"

// Schema returns the DDL statements to create all messaging tables with the given prefix.
// All statements use IF NOT EXISTS for idempotent migration.
func Schema(prefix string) []string {
	cfg := Config{TablePrefix: prefix}
	dedup := cfg.tableName("message_dedup")
	hot := cfg.tableName("message_hot")
	archive := cfg.tableName("message_archive")

	return []string{
		// Dedup table (inbox only)
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    id             BIGSERIAL PRIMARY KEY,
    consumer_group VARCHAR(100) NOT NULL,
    message_id     VARCHAR(255) NOT NULL,
    event_type     VARCHAR(255) NOT NULL,
    source         VARCHAR(100),
    first_received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expire_at      TIMESTAMPTZ,
    UNIQUE (consumer_group, message_id)
)`, dedup),

		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_expire
    ON %s (expire_at) WHERE expire_at IS NOT NULL`, dedup, dedup),

		// Hot table (shared inbox + outbox)
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    id             BIGSERIAL PRIMARY KEY,
    direction      VARCHAR(20) NOT NULL,
    consumer_group VARCHAR(100),
    message_id     VARCHAR(255) NOT NULL,
    event_id       VARCHAR(255),
    event_type     VARCHAR(255) NOT NULL,
    payload        JSONB NOT NULL,
    headers        JSONB,
    source         VARCHAR(100),
    status         VARCHAR(30) NOT NULL,
    retry_count    INT NOT NULL DEFAULT 0,
    max_retry      INT NOT NULL DEFAULT 0,
    next_retry_at  TIMESTAMPTZ,
    claimed_by     VARCHAR(100),
    claimed_at     TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at   TIMESTAMPTZ,
    published_at   TIMESTAMPTZ,
    last_error     TEXT
)`, hot),

		// Partial unique index for inbox dedup
		fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS uq_%s_inbox
    ON %s (direction, consumer_group, message_id)
    WHERE direction = 'INBOX'`, hot, hot),

		// Partial unique index for outbox dedup
		fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS uq_%s_outbox
    ON %s (direction, event_id)
    WHERE direction = 'OUTBOX'`, hot, hot),

		// Claim query index
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_claim
    ON %s (direction, status, next_retry_at)
    WHERE status IN ('PENDING', 'RETRY')`, hot, hot),

		// Archive table (shared)
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    id             BIGSERIAL PRIMARY KEY,
    direction      VARCHAR(20) NOT NULL,
    consumer_group VARCHAR(100),
    message_id     VARCHAR(255) NOT NULL,
    event_id       VARCHAR(255),
    event_type     VARCHAR(255) NOT NULL,
    payload        JSONB NOT NULL,
    headers        JSONB,
    source         VARCHAR(100),
    final_status   VARCHAR(30) NOT NULL,
    retry_count    INT NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ NOT NULL,
    processed_at   TIMESTAMPTZ,
    published_at   TIMESTAMPTZ,
    archived_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error     TEXT
)`, archive),

		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_direction
    ON %s (direction, created_at)`, archive, archive),
	}
}
