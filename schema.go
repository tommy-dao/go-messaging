package message

import "fmt"

// Schema returns the DDL statements to create the 3 messaging tables scoped
// by name (e.g. name="asset" -> message_asset_hot). All statements use
// IF NOT EXISTS for idempotent migration.
func Schema(name string) ([]string, error) {
	if err := assertValidName(name); err != nil {
		return nil, err
	}
	dedup := tableName(name, "dedup")
	hot := tableName(name, "hot")
	archive := tableName(name, "archive")

	return []string{
		// Dedup — receive()-only, message_id PK (global dedup).
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    message_id     TEXT NOT NULL PRIMARY KEY,
    partition_key  VARCHAR(100),
    topic          VARCHAR(255) NOT NULL,
    source         VARCHAR(100),
    first_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    expire_at      TIMESTAMPTZ
)`, dedup),

		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_expire
    ON %s (expire_at) WHERE expire_at IS NOT NULL`, dedup, dedup),

		// Hot — live queue + DLQ (FAILED rows stay here until RetryFailed or
		// ArchiveExhausted). message_id PK is the natural key for both
		// receive() and enqueue() — ON CONFLICT DO NOTHING makes both idempotent.
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    message_id     TEXT NOT NULL PRIMARY KEY,
    partition_key  VARCHAR(100),
    topic          VARCHAR(255) NOT NULL,
    payload        JSONB NOT NULL,
    headers        JSONB,
    source         VARCHAR(100),
    encrypted      BOOLEAN NOT NULL DEFAULT false,
    status         VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    retry_count    INT NOT NULL DEFAULT 0,
    max_retry      INT NOT NULL DEFAULT 0,
    next_retry_at  TIMESTAMPTZ,
    processing_by  VARCHAR(100),
    processing_at  TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at   TIMESTAMPTZ,
    last_error     TEXT
)`, hot),

		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_claim
    ON %s (status, next_retry_at, created_at)
    WHERE status IN ('PENDING', 'RETRY')`, hot, hot),

		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_failed
    ON %s (status, processed_at) WHERE status = 'FAILED'`, hot, hot),

		// Archive — append-only, surrogate PK. message_id is intentionally
		// NOT unique: a message_id reprocessed after a dedup TTL expiry adds
		// a new history row instead of overwriting the previous one.
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    message_id     TEXT NOT NULL,
    partition_key  VARCHAR(100),
    topic          VARCHAR(255) NOT NULL,
    payload        JSONB NOT NULL,
    headers        JSONB,
    source         VARCHAR(100),
    encrypted      BOOLEAN NOT NULL DEFAULT false,
    final_status   VARCHAR(20) NOT NULL,
    retry_count    INT NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ NOT NULL,
    processed_at   TIMESTAMPTZ,
    archived_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error     TEXT
)`, archive),

		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_message_id
    ON %s (message_id)`, archive, archive),

		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_topic
    ON %s (topic, created_at)`, archive, archive),

		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_archived
    ON %s (archived_at)`, archive, archive),
	}, nil
}

// dropSchema returns the DDL to drop all 3 tables for name, in dependency-safe order.
func dropSchema(name string) ([]string, error) {
	if err := assertValidName(name); err != nil {
		return nil, err
	}
	return []string{
		fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tableName(name, "archive")),
		fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tableName(name, "hot")),
		fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tableName(name, "dedup")),
	}, nil
}
