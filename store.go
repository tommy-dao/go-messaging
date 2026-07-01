package message

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const hotCols = `message_id, partition_key, topic, payload, headers, source, encrypted, status, retry_count, max_retry, next_retry_at, processing_by, processing_at, created_at, processed_at, last_error`

const archiveCols = `id, message_id, partition_key, topic, payload, headers, source, encrypted, final_status, retry_count, created_at, processed_at, archived_at, last_error`

type store struct {
	db       *sql.DB
	cfg      Config
	cipher   *CipherRegistry
	dedupTbl string
	hotTbl   string
	archTbl  string
}

func newStore(db *sql.DB, cfg Config, cipher *CipherRegistry) *store {
	cfg = cfg.withDefaults()
	return &store{
		db:       db,
		cfg:      cfg,
		cipher:   cipher,
		dedupTbl: tableName(cfg.Name, "dedup"),
		hotTbl:   tableName(cfg.Name, "hot"),
		archTbl:  tableName(cfg.Name, "archive"),
	}
}

// prepareFields marshals headers to JSON and, when a cipher is configured,
// encrypts both payload and headers. Encryption is instance-wide: on iff
// s.cipher != nil, never toggled per message — callers derive the stored
// `encrypted` column value the same way, via s.cipher != nil.
func (s *store) prepareFields(payload []byte, headers map[string]string) (outPayload, outHeaders []byte, err error) {
	headersJSON, err := json.Marshal(headers)
	if err != nil {
		return nil, nil, fmt.Errorf("message: marshal headers: %w", err)
	}
	if s.cipher == nil {
		return payload, headersJSON, nil
	}
	outPayload, err = s.cipher.Encrypt(payload)
	if err != nil {
		return nil, nil, fmt.Errorf("message: encrypt payload: %w", err)
	}
	outHeaders, err = s.cipher.Encrypt(headersJSON)
	if err != nil {
		return nil, nil, fmt.Errorf("message: encrypt headers: %w", err)
	}
	return outPayload, outHeaders, nil
}

// hydrate decrypts msg.Payload/msg.Headers in place when msg.Encrypted, then
// always unmarshals headers into msg.Headers. Returns an error if the row is
// encrypted but no cipher is configured, or decryption/auth fails — callers
// on the processing path (handleOne) must dead-letter the row on error rather
// than let it poison the batch.
func (s *store) hydrate(msg *Message) error {
	payload, headers := msg.Payload, msg.rawHeaders
	if msg.Encrypted {
		if s.cipher == nil {
			return fmt.Errorf("message: decrypt %s: %w", msg.MessageID, ErrNoCipher)
		}
		var err error
		payload, err = s.cipher.Decrypt(payload)
		if err != nil {
			return fmt.Errorf("message: decrypt payload %s: %w", msg.MessageID, err)
		}
		if headers != nil {
			headers, err = s.cipher.Decrypt(headers)
			if err != nil {
				return fmt.Errorf("message: decrypt headers %s: %w", msg.MessageID, err)
			}
		}
	}
	msg.Payload = payload
	msg.rawHeaders = nil
	if headers != nil {
		if err := json.Unmarshal(headers, &msg.Headers); err != nil {
			return fmt.Errorf("message: unmarshal headers %s: %w", msg.MessageID, err)
		}
	}
	return nil
}

// hydrateSafe is the best-effort variant used by query/admin paths: on
// decrypt failure it leaves the row as-is (still holding ciphertext) instead
// of erroring, so DLQ rows with a broken cipher remain listable.
func (s *store) hydrateSafe(msg *Message) {
	_ = s.hydrate(msg)
}

func (s *store) hydrateArchived(msg *ArchivedMessage) error {
	payload, headers := msg.Payload, msg.rawHeaders
	if msg.Encrypted {
		if s.cipher == nil {
			return fmt.Errorf("message: decrypt archive %d: %w", msg.ID, ErrNoCipher)
		}
		var err error
		payload, err = s.cipher.Decrypt(payload)
		if err != nil {
			return fmt.Errorf("message: decrypt archive payload %d: %w", msg.ID, err)
		}
		if headers != nil {
			headers, err = s.cipher.Decrypt(headers)
			if err != nil {
				return fmt.Errorf("message: decrypt archive headers %d: %w", msg.ID, err)
			}
		}
	}
	msg.Payload = payload
	msg.rawHeaders = nil
	if headers != nil {
		if err := json.Unmarshal(headers, &msg.Headers); err != nil {
			return fmt.Errorf("message: unmarshal archive headers %d: %w", msg.ID, err)
		}
	}
	return nil
}

func (s *store) hydrateArchivedSafe(msg *ArchivedMessage) {
	_ = s.hydrateArchived(msg)
}

// insertDedup inserts a dedup record. Returns ErrDuplicate if message_id already exists.
// Dedup is global (message_id alone), not scoped per partition/consumer.
func (s *store) insertDedup(ctx context.Context, db DBTX, messageID, partitionKey, topic, source string, expireAt *time.Time) error {
	query := fmt.Sprintf(`INSERT INTO %s (message_id, partition_key, topic, source, expire_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (message_id) DO NOTHING`, s.dedupTbl)

	result, err := db.ExecContext(ctx, query, messageID, nullString(partitionKey), topic, nullString(source), expireAt)
	if err != nil {
		return fmt.Errorf("message: insert dedup: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("message: insert dedup rows affected: %w", err)
	}
	if rows == 0 {
		return ErrDuplicate
	}
	return nil
}

// insertHot inserts a message into the hot table. Idempotent via
// ON CONFLICT (message_id) DO NOTHING — used directly by both Receive
// (after a successful dedup insert) and Enqueue.
func (s *store) insertHot(ctx context.Context, db DBTX, msg *Message) error {
	payload, headers, err := s.prepareFields(msg.Payload, msg.Headers)
	if err != nil {
		return err
	}

	query := fmt.Sprintf(`INSERT INTO %s (message_id, partition_key, topic, payload, headers, source, encrypted, status, retry_count, max_retry, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 'PENDING', 0, $8, $9)
		ON CONFLICT (message_id) DO NOTHING`, s.hotTbl)

	_, err = db.ExecContext(ctx, query,
		msg.MessageID,
		nullString(msg.PartitionKey),
		msg.Topic,
		payload,
		headers,
		nullString(msg.Source),
		s.cipher != nil,
		msg.MaxRetries,
		time.Now(),
	)
	if err != nil {
		return fmt.Errorf("message: insert hot: %w", err)
	}
	return nil
}

// insertHotBatch inserts multiple messages in one round-trip. Caller must
// ensure message IDs are unique within the batch.
func (s *store) insertHotBatch(ctx context.Context, db DBTX, msgs []*Message) error {
	if len(msgs) == 0 {
		return nil
	}
	rows := make([]string, 0, len(msgs))
	args := make([]any, 0, len(msgs)*9)
	now := time.Now()
	encrypted := s.cipher != nil
	for _, msg := range msgs {
		payload, headers, err := s.prepareFields(msg.Payload, msg.Headers)
		if err != nil {
			return err
		}
		base := len(args)
		rows = append(rows, fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,'PENDING',0,$%d,$%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9))
		args = append(args, msg.MessageID, nullString(msg.PartitionKey), msg.Topic, payload, headers,
			nullString(msg.Source), encrypted, msg.MaxRetries, now)
	}
	query := fmt.Sprintf(`INSERT INTO %s (message_id, partition_key, topic, payload, headers, source, encrypted, status, retry_count, max_retry, created_at)
		VALUES %s ON CONFLICT (message_id) DO NOTHING`, s.hotTbl, strings.Join(rows, ", "))
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("message: insert hot batch: %w", err)
	}
	return nil
}

// claimBatch atomically claims up to size PENDING/RETRY messages whose
// next_retry_at has elapsed. Decryption is intentionally deferred to
// handleOne so a single undecryptable row dead-letters instead of aborting
// the whole batch.
func (s *store) claimBatch(ctx context.Context, size int) ([]*Message, error) {
	query := fmt.Sprintf(`UPDATE %s SET
		status = 'PROCESSING',
		processing_by = $1,
		processing_at = now()
	WHERE message_id IN (
		SELECT message_id FROM %s
		WHERE status IN ('PENDING', 'RETRY')
		  AND (next_retry_at IS NULL OR next_retry_at <= now())
		ORDER BY created_at
		FOR UPDATE SKIP LOCKED
		LIMIT $2
	)
	RETURNING %s`, s.hotTbl, s.hotTbl, hotCols)

	rows, err := s.db.QueryContext(ctx, query, s.cfg.WorkerID, size)
	if err != nil {
		return nil, fmt.Errorf("message: claim batch: %w", err)
	}
	defer rows.Close()
	return s.scanMessages(rows)
}

// markDoneAndArchive atomically finalizes messageID as PROCESSED: verifies
// this worker still owns the row (guards against a stale worker racing a
// RecoverStuck reclaim), copies it to archive, and deletes it from hot — all
// in one transaction. owned=false means ownership was lost (no-op, safe to ignore).
func (s *store) markDoneAndArchive(ctx context.Context, messageID string, ts time.Time) (owned bool, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("message: archive begin tx: %w", err)
	}
	defer tx.Rollback()

	updateSQL := fmt.Sprintf(`UPDATE %s SET processed_at = $3
		WHERE message_id = $1 AND processing_by = $2 AND status = 'PROCESSING'`, s.hotTbl)
	res, err := tx.ExecContext(ctx, updateSQL, messageID, s.cfg.WorkerID, ts)
	if err != nil {
		return false, fmt.Errorf("message: mark done: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return false, nil
	}

	insertSQL := fmt.Sprintf(`INSERT INTO %s (message_id, partition_key, topic, payload, headers, source, encrypted, final_status, retry_count, created_at, processed_at, last_error)
		SELECT message_id, partition_key, topic, payload, headers, source, encrypted, 'PROCESSED', retry_count, created_at, processed_at, last_error
		FROM %s WHERE message_id = $1`, s.archTbl, s.hotTbl)
	if _, err := tx.ExecContext(ctx, insertSQL, messageID); err != nil {
		return false, fmt.Errorf("message: archive insert: %w", err)
	}

	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE message_id = $1`, s.hotTbl), messageID); err != nil {
		return false, fmt.Errorf("message: archive delete: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("message: archive commit: %w", err)
	}
	return true, nil
}

// markRetry sets the message to RETRY with the next retry time, releasing
// ownership. Guarded by processing_by/status so a stale worker can't
// overwrite a row reclaimed by RecoverStuck.
func (s *store) markRetry(ctx context.Context, messageID string, retryCount int, nextRetryAt time.Time, lastError string) (owned bool, err error) {
	query := fmt.Sprintf(`UPDATE %s SET
		status = 'RETRY',
		retry_count = $2,
		next_retry_at = $3,
		last_error = $4,
		processing_by = NULL,
		processing_at = NULL
	WHERE message_id = $1 AND processing_by = $5 AND status = 'PROCESSING'`, s.hotTbl)

	res, err := s.db.ExecContext(ctx, query, messageID, retryCount, nextRetryAt, truncateError(lastError), s.cfg.WorkerID)
	if err != nil {
		return false, fmt.Errorf("message: mark retry: %w", err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// markFailed marks a message as FAILED via the processing path (owner-guarded).
// The row stays in hot as a DLQ entry — see RetryFailed/RetryOne/ArchiveExhausted.
func (s *store) markFailed(ctx context.Context, messageID string, lastError string) (owned bool, err error) {
	query := fmt.Sprintf(`UPDATE %s SET
		status = 'FAILED',
		last_error = $2,
		processed_at = now(),
		processing_by = NULL,
		processing_at = NULL
	WHERE message_id = $1 AND processing_by = $3 AND status = 'PROCESSING'`, s.hotTbl)

	res, err := s.db.ExecContext(ctx, query, messageID, truncateError(lastError), s.cfg.WorkerID)
	if err != nil {
		return false, fmt.Errorf("message: mark failed: %w", err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// deadLetter is the admin/no-handler path: forces FAILED unconditionally
// (no ownership check), leaving the row in hot as a DLQ entry.
func (s *store) deadLetter(ctx context.Context, messageID string, reason string) error {
	query := fmt.Sprintf(`UPDATE %s SET
		status = 'FAILED',
		last_error = $2,
		processed_at = now(),
		processing_by = NULL,
		processing_at = NULL
	WHERE message_id = $1`, s.hotTbl)
	_, err := s.db.ExecContext(ctx, query, messageID, truncateError(reason))
	if err != nil {
		return fmt.Errorf("message: dead letter: %w", err)
	}
	return nil
}

// scanMessageRow scans one hotCols row into a Message, leaving Payload/rawHeaders
// as-is (still encrypted if Encrypted) — the caller decides how to hydrate.
func scanMessageRow(rows *sql.Rows) (*Message, error) {
	m := &Message{}
	var partitionKey, source, processingBy, lastError sql.NullString
	if err := rows.Scan(
		&m.MessageID, &partitionKey, &m.Topic,
		&m.Payload, &m.rawHeaders, &source, &m.Encrypted,
		&m.Status, &m.RetryCount, &m.MaxRetries,
		&m.NextRetryAt, &processingBy, &m.ProcessingAt, &m.CreatedAt,
		&m.ProcessedAt, &lastError,
	); err != nil {
		return nil, fmt.Errorf("message: scan row: %w", err)
	}
	m.PartitionKey = partitionKey.String
	m.Source = source.String
	if processingBy.Valid {
		m.ProcessingBy = &processingBy.String
	}
	if lastError.Valid {
		m.LastError = &lastError.String
	}
	return m, nil
}

// scanMessages scans hotCols rows, deferring decryption of encrypted rows to
// the caller (see claimBatch's doc comment) — only unencrypted rows are
// hydrated (header-unmarshal) here, and any hydrate error aborts the whole scan.
func (s *store) scanMessages(rows *sql.Rows) ([]*Message, error) {
	var msgs []*Message
	for rows.Next() {
		m, err := scanMessageRow(rows)
		if err != nil {
			return nil, err
		}
		if !m.Encrypted {
			if err := s.hydrate(m); err != nil {
				return nil, err
			}
		}
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}

// scanMessagesSafe is the query/admin-path variant: every row (encrypted or
// not) is hydrated best-effort via hydrateSafe, never erroring the scan.
func (s *store) scanMessagesSafe(rows *sql.Rows) ([]*Message, error) {
	var msgs []*Message
	for rows.Next() {
		m, err := scanMessageRow(rows)
		if err != nil {
			return nil, err
		}
		s.hydrateSafe(m)
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}

// scanArchivedRow scans one archiveCols row into an ArchivedMessage, leaving
// Payload/rawHeaders as-is — the caller decides how to hydrate.
func scanArchivedRow(rows *sql.Rows) (*ArchivedMessage, error) {
	m := &ArchivedMessage{}
	var partitionKey, source, lastError sql.NullString
	if err := rows.Scan(
		&m.ID, &m.MessageID, &partitionKey, &m.Topic,
		&m.Payload, &m.rawHeaders, &source, &m.Encrypted,
		&m.FinalStatus, &m.RetryCount, &m.CreatedAt, &m.ProcessedAt,
		&m.ArchivedAt, &lastError,
	); err != nil {
		return nil, fmt.Errorf("message: scan archive row: %w", err)
	}
	m.PartitionKey = partitionKey.String
	m.Source = source.String
	if lastError.Valid {
		m.LastError = &lastError.String
	}
	return m, nil
}

// scanArchivedSafe is the query/admin-path variant: every row is hydrated
// best-effort via hydrateArchivedSafe, never erroring the scan.
func (s *store) scanArchivedSafe(rows *sql.Rows) ([]*ArchivedMessage, error) {
	var out []*ArchivedMessage
	for rows.Next() {
		m, err := scanArchivedRow(rows)
		if err != nil {
			return nil, err
		}
		s.hydrateArchivedSafe(m)
		out = append(out, m)
	}
	return out, rows.Err()
}

func nullString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
