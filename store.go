package message

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

type store struct {
	db       *sql.DB
	cfg      Config
	dedupTbl string
	hotTbl   string
	archTbl  string
}

func newStore(db *sql.DB, cfg Config) *store {
	cfg = cfg.withDefaults()
	return &store{
		db:       db,
		cfg:      cfg,
		dedupTbl: cfg.tableName("message_dedup"),
		hotTbl:   cfg.tableName("message_hot"),
		archTbl:  cfg.tableName("message_archive"),
	}
}

// insertDedup inserts a dedup record. Returns ErrDuplicate if the message already exists.
func (s *store) insertDedup(ctx context.Context, db DBTX, consumerGroup, messageID, eventType, source string, expireAt *time.Time) error {
	query := fmt.Sprintf(`INSERT INTO %s (consumer_group, message_id, event_type, source, expire_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (consumer_group, message_id) DO NOTHING`, s.dedupTbl)

	result, err := db.ExecContext(ctx, query, consumerGroup, messageID, eventType, source, expireAt)
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

// insertHot inserts a message into the hot table.
func (s *store) insertHot(ctx context.Context, db DBTX, msg *Message) error {
	headers, err := json.Marshal(msg.Headers)
	if err != nil {
		return fmt.Errorf("message: marshal headers: %w", err)
	}

	query := fmt.Sprintf(`INSERT INTO %s
		(direction, consumer_group, message_id, event_id, event_type, payload, headers, source, status, retry_count, max_retry, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		RETURNING id`, s.hotTbl)

	return db.QueryRowContext(ctx, query,
		msg.Direction,
		msg.ConsumerGroup,
		msg.MessageID,
		msg.EventID,
		msg.EventType,
		msg.Payload,
		headers,
		msg.Source,
		StatusPending,
		0,
		msg.MaxRetries,
		time.Now(),
	).Scan(&msg.ID)
}

// claimBatch claims up to `size` messages for the given direction and returns them.
func (s *store) claimBatch(ctx context.Context, direction Direction, consumerGroup string, size int) ([]*Message, error) {
	workerID := s.cfg.WorkerID

	var query string
	var args []any

	if direction == DirectionInbox {
		query = fmt.Sprintf(`UPDATE %s SET
			status = 'CLAIMED',
			claimed_by = $1,
			claimed_at = now()
		WHERE id IN (
			SELECT id FROM %s
			WHERE direction = $2
			  AND status IN ('PENDING', 'RETRY')
			  AND (next_retry_at IS NULL OR next_retry_at <= now())
			  AND claimed_at IS NULL
			  AND consumer_group = $3
			ORDER BY created_at
			FOR UPDATE SKIP LOCKED
			LIMIT $4
		)
		RETURNING id, direction, consumer_group, message_id, event_id, event_type,
		          payload, headers, source, status, retry_count, max_retry,
		          next_retry_at, claimed_by, claimed_at, created_at,
		          processed_at, published_at, last_error`,
			s.hotTbl, s.hotTbl)
		args = []any{workerID, string(direction), consumerGroup, size}
	} else {
		query = fmt.Sprintf(`UPDATE %s SET
			status = 'CLAIMED',
			claimed_by = $1,
			claimed_at = now()
		WHERE id IN (
			SELECT id FROM %s
			WHERE direction = $2
			  AND status IN ('PENDING', 'RETRY')
			  AND (next_retry_at IS NULL OR next_retry_at <= now())
			  AND claimed_at IS NULL
			ORDER BY created_at
			FOR UPDATE SKIP LOCKED
			LIMIT $3
		)
		RETURNING id, direction, consumer_group, message_id, event_id, event_type,
		          payload, headers, source, status, retry_count, max_retry,
		          next_retry_at, claimed_by, claimed_at, created_at,
		          processed_at, published_at, last_error`,
			s.hotTbl, s.hotTbl)
		args = []any{workerID, string(direction), size}
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("message: claim batch: %w", err)
	}
	defer rows.Close()

	return scanMessages(rows)
}

// markDone marks a message as done.
func (s *store) markDone(ctx context.Context, id int64, ts time.Time, field string) error {
	query := fmt.Sprintf(`UPDATE %s SET status = 'DONE', %s = $2 WHERE id = $1`, s.hotTbl, field)
	_, err := s.db.ExecContext(ctx, query, id, ts)
	if err != nil {
		return fmt.Errorf("message: mark done: %w", err)
	}
	return nil
}

// markRetry sets the message to retry status with the next retry time.
func (s *store) markRetry(ctx context.Context, id int64, retryCount int, nextRetryAt time.Time, lastError string) error {
	query := fmt.Sprintf(`UPDATE %s SET
		status = 'RETRY',
		retry_count = $2,
		next_retry_at = $3,
		last_error = $4,
		claimed_by = NULL,
		claimed_at = NULL
	WHERE id = $1`, s.hotTbl)

	_, err := s.db.ExecContext(ctx, query, id, retryCount, nextRetryAt, lastError)
	if err != nil {
		return fmt.Errorf("message: mark retry: %w", err)
	}
	return nil
}

// markFailed marks a message as permanently failed.
func (s *store) markFailed(ctx context.Context, id int64, lastError string) error {
	query := fmt.Sprintf(`UPDATE %s SET status = 'FAILED', last_error = $2 WHERE id = $1`, s.hotTbl)
	_, err := s.db.ExecContext(ctx, query, id, lastError)
	if err != nil {
		return fmt.Errorf("message: mark failed: %w", err)
	}
	return nil
}

// archiveByID moves a message from hot to archive atomically.
func (s *store) archiveByID(ctx context.Context, id int64, finalStatus string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("message: archive begin tx: %w", err)
	}
	defer tx.Rollback()

	insertSQL := fmt.Sprintf(`INSERT INTO %s
		(direction, consumer_group, message_id, event_id, event_type, payload, headers, source,
		 final_status, retry_count, created_at, processed_at, published_at, last_error)
		SELECT direction, consumer_group, message_id, event_id, event_type, payload, headers, source,
		       $2, retry_count, created_at, processed_at, published_at, last_error
		FROM %s WHERE id = $1`, s.archTbl, s.hotTbl)

	result, err := tx.ExecContext(ctx, insertSQL, id, finalStatus)
	if err != nil {
		return fmt.Errorf("message: archive insert: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("message: archive rows affected: %w", err)
	}
	if rows == 0 {
		return ErrNotFound
	}

	deleteSQL := fmt.Sprintf(`DELETE FROM %s WHERE id = $1`, s.hotTbl)
	if _, err := tx.ExecContext(ctx, deleteSQL, id); err != nil {
		return fmt.Errorf("message: archive delete: %w", err)
	}

	return tx.Commit()
}

// cleanupDedup deletes expired dedup entries.
func (s *store) cleanupDedup(ctx context.Context) (int64, error) {
	query := fmt.Sprintf(`DELETE FROM %s WHERE expire_at IS NOT NULL AND expire_at < now()`, s.dedupTbl)
	result, err := s.db.ExecContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("message: cleanup dedup: %w", err)
	}
	return result.RowsAffected()
}

// recoverStuck resets messages that have been claimed for too long.
func (s *store) recoverStuck(ctx context.Context, direction Direction, timeout time.Duration) (int64, error) {
	query := fmt.Sprintf(`UPDATE %s SET
		status = 'RETRY',
		claimed_by = NULL,
		claimed_at = NULL,
		next_retry_at = now()
	WHERE direction = $1
	  AND status = 'CLAIMED'
	  AND claimed_at < now() - $2::interval`, s.hotTbl)

	result, err := s.db.ExecContext(ctx, query, string(direction), fmt.Sprintf("%d seconds", int(timeout.Seconds())))
	if err != nil {
		return 0, fmt.Errorf("message: recover stuck: %w", err)
	}
	return result.RowsAffected()
}

// scanMessages scans sql.Rows into Message slices.
func scanMessages(rows *sql.Rows) ([]*Message, error) {
	var msgs []*Message
	for rows.Next() {
		m := &Message{}
		var headers []byte
		err := rows.Scan(
			&m.ID, &m.Direction, &m.ConsumerGroup, &m.MessageID, &m.EventID,
			&m.EventType, &m.Payload, &headers, &m.Source,
			&m.Status, &m.RetryCount, &m.MaxRetries,
			&m.NextRetryAt, &m.ClaimedBy, &m.ClaimedAt, &m.CreatedAt,
			&m.ProcessedAt, &m.PublishedAt, &m.LastError,
		)
		if err != nil {
			return nil, fmt.Errorf("message: scan row: %w", err)
		}
		if headers != nil {
			_ = json.Unmarshal(headers, &m.Headers)
		}
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}
