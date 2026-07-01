package message

import (
	"context"
	"fmt"

	"github.com/lib/pq"
)

// CleanupResult summarizes one RunAll pass.
type CleanupResult struct {
	DedupPurged       int64
	Recovered         int64
	ExhaustedArchived int64
}

// purgeBatched runs `DELETE FROM table WHERE ctid IN (SELECT ctid FROM table
// WHERE <predicate> LIMIT batchSize)` repeatedly until a batch returns fewer
// than batchSize rows, avoiding a single unbounded DELETE that would
// lock/WAL-burst a large table. predicateArgs are appended after batchSize
// (so the predicate SQL must reference $2, $3, ...).
func (m *Messaging) purgeBatched(ctx context.Context, table, predicate string, batchSize int, predicateArgs ...any) (int64, error) {
	query := fmt.Sprintf(`DELETE FROM %s WHERE ctid IN (
		SELECT ctid FROM %s WHERE %s LIMIT $1
	)`, table, table, predicate)
	args := append([]any{batchSize}, predicateArgs...)

	var total int64
	for {
		res, err := m.store.db.ExecContext(ctx, query, args...)
		if err != nil {
			return total, fmt.Errorf("message: purge %s: %w", table, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return total, err
		}
		total += n
		if n < int64(batchSize) {
			return total, nil
		}
	}
}

// PurgeExpiredDedup deletes dedup entries whose expire_at has passed, in
// batches of batchSize. batchSize<=0 defaults to 1000.
func (m *Messaging) PurgeExpiredDedup(ctx context.Context, batchSize int) (int64, error) {
	if batchSize <= 0 {
		batchSize = 1000
	}
	return m.purgeBatched(ctx, m.store.dedupTbl, "expire_at IS NOT NULL AND expire_at < now()", batchSize)
}

// RecoverStuck resets messages stuck in PROCESSING longer than
// Config.ClaimTimeout: retry_count is incremented and the row goes back to
// RETRY, or to FAILED (dead-lettered, reason "stuck recovery exhausted") once
// retry_count exceeds max_retry — this bounds the CLAIMED<->RETRY loop for a
// handler that always crashes. Returns the number of rows recovered.
func (m *Messaging) RecoverStuck(ctx context.Context) (int64, error) {
	query := fmt.Sprintf(`UPDATE %s SET
		retry_count = retry_count + 1,
		status = CASE WHEN retry_count + 1 > max_retry THEN 'FAILED' ELSE 'RETRY' END,
		last_error = CASE WHEN retry_count + 1 > max_retry THEN 'stuck recovery exhausted' ELSE last_error END,
		processed_at = CASE WHEN retry_count + 1 > max_retry THEN now() ELSE processed_at END,
		next_retry_at = CASE WHEN retry_count + 1 > max_retry THEN next_retry_at ELSE now() END,
		processing_by = NULL,
		processing_at = NULL
	WHERE status = 'PROCESSING'
	  AND processing_at < now() - ($1::bigint * interval '1 millisecond')
	RETURNING message_id`, m.store.hotTbl)

	rows, err := m.store.db.QueryContext(ctx, query, m.cfg.ClaimTimeout.Milliseconds())
	if err != nil {
		return 0, fmt.Errorf("message: recover stuck: %w", err)
	}
	defer rows.Close()
	var n int64
	for rows.Next() {
		n++
	}
	return n, rows.Err()
}

// ArchiveExhausted moves up to batchSize FAILED rows older than
// Config.FailedRetention (measured from processed_at, i.e. the time they
// reached FAILED) into archive and removes them from hot. batchSize<=0
// defaults to 500. Call periodically (e.g. via RunAll) — it processes one
// batch per call, not an exhaustive drain.
func (m *Messaging) ArchiveExhausted(ctx context.Context, batchSize int) (int64, error) {
	if batchSize <= 0 {
		batchSize = 500
	}
	tx, err := m.store.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("message: archive exhausted: begin tx: %w", err)
	}
	defer tx.Rollback()

	selectSQL := fmt.Sprintf(`SELECT message_id FROM %s
		WHERE status = 'FAILED' AND processed_at < now() - ($1::bigint * interval '1 millisecond')
		ORDER BY processed_at
		FOR UPDATE SKIP LOCKED
		LIMIT $2`, m.store.hotTbl)
	rows, err := tx.QueryContext(ctx, selectSQL, m.cfg.FailedRetention.Milliseconds(), batchSize)
	if err != nil {
		return 0, fmt.Errorf("message: archive exhausted: select: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, fmt.Errorf("message: archive exhausted: scan: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()
	if len(ids) == 0 {
		return 0, nil
	}

	insertSQL := fmt.Sprintf(`INSERT INTO %s (message_id, partition_key, topic, payload, headers, source, encrypted, final_status, retry_count, created_at, processed_at, last_error)
		SELECT message_id, partition_key, topic, payload, headers, source, encrypted, 'FAILED', retry_count, created_at, processed_at, last_error
		FROM %s WHERE message_id = ANY($1)`, m.store.archTbl, m.store.hotTbl)
	if _, err := tx.ExecContext(ctx, insertSQL, pq.Array(ids)); err != nil {
		return 0, fmt.Errorf("message: archive exhausted: insert: %w", err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE message_id = ANY($1)`, m.store.hotTbl), pq.Array(ids)); err != nil {
		return 0, fmt.Errorf("message: archive exhausted: delete: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("message: archive exhausted: commit: %w", err)
	}
	return int64(len(ids)), nil
}

// PurgeOldArchive deletes archive rows older than retentionMillis (by
// archived_at), in batches of batchSize. batchSize<=0 defaults to 1000.
func (m *Messaging) PurgeOldArchive(ctx context.Context, retentionMillis int64, batchSize int) (int64, error) {
	if batchSize <= 0 {
		batchSize = 1000
	}
	return m.purgeBatched(ctx, m.store.archTbl, "archived_at < now() - ($2::bigint * interval '1 millisecond')", batchSize, retentionMillis)
}

// RunAll runs one pass of PurgeExpiredDedup, RecoverStuck, and
// ArchiveExhausted with default batch sizes. Intended to be called
// periodically (e.g. from the caller's own cron).
func (m *Messaging) RunAll(ctx context.Context) (CleanupResult, error) {
	var res CleanupResult
	var err error
	if res.DedupPurged, err = m.PurgeExpiredDedup(ctx, 0); err != nil {
		return res, err
	}
	if res.Recovered, err = m.RecoverStuck(ctx); err != nil {
		return res, err
	}
	if res.ExhaustedArchived, err = m.ArchiveExhausted(ctx, 0); err != nil {
		return res, err
	}
	return res, nil
}
