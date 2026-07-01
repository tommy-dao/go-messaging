package message

import (
	"context"
	"fmt"
	"time"
)

// PageOptions bounds a listing query.
type PageOptions struct {
	Limit  int
	Offset int
}

func (p PageOptions) withDefaults() PageOptions {
	if p.Limit <= 0 {
		p.Limit = 100
	}
	return p
}

// MessageCountByStatus is the result of GetMetrics.
type MessageCountByStatus struct {
	Pending    int64
	Processing int64
	Retry      int64
	Failed     int64
}

// FilterOptions narrows admin/query operations by topic and/or partition key.
type FilterOptions struct {
	Topic        string
	PartitionKey string
}

// whereBuilder accumulates a "col = $N" AND-chain alongside its positional
// args, so the placeholder index is always derived from len(args) — used by
// every admin/query method below to avoid re-deriving $N by hand at each call site.
type whereBuilder struct {
	clause string
	args   []any
}

// and appends " AND <cond> $N" where $N is the next placeholder position, and
// appends val to args. cond must end just before the placeholder, e.g. "topic =".
func (w *whereBuilder) and(cond string, val any) {
	w.args = append(w.args, val)
	w.clause += fmt.Sprintf(" AND %s $%d", cond, len(w.args))
}

func (w *whereBuilder) applyFilter(opts FilterOptions) {
	if opts.Topic != "" {
		w.and("topic =", opts.Topic)
	}
	if opts.PartitionKey != "" {
		w.and("partition_key =", opts.PartitionKey)
	}
}

// DeadLetter forces messageID to FAILED unconditionally (no ownership
// check — this is an admin action, unlike the processing-path markFailed).
// The row stays in hot as a DLQ entry.
func (m *Messaging) DeadLetter(ctx context.Context, messageID string, reason string) error {
	return m.store.deadLetter(ctx, messageID, reason)
}

// RetryFailedOptions filters and bounds RetryFailed.
type RetryFailedOptions struct {
	FilterOptions
	Before *time.Time // only rows FAILED at or before this time (fences a drain loop off newly-failing rows)
	Limit  int        // default 1000
}

// RetryFailed resets up to opts.Limit FAILED rows (oldest first) back to
// PENDING, clearing retry_count/last_error. Always bounded — pass Limit to
// override the default of 1000.
func (m *Messaging) RetryFailed(ctx context.Context, opts RetryFailedOptions) (int64, error) {
	if opts.Limit <= 0 {
		opts.Limit = 1000
	}
	wb := &whereBuilder{clause: `status = 'FAILED'`, args: []any{opts.Limit}}
	wb.applyFilter(opts.FilterOptions)
	if opts.Before != nil {
		wb.and("processed_at <=", *opts.Before)
	}
	query := fmt.Sprintf(`UPDATE %s SET status = 'PENDING', retry_count = 0, next_retry_at = NULL, last_error = NULL, processed_at = NULL
		WHERE message_id IN (
			SELECT message_id FROM %s WHERE %s ORDER BY processed_at LIMIT $1
		)`, m.store.hotTbl, m.store.hotTbl, wb.clause)
	res, err := m.store.db.ExecContext(ctx, query, wb.args...)
	if err != nil {
		return 0, fmt.Errorf("message: retry failed: %w", err)
	}
	return res.RowsAffected()
}

// RetryAllFailedOptions filters RetryAllFailed and sets its per-iteration batch size.
type RetryAllFailedOptions struct {
	FilterOptions
	BatchSize int // default 500
}

// RetryAllFailed drains every FAILED row present "as of now" back to PENDING,
// looping RetryFailed in batches. The cutoff is fixed at call time (fenced by
// Before=now()) so rows that fail again after re-queueing don't cause an
// infinite loop.
func (m *Messaging) RetryAllFailed(ctx context.Context, opts RetryAllFailedOptions) (int64, error) {
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 500
	}
	cutoff := time.Now()
	var total int64
	for {
		n, err := m.RetryFailed(ctx, RetryFailedOptions{
			FilterOptions: opts.FilterOptions,
			Before:        &cutoff,
			Limit:         batchSize,
		})
		if err != nil {
			return total, err
		}
		total += n
		if n < int64(batchSize) {
			return total, nil
		}
	}
}

// RetryOne resets a single FAILED row back to PENDING.
func (m *Messaging) RetryOne(ctx context.Context, messageID string) error {
	query := fmt.Sprintf(`UPDATE %s SET status = 'PENDING', retry_count = 0, next_retry_at = NULL, last_error = NULL, processed_at = NULL
		WHERE message_id = $1 AND status = 'FAILED'`, m.store.hotTbl)
	res, err := m.store.db.ExecContext(ctx, query, messageID)
	if err != nil {
		return fmt.Errorf("message: retry one: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// GetMetrics returns row counts by status in the hot table, optionally filtered.
func (m *Messaging) GetMetrics(ctx context.Context, opts FilterOptions) (MessageCountByStatus, error) {
	wb := &whereBuilder{clause: "1=1"}
	wb.applyFilter(opts)
	query := fmt.Sprintf(`SELECT status, COUNT(*) FROM %s WHERE %s GROUP BY status`, m.store.hotTbl, wb.clause)
	rows, err := m.store.db.QueryContext(ctx, query, wb.args...)
	if err != nil {
		return MessageCountByStatus{}, fmt.Errorf("message: get metrics: %w", err)
	}
	defer rows.Close()

	var out MessageCountByStatus
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return MessageCountByStatus{}, fmt.Errorf("message: scan metrics: %w", err)
		}
		switch Status(status) {
		case StatusPending:
			out.Pending = count
		case StatusProcessing:
			out.Processing = count
		case StatusRetry:
			out.Retry = count
		case StatusFailed:
			out.Failed = count
		}
	}
	return out, rows.Err()
}

func (m *Messaging) listByStatus(ctx context.Context, status Status, opts FilterOptions, page PageOptions) ([]*Message, error) {
	page = page.withDefaults()
	wb := &whereBuilder{clause: "status = $1", args: []any{string(status)}}
	wb.applyFilter(opts)
	wb.args = append(wb.args, page.Limit, page.Offset)
	query := fmt.Sprintf(`SELECT %s FROM %s WHERE %s ORDER BY created_at LIMIT $%d OFFSET $%d`,
		hotCols, m.store.hotTbl, wb.clause, len(wb.args)-1, len(wb.args))
	rows, err := m.store.db.QueryContext(ctx, query, wb.args...)
	if err != nil {
		return nil, fmt.Errorf("message: list %s: %w", status, err)
	}
	defer rows.Close()
	msgs, err := m.store.scanMessagesSafe(rows)
	if err != nil {
		return nil, err
	}
	return msgs, nil
}

// ListFailed pages FAILED (DLQ) rows, oldest first.
func (m *Messaging) ListFailed(ctx context.Context, opts FilterOptions, page PageOptions) ([]*Message, error) {
	return m.listByStatus(ctx, StatusFailed, opts, page)
}

// ListPending pages PENDING rows, oldest first.
func (m *Messaging) ListPending(ctx context.Context, opts FilterOptions, page PageOptions) ([]*Message, error) {
	return m.listByStatus(ctx, StatusPending, opts, page)
}

// FindByMessageID returns the hot row for messageID, or nil if not found.
func (m *Messaging) FindByMessageID(ctx context.Context, messageID string) (*Message, error) {
	query := fmt.Sprintf(`SELECT %s FROM %s WHERE message_id = $1`, hotCols, m.store.hotTbl)
	rows, err := m.store.db.QueryContext(ctx, query, messageID)
	if err != nil {
		return nil, fmt.Errorf("message: find by message id: %w", err)
	}
	defer rows.Close()
	msgs, err := m.store.scanMessagesSafe(rows)
	if err != nil {
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, nil
	}
	return msgs[0], nil
}

// ListArchivedOptions filters ListArchived.
type ListArchivedOptions struct {
	Topic       string
	FinalStatus FinalStatus // empty = any
}

// ListArchived pages archive rows, newest first.
func (m *Messaging) ListArchived(ctx context.Context, opts ListArchivedOptions, page PageOptions) ([]*ArchivedMessage, error) {
	page = page.withDefaults()
	wb := &whereBuilder{clause: "1=1"}
	if opts.Topic != "" {
		wb.and("topic =", opts.Topic)
	}
	if opts.FinalStatus != "" {
		wb.and("final_status =", string(opts.FinalStatus))
	}
	wb.args = append(wb.args, page.Limit, page.Offset)
	query := fmt.Sprintf(`SELECT %s FROM %s WHERE %s ORDER BY archived_at DESC LIMIT $%d OFFSET $%d`,
		archiveCols, m.store.archTbl, wb.clause, len(wb.args)-1, len(wb.args))
	rows, err := m.store.db.QueryContext(ctx, query, wb.args...)
	if err != nil {
		return nil, fmt.Errorf("message: list archived: %w", err)
	}
	defer rows.Close()
	return m.store.scanArchivedSafe(rows)
}

// IsDuplicate reports whether messageID is currently present in the dedup table.
func (m *Messaging) IsDuplicate(ctx context.Context, messageID string) (bool, error) {
	var exists bool
	query := fmt.Sprintf(`SELECT EXISTS(SELECT 1 FROM %s WHERE message_id = $1)`, m.store.dedupTbl)
	if err := m.store.db.QueryRowContext(ctx, query, messageID).Scan(&exists); err != nil {
		return false, fmt.Errorf("message: is duplicate: %w", err)
	}
	return exists, nil
}

// RemoveDedup deletes messageID's dedup entry, allowing it to be received
// again. Returns false if no entry existed.
func (m *Messaging) RemoveDedup(ctx context.Context, messageID string) (bool, error) {
	query := fmt.Sprintf(`DELETE FROM %s WHERE message_id = $1`, m.store.dedupTbl)
	res, err := m.store.db.ExecContext(ctx, query, messageID)
	if err != nil {
		return false, fmt.Errorf("message: remove dedup: %w", err)
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// scanMessagesSafe/scanArchivedSafe (query/admin paths, using hydrateSafe so
// a broken cipher doesn't hide DLQ rows from listing) live in store.go next
// to their row-scanning counterparts.
