package message

import (
	"context"
	"errors"
	"testing"
	"time"
)

// receiveInTx is a test helper that wraps Receive in its own transaction.
func receiveInTx(t *testing.T, m *Messaging, ctx context.Context, msg *Message) (bool, error) {
	t.Helper()
	tx, err := m.store.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	isNew, err := m.Receive(ctx, tx, msg)
	if err != nil {
		tx.Rollback()
		return false, err
	}
	return isNew, tx.Commit()
}

func TestReceive_Success(t *testing.T) {
	m := newTestMessaging(t, Config{DedupTTL: time.Hour})
	ctx := context.Background()

	isNew, err := receiveInTx(t, m, ctx, &Message{
		MessageID:    "msg-001",
		Topic:        "order.created",
		PartitionKey: "order-service",
		Payload:      []byte(`{"id":1}`),
		Source:       "api",
	})
	if err != nil {
		t.Fatalf("Receive failed: %v", err)
	}
	if !isNew {
		t.Error("isNew = false, want true")
	}

	if n := countRows(t, m.store.db, m.store.dedupTbl); n != 1 {
		t.Errorf("dedup rows = %d, want 1", n)
	}
	if n := countRows(t, m.store.db, m.store.hotTbl); n != 1 {
		t.Errorf("hot rows = %d, want 1", n)
	}
}

func TestReceive_Duplicate(t *testing.T) {
	m := newTestMessaging(t, Config{DedupTTL: time.Hour})
	ctx := context.Background()

	msg := &Message{
		MessageID:    "msg-dup",
		Topic:        "order.created",
		PartitionKey: "order-service",
		Payload:      []byte(`{"id":1}`),
	}

	if _, err := receiveInTx(t, m, ctx, msg); err != nil {
		t.Fatalf("first Receive failed: %v", err)
	}
	isNew, err := receiveInTx(t, m, ctx, msg)
	if err != nil {
		t.Fatalf("duplicate Receive should return nil error, got: %v", err)
	}
	if isNew {
		t.Error("isNew = true, want false (duplicate)")
	}

	if n := countRows(t, m.store.db, m.store.hotTbl); n != 1 {
		t.Errorf("hot rows = %d, want 1 (dedup should prevent second insert)", n)
	}
}

func TestProcessBatch_Success(t *testing.T) {
	m := newTestMessaging(t, Config{DedupTTL: time.Hour, WorkerID: "worker-1"})
	ctx := context.Background()

	if _, err := receiveInTx(t, m, ctx, &Message{
		MessageID: "msg-process",
		Topic:     "test.event",
		Payload:   []byte(`{}`),
	}); err != nil {
		t.Fatalf("Receive failed: %v", err)
	}

	n, err := m.ProcessBatch(ctx, 10, func(ctx context.Context, msg *Message) error {
		return nil
	})
	if err != nil {
		t.Fatalf("ProcessBatch failed: %v", err)
	}
	if n != 1 {
		t.Errorf("processed = %d, want 1", n)
	}

	if n := countRows(t, m.store.db, m.store.hotTbl); n != 0 {
		t.Errorf("hot rows = %d, want 0 (should be archived)", n)
	}
	if n := countRows(t, m.store.db, m.store.archTbl); n != 1 {
		t.Errorf("archive rows = %d, want 1", n)
	}

	var finalStatus string
	if err := m.store.db.QueryRow("SELECT final_status FROM " + m.store.archTbl + " LIMIT 1").Scan(&finalStatus); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if finalStatus != "PROCESSED" {
		t.Errorf("final_status = %s, want PROCESSED", finalStatus)
	}
}

func TestProcessBatch_Retry(t *testing.T) {
	m := newTestMessaging(t, Config{
		DedupTTL:          time.Hour,
		WorkerID:          "worker-1",
		DefaultMaxRetries: 3,
		DefaultBackoff:    FixedBackoff{Interval: time.Second},
	})
	ctx := context.Background()

	if _, err := receiveInTx(t, m, ctx, &Message{
		MessageID: "msg-retry",
		Topic:     "test.event",
		Payload:   []byte(`{}`),
	}); err != nil {
		t.Fatalf("Receive failed: %v", err)
	}

	n, err := m.ProcessBatch(ctx, 10, func(ctx context.Context, msg *Message) error {
		return errors.New("temporary failure")
	})
	if err != nil {
		t.Fatalf("ProcessBatch failed: %v", err)
	}
	if n != 1 {
		t.Errorf("processed = %d, want 1", n)
	}

	if n := countRows(t, m.store.db, m.store.hotTbl); n != 1 {
		t.Errorf("hot rows = %d, want 1 (should be retrying)", n)
	}

	var retryCount int
	var status string
	err = m.store.db.QueryRow("SELECT retry_count, status FROM "+m.store.hotTbl+" LIMIT 1").Scan(&retryCount, &status)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if retryCount != 1 {
		t.Errorf("retry_count = %d, want 1", retryCount)
	}
	if status != "RETRY" {
		t.Errorf("status = %s, want RETRY", status)
	}
}

func TestProcessBatch_MaxRetryExceeded_StaysInHotAsDLQ(t *testing.T) {
	m := newTestMessaging(t, Config{
		DedupTTL:          time.Hour,
		WorkerID:          "worker-1",
		DefaultMaxRetries: 1,
		DefaultBackoff:    FixedBackoff{Interval: 0},
	})
	ctx := context.Background()

	if _, err := receiveInTx(t, m, ctx, &Message{
		MessageID: "msg-fail",
		Topic:     "test.event",
		Payload:   []byte(`{}`),
	}); err != nil {
		t.Fatalf("Receive failed: %v", err)
	}

	handler := func(ctx context.Context, msg *Message) error {
		return errors.New("permanent failure")
	}

	m.ProcessBatch(ctx, 10, handler)
	m.store.db.Exec("UPDATE " + m.store.hotTbl + " SET next_retry_at = now() - interval '1 minute'")
	m.ProcessBatch(ctx, 10, handler)

	// FAILED rows stay in hot as a DLQ, they are not archived immediately.
	if n := countRows(t, m.store.db, m.store.hotTbl); n != 1 {
		t.Errorf("hot rows = %d, want 1 (FAILED stays in hot as DLQ)", n)
	}
	if n := countRows(t, m.store.db, m.store.archTbl); n != 0 {
		t.Errorf("archive rows = %d, want 0 (not archived yet)", n)
	}

	var status string
	if err := m.store.db.QueryRow("SELECT status FROM " + m.store.hotTbl + " LIMIT 1").Scan(&status); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if status != "FAILED" {
		t.Errorf("status = %s, want FAILED", status)
	}
}

func TestProcessBatch_NoHandler_DeadLetters(t *testing.T) {
	m := newTestMessaging(t, Config{WorkerID: "worker-1"})
	ctx := context.Background()

	if err := m.Enqueue(ctx, nil, &Message{MessageID: "msg-nohandler", Topic: "unregistered.topic", Payload: []byte(`{}`)}); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	n, err := m.ProcessBatch(ctx, 10, nil)
	if err != nil {
		t.Fatalf("ProcessBatch failed: %v", err)
	}
	if n != 1 {
		t.Errorf("claimed = %d, want 1", n)
	}

	var status string
	if err := m.store.db.QueryRow("SELECT status FROM " + m.store.hotTbl + " WHERE message_id = 'msg-nohandler'").Scan(&status); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if status != "FAILED" {
		t.Errorf("status = %s, want FAILED", status)
	}
}

func TestOwnershipGuard_StaleWorkerCannotOverwriteReclaimedRow(t *testing.T) {
	m := newTestMessaging(t, Config{WorkerID: "worker-1"})
	ctx := context.Background()

	if _, err := receiveInTx(t, m, ctx, &Message{MessageID: "msg-race", Topic: "t", Payload: []byte(`{}`)}); err != nil {
		t.Fatalf("Receive failed: %v", err)
	}

	msgs, err := m.store.claimBatch(ctx, 10)
	if err != nil {
		t.Fatalf("claimBatch failed: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("claimed %d, want 1", len(msgs))
	}

	// Simulate the row having been reclaimed by another worker (e.g. via RecoverStuck).
	if _, err := m.store.db.Exec("UPDATE "+m.store.hotTbl+" SET processing_by = 'worker-2' WHERE message_id = $1", "msg-race"); err != nil {
		t.Fatalf("update failed: %v", err)
	}

	owned, err := m.store.markDoneAndArchive(ctx, "msg-race", time.Now())
	if err != nil {
		t.Fatalf("markDoneAndArchive failed: %v", err)
	}
	if owned {
		t.Error("owned = true, want false (worker-1 lost ownership to worker-2)")
	}

	var processingBy string
	if err := m.store.db.QueryRow("SELECT processing_by FROM "+m.store.hotTbl+" WHERE message_id = $1", "msg-race").Scan(&processingBy); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if processingBy != "worker-2" {
		t.Errorf("processing_by = %s, want worker-2 (worker-1's stale write must not apply)", processingBy)
	}
}
