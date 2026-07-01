package message

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPurgeExpiredDedup(t *testing.T) {
	m := newTestMessaging(t, Config{DedupTTL: time.Millisecond})
	ctx := context.Background()

	receiveInTx(t, m, ctx, &Message{MessageID: "msg-expire", Topic: "test", Payload: []byte(`{}`)})

	m.store.db.Exec("UPDATE " + m.store.dedupTbl + " SET expire_at = now() - interval '1 hour'")

	n, err := m.PurgeExpiredDedup(ctx, 0)
	if err != nil {
		t.Fatalf("PurgeExpiredDedup failed: %v", err)
	}
	if n != 1 {
		t.Errorf("purged = %d, want 1", n)
	}

	if remaining := countRows(t, m.store.db, m.store.dedupTbl); remaining != 0 {
		t.Errorf("dedup rows = %d, want 0", remaining)
	}
}

func TestRecoverStuck_ReturnsToRetry(t *testing.T) {
	m := newTestMessaging(t, Config{WorkerID: "w1", ClaimTimeout: time.Second})
	ctx := context.Background()

	receiveInTx(t, m, ctx, &Message{MessageID: "msg-ok", Topic: "test", Payload: []byte(`{}`)})
	m.ProcessBatch(ctx, 10, func(ctx context.Context, msg *Message) error { return nil })

	receiveInTx(t, m, ctx, &Message{MessageID: "msg-stuck", Topic: "test", Payload: []byte(`{}`)})
	m.store.db.Exec("UPDATE " + m.store.hotTbl + " SET status = 'PROCESSING', processing_by = 'dead-worker', processing_at = now() - interval '10 minutes' WHERE message_id = 'msg-stuck'")

	n, err := m.RecoverStuck(ctx)
	if err != nil {
		t.Fatalf("RecoverStuck failed: %v", err)
	}
	if n != 1 {
		t.Errorf("recovered = %d, want 1", n)
	}

	var status string
	var retryCount int
	m.store.db.QueryRow("SELECT status, retry_count FROM "+m.store.hotTbl+" WHERE message_id = 'msg-stuck'").Scan(&status, &retryCount)
	if status != "RETRY" {
		t.Errorf("status = %s, want RETRY", status)
	}
	if retryCount != 1 {
		t.Errorf("retry_count = %d, want 1", retryCount)
	}
}

func TestRecoverStuck_DeadLettersWhenBudgetExhausted(t *testing.T) {
	m := newTestMessaging(t, Config{WorkerID: "w1", ClaimTimeout: time.Second, DefaultMaxRetries: 1})
	ctx := context.Background()

	receiveInTx(t, m, ctx, &Message{MessageID: "msg-poison", Topic: "test", Payload: []byte(`{}`)})
	m.store.db.Exec("UPDATE " + m.store.hotTbl + " SET status = 'PROCESSING', processing_by = 'dead-worker', processing_at = now() - interval '10 minutes', retry_count = 1 WHERE message_id = 'msg-poison'")

	n, err := m.RecoverStuck(ctx)
	if err != nil {
		t.Fatalf("RecoverStuck failed: %v", err)
	}
	if n != 1 {
		t.Errorf("recovered = %d, want 1", n)
	}

	var status string
	m.store.db.QueryRow("SELECT status FROM " + m.store.hotTbl + " WHERE message_id = 'msg-poison'").Scan(&status)
	if status != "FAILED" {
		t.Errorf("status = %s, want FAILED (retry budget exhausted)", status)
	}
}

func TestArchiveExhausted(t *testing.T) {
	m := newTestMessaging(t, Config{WorkerID: "w1", DefaultMaxRetries: 1, FailedRetention: time.Millisecond})
	ctx := context.Background()

	receiveInTx(t, m, ctx, &Message{MessageID: "msg-exhaust", Topic: "test", Payload: []byte(`{}`)})
	handler := func(ctx context.Context, msg *Message) error { return errors.New("boom") }
	m.ProcessBatch(ctx, 10, handler)
	m.store.db.Exec("UPDATE " + m.store.hotTbl + " SET next_retry_at = now() - interval '1 minute'")
	m.ProcessBatch(ctx, 10, handler)

	var status string
	m.store.db.QueryRow("SELECT status FROM " + m.store.hotTbl + " WHERE message_id = 'msg-exhaust'").Scan(&status)
	if status != "FAILED" {
		t.Fatalf("precondition failed: status = %s, want FAILED", status)
	}

	time.Sleep(10 * time.Millisecond)

	n, err := m.ArchiveExhausted(ctx, 0)
	if err != nil {
		t.Fatalf("ArchiveExhausted failed: %v", err)
	}
	if n != 1 {
		t.Errorf("archived = %d, want 1", n)
	}

	if remaining := countRows(t, m.store.db, m.store.hotTbl); remaining != 0 {
		t.Errorf("hot rows = %d, want 0", remaining)
	}
	var finalStatus string
	m.store.db.QueryRow("SELECT final_status FROM " + m.store.archTbl + " WHERE message_id = 'msg-exhaust'").Scan(&finalStatus)
	if finalStatus != "FAILED" {
		t.Errorf("final_status = %s, want FAILED", finalStatus)
	}
}
