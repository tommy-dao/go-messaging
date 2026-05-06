package message

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestInbox_Receive_Success(t *testing.T) {
	db := setupTestDB(t)
	cfg := Config{TablePrefix: testPrefix, DedupTTL: time.Hour}
	m := New(db, cfg)
	ctx := context.Background()

	err := m.Inbox.Receive(ctx, InboxMessage{
		ConsumerGroup: "order-service",
		MessageID:     "msg-001",
		EventType:     "order.created",
		Payload:       []byte(`{"id":1}`),
		Source:        "api",
	})
	if err != nil {
		t.Fatalf("Receive failed: %v", err)
	}

	if n := countRows(t, db, testPrefix+"_message_dedup"); n != 1 {
		t.Errorf("dedup rows = %d, want 1", n)
	}
	if n := countRows(t, db, testPrefix+"_message_hot"); n != 1 {
		t.Errorf("hot rows = %d, want 1", n)
	}
}

func TestInbox_Receive_Duplicate(t *testing.T) {
	db := setupTestDB(t)
	cfg := Config{TablePrefix: testPrefix, DedupTTL: time.Hour}
	m := New(db, cfg)
	ctx := context.Background()

	msg := InboxMessage{
		ConsumerGroup: "order-service",
		MessageID:     "msg-dup",
		EventType:     "order.created",
		Payload:       []byte(`{"id":1}`),
	}

	if err := m.Inbox.Receive(ctx, msg); err != nil {
		t.Fatalf("first Receive failed: %v", err)
	}

	// Second call should return nil (idempotent)
	if err := m.Inbox.Receive(ctx, msg); err != nil {
		t.Fatalf("duplicate Receive should return nil, got: %v", err)
	}

	// Should still have only 1 hot message
	if n := countRows(t, db, testPrefix+"_message_hot"); n != 1 {
		t.Errorf("hot rows = %d, want 1 (dedup should prevent second insert)", n)
	}
}

func TestInbox_ProcessBatch_Success(t *testing.T) {
	db := setupTestDB(t)
	cfg := Config{TablePrefix: testPrefix, DedupTTL: time.Hour, WorkerID: "worker-1"}
	m := New(db, cfg)
	ctx := context.Background()

	// Insert a message
	if err := m.Inbox.Receive(ctx, InboxMessage{
		ConsumerGroup: "svc",
		MessageID:     "msg-process",
		EventType:     "test.event",
		Payload:       []byte(`{}`),
	}); err != nil {
		t.Fatalf("Receive failed: %v", err)
	}

	// Process it
	n, err := m.Inbox.ProcessBatch(ctx, "svc", 10, func(ctx context.Context, msg *Message) error {
		return nil // success
	})
	if err != nil {
		t.Fatalf("ProcessBatch failed: %v", err)
	}
	if n != 1 {
		t.Errorf("processed = %d, want 1", n)
	}

	// Hot should be empty (archived)
	if n := countRows(t, db, testPrefix+"_message_hot"); n != 0 {
		t.Errorf("hot rows = %d, want 0 (should be archived)", n)
	}

	// Archive should have 1
	if n := countRows(t, db, testPrefix+"_message_archive"); n != 1 {
		t.Errorf("archive rows = %d, want 1", n)
	}
}

func TestInbox_ProcessBatch_Retry(t *testing.T) {
	db := setupTestDB(t)
	cfg := Config{
		TablePrefix:       testPrefix,
		DedupTTL:          time.Hour,
		WorkerID:          "worker-1",
		DefaultMaxRetries: 3,
		DefaultBackoff:    FixedBackoff{Interval: time.Second},
	}
	m := New(db, cfg)
	ctx := context.Background()

	if err := m.Inbox.Receive(ctx, InboxMessage{
		ConsumerGroup: "svc",
		MessageID:     "msg-retry",
		EventType:     "test.event",
		Payload:       []byte(`{}`),
	}); err != nil {
		t.Fatalf("Receive failed: %v", err)
	}

	// Fail the handler
	n, err := m.Inbox.ProcessBatch(ctx, "svc", 10, func(ctx context.Context, msg *Message) error {
		return errors.New("temporary failure")
	})
	if err != nil {
		t.Fatalf("ProcessBatch failed: %v", err)
	}
	if n != 1 {
		t.Errorf("processed = %d, want 1", n)
	}

	// Message should still be in hot (with retry status)
	if n := countRows(t, db, testPrefix+"_message_hot"); n != 1 {
		t.Errorf("hot rows = %d, want 1 (should be retrying)", n)
	}

	// Verify retry_count was incremented
	var retryCount int
	var status string
	err = db.QueryRow("SELECT retry_count, status FROM " + testPrefix + "_message_hot LIMIT 1").Scan(&retryCount, &status)
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

func TestInbox_ProcessBatch_MaxRetryExceeded(t *testing.T) {
	db := setupTestDB(t)
	cfg := Config{
		TablePrefix:       testPrefix,
		DedupTTL:          time.Hour,
		WorkerID:          "worker-1",
		DefaultMaxRetries: 1,
		DefaultBackoff:    FixedBackoff{Interval: 0}, // immediate retry
	}
	m := New(db, cfg)
	ctx := context.Background()

	if err := m.Inbox.Receive(ctx, InboxMessage{
		ConsumerGroup: "svc",
		MessageID:     "msg-fail",
		EventType:     "test.event",
		Payload:       []byte(`{}`),
	}); err != nil {
		t.Fatalf("Receive failed: %v", err)
	}

	handler := func(ctx context.Context, msg *Message) error {
		return errors.New("permanent failure")
	}

	// First attempt: should go to RETRY
	m.Inbox.ProcessBatch(ctx, "svc", 10, handler)

	// Update next_retry_at to past so it can be claimed again
	db.Exec("UPDATE " + testPrefix + "_message_hot SET next_retry_at = now() - interval '1 minute'")

	// Second attempt: should exceed max retries and archive as FAILED
	m.Inbox.ProcessBatch(ctx, "svc", 10, handler)

	if n := countRows(t, db, testPrefix+"_message_hot"); n != 0 {
		t.Errorf("hot rows = %d, want 0 (should be archived as failed)", n)
	}

	// Should be in archive with FAILED status
	var finalStatus string
	err := db.QueryRow("SELECT final_status FROM " + testPrefix + "_message_archive LIMIT 1").Scan(&finalStatus)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if finalStatus != "FAILED" {
		t.Errorf("final_status = %s, want FAILED", finalStatus)
	}
}
