package message

import (
	"context"
	"errors"
	"testing"
)

func TestOutbox_Add_Success(t *testing.T) {
	db := setupTestDB(t)
	cfg := Config{TablePrefix: testPrefix}
	m := New(db, cfg)
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx failed: %v", err)
	}

	err = m.Outbox.Add(ctx, tx, OutboxEvent{
		EventID:   "evt-001",
		EventType: "order.confirmed",
		Payload:   []byte(`{"order_id":123}`),
		Source:    "order-service",
	})
	if err != nil {
		tx.Rollback()
		t.Fatalf("Add failed: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	if n := countRows(t, db, testPrefix+"_message_hot"); n != 1 {
		t.Errorf("hot rows = %d, want 1", n)
	}
}

func TestOutbox_Add_TransactionRollback(t *testing.T) {
	db := setupTestDB(t)
	cfg := Config{TablePrefix: testPrefix}
	m := New(db, cfg)
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx failed: %v", err)
	}

	err = m.Outbox.Add(ctx, tx, OutboxEvent{
		EventID:   "evt-rollback",
		EventType: "order.confirmed",
		Payload:   []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Rollback instead of commit
	tx.Rollback()

	// Message should NOT be in hot table
	if n := countRows(t, db, testPrefix+"_message_hot"); n != 0 {
		t.Errorf("hot rows = %d, want 0 (transaction was rolled back)", n)
	}
}

func TestOutbox_PublishBatch_Success(t *testing.T) {
	db := setupTestDB(t)
	cfg := Config{TablePrefix: testPrefix, WorkerID: "relay-1"}
	m := New(db, cfg)
	ctx := context.Background()

	// Add an event
	tx, _ := db.BeginTx(ctx, nil)
	m.Outbox.Add(ctx, tx, OutboxEvent{
		EventID:   "evt-pub",
		EventType: "test.event",
		Payload:   []byte(`{}`),
	})
	tx.Commit()

	// Publish it
	n, err := m.Outbox.PublishBatch(ctx, 10, func(ctx context.Context, msg *Message) error {
		return nil // success
	})
	if err != nil {
		t.Fatalf("PublishBatch failed: %v", err)
	}
	if n != 1 {
		t.Errorf("published = %d, want 1", n)
	}

	// Hot should be empty
	if n := countRows(t, db, testPrefix+"_message_hot"); n != 0 {
		t.Errorf("hot rows = %d, want 0", n)
	}

	// Archive should have 1
	if n := countRows(t, db, testPrefix+"_message_archive"); n != 1 {
		t.Errorf("archive rows = %d, want 1", n)
	}
}

func TestOutbox_PublishBatch_Retry(t *testing.T) {
	db := setupTestDB(t)
	cfg := Config{TablePrefix: testPrefix, WorkerID: "relay-1", DefaultMaxRetries: 3}
	m := New(db, cfg)
	ctx := context.Background()

	tx, _ := db.BeginTx(ctx, nil)
	m.Outbox.Add(ctx, tx, OutboxEvent{
		EventID:   "evt-retry",
		EventType: "test.event",
		Payload:   []byte(`{}`),
	})
	tx.Commit()

	// Fail publishing
	n, err := m.Outbox.PublishBatch(ctx, 10, func(ctx context.Context, msg *Message) error {
		return errors.New("broker down")
	})
	if err != nil {
		t.Fatalf("PublishBatch failed: %v", err)
	}
	if n != 1 {
		t.Errorf("published = %d, want 1", n)
	}

	// Still in hot with RETRY status
	var status string
	db.QueryRow("SELECT status FROM " + testPrefix + "_message_hot LIMIT 1").Scan(&status)
	if status != "RETRY" {
		t.Errorf("status = %s, want RETRY", status)
	}
}
