package message

import (
	"context"
	"errors"
	"testing"
)

func TestEnqueue_WithTx(t *testing.T) {
	m := newTestMessaging(t, Config{})
	ctx := context.Background()

	tx, err := m.store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx failed: %v", err)
	}

	err = m.Enqueue(ctx, tx, &Message{
		MessageID: "evt-001",
		Topic:     "order.confirmed",
		Payload:   []byte(`{"order_id":123}`),
		Source:    "order-service",
	})
	if err != nil {
		tx.Rollback()
		t.Fatalf("Enqueue failed: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	if n := countRows(t, m.store.db, m.store.hotTbl); n != 1 {
		t.Errorf("hot rows = %d, want 1", n)
	}
}

func TestEnqueue_TxRollback(t *testing.T) {
	m := newTestMessaging(t, Config{})
	ctx := context.Background()

	tx, err := m.store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx failed: %v", err)
	}

	err = m.Enqueue(ctx, tx, &Message{
		MessageID: "evt-rollback",
		Topic:     "order.confirmed",
		Payload:   []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	tx.Rollback()

	if n := countRows(t, m.store.db, m.store.hotTbl); n != 0 {
		t.Errorf("hot rows = %d, want 0 (transaction was rolled back)", n)
	}
}

func TestEnqueue_NilTx(t *testing.T) {
	m := newTestMessaging(t, Config{})
	ctx := context.Background()

	if err := m.Enqueue(ctx, nil, &Message{
		MessageID: "evt-notx",
		Topic:     "order.confirmed",
		Payload:   []byte(`{}`),
	}); err != nil {
		t.Fatalf("Enqueue with nil tx failed: %v", err)
	}

	if n := countRows(t, m.store.db, m.store.hotTbl); n != 1 {
		t.Errorf("hot rows = %d, want 1", n)
	}
}

func TestEnqueue_Idempotent(t *testing.T) {
	m := newTestMessaging(t, Config{})
	ctx := context.Background()

	msg := &Message{MessageID: "evt-dup", Topic: "order.confirmed", Payload: []byte(`{}`)}
	if err := m.Enqueue(ctx, nil, msg); err != nil {
		t.Fatalf("first Enqueue failed: %v", err)
	}
	if err := m.Enqueue(ctx, nil, msg); err != nil {
		t.Fatalf("second Enqueue failed: %v", err)
	}

	if n := countRows(t, m.store.db, m.store.hotTbl); n != 1 {
		t.Errorf("hot rows = %d, want 1 (message_id PK makes Enqueue idempotent)", n)
	}
}

func TestEnqueueBatch(t *testing.T) {
	m := newTestMessaging(t, Config{})
	ctx := context.Background()

	err := m.EnqueueBatch(ctx, nil, []*Message{
		{MessageID: "evt-b1", Topic: "t", Payload: []byte(`{}`)},
		{MessageID: "evt-b2", Topic: "t", Payload: []byte(`{}`)},
		{MessageID: "evt-b3", Topic: "t", Payload: []byte(`{}`)},
	})
	if err != nil {
		t.Fatalf("EnqueueBatch failed: %v", err)
	}

	if n := countRows(t, m.store.db, m.store.hotTbl); n != 3 {
		t.Errorf("hot rows = %d, want 3", n)
	}
}

func TestProcessBatch_EnqueuedEvent_Success(t *testing.T) {
	m := newTestMessaging(t, Config{WorkerID: "relay-1"})
	ctx := context.Background()

	m.Enqueue(ctx, nil, &Message{
		MessageID: "evt-pub",
		Topic:     "test.event",
		Payload:   []byte(`{}`),
	})

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
		t.Errorf("hot rows = %d, want 0", n)
	}
	if n := countRows(t, m.store.db, m.store.archTbl); n != 1 {
		t.Errorf("archive rows = %d, want 1", n)
	}
}

func TestProcessBatch_EnqueuedEvent_Retry(t *testing.T) {
	m := newTestMessaging(t, Config{WorkerID: "relay-1", DefaultMaxRetries: 3})
	ctx := context.Background()

	m.Enqueue(ctx, nil, &Message{
		MessageID: "evt-retry",
		Topic:     "test.event",
		Payload:   []byte(`{}`),
	})

	n, err := m.ProcessBatch(ctx, 10, func(ctx context.Context, msg *Message) error {
		return errors.New("broker down")
	})
	if err != nil {
		t.Fatalf("ProcessBatch failed: %v", err)
	}
	if n != 1 {
		t.Errorf("processed = %d, want 1", n)
	}

	var status string
	m.store.db.QueryRow("SELECT status FROM " + m.store.hotTbl + " LIMIT 1").Scan(&status)
	if status != "RETRY" {
		t.Errorf("status = %s, want RETRY", status)
	}
}
