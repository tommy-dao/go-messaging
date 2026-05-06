package message

import (
	"context"
	"testing"
	"time"
)

func TestCleanup_PurgeExpiredDedup(t *testing.T) {
	db := setupTestDB(t)
	cfg := Config{TablePrefix: testPrefix, DedupTTL: time.Millisecond}
	m := New(db, cfg)
	ctx := context.Background()

	// Insert a message (creates dedup with very short TTL)
	m.Inbox.Receive(ctx, InboxMessage{
		ConsumerGroup: "svc",
		MessageID:     "msg-expire",
		EventType:     "test",
		Payload:       []byte(`{}`),
	})

	// Force expire_at to past
	db.Exec("UPDATE " + testPrefix + "_message_dedup SET expire_at = now() - interval '1 hour'")

	n, err := m.Cleanup.PurgeExpiredDedup(ctx)
	if err != nil {
		t.Fatalf("PurgeExpiredDedup failed: %v", err)
	}
	if n != 1 {
		t.Errorf("purged = %d, want 1", n)
	}

	if remaining := countRows(t, db, testPrefix+"_message_dedup"); remaining != 0 {
		t.Errorf("dedup rows = %d, want 0", remaining)
	}
}

func TestCleanup_RecoverStuckInbox(t *testing.T) {
	db := setupTestDB(t)
	cfg := Config{TablePrefix: testPrefix, WorkerID: "w1", ClaimTimeout: time.Second}
	m := New(db, cfg)
	ctx := context.Background()

	// Insert and claim a message
	m.Inbox.Receive(ctx, InboxMessage{
		ConsumerGroup: "svc",
		MessageID:     "msg-stuck",
		EventType:     "test",
		Payload:       []byte(`{}`),
	})

	// Claim it
	m.Inbox.ProcessBatch(ctx, "svc", 10, func(ctx context.Context, msg *Message) error {
		// Simulate a stuck handler: we'll artificially set claimed_at to the past after this
		return nil
	})

	// Re-insert for stuck test
	db.Exec("DELETE FROM " + testPrefix + "_message_archive")
	m.Inbox.Receive(ctx, InboxMessage{
		ConsumerGroup: "svc",
		MessageID:     "msg-stuck-2",
		EventType:     "test",
		Payload:       []byte(`{}`),
	})

	// Simulate stuck: set status to CLAIMED with old claimed_at
	db.Exec("UPDATE "+testPrefix+"_message_hot SET status = 'CLAIMED', claimed_by = 'dead-worker', claimed_at = now() - interval '10 minutes' WHERE message_id = 'msg-stuck-2'")

	n, err := m.Cleanup.RecoverStuckInbox(ctx)
	if err != nil {
		t.Fatalf("RecoverStuckInbox failed: %v", err)
	}
	if n != 1 {
		t.Errorf("recovered = %d, want 1", n)
	}

	// Should be back to RETRY
	var status string
	db.QueryRow("SELECT status FROM " + testPrefix + "_message_hot WHERE message_id = 'msg-stuck-2'").Scan(&status)
	if status != "RETRY" {
		t.Errorf("status = %s, want RETRY", status)
	}
}
