package message

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestDeadLetter(t *testing.T) {
	m := newTestMessaging(t, Config{WorkerID: "w1"})
	ctx := context.Background()
	m.Enqueue(ctx, nil, &Message{MessageID: "msg-dl", Topic: "t", Payload: []byte(`{}`)})

	if err := m.DeadLetter(ctx, "msg-dl", "manual dead letter"); err != nil {
		t.Fatalf("DeadLetter failed: %v", err)
	}

	var status, lastError string
	m.store.db.QueryRow("SELECT status, last_error FROM "+m.store.hotTbl+" WHERE message_id = 'msg-dl'").Scan(&status, &lastError)
	if status != "FAILED" {
		t.Errorf("status = %s, want FAILED", status)
	}
	if lastError != "manual dead letter" {
		t.Errorf("last_error = %q, want %q", lastError, "manual dead letter")
	}
}

func TestRetryFailedAndRetryOne(t *testing.T) {
	m := newTestMessaging(t, Config{WorkerID: "w1"})
	ctx := context.Background()
	m.Enqueue(ctx, nil, &Message{MessageID: "msg-a", Topic: "t", Payload: []byte(`{}`)})
	m.Enqueue(ctx, nil, &Message{MessageID: "msg-b", Topic: "t", Payload: []byte(`{}`)})
	m.DeadLetter(ctx, "msg-a", "boom")
	m.DeadLetter(ctx, "msg-b", "boom")

	if err := m.RetryOne(ctx, "msg-a"); err != nil {
		t.Fatalf("RetryOne failed: %v", err)
	}
	var status string
	m.store.db.QueryRow("SELECT status FROM " + m.store.hotTbl + " WHERE message_id = 'msg-a'").Scan(&status)
	if status != "PENDING" {
		t.Errorf("msg-a status = %s, want PENDING", status)
	}

	n, err := m.RetryFailed(ctx, RetryFailedOptions{})
	if err != nil {
		t.Fatalf("RetryFailed failed: %v", err)
	}
	if n != 1 {
		t.Errorf("RetryFailed affected = %d, want 1 (only msg-b still FAILED)", n)
	}
}

func TestRetryAllFailed(t *testing.T) {
	m := newTestMessaging(t, Config{WorkerID: "w1"})
	ctx := context.Background()
	for _, id := range []string{"f1", "f2", "f3"} {
		m.Enqueue(ctx, nil, &Message{MessageID: id, Topic: "t", Payload: []byte(`{}`)})
		m.DeadLetter(ctx, id, "boom")
	}

	n, err := m.RetryAllFailed(ctx, RetryAllFailedOptions{BatchSize: 2})
	if err != nil {
		t.Fatalf("RetryAllFailed failed: %v", err)
	}
	if n != 3 {
		t.Errorf("retried = %d, want 3", n)
	}

	metrics, err := m.GetMetrics(ctx, FilterOptions{})
	if err != nil {
		t.Fatalf("GetMetrics failed: %v", err)
	}
	if metrics.Pending != 3 || metrics.Failed != 0 {
		t.Errorf("metrics = %+v, want Pending=3 Failed=0", metrics)
	}
}

func TestGetMetrics(t *testing.T) {
	m := newTestMessaging(t, Config{WorkerID: "w1"})
	ctx := context.Background()
	m.Enqueue(ctx, nil, &Message{MessageID: "m1", Topic: "t", Payload: []byte(`{}`)})
	m.Enqueue(ctx, nil, &Message{MessageID: "m2", Topic: "t", Payload: []byte(`{}`)})
	m.DeadLetter(ctx, "m2", "boom")

	metrics, err := m.GetMetrics(ctx, FilterOptions{})
	if err != nil {
		t.Fatalf("GetMetrics failed: %v", err)
	}
	if metrics.Pending != 1 {
		t.Errorf("Pending = %d, want 1", metrics.Pending)
	}
	if metrics.Failed != 1 {
		t.Errorf("Failed = %d, want 1", metrics.Failed)
	}
}

func TestListFailedListPendingFindByMessageID(t *testing.T) {
	m := newTestMessaging(t, Config{WorkerID: "w1"})
	ctx := context.Background()
	m.Enqueue(ctx, nil, &Message{MessageID: "lp-1", Topic: "t", Payload: []byte(`{"x":1}`)})
	m.Enqueue(ctx, nil, &Message{MessageID: "lp-2", Topic: "t", Payload: []byte(`{}`)})
	m.DeadLetter(ctx, "lp-2", "boom")

	pending, err := m.ListPending(ctx, FilterOptions{}, PageOptions{})
	if err != nil {
		t.Fatalf("ListPending failed: %v", err)
	}
	if len(pending) != 1 || pending[0].MessageID != "lp-1" {
		t.Errorf("ListPending = %+v, want [lp-1]", pending)
	}

	failed, err := m.ListFailed(ctx, FilterOptions{}, PageOptions{})
	if err != nil {
		t.Fatalf("ListFailed failed: %v", err)
	}
	if len(failed) != 1 || failed[0].MessageID != "lp-2" {
		t.Errorf("ListFailed = %+v, want [lp-2]", failed)
	}

	found, err := m.FindByMessageID(ctx, "lp-1")
	if err != nil {
		t.Fatalf("FindByMessageID failed: %v", err)
	}
	var gotPayload map[string]int
	if found == nil {
		t.Fatal("FindByMessageID = nil, want lp-1")
	}
	if err := json.Unmarshal(found.Payload, &gotPayload); err != nil || gotPayload["x"] != 1 {
		t.Errorf("FindByMessageID payload = %s, want {\"x\":1}", found.Payload)
	}

	missing, err := m.FindByMessageID(ctx, "does-not-exist")
	if err != nil {
		t.Fatalf("FindByMessageID failed: %v", err)
	}
	if missing != nil {
		t.Errorf("FindByMessageID(missing) = %+v, want nil", missing)
	}
}

func TestListArchived(t *testing.T) {
	m := newTestMessaging(t, Config{WorkerID: "w1"})
	ctx := context.Background()
	m.Enqueue(ctx, nil, &Message{MessageID: "arc-1", Topic: "t", Payload: []byte(`{}`)})
	m.ProcessBatch(ctx, 10, func(ctx context.Context, msg *Message) error { return nil })

	archived, err := m.ListArchived(ctx, ListArchivedOptions{}, PageOptions{})
	if err != nil {
		t.Fatalf("ListArchived failed: %v", err)
	}
	if len(archived) != 1 || archived[0].FinalStatus != FinalProcessed {
		t.Errorf("ListArchived = %+v, want 1 PROCESSED row", archived)
	}
}

func TestIsDuplicateAndRemoveDedup(t *testing.T) {
	m := newTestMessaging(t, Config{})
	ctx := context.Background()
	receiveInTx(t, m, ctx, &Message{MessageID: "dd-1", Topic: "t", Payload: []byte(`{}`)})

	dup, err := m.IsDuplicate(ctx, "dd-1")
	if err != nil {
		t.Fatalf("IsDuplicate failed: %v", err)
	}
	if !dup {
		t.Error("IsDuplicate = false, want true")
	}

	removed, err := m.RemoveDedup(ctx, "dd-1")
	if err != nil {
		t.Fatalf("RemoveDedup failed: %v", err)
	}
	if !removed {
		t.Error("RemoveDedup = false, want true")
	}

	dup, err = m.IsDuplicate(ctx, "dd-1")
	if err != nil {
		t.Fatalf("IsDuplicate failed: %v", err)
	}
	if dup {
		t.Error("IsDuplicate = true after RemoveDedup, want false")
	}
}

func TestRegisterDispatch_TopicRouting(t *testing.T) {
	m := newTestMessaging(t, Config{WorkerID: "w1"})
	ctx := context.Background()

	var got string
	m.Register("topic.a", func(ctx context.Context, msg *Message) error {
		got = msg.Topic
		return nil
	})
	m.RegisterDefault(func(ctx context.Context, msg *Message) error {
		return errors.New("should not be called")
	})

	m.Enqueue(ctx, nil, &Message{MessageID: "route-1", Topic: "topic.a", Payload: []byte(`{}`)})

	if _, err := m.ProcessBatch(ctx, 10, nil); err != nil {
		t.Fatalf("ProcessBatch failed: %v", err)
	}
	if got != "topic.a" {
		t.Errorf("handler got topic %q, want topic.a", got)
	}
}
