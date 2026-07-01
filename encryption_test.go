package message

import (
	"context"
	"sync"
	"testing"
)

func TestEncryption_RoundTripThroughProcessBatch(t *testing.T) {
	cipher := NewDefaultCipher("test-secret")
	m := newTestMessaging(t, Config{WorkerID: "w1"}, WithCiphers(cipher.Version(), cipher))
	ctx := context.Background()

	m.Enqueue(ctx, nil, &Message{
		MessageID: "enc-1",
		Topic:     "t",
		Payload:   []byte(`{"secret":"payload"}`),
		Headers:   map[string]string{"x-trace": "abc"},
	})

	var encrypted bool
	m.store.db.QueryRow("SELECT encrypted FROM " + m.store.hotTbl + " WHERE message_id = 'enc-1'").Scan(&encrypted)
	if !encrypted {
		t.Fatal("row should be marked encrypted")
	}

	var gotPayload string
	var gotHeaders map[string]string
	_, err := m.ProcessBatch(ctx, 10, func(ctx context.Context, msg *Message) error {
		gotPayload = string(msg.Payload)
		gotHeaders = msg.Headers
		return nil
	})
	if err != nil {
		t.Fatalf("ProcessBatch failed: %v", err)
	}
	if gotPayload != `{"secret":"payload"}` {
		t.Errorf("payload = %s, want plaintext round-trip", gotPayload)
	}
	if gotHeaders["x-trace"] != "abc" {
		t.Errorf("headers = %+v, want x-trace=abc", gotHeaders)
	}
}

func TestEncryption_DecryptFailure_DeadLettersRowNotBatch(t *testing.T) {
	cipher := NewDefaultCipher("test-secret")
	m := newTestMessaging(t, Config{WorkerID: "w1"}, WithCiphers(cipher.Version(), cipher))
	ctx := context.Background()

	m.Enqueue(ctx, nil, &Message{MessageID: "enc-ok", Topic: "t", Payload: []byte(`{}`)})
	m.Enqueue(ctx, nil, &Message{MessageID: "enc-broken", Topic: "t", Payload: []byte(`{}`)})

	// Simulate a payload encrypted under a version this instance no longer has.
	m.store.db.Exec(`UPDATE ` + m.store.hotTbl + ` SET payload = '"vX:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"' WHERE message_id = 'enc-broken'`)

	var handled int
	n, err := m.ProcessBatch(ctx, 10, func(ctx context.Context, msg *Message) error {
		handled++
		return nil
	})
	if err != nil {
		t.Fatalf("ProcessBatch failed: %v", err)
	}
	if n != 2 {
		t.Errorf("claimed = %d, want 2", n)
	}
	if handled != 1 {
		t.Errorf("handler invoked %d times, want 1 (enc-broken must be skipped, not crash the batch)", handled)
	}

	var status, lastError string
	m.store.db.QueryRow("SELECT status, last_error FROM "+m.store.hotTbl+" WHERE message_id = 'enc-broken'").Scan(&status, &lastError)
	if status != "FAILED" {
		t.Errorf("enc-broken status = %s, want FAILED", status)
	}
	if lastError == "" {
		t.Error("enc-broken last_error should be set")
	}
}

func TestEncryption_KeyRotation_OldRowsStillProcessable(t *testing.T) {
	v1 := NewAES256GCMCipher("v1", "secret-1")
	m1 := newTestMessaging(t, Config{WorkerID: "w1"}, WithCiphers("v1", v1))
	ctx := context.Background()

	m1.Enqueue(ctx, nil, &Message{MessageID: "rot-1", Topic: "t", Payload: []byte(`{"v":1}`)})

	// Rotate: new instance on the same DB/tables, v2 is current but v1 stays registered.
	v2 := NewAES256GCMCipher("v2", "secret-2")
	db := m1.store.db
	cfg := Config{Name: testName, WorkerID: "w2"}
	m2, err := New(db, cfg, WithCiphers("v2", v1, v2))
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	m2.Enqueue(ctx, nil, &Message{MessageID: "rot-2", Topic: "t", Payload: []byte(`{"v":2}`)})

	var mu sync.Mutex
	seen := map[string]string{}
	_, err = m2.ProcessBatch(ctx, 10, func(ctx context.Context, msg *Message) error {
		mu.Lock()
		seen[msg.MessageID] = string(msg.Payload)
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("ProcessBatch failed: %v", err)
	}
	if seen["rot-1"] != `{"v":1}` {
		t.Errorf("rot-1 payload = %s, want v1 plaintext (must decrypt with retained old cipher)", seen["rot-1"])
	}
	if seen["rot-2"] != `{"v":2}` {
		t.Errorf("rot-2 payload = %s, want v2 plaintext", seen["rot-2"])
	}
}
