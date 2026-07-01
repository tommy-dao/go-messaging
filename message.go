package message

import (
	"context"
	"time"
)

// Status represents the processing state of a hot-table message.
type Status string

const (
	StatusPending    Status = "PENDING"
	StatusProcessing Status = "PROCESSING"
	StatusRetry      Status = "RETRY"
	// StatusFailed messages stay in the hot table (acting as a DLQ) until
	// RetryFailed/RetryOne re-queues them or ArchiveExhausted archives them
	// after Config.FailedRetention.
	StatusFailed Status = "FAILED"
)

// FinalStatus represents the terminal outcome recorded in the archive table.
type FinalStatus string

const (
	FinalProcessed FinalStatus = "PROCESSED"
	FinalFailed    FinalStatus = "FAILED"
)

// Message is the row representation for the hot table.
//
// PartitionKey is metadata only (e.g. source/scope) — it is not part of any
// key. MessageID is the sole natural key: it drives dedup (Receive) and
// idempotency (Enqueue) alike.
type Message struct {
	MessageID    string
	PartitionKey string
	Topic        string
	Payload      []byte // plaintext JSON after hydration; caller-supplied JSON on write
	Headers      map[string]string
	Source       string
	Encrypted    bool
	Status       Status
	RetryCount   int
	MaxRetries   int
	NextRetryAt  *time.Time
	ProcessingBy *string
	ProcessingAt *time.Time
	CreatedAt    time.Time
	ProcessedAt  *time.Time
	LastError    *string

	// rawHeaders holds the not-yet-decrypted headers JSON between claim/scan
	// and hydrate(). Never populated once Headers has been set.
	rawHeaders []byte
}

// ArchivedMessage is the row representation for the archive table.
// Archive is append-only: id is a surrogate key, message_id is not unique —
// reprocessing the same message_id after a dedup TTL expiry adds a new row
// instead of overwriting history.
type ArchivedMessage struct {
	ID           int64
	MessageID    string
	PartitionKey string
	Topic        string
	Payload      []byte
	Headers      map[string]string
	Source       string
	Encrypted    bool
	FinalStatus  FinalStatus
	RetryCount   int
	CreatedAt    time.Time
	ProcessedAt  *time.Time
	ArchivedAt   time.Time
	LastError    *string

	rawHeaders []byte
}

// Handler processes a single message. Return an error to trigger retry (or
// FAILED once MaxRetries is exceeded).
type Handler func(ctx context.Context, msg *Message) error
