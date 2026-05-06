package message

import (
	"context"
	"time"
)

// Direction indicates whether a message belongs to the inbox or outbox flow.
type Direction string

const (
	DirectionInbox  Direction = "INBOX"
	DirectionOutbox Direction = "OUTBOX"
)

// Status represents the processing state of a message in the hot table.
type Status string

const (
	StatusPending Status = "PENDING"
	StatusClaimed Status = "CLAIMED"
	StatusDone    Status = "DONE"
	StatusFailed  Status = "FAILED"
)

// Message is the shared row representation for both hot and archive tables.
type Message struct {
	ID            int64
	Direction     Direction
	ConsumerGroup string
	MessageID     string
	EventID       string
	EventType     string
	Payload       []byte
	Headers       map[string]string
	Source        string
	Status        Status
	RetryCount    int
	MaxRetries    int
	NextRetryAt   *time.Time
	ClaimedBy     *string
	ClaimedAt     *time.Time
	CreatedAt     time.Time
	ProcessedAt   *time.Time
	PublishedAt   *time.Time
	LastError     *string
}

// InboxMessage is the input to Inbox.Receive().
type InboxMessage struct {
	ConsumerGroup string
	MessageID     string
	EventType     string
	Payload       []byte
	Headers       map[string]string
	Source        string
	MaxRetries    int // 0 means use config default
}

// OutboxEvent is the input to Outbox.Add().
type OutboxEvent struct {
	EventID    string
	EventType  string
	Payload    []byte
	Headers    map[string]string
	Source     string
	MaxRetries int // 0 means use config default
}

// Handler processes a single inbox message. Return error to trigger retry.
type Handler func(ctx context.Context, msg *Message) error

// Publisher sends an outbox event to the broker. Return error to trigger retry.
type Publisher func(ctx context.Context, msg *Message) error
