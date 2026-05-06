package message

import (
	"context"
	"time"
)

// Inbox handles idempotent message receiving with dedup, retry, and archival.
type Inbox struct {
	store   *store
	cfg     Config
	backoff Backoff
	metrics Metrics
}

// newInbox creates a new Inbox from an internal store.
func newInbox(s *store, cfg Config, opts ...Option) *Inbox {
	o := applyOptions(opts)
	backoff := o.backoff
	if backoff == nil {
		backoff = cfg.DefaultBackoff
	}
	return &Inbox{
		store:   s,
		cfg:     cfg,
		backoff: backoff,
		metrics: o.metrics,
	}
}

// Receive processes an incoming message idempotently.
// Duplicate messages return nil (not an error).
func (ib *Inbox) Receive(ctx context.Context, msg InboxMessage) error {
	maxRetries := msg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = ib.cfg.DefaultMaxRetries
	}

	var expireAt *time.Time
	if ib.cfg.DedupTTL > 0 {
		t := time.Now().Add(ib.cfg.DedupTTL)
		expireAt = &t
	}

	tx, err := ib.store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Insert dedup record
	err = ib.store.insertDedup(ctx, tx, msg.ConsumerGroup, msg.MessageID, msg.EventType, msg.Source, expireAt)
	if err == ErrDuplicate {
		return nil // idempotent: duplicate is not an error
	}
	if err != nil {
		return err
	}

	// Insert into hot table
	hotMsg := &Message{
		Direction:     DirectionInbox,
		ConsumerGroup: msg.ConsumerGroup,
		MessageID:     msg.MessageID,
		EventType:     msg.EventType,
		Payload:       msg.Payload,
		Headers:       msg.Headers,
		Source:        msg.Source,
		MaxRetries:    maxRetries,
	}
	if err := ib.store.insertHot(ctx, tx, hotMsg); err != nil {
		return err
	}

	return tx.Commit()
}

// ProcessBatch claims up to `size` messages for the given consumer group and processes them.
// Returns the number of messages processed.
func (ib *Inbox) ProcessBatch(ctx context.Context, consumerGroup string, size int, handler Handler) (int, error) {
	msgs, err := ib.store.claimBatch(ctx, DirectionInbox, consumerGroup, size)
	if err != nil {
		return 0, err
	}

	processed := 0
	for _, msg := range msgs {
		if err := handler(ctx, msg); err != nil {
			ib.handleRetry(ctx, msg, err)
		} else {
			now := time.Now()
			if markErr := ib.store.markDone(ctx, msg.ID, now, "processed_at"); markErr == nil {
				_ = ib.store.archiveByID(ctx, msg.ID, "PROCESSED")
				ib.metrics.InboxProcessed(msg.ConsumerGroup, msg.EventType)
			}
		}
		processed++
	}

	return processed, nil
}

func (ib *Inbox) handleRetry(ctx context.Context, msg *Message, handlerErr error) {
	retryCount := msg.RetryCount + 1

	if retryCount > msg.MaxRetries {
		_ = ib.store.markFailed(ctx, msg.ID, handlerErr.Error())
		_ = ib.store.archiveByID(ctx, msg.ID, "FAILED")
		ib.metrics.InboxFailed(msg.ConsumerGroup, msg.EventType)
		return
	}

	delay := ib.backoff.Next(retryCount)
	nextRetryAt := time.Now().Add(delay)
	_ = ib.store.markRetry(ctx, msg.ID, retryCount, nextRetryAt, handlerErr.Error())
	ib.metrics.InboxRetry(msg.ConsumerGroup, msg.EventType)
}
