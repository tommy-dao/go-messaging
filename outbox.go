package message

import (
	"context"
	"fmt"
	"time"
)

// Outbox handles transactional event publishing with relay, retry, and archival.
type Outbox struct {
	store   *store
	cfg     Config
	backoff Backoff
	metrics Metrics
}

// newOutbox is the internal constructor.
func newOutbox(s *store, cfg Config, opts ...Option) *Outbox {
	o := applyOptions(opts)
	backoff := o.backoff
	if backoff == nil {
		backoff = cfg.DefaultBackoff
	}
	return &Outbox{
		store:   s,
		cfg:     cfg,
		backoff: backoff,
		metrics: o.metrics,
	}
}

// Add inserts an outbox event within the caller's transaction.
// The event becomes visible only when the caller commits the transaction.
// tx must be non-nil; use AddDirect if you don't have a business transaction.
func (ob *Outbox) Add(ctx context.Context, tx Tx, evt OutboxEvent) error {
	maxRetries := evt.MaxRetries
	if maxRetries <= 0 {
		maxRetries = ob.cfg.DefaultMaxRetries
	}

	msg := &Message{
		Direction:  DirectionOutbox,
		EventID:    evt.EventID,
		EventType:  evt.EventType,
		Payload:    evt.Payload,
		Headers:    evt.Headers,
		Source:     evt.Source,
		MaxRetries: maxRetries,
	}
	return ob.store.insertHot(ctx, tx, msg)
}

// AddDirect inserts an outbox event using its own short-lived transaction.
// Use this only when you have no business transaction to participate in;
// prefer Add() so the event commits atomically with your business write.
func (ob *Outbox) AddDirect(ctx context.Context, evt OutboxEvent) error {
	tx, err := ob.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("message: outbox add direct: begin tx: %w", err)
	}
	defer tx.Rollback()

	if err := ob.Add(ctx, tx, evt); err != nil {
		return err
	}
	return tx.Commit()
}

// PublishBatch claims up to `size` outbox events and publishes each via the publisher.
// Returns the number of events published.
func (ob *Outbox) PublishBatch(ctx context.Context, size int, publisher Publisher) (int, error) {
	msgs, err := ob.store.claimBatch(ctx, DirectionOutbox, "", size)
	if err != nil {
		return 0, err
	}

	published := 0
	for _, msg := range msgs {
		if err := publisher(ctx, msg); err != nil {
			ob.handleRetry(ctx, msg, err)
		} else {
			now := time.Now()
			if markErr := ob.store.markDone(ctx, msg.ID, now, "published_at"); markErr == nil {
				_ = ob.store.archiveByID(ctx, msg.ID, "PUBLISHED")
				ob.metrics.OutboxPublished(msg.EventType)
			}
		}
		published++
	}

	return published, nil
}

func (ob *Outbox) handleRetry(ctx context.Context, msg *Message, publishErr error) {
	retryCount := msg.RetryCount + 1

	if retryCount > msg.MaxRetries {
		_ = ob.store.markFailed(ctx, msg.ID, publishErr.Error())
		_ = ob.store.archiveByID(ctx, msg.ID, "FAILED")
		ob.metrics.OutboxFailed(msg.EventType)
		return
	}

	delay := ob.backoff.Next(retryCount)
	nextRetryAt := time.Now().Add(delay)
	_ = ob.store.markRetry(ctx, msg.ID, retryCount, nextRetryAt, publishErr.Error())
	ob.metrics.OutboxRetry(msg.EventType)
}
