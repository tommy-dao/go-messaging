package message

import "context"

// Cleanup handles dedup expiry and stuck claim recovery.
type Cleanup struct {
	store *store
	cfg   Config
}

// newCleanup creates a new Cleanup instance.
func newCleanup(s *store, cfg Config) *Cleanup {
	return &Cleanup{store: s, cfg: cfg}
}

// PurgeExpiredDedup deletes dedup entries whose expire_at has passed.
// Returns the number of entries deleted.
func (c *Cleanup) PurgeExpiredDedup(ctx context.Context) (int64, error) {
	return c.store.cleanupDedup(ctx)
}

// RecoverStuckInbox resets inbox messages that have been claimed longer than ClaimTimeout.
// Returns the number of messages recovered.
func (c *Cleanup) RecoverStuckInbox(ctx context.Context) (int64, error) {
	return c.store.recoverStuck(ctx, DirectionInbox, c.cfg.ClaimTimeout)
}

// RecoverStuckOutbox resets outbox messages that have been claimed longer than ClaimTimeout.
// Returns the number of messages recovered.
func (c *Cleanup) RecoverStuckOutbox(ctx context.Context) (int64, error) {
	return c.store.recoverStuck(ctx, DirectionOutbox, c.cfg.ClaimTimeout)
}
