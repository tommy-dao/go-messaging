package message

import "database/sql"

// Messaging is the top-level entry point providing access to Inbox, Outbox, and Cleanup.
type Messaging struct {
	Inbox   *Inbox
	Outbox  *Outbox
	Cleanup *Cleanup
}

// New creates a new Messaging instance with shared store.
func New(db *sql.DB, cfg Config, opts ...Option) *Messaging {
	s := newStore(db, cfg)
	cfg = cfg.withDefaults()
	return &Messaging{
		Inbox:   newInbox(s, cfg, opts...),
		Outbox:  newOutbox(s, cfg, opts...),
		Cleanup: newCleanup(s, cfg),
	}
}
