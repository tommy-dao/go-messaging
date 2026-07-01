package message

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"sync"
	"time"
)

// Messaging is the entry point for all messaging operations: two entry
// points (Receive, Enqueue) funnel into one shared hot table, drained by
// ProcessBatch and routed to handlers by topic.
type Messaging struct {
	store          *store
	cfg            Config
	backoff        Backoff
	metrics        Metrics
	logger         Logger
	mu             sync.RWMutex
	handlers       map[string]topicRegistration
	defaultHandler *topicRegistration
}

type topicRegistration struct {
	handler Handler
	opts    callOptions
}

// New creates a new Messaging instance. cfg.Name scopes the underlying
// tables (message_<name>_hot, etc.) — see assertValidName for constraints.
func New(db *sql.DB, cfg Config, opts ...Option) (*Messaging, error) {
	if err := assertValidName(cfg.Name); err != nil {
		return nil, err
	}
	o := applyOptions(opts)
	cfg = cfg.withDefaults()
	backoff := o.backoff
	if backoff == nil {
		backoff = cfg.DefaultBackoff
	}

	var cipher *CipherRegistry
	if len(o.ciphers) > 0 {
		cur := o.currentCipherVersion
		if cur == "" {
			if len(o.ciphers) == 1 {
				cur = o.ciphers[0].Version()
			} else {
				return nil, fmt.Errorf("message: WithCiphers: currentVersion is required when more than one cipher is given")
			}
		}
		reg, err := NewCipherRegistry(o.ciphers, cur)
		if err != nil {
			return nil, err
		}
		cipher = reg
	}

	return &Messaging{
		store:    newStore(db, cfg, cipher),
		cfg:      cfg,
		backoff:  backoff,
		metrics:  o.metrics,
		logger:   o.logger,
		handlers: make(map[string]topicRegistration),
	}, nil
}

// Receive processes an incoming message idempotently. Returns isNew=false
// for a duplicate message_id (dedup hit) — the hot insert is skipped and the
// caller's tx (if any) is left uncommitted-but-valid, not rolled back.
// If tx is non-nil, dedup+hot insert are atomic with the caller's write.
// msg.MessageID and msg.Topic are required. msg.MaxRetries defaults to
// Config.DefaultMaxRetries if 0.
func (m *Messaging) Receive(ctx context.Context, tx Tx, msg *Message) (isNew bool, err error) {
	if msg.MaxRetries <= 0 {
		msg.MaxRetries = m.cfg.DefaultMaxRetries
	}

	var expireAt *time.Time
	if m.cfg.DedupTTL > 0 {
		t := time.Now().Add(m.cfg.DedupTTL)
		expireAt = &t
	}

	run := func(db DBTX) (bool, error) {
		err := m.store.insertDedup(ctx, db, msg.MessageID, msg.PartitionKey, msg.Topic, msg.Source, expireAt)
		if err == ErrDuplicate {
			m.metrics.DedupHit()
			m.logger.Log(ctx, DEBUG, "receive.duplicate", msg)
			return false, nil
		}
		if err != nil {
			m.logger.Log(ctx, ERROR, "receive.error", msg)
			return false, err
		}
		if err := m.store.insertHot(ctx, db, msg); err != nil {
			m.logger.Log(ctx, ERROR, "receive.error", msg)
			return false, err
		}
		m.metrics.DedupMiss()
		m.logger.Log(ctx, INFO, "receive", msg)
		return true, nil
	}

	if tx != nil {
		return run(tx)
	}
	return run(m.store.db)
}

// Enqueue inserts a message for later processing, idempotent via message_id.
// If tx is non-nil the insert participates in the caller's transaction
// (atomic with a business write). If tx is nil an internal transaction is used.
// msg.MessageID and msg.Topic are required. msg.MaxRetries defaults to
// Config.DefaultMaxRetries if 0.
func (m *Messaging) Enqueue(ctx context.Context, tx Tx, msg *Message) error {
	if msg.MaxRetries <= 0 {
		msg.MaxRetries = m.cfg.DefaultMaxRetries
	}
	if tx != nil {
		if err := m.store.insertHot(ctx, tx, msg); err != nil {
			m.logger.Log(ctx, ERROR, "enqueue.error", msg)
			return err
		}
		m.logger.Log(ctx, INFO, "enqueue", msg)
		return nil
	}
	itx, err := m.store.db.BeginTx(ctx, nil)
	if err != nil {
		m.logger.Log(ctx, ERROR, "enqueue.error", msg)
		return fmt.Errorf("message: enqueue: begin tx: %w", err)
	}
	defer itx.Rollback()
	if err := m.store.insertHot(ctx, itx, msg); err != nil {
		m.logger.Log(ctx, ERROR, "enqueue.error", msg)
		return err
	}
	if err := itx.Commit(); err != nil {
		m.logger.Log(ctx, ERROR, "enqueue.error", msg)
		return err
	}
	m.logger.Log(ctx, INFO, "enqueue", msg)
	return nil
}

// EnqueueBatch inserts multiple messages in one round-trip. Caller must
// ensure message IDs are unique within the batch. If tx is nil an internal
// transaction is used.
func (m *Messaging) EnqueueBatch(ctx context.Context, tx Tx, msgs []*Message) error {
	if len(msgs) == 0 {
		return nil
	}
	for _, msg := range msgs {
		if msg.MaxRetries <= 0 {
			msg.MaxRetries = m.cfg.DefaultMaxRetries
		}
	}
	if tx != nil {
		return m.store.insertHotBatch(ctx, tx, msgs)
	}
	itx, err := m.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("message: enqueue batch: begin tx: %w", err)
	}
	defer itx.Rollback()
	if err := m.store.insertHotBatch(ctx, itx, msgs); err != nil {
		return err
	}
	return itx.Commit()
}

// Register binds a handler to a topic, used by ProcessBatch when called
// without an explicit handler. '*' is reserved — use RegisterDefault for a
// wildcard fallback. Not safe to call concurrently with ProcessBatch.
func (m *Messaging) Register(topic string, handler Handler, opts ...CallOption) *Messaging {
	m.mu.Lock()
	m.handlers[topic] = topicRegistration{handler: handler, opts: applyCallOptions(opts)}
	m.mu.Unlock()
	return m
}

// RegisterDefault sets a fallback handler for messages whose topic has no
// registered handler.
func (m *Messaging) RegisterDefault(handler Handler, opts ...CallOption) *Messaging {
	reg := topicRegistration{handler: handler, opts: applyCallOptions(opts)}
	m.mu.Lock()
	m.defaultHandler = &reg
	m.mu.Unlock()
	return m
}

// ProcessBatch claims up to size messages and processes them concurrently,
// bounded by Config.Concurrency. If handler is non-nil it is used for every
// claimed message; otherwise each message is routed by topic via
// Register/RegisterDefault. A message whose topic has no handler (registry
// path only) is dead-lettered with reason "no handler for topic: ...".
// Returns the number of messages claimed.
func (m *Messaging) ProcessBatch(ctx context.Context, size int, handler Handler, opts ...CallOption) (int, error) {
	co := applyCallOptions(opts)
	msgs, err := m.store.claimBatch(ctx, size)
	if err != nil {
		return 0, err
	}

	m.mu.RLock()
	handlers := make(map[string]topicRegistration, len(m.handlers))
	maps.Copy(handlers, m.handlers)
	defaultHandler := m.defaultHandler
	m.mu.RUnlock()

	sem := make(chan struct{}, m.cfg.Concurrency)
	var wg sync.WaitGroup
	for _, msg := range msgs {
		fn, callOpts := handler, co
		if fn == nil {
			reg, ok := handlers[msg.Topic]
			if !ok && defaultHandler != nil {
				reg, ok = *defaultHandler, true
			}
			if !ok {
				_ = m.store.deadLetter(ctx, msg.MessageID, "no handler for topic: "+msg.Topic)
				m.metrics.MessageFailed(msg.Topic)
				m.logger.Log(ctx, ERROR, "process.unhandled", msg)
				continue
			}
			fn, callOpts = reg.handler, reg.opts
		}
		wg.Go(func() {
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()
			m.handleOne(ctx, msg, fn, callOpts)
		})
	}
	wg.Wait()

	return len(msgs), nil
}

func (m *Messaging) handleOne(ctx context.Context, msg *Message, handler Handler, co callOptions) {
	// Decrypt here (not in claimBatch) so one undecryptable row dead-letters
	// instead of failing the whole batch scan.
	if msg.Encrypted {
		if err := m.store.hydrate(msg); err != nil {
			_ = m.store.deadLetter(ctx, msg.MessageID, "message decrypt failed: "+toErrorMessage(err))
			m.metrics.MessageFailed(msg.Topic)
			m.logger.Log(ctx, ERROR, "process.decrypt_failed", msg)
			return
		}
	}

	hctx, cancel := m.handlerCtx(ctx, co)
	defer cancel()

	if err := handler(hctx, msg); err != nil {
		m.handleRetry(ctx, msg, err)
		return
	}
	owned, err := m.store.markDoneAndArchive(ctx, msg.MessageID, time.Now())
	if err != nil {
		m.logger.Log(ctx, ERROR, "process.archive_error", msg)
		return
	}
	if !owned {
		return // ownership lost to RecoverStuck/another worker — leave the new owner's result alone
	}
	m.metrics.MessageProcessed(msg.Topic)
	m.logger.Log(ctx, INFO, "processed", msg)
}

func (m *Messaging) handleRetry(ctx context.Context, msg *Message, handlerErr error) {
	retryCount := msg.RetryCount + 1
	if retryCount > msg.MaxRetries {
		owned, err := m.store.markFailed(ctx, msg.MessageID, handlerErr.Error())
		if err == nil && owned {
			m.metrics.MessageFailed(msg.Topic)
			m.logger.Log(ctx, ERROR, "process.failed", msg)
		}
		return
	}
	delay := m.backoff.Next(retryCount)
	owned, err := m.store.markRetry(ctx, msg.MessageID, retryCount, time.Now().Add(delay), handlerErr.Error())
	if err == nil && owned {
		m.metrics.MessageRetry(msg.Topic)
		m.logger.Log(ctx, WARN, "process.retry", msg)
	}
}

func (m *Messaging) handlerCtx(parent context.Context, co callOptions) (context.Context, context.CancelFunc) {
	timeout := m.cfg.HandlerTimeout
	if co.handlerTimeout != nil {
		timeout = *co.handlerTimeout
	}
	if timeout > 0 {
		return context.WithTimeout(parent, timeout)
	}
	return context.WithCancel(parent)
}
