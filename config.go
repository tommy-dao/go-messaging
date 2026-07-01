package message

import "time"

// Config holds configuration for a Messaging instance.
type Config struct {
	// Name scopes this instance's tables: message_<name>_hot, etc.
	// Empty selects the unscoped message_hot / message_dedup / message_archive tables.
	Name string

	DefaultMaxRetries int
	DefaultBackoff    Backoff
	WorkerID          string        // identifies this instance for processing_by
	ClaimTimeout      time.Duration // PROCESSING duration after which RecoverStuck reclaims a row
	DedupTTL          time.Duration // dedup entry expiration
	FailedRetention   time.Duration // how long a FAILED row stays in hot before ArchiveExhausted archives it
	Concurrency       int           // max goroutines per ProcessBatch call; defaults to 10
	HandlerTimeout    time.Duration // per-message timeout for the handler; 0 means no timeout
}

func (c Config) withDefaults() Config {
	if c.DefaultMaxRetries <= 0 {
		c.DefaultMaxRetries = 5
	}
	if c.DefaultBackoff == nil {
		c.DefaultBackoff = LinearBackoff{BaseDelay: time.Second}
	}
	if c.WorkerID == "" {
		c.WorkerID = "default"
	}
	if c.ClaimTimeout <= 0 {
		c.ClaimTimeout = 5 * time.Minute
	}
	if c.DedupTTL <= 0 {
		c.DedupTTL = 24 * time.Hour
	}
	if c.FailedRetention <= 0 {
		c.FailedRetention = 7 * 24 * time.Hour
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 10
	}
	return c
}

// Option configures optional settings for the Messaging instance.
type Option func(*options)

type options struct {
	metrics              Metrics
	backoff              Backoff
	logger               Logger
	ciphers              []MessageCipher
	currentCipherVersion string
}

// WithMetrics sets a custom metrics implementation.
func WithMetrics(m Metrics) Option {
	return func(o *options) { o.metrics = m }
}

// WithBackoff overrides the default backoff strategy.
func WithBackoff(b Backoff) Option {
	return func(o *options) { o.backoff = b }
}

// WithLogger sets the structured logger. By default nothing is logged.
func WithLogger(l Logger) Option {
	return func(o *options) { o.logger = l }
}

// WithCiphers enables at-rest encryption for every message written by this
// instance (encryption is instance-wide, not per-message or per-handler:
// encryption is on iff at least one cipher is provided). currentVersion picks
// which cipher encrypts new writes; it must be one of ciphers' versions, and
// may be omitted when exactly one cipher is given. Key rotation: add a new
// cipher version and point currentVersion at it — older versions must stay in
// the slice as long as any stored row (including DLQ) still uses them, or
// that row will dead-letter on decrypt.
func WithCiphers(currentVersion string, ciphers ...MessageCipher) Option {
	return func(o *options) {
		o.ciphers = ciphers
		o.currentCipherVersion = currentVersion
	}
}

// RunnerOptions configures the opt-in built-in polling loop — pass to
// NewRunner. Without a Runner, callers must invoke ProcessBatch themselves
// (e.g. from their own cron).
type RunnerOptions struct {
	BatchSize    int           // rows claimed per ProcessBatch call
	PollInterval time.Duration // delay before the next poll when the last batch was non-empty (busy)
	IdleInterval time.Duration // delay before the next poll when the last batch was empty (idle)
}

func (o RunnerOptions) withDefaults() RunnerOptions {
	if o.BatchSize <= 0 {
		o.BatchSize = 50
	}
	if o.IdleInterval <= 0 {
		o.IdleInterval = 200 * time.Millisecond
	}
	return o
}

func applyOptions(opts []Option) options {
	o := options{
		metrics: NoopMetrics{},
		logger:  NoopLogger{},
	}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// CallOption configures a single ProcessBatch call.
// Per-call options take priority over the global Config values.
type CallOption func(*callOptions)

type callOptions struct {
	handlerTimeout *time.Duration // nil = use Config.HandlerTimeout
}

// WithCallTimeout sets a per-call handler timeout, overriding Config.HandlerTimeout.
// Pass 0 to explicitly disable the timeout for this call.
func WithCallTimeout(d time.Duration) CallOption {
	return func(o *callOptions) { o.handlerTimeout = &d }
}

func applyCallOptions(opts []CallOption) callOptions {
	var o callOptions
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
