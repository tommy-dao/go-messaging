package message

import "time"

// Config holds configuration for the messaging library.
type Config struct {
	TablePrefix       string
	DefaultMaxRetries int
	DefaultBackoff    Backoff
	ClaimTimeout      time.Duration // stuck claim recovery threshold
	DedupTTL          time.Duration // dedup entry expiration
	WorkerID          string        // identifies this instance for claimed_by
}

func (c Config) withDefaults() Config {
	if c.DefaultMaxRetries <= 0 {
		c.DefaultMaxRetries = 5
	}
	if c.DefaultBackoff == nil {
		c.DefaultBackoff = LinearBackoff{BaseDelay: time.Second}
	}
	if c.ClaimTimeout <= 0 {
		c.ClaimTimeout = 5 * time.Minute
	}
	if c.DedupTTL <= 0 {
		c.DedupTTL = 24 * time.Hour
	}
	if c.WorkerID == "" {
		c.WorkerID = "default"
	}
	return c
}

// tableName returns the prefixed table name.
func (c Config) tableName(name string) string {
	if c.TablePrefix == "" {
		return name
	}
	return c.TablePrefix + "_" + name
}

// Option configures optional settings for Inbox, Outbox, or Cleanup.
type Option func(*options)

type options struct {
	metrics Metrics
	backoff Backoff
}

// WithMetrics sets a custom metrics implementation.
func WithMetrics(m Metrics) Option {
	return func(o *options) {
		o.metrics = m
	}
}

// WithBackoff overrides the default backoff strategy.
func WithBackoff(b Backoff) Option {
	return func(o *options) {
		o.backoff = b
	}
}

func applyOptions(opts []Option) options {
	o := options{
		metrics: NoopMetrics{},
	}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
