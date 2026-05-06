package message

// Metrics defines the interface for recording messaging metrics.
// Implement this interface to plug in Prometheus, OpenTelemetry, etc.
type Metrics interface {
	InboxProcessed(consumerGroup, eventType string)
	InboxRetry(consumerGroup, eventType string)
	InboxFailed(consumerGroup, eventType string)
	OutboxPublished(eventType string)
	OutboxRetry(eventType string)
	OutboxFailed(eventType string)
}

// NoopMetrics is a no-op implementation of Metrics.
type NoopMetrics struct{}

func (NoopMetrics) InboxProcessed(_, _ string) {}
func (NoopMetrics) InboxRetry(_, _ string)     {}
func (NoopMetrics) InboxFailed(_, _ string)    {}
func (NoopMetrics) OutboxPublished(_ string)   {}
func (NoopMetrics) OutboxRetry(_ string)       {}
func (NoopMetrics) OutboxFailed(_ string)      {}
