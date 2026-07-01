package message

// Metrics defines the interface for recording messaging metrics.
// Implement this interface to plug in Prometheus, OpenTelemetry, etc.
type Metrics interface {
	MessageProcessed(topic string)
	MessageRetry(topic string)
	MessageFailed(topic string)
	DedupHit()  // Receive() returned isNew=false (duplicate)
	DedupMiss() // Receive() returned isNew=true (new)
}

// NoopMetrics is a no-op implementation of Metrics.
type NoopMetrics struct{}

func (NoopMetrics) MessageProcessed(_ string) {}
func (NoopMetrics) MessageRetry(_ string)     {}
func (NoopMetrics) MessageFailed(_ string)    {}
func (NoopMetrics) DedupHit()                 {}
func (NoopMetrics) DedupMiss()                {}
