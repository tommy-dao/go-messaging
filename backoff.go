package message

import (
	"math"
	"time"
)

// Backoff calculates the delay before the next retry attempt.
type Backoff interface {
	Next(retryCount int) time.Duration
}

// FixedBackoff returns the same interval for every retry.
type FixedBackoff struct {
	Interval time.Duration
}

func (b FixedBackoff) Next(_ int) time.Duration {
	return b.Interval
}

// LinearBackoff increases delay linearly: BaseDelay * retryCount.
type LinearBackoff struct {
	BaseDelay time.Duration
}

func (b LinearBackoff) Next(retryCount int) time.Duration {
	if retryCount < 1 {
		retryCount = 1
	}
	return b.BaseDelay * time.Duration(retryCount)
}

// ExponentialBackoff increases delay exponentially: BaseDelay * 2^(retryCount-1), capped at MaxDelay.
type ExponentialBackoff struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

func (b ExponentialBackoff) Next(retryCount int) time.Duration {
	if retryCount < 1 {
		retryCount = 1
	}
	delay := time.Duration(float64(b.BaseDelay) * math.Pow(2, float64(retryCount-1)))
	if b.MaxDelay > 0 && delay > b.MaxDelay {
		delay = b.MaxDelay
	}
	return delay
}
