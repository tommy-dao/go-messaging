package message

import (
	"testing"
	"time"
)

func TestFixedBackoff(t *testing.T) {
	b := FixedBackoff{Interval: 5 * time.Second}

	tests := []struct {
		retry int
		want  time.Duration
	}{
		{1, 5 * time.Second},
		{2, 5 * time.Second},
		{5, 5 * time.Second},
		{100, 5 * time.Second},
	}

	for _, tt := range tests {
		got := b.Next(tt.retry)
		if got != tt.want {
			t.Errorf("FixedBackoff.Next(%d) = %v, want %v", tt.retry, got, tt.want)
		}
	}
}

func TestLinearBackoff(t *testing.T) {
	b := LinearBackoff{BaseDelay: time.Second}

	tests := []struct {
		retry int
		want  time.Duration
	}{
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 3 * time.Second},
		{5, 5 * time.Second},
		{0, 1 * time.Second}, // clamped to 1
	}

	for _, tt := range tests {
		got := b.Next(tt.retry)
		if got != tt.want {
			t.Errorf("LinearBackoff.Next(%d) = %v, want %v", tt.retry, got, tt.want)
		}
	}
}

func TestExponentialBackoff(t *testing.T) {
	b := ExponentialBackoff{
		BaseDelay: time.Second,
		MaxDelay:  30 * time.Second,
	}

	tests := []struct {
		retry int
		want  time.Duration
	}{
		{1, 1 * time.Second},  // 1 * 2^0 = 1s
		{2, 2 * time.Second},  // 1 * 2^1 = 2s
		{3, 4 * time.Second},  // 1 * 2^2 = 4s
		{4, 8 * time.Second},  // 1 * 2^3 = 8s
		{5, 16 * time.Second}, // 1 * 2^4 = 16s
		{6, 30 * time.Second}, // 1 * 2^5 = 32s, capped at 30s
		{10, 30 * time.Second},
	}

	for _, tt := range tests {
		got := b.Next(tt.retry)
		if got != tt.want {
			t.Errorf("ExponentialBackoff.Next(%d) = %v, want %v", tt.retry, got, tt.want)
		}
	}
}

func TestExponentialBackoff_NoMaxDelay(t *testing.T) {
	b := ExponentialBackoff{
		BaseDelay: time.Second,
		MaxDelay:  0, // no cap
	}

	got := b.Next(10)
	want := time.Duration(float64(time.Second) * 512) // 2^9 = 512
	if got != want {
		t.Errorf("ExponentialBackoff.Next(10) = %v, want %v", got, want)
	}
}
