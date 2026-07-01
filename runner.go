package message

import (
	"context"
	"sync"
	"time"
)

// Runner is an opt-in background loop that repeatedly calls ProcessBatch:
// busy (last batch non-empty) it re-polls after PollInterval; idle (empty
// batch) it waits IdleInterval before polling again. Without a Runner,
// callers must invoke ProcessBatch themselves (e.g. from their own cron) —
// this type just wraps that in a goroutine for convenience.
//
// Only interval-mode scheduling is built in; wall-clock cron scheduling is
// left to the caller (e.g. a cron library calling ProcessBatch directly) to
// avoid pulling in a cron dependency here.
type Runner struct {
	messaging *Messaging
	opts      RunnerOptions
	handler   Handler // nil = route via Register/RegisterDefault

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// NewRunner builds a Runner with the given options. handler is optional; nil
// routes each message via Register/RegisterDefault (same as ProcessBatch).
func NewRunner(m *Messaging, opts RunnerOptions, handler Handler) *Runner {
	return &Runner{messaging: m, opts: opts.withDefaults(), handler: handler}
}

// Start launches the polling loop in a goroutine. Safe to call once; call
// Stop before starting again.
func (r *Runner) Start(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cancel != nil {
		return // already running
	}
	loopCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	r.done = make(chan struct{})
	go r.loop(loopCtx)
}

// Stop cancels the polling loop and waits for the in-flight batch to finish.
func (r *Runner) Stop() {
	r.mu.Lock()
	cancel, done := r.cancel, r.done
	r.cancel, r.done = nil, nil
	r.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	<-done
}

func (r *Runner) loop(ctx context.Context) {
	defer close(r.done)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		n, err := r.messaging.ProcessBatch(ctx, r.opts.BatchSize, r.handler)
		delay := r.opts.IdleInterval
		if err == nil && n > 0 {
			delay = r.opts.PollInterval
		}
		if delay <= 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}
