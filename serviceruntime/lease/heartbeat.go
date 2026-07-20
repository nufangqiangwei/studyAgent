package lease

import (
	"context"
	"sync"
	"time"
)

type Heartbeat struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	mu  sync.RWMutex
	err error
}

func Start(parent context.Context, interval time.Duration, renew func(context.Context) error) *Heartbeat {
	if parent == nil {
		parent = context.Background()
	}
	if interval <= 0 {
		interval = time.Second
	}
	ctx, cancel := context.WithCancel(parent)
	h := &Heartbeat{ctx: ctx, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(h.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := renew(ctx); err != nil {
					h.mu.Lock()
					h.err = err
					h.mu.Unlock()
					cancel()
					return
				}
			}
		}
	}()
	return h
}

func (h *Heartbeat) Context() context.Context {
	if h == nil {
		return context.Background()
	}
	return h.ctx
}

func (h *Heartbeat) Err() error {
	if h == nil {
		return nil
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.err
}

func (h *Heartbeat) Stop() error {
	if h == nil {
		return nil
	}
	h.cancel()
	<-h.done
	return h.Err()
}

func Interval(durations ...time.Duration) time.Duration {
	var shortest time.Duration
	for _, duration := range durations {
		if duration > 0 && (shortest == 0 || duration < shortest) {
			shortest = duration
		}
	}
	if shortest <= 0 {
		return time.Second
	}
	interval := shortest / 3
	if interval <= 0 {
		return time.Millisecond
	}
	return interval
}
