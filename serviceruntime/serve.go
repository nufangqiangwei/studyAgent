package serviceruntime

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/instance"
	"context"
	"fmt"
	"sync"
	"time"
)

type ServeOptions struct {
	PollInterval time.Duration
}

func (o ServeOptions) withDefaults() ServeOptions {
	if o.PollInterval <= 0 {
		o.PollInterval = 10 * time.Millisecond
	}
	return o
}

// Serve continuously advances inboxes, outboxes, and effects until ctx is
// cancelled. A separate goroutine is used for each ready service instance, so
// one Service.Handle may synchronously wait for another service without
// stopping that target service from running.
func (r *Runtime) Serve(ctx context.Context) error {
	return r.ServeWithOptions(ctx, ServeOptions{})
}

func (r *Runtime) ServeWithOptions(ctx context.Context, options ServeOptions) error {
	if r == nil {
		return fmt.Errorf("service runtime is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if r.Status() != RuntimeLive {
		return fmt.Errorf("service runtime is not live")
	}
	serveCtx, cancel := context.WithCancel(ctx)
	if err := r.beginServing(cancel); err != nil {
		cancel()
		return err
	}
	defer r.finishServing()
	defer cancel()

	options = options.withDefaults()
	var background sync.WaitGroup
	background.Add(3)
	go func() {
		defer background.Done()
		r.serveMailboxes(serveCtx, options.PollInterval)
	}()
	go func() {
		defer background.Done()
		r.serveOutbox(serveCtx, options.PollInterval)
	}()
	go func() {
		defer background.Done()
		r.serveEffects(serveCtx, options.PollInterval)
	}()

	<-serveCtx.Done()
	background.Wait()
	return nil
}

func (r *Runtime) beginServing(cancel context.CancelFunc) error {
	r.serveMu.Lock()
	defer r.serveMu.Unlock()
	if r.serving {
		return fmt.Errorf("service runtime is already serving")
	}
	r.serving = true
	r.serveCancel = cancel
	r.serveDone = make(chan struct{})
	return nil
}

func (r *Runtime) finishServing() {
	r.serveMu.Lock()
	done := r.serveDone
	r.serving = false
	r.serveCancel = nil
	r.serveDone = nil
	if done != nil {
		close(done)
	}
	r.serveMu.Unlock()
}

func (r *Runtime) serveMailboxes(ctx context.Context, pollInterval time.Duration) {
	completed := make(chan contract.ServiceInstanceID)
	scheduled := make(map[contract.ServiceInstanceID]struct{})
	var handlers sync.WaitGroup
	defer handlers.Wait()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	scan := func() {
		spec := r.plan.Runtime()
		records, err := r.storage.Instances().List(ctx, instance.Query{RuntimeID: spec.ID, PlanRevision: spec.Revision})
		if err != nil {
			return
		}
		for _, record := range records {
			if record.Lifecycle == instance.Terminated || record.Lifecycle == instance.Draining {
				continue
			}
			if _, exists := scheduled[record.InstanceID]; exists {
				continue
			}
			pending, countErr := r.storage.Inbox().CountPending(ctx, record.MailboxID)
			if countErr != nil || pending == 0 {
				continue
			}
			scheduled[record.InstanceID] = struct{}{}
			handlers.Add(1)
			go func(instanceID contract.ServiceInstanceID) {
				defer handlers.Done()
				_, _ = r.host.HandleNext(ctx, instanceID)
				select {
				case completed <- instanceID:
				case <-ctx.Done():
				}
			}(record.InstanceID)
		}
	}

	scan()
	for {
		select {
		case <-ctx.Done():
			return
		case instanceID := <-completed:
			delete(scheduled, instanceID)
			scan()
		case <-ticker.C:
			scan()
		}
	}
}

func (r *Runtime) serveOutbox(ctx context.Context, pollInterval time.Duration) {
	for {
		result, err := r.bus.DispatchNextOutbox(ctx, r.ownerID+".outbox")
		if ctx.Err() != nil {
			return
		}
		if err == nil && !result.Idle {
			continue
		}
		if !waitForWork(ctx, pollInterval) {
			return
		}
	}
}

func (r *Runtime) serveEffects(ctx context.Context, pollInterval time.Duration) {
	for {
		result, err := r.effects.DispatchNext(ctx, r.ownerID+".effect")
		if ctx.Err() != nil {
			return
		}
		if err == nil && !result.Idle {
			continue
		}
		if !waitForWork(ctx, pollInterval) {
			return
		}
	}
}

func waitForWork(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
