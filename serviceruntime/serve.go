package serviceruntime

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/fault"
	"agent/serviceruntime/instance"
	"agent/serviceruntime/persistence"
	"context"
	"errors"
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
	errCh := make(chan error, 3)
	background.Add(3)
	go func() {
		defer background.Done()
		errCh <- r.serveMailboxes(serveCtx, options.PollInterval)
	}()
	go func() {
		defer background.Done()
		errCh <- r.serveOutbox(serveCtx, options.PollInterval)
	}()
	go func() {
		defer background.Done()
		errCh <- r.serveEffects(serveCtx, options.PollInterval)
	}()

	var serveErr error
	select {
	case <-serveCtx.Done():
	case serveErr = <-errCh:
		cancel()
	}
	background.Wait()
	if serveErr != nil && ctx.Err() == nil {
		r.setStatus(RuntimeFailed)
		_ = r.bus.Pause(context.Background())
		return serveErr
	}
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

func (r *Runtime) serveMailboxes(ctx context.Context, pollInterval time.Duration) error {
	ctx, cancel := context.WithCancel(ctx)
	type completion struct {
		instanceID contract.ServiceInstanceID
		err        error
	}
	completed := make(chan completion)
	scheduled := make(map[contract.ServiceInstanceID]struct{})
	var handlers sync.WaitGroup
	defer handlers.Wait()
	defer cancel()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	scan := func() error {
		spec := r.plan.Runtime()
		records, err := r.storage.Instances().List(ctx, instance.Query{RuntimeID: spec.ID})
		if err != nil {
			return err
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
				_, handleErr := r.host.HandleNext(ctx, instanceID)
				select {
				case completed <- completion{instanceID: instanceID, err: handleErr}:
				case <-ctx.Done():
				}
			}(record.InstanceID)
		}
		return nil
	}

	if err := scan(); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case result := <-completed:
			delete(scheduled, result.instanceID)
			if fault.IsKind(result.err, fault.CorruptState) {
				return result.err
			}
			if err := scan(); err != nil {
				return err
			}
		case <-ticker.C:
			if err := scan(); err != nil {
				return err
			}
		}
	}
}

func (r *Runtime) serveOutbox(ctx context.Context, pollInterval time.Duration) error {
	for {
		result, err := r.bus.DispatchNextOutbox(ctx, r.ownerID+".outbox")
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, persistence.ErrClosed) || fault.IsKind(err, fault.CorruptState) {
			return err
		}
		if err == nil && !result.Idle {
			continue
		}
		if !waitForWork(ctx, pollInterval) {
			return nil
		}
	}
}

func (r *Runtime) serveEffects(ctx context.Context, pollInterval time.Duration) error {
	for {
		result, err := r.effects.DispatchNext(ctx, r.ownerID+".effect")
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, persistence.ErrClosed) || fault.IsKind(err, fault.CorruptState) {
			return err
		}
		if err == nil && !result.Idle {
			continue
		}
		if !waitForWork(ctx, pollInterval) {
			return nil
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
