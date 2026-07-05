package runtime

import (
	reactor2 "agent/internal/runtime/reactor"
	"context"
	"fmt"
)

type SyncEffectDispatcher struct{}

func NewSyncEffectDispatcher() *SyncEffectDispatcher {
	return &SyncEffectDispatcher{}
}

func (d *SyncEffectDispatcher) DispatchEffect(ctx context.Context, request reactor2.EffectDispatchRequest) error {
	if d == nil {
		return fmt.Errorf("sync effect dispatcher is nil")
	}
	if request.Executor == nil {
		return fmt.Errorf("effect dispatcher: executor is required")
	}
	if request.Reporter == nil {
		return fmt.Errorf("effect dispatcher: reporter is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	effectCtx := request.Context
	if effectCtx == nil {
		effectCtx = context.Background()
	}
	defer func() {
		if request.OnDone != nil {
			request.OnDone()
		}
	}()

	if err := acquire(effectCtx, request.Semaphore); err != nil {
		_ = request.Reporter.EffectFailed(liveContext(effectCtx), request.Runtime, request.Effect, reactor2.StageEffectExecute, err)
		return nil
	}
	defer release(request.Semaphore)

	if request.Timeout > 0 {
		var cancel context.CancelFunc
		effectCtx, cancel = context.WithTimeout(effectCtx, request.Timeout)
		defer cancel()
	}

	runtime := request.Runtime.Clone()
	effect := request.Effect.Clone()
	_ = request.Reporter.EffectStarted(effectCtx, runtime, effect)
	effectResult, err := request.Executor.ExecuteEffect(effectCtx, runtime.Clone(), effect.Clone())
	if err != nil {
		_ = request.Reporter.EffectFailed(liveContext(effectCtx), runtime, effect, reactor2.StageEffectExecute, err)
		return nil
	}
	if _, err := request.Reporter.EffectResultEvents(effectCtx, runtime, effect, effectResult.Clone().Events); err != nil {
		_ = request.Reporter.EffectFailed(liveContext(effectCtx), runtime, effect, reactor2.StagePublish, err)
		return nil
	}
	_ = request.Reporter.EffectSucceeded(effectCtx, runtime, effect)
	return nil
}

func acquire(ctx context.Context, sem chan struct{}) error {
	if sem == nil {
		return nil
	}
	select {
	case sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func release(sem chan struct{}) {
	if sem == nil {
		return
	}
	<-sem
}

func liveContext(ctx context.Context) context.Context {
	if ctx == nil || ctx.Err() != nil {
		return context.Background()
	}
	return ctx
}
