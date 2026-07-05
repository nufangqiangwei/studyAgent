package reactor

import (
	"context"
	"fmt"
)

type AsyncEffectDispatcher struct{}

func NewAsyncEffectDispatcher() *AsyncEffectDispatcher {
	return &AsyncEffectDispatcher{}
}

func (d *AsyncEffectDispatcher) DispatchEffect(ctx context.Context, request EffectDispatchRequest) error {
	if d == nil {
		return fmt.Errorf("async effect dispatcher is nil")
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
	runtime := request.Runtime.Clone()
	effect := request.Effect.Clone()
	executor := request.Executor
	reporter := request.Reporter
	timeout := request.Timeout
	sem := request.Semaphore
	onDone := request.OnDone

	go func() {
		defer func() {
			if onDone != nil {
				onDone()
			}
		}()

		if err := acquire(effectCtx, sem); err != nil {
			_ = reporter.EffectFailed(liveContext(effectCtx), runtime, effect, StageEffectExecute, err)
			return
		}
		defer release(sem)

		if timeout > 0 {
			var cancel context.CancelFunc
			effectCtx, cancel = context.WithTimeout(effectCtx, timeout)
			defer cancel()
		}

		_ = reporter.EffectStarted(effectCtx, runtime, effect)
		effectResult, err := executor.ExecuteEffect(effectCtx, runtime.Clone(), effect.Clone())
		if err != nil {
			_ = reporter.EffectFailed(liveContext(effectCtx), runtime, effect, StageEffectExecute, err)
			return
		}

		effectResult = effectResult.Clone()
		if _, err := reporter.EffectResultEvents(effectCtx, runtime, effect, effectResult.Events); err != nil {
			_ = reporter.EffectFailed(liveContext(effectCtx), runtime, effect, StagePublish, err)
			return
		}
		_ = reporter.EffectSucceeded(effectCtx, runtime, effect)
	}()

	return nil
}
