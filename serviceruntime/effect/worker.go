package effect

import (
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/persistence"
	"context"
	"errors"
	"fmt"
	"time"
)

type UnknownOutcome interface {
	error
	OutcomeUnknown() bool
}

type WorkResult struct {
	EffectID string
	Status   persistence.EffectStatus
	Idle     bool
}

type Worker interface {
	DispatchNext(ctx context.Context, ownerID string) (WorkResult, error)
	Reconcile(ctx context.Context, effect persistence.EffectRecord, ownerID string) (ReconciliationResult, error)
}

type RuntimeWorker struct {
	plan        *building.RuntimePlan
	store       persistence.EffectStore
	registry    *Registry
	clock       contract.Clock
	lease       time.Duration
	maxAttempts int
}

type WorkerOptions struct {
	Plan        *building.RuntimePlan
	Store       persistence.EffectStore
	Registry    *Registry
	Clock       contract.Clock
	Lease       time.Duration
	MaxAttempts int
}

func NewWorker(options WorkerOptions) (*RuntimeWorker, error) {
	if options.Plan == nil || options.Store == nil || options.Registry == nil {
		return nil, fmt.Errorf("effect worker requires plan, store and registry")
	}
	policy := options.Plan.Recovery()
	if options.Lease <= 0 {
		options.Lease = policy.EffectLease
	}
	if options.MaxAttempts <= 0 {
		options.MaxAttempts = policy.MaxDeliveryAttempts
	}
	return &RuntimeWorker{plan: options.Plan, store: options.Store, registry: options.Registry, clock: options.Clock, lease: options.Lease, maxAttempts: options.MaxAttempts}, nil
}

func (w *RuntimeWorker) DispatchNext(ctx context.Context, ownerID string) (WorkResult, error) {
	if w == nil {
		return WorkResult{}, fmt.Errorf("effect worker is nil")
	}
	spec := w.plan.Runtime()
	claim, ok, err := w.store.ClaimNext(ctx, spec.ID, ownerID, w.lease)
	if err != nil || !ok {
		return WorkResult{Idle: !ok}, err
	}
	if claim.Record.Status == persistence.EffectStarted || claim.Record.Status == persistence.EffectReconciliationRequired {
		result, reconcileErr := w.reconcileClaim(ctx, claim)
		return WorkResult{EffectID: claim.Record.EffectID, Status: reconciliationStatus(result.Action)}, reconcileErr
	}
	executor, found := w.registry.ResolveExecutor(claim.Record.ExecutorRef)
	if !found {
		err = fmt.Errorf("effect executor %q is not registered", claim.Record.ExecutorRef)
		_ = w.store.MarkTerminalFailed(ctx, claim, err)
		return WorkResult{EffectID: claim.Record.EffectID, Status: persistence.EffectTerminalFailed}, err
	}
	if err := w.store.MarkStarted(ctx, claim); err != nil {
		return WorkResult{}, err
	}
	result, executeErr := executor.ExecuteEffect(ctx, claim.Record.Clone())
	if executeErr == nil {
		err = w.store.MarkSucceeded(ctx, claim, result.Payload)
		return WorkResult{EffectID: claim.Record.EffectID, Status: persistence.EffectSucceeded}, err
	}
	var unknown UnknownOutcome
	if errors.As(executeErr, &unknown) && unknown.OutcomeUnknown() {
		err = w.store.RequireReconciliation(ctx, claim, executeErr)
		if err != nil {
			return WorkResult{}, err
		}
		return WorkResult{EffectID: claim.Record.EffectID, Status: persistence.EffectReconciliationRequired}, executeErr
	}
	if claim.Record.Attempt >= w.maxAttempts {
		err = w.store.MarkTerminalFailed(ctx, claim, executeErr)
		return WorkResult{EffectID: claim.Record.EffectID, Status: persistence.EffectTerminalFailed}, firstError(executeErr, err)
	}
	retryAt := w.now().Add(time.Duration(claim.Record.Attempt) * time.Second)
	err = w.store.MarkFailed(ctx, claim, executeErr, &retryAt)
	return WorkResult{EffectID: claim.Record.EffectID, Status: persistence.EffectFailed}, firstError(executeErr, err)
}

func (w *RuntimeWorker) Reconcile(ctx context.Context, record persistence.EffectRecord, ownerID string) (ReconciliationResult, error) {
	if w == nil {
		return ReconciliationResult{}, fmt.Errorf("effect worker is nil")
	}
	claim, err := w.store.Claim(ctx, record.EffectID, ownerID, w.lease)
	if err != nil {
		return ReconciliationResult{}, err
	}
	return w.reconcileClaim(ctx, claim)
}

func (w *RuntimeWorker) reconcileClaim(ctx context.Context, claim persistence.EffectClaim) (ReconciliationResult, error) {
	reconciler, found := w.registry.ResolveReconciler(claim.Record.ExecutorRef)
	if !found {
		err := fmt.Errorf("effect reconciler %q is not registered", claim.Record.ExecutorRef)
		_ = w.store.RequireReconciliation(ctx, claim, err)
		return ReconciliationResult{}, err
	}
	result, reconcileErr := reconciler.ReconcileEffect(ctx, claim.Record.Clone())
	if reconcileErr != nil {
		_ = w.store.RequireReconciliation(ctx, claim, reconcileErr)
		return result, reconcileErr
	}
	var err error
	switch result.Action {
	case ReconcileComplete:
		err = w.store.MarkSucceeded(ctx, claim, result.Result)
	case ReconcileRetry:
		err = w.store.MarkFailed(ctx, claim, fmt.Errorf("reconciliation requested retry: %s", result.Reason), result.RetryAt)
	case ReconcileFail:
		err = w.store.MarkTerminalFailed(ctx, claim, fmt.Errorf("reconciliation failed: %s", result.Reason))
	case ReconcileAskUser, ReconcileCompensate:
		err = w.store.RequireReconciliation(ctx, claim, fmt.Errorf("reconciliation requires %s: %s", result.Action, result.Reason))
	default:
		err = fmt.Errorf("unsupported reconciliation action %q", result.Action)
		_ = w.store.RequireReconciliation(ctx, claim, err)
	}
	return result, err
}

func reconciliationStatus(action ReconciliationAction) persistence.EffectStatus {
	switch action {
	case ReconcileComplete:
		return persistence.EffectSucceeded
	case ReconcileRetry:
		return persistence.EffectFailed
	case ReconcileFail:
		return persistence.EffectTerminalFailed
	default:
		return persistence.EffectReconciliationRequired
	}
}

func (w *RuntimeWorker) now() time.Time {
	if w.clock == nil {
		return time.Now().UTC()
	}
	return w.clock.Now().UTC()
}

func firstError(primary, secondary error) error {
	if primary != nil {
		return primary
	}
	return secondary
}
