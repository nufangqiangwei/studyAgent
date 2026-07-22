package effect

import (
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/fault"
	leaseguard "agent/serviceruntime/lease"
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
	plans       building.PlanResolver
	store       persistence.EffectStore
	registry    *Registry
	clock       contract.Clock
	lease       time.Duration
	maxAttempts int
	retryPolicy fault.RetryPolicy
}

type WorkerOptions struct {
	Plan        *building.RuntimePlan
	Plans       building.PlanResolver
	Store       persistence.EffectStore
	Registry    *Registry
	Clock       contract.Clock
	Lease       time.Duration
	MaxAttempts int
	RetryPolicy fault.RetryPolicy
}

func NewWorker(options WorkerOptions) (*RuntimeWorker, error) {
	if options.Plan == nil || options.Store == nil || options.Registry == nil {
		return nil, fmt.Errorf("effect worker requires plan, store and registry")
	}
	if options.Plans == nil {
		options.Plans = building.NewPlanCatalog(options.Plan)
	}
	policy := options.Plan.Recovery()
	if options.Lease <= 0 {
		options.Lease = policy.EffectLease
	}
	if options.MaxAttempts <= 0 {
		options.MaxAttempts = policy.MaxDeliveryAttempts
	}
	if options.RetryPolicy == nil {
		options.RetryPolicy = fault.ExponentialRetryPolicy{}
	}
	return &RuntimeWorker{plan: options.Plan, plans: options.Plans, store: options.Store, registry: options.Registry, clock: options.Clock, lease: options.Lease, maxAttempts: options.MaxAttempts, retryPolicy: options.RetryPolicy}, nil
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
	heartbeat := leaseguard.Start(ctx, leaseguard.Interval(w.lease), func(renewCtx context.Context) error {
		return w.store.RenewClaim(renewCtx, claim, w.lease)
	})
	defer heartbeat.Stop()
	workCtx := heartbeat.Context()
	if claim.Record.Status == persistence.EffectStarted || claim.Record.Status == persistence.EffectReconciliationRequired {
		result, reconcileErr := w.reconcileClaim(workCtx, claim)
		if reconcileErr == nil && heartbeat.Err() != nil {
			reconcileErr = fault.Wrap(fault.LeaseLost, "renew_effect_claim", heartbeat.Err())
		}
		return WorkResult{EffectID: claim.Record.EffectID, Status: reconciliationStatus(result)}, reconcileErr
	}
	if claim.Record.Deadline != nil && !claim.Record.Deadline.After(w.now()) {
		err = fault.Wrap(fault.Permanent, "execute_effect", fmt.Errorf("effect %q deadline has expired", claim.Record.EffectID))
		status, storeErr := w.markTerminalFailed(ctx, claim, err)
		return WorkResult{EffectID: claim.Record.EffectID, Status: status}, firstError(err, storeErr)
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
	result, executeErr := executor.ExecuteEffect(workCtx, claim.Record.Clone())
	if executeErr == nil {
		executeErr = validateEffectResultPayload(w.payloadPolicy(claim.Record), result.Payload)
	}
	if executeErr == nil && heartbeat.Err() != nil {
		executeErr = fault.Wrap(fault.LeaseLost, "renew_effect_claim", heartbeat.Err())
	}
	if executeErr == nil {
		err = w.store.MarkSucceeded(ctx, claim, persistence.EffectResult{Payload: result.Payload, Metadata: result.Metadata})
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
	retry := w.retryPolicy.DecideRetry(fault.RetryInput{Error: executeErr, Attempt: claim.Record.Attempt, MaxAttempts: w.maxAttempts, Now: w.now()})
	if !retry.Retry {
		status, terminalErr := w.markTerminalFailed(ctx, claim, executeErr)
		return WorkResult{EffectID: claim.Record.EffectID, Status: status}, firstError(executeErr, terminalErr)
	}
	err = w.store.MarkFailed(ctx, claim, executeErr, &retry.RetryAt)
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
	heartbeat := leaseguard.Start(ctx, leaseguard.Interval(w.lease), func(renewCtx context.Context) error {
		return w.store.RenewClaim(renewCtx, claim, w.lease)
	})
	defer heartbeat.Stop()
	result, reconcileErr := w.reconcileClaim(heartbeat.Context(), claim)
	if reconcileErr == nil && heartbeat.Err() != nil {
		reconcileErr = fault.Wrap(fault.LeaseLost, "renew_effect_claim", heartbeat.Err())
	}
	return result, reconcileErr
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
		if payloadErr := validateEffectResultPayload(w.payloadPolicy(claim.Record), result.Result); payloadErr != nil {
			result.status, err = w.markTerminalFailed(ctx, claim, payloadErr)
			return result, firstError(payloadErr, err)
		}
		err = w.store.MarkSucceeded(ctx, claim, persistence.EffectResult{Payload: result.Result})
		result.status = persistence.EffectSucceeded
	case ReconcileRetry:
		err = w.store.MarkFailed(ctx, claim, fmt.Errorf("reconciliation requested retry: %s", result.Reason), result.RetryAt)
		result.status = persistence.EffectFailed
	case ReconcileFail:
		result.status, err = w.markTerminalFailed(ctx, claim, fmt.Errorf("reconciliation failed: %s", result.Reason))
	case ReconcileAskUser, ReconcileCompensate:
		err = w.store.RequireReconciliation(ctx, claim, fmt.Errorf("reconciliation requires %s: %s", result.Action, result.Reason))
		result.status = persistence.EffectReconciliationRequired
	default:
		err = fmt.Errorf("unsupported reconciliation action %q", result.Action)
		_ = w.store.RequireReconciliation(ctx, claim, err)
		result.status = persistence.EffectReconciliationRequired
	}
	return result, err
}

func (w *RuntimeWorker) markTerminalFailed(ctx context.Context, claim persistence.EffectClaim, cause error) (persistence.EffectStatus, error) {
	if notifier, found := w.registry.ResolveTerminalFailure(claim.Record.ExecutorRef); found {
		if notifyErr := notifier.NotifyTerminalFailure(ctx, claim.Record.Clone(), cause); notifyErr != nil {
			storeErr := w.store.RequireReconciliation(ctx, claim, notifyErr)
			return persistence.EffectReconciliationRequired, firstError(notifyErr, storeErr)
		}
	}
	if err := w.store.MarkTerminalFailed(ctx, claim, cause); err != nil {
		return persistence.EffectTerminalFailed, err
	}
	return persistence.EffectTerminalFailed, nil
}

func (w *RuntimeWorker) payloadPolicy(record persistence.EffectRecord) building.InlinePayloadPolicy {
	if plan, found := w.plans.ResolvePlan(record.RuntimeID, record.PlanRevision); found {
		return plan.Payloads()
	}
	return w.plan.Payloads()
}
func validateEffectResultPayload(policy building.InlinePayloadPolicy, payload []byte) error {
	if len(payload) <= policy.MaxEffectBytes {
		return nil
	}
	return fault.Wrap(fault.Permanent, "validate_effect_result_payload", fmt.Errorf("effect result payload is %d bytes; maximum inline size is %d bytes; store the content as an artifact and return ArtifactRef", len(payload), policy.MaxEffectBytes))
}

func reconciliationStatus(result ReconciliationResult) persistence.EffectStatus {
	if result.status != "" {
		return result.status
	}
	switch result.Action {
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
