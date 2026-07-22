package effect

import (
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/persistence"
	"context"
	"fmt"
	"testing"
	"time"
)

type terminalTestClock struct{ now time.Time }

func (c terminalTestClock) Now() time.Time { return c.now }

type terminalTestStore struct {
	record                 persistence.EffectRecord
	claimed                bool
	terminal               bool
	reconciliationRequired bool
}

func (s *terminalTestStore) ClaimNext(context.Context, contract.RuntimeID, string, time.Duration) (persistence.EffectClaim, bool, error) {
	if s.claimed {
		return persistence.EffectClaim{}, false, nil
	}
	s.claimed = true
	return persistence.EffectClaim{Record: s.record, LeaseToken: "lease"}, true, nil
}

func (s *terminalTestStore) Claim(context.Context, string, string, time.Duration) (persistence.EffectClaim, error) {
	return persistence.EffectClaim{Record: s.record, LeaseToken: "lease"}, nil
}

func (*terminalTestStore) RenewClaim(context.Context, persistence.EffectClaim, time.Duration) error {
	return nil
}
func (*terminalTestStore) MarkStarted(context.Context, persistence.EffectClaim) error { return nil }
func (*terminalTestStore) MarkSucceeded(context.Context, persistence.EffectClaim, persistence.EffectResult) error {
	return nil
}
func (*terminalTestStore) MarkFailed(context.Context, persistence.EffectClaim, error, *time.Time) error {
	return nil
}
func (s *terminalTestStore) MarkTerminalFailed(context.Context, persistence.EffectClaim, error) error {
	s.terminal = true
	return nil
}
func (s *terminalTestStore) RequireReconciliation(context.Context, persistence.EffectClaim, error) error {
	s.reconciliationRequired = true
	return nil
}
func (*terminalTestStore) ListUnfinished(context.Context, contract.RuntimeID) ([]persistence.EffectRecord, error) {
	return nil, nil
}

func terminalTestPlan(t *testing.T) *building.RuntimePlan {
	t.Helper()
	register := building.NewRegister(nil)
	if err := register.RegisterEffectExecutor("test.terminal@v1"); err != nil {
		t.Fatal(err)
	}
	plan, err := register.Compile(context.Background(), building.RuntimeManifest{
		Runtime:  building.RuntimeSpec{ID: "terminal-test", Revision: "v1"},
		Recovery: building.RecoveryPolicy{MaxDeliveryAttempts: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestTerminalFailureNotifierRunsBeforeEffectBecomesTerminal(t *testing.T) {
	now := time.Date(2026, 7, 21, 16, 0, 0, 0, time.UTC)
	store := &terminalTestStore{record: persistence.EffectRecord{
		EffectID: "effect-terminal", RuntimeID: "terminal-test", PlanRevision: "v1",
		ExecutorRef: "test.terminal@v1", Status: persistence.EffectPlanned, Attempt: 1,
	}}
	notified := false
	registry := NewRegistry()
	if err := registry.Register(Spec{
		Ref: "test.terminal@v1", Type: "test.terminal",
		Executor: ExecutorFunc(func(context.Context, persistence.EffectRecord) (ExecutionResult, error) {
			return ExecutionResult{}, fmt.Errorf("permanent failure")
		}),
		TerminalFailure: TerminalFailureNotifierFunc(func(context.Context, persistence.EffectRecord, error) error {
			notified = true
			if store.terminal {
				t.Fatal("effect became terminal before durable business notification")
			}
			return nil
		}),
	}); err != nil {
		t.Fatal(err)
	}
	worker, err := NewWorker(WorkerOptions{
		Plan: terminalTestPlan(t), Store: store, Registry: registry,
		Clock: terminalTestClock{now: now}, MaxAttempts: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := worker.DispatchNext(context.Background(), "owner")
	if err == nil || result.Status != persistence.EffectTerminalFailed || !notified || !store.terminal {
		t.Fatalf("result=%#v notified=%v terminal=%v err=%v", result, notified, store.terminal, err)
	}
}

func TestTerminalNotificationFailureRequiresReconciliation(t *testing.T) {
	store := &terminalTestStore{record: persistence.EffectRecord{
		EffectID: "effect-notification", RuntimeID: "terminal-test", PlanRevision: "v1",
		ExecutorRef: "test.terminal@v1", Status: persistence.EffectPlanned, Attempt: 1,
	}}
	registry := NewRegistry()
	if err := registry.Register(Spec{
		Ref: "test.terminal@v1", Type: "test.terminal",
		Executor: ExecutorFunc(func(context.Context, persistence.EffectRecord) (ExecutionResult, error) {
			return ExecutionResult{}, fmt.Errorf("permanent failure")
		}),
		TerminalFailure: TerminalFailureNotifierFunc(func(context.Context, persistence.EffectRecord, error) error {
			return fmt.Errorf("durable ingress unavailable")
		}),
	}); err != nil {
		t.Fatal(err)
	}
	worker, err := NewWorker(WorkerOptions{
		Plan: terminalTestPlan(t), Store: store, Registry: registry,
		Clock: terminalTestClock{now: time.Now().UTC()}, MaxAttempts: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := worker.DispatchNext(context.Background(), "owner")
	if err == nil || result.Status != persistence.EffectReconciliationRequired || store.terminal || !store.reconciliationRequired {
		t.Fatalf("result=%#v terminal=%v reconcile=%v err=%v", result, store.terminal, store.reconciliationRequired, err)
	}
}

func TestReconciliationTerminalNotificationFailureReportsRequiredStatus(t *testing.T) {
	store := &terminalTestStore{record: persistence.EffectRecord{
		EffectID: "effect-reconcile-notification", RuntimeID: "terminal-test", PlanRevision: "v1",
		ExecutorRef: "test.terminal@v1", Status: persistence.EffectStarted, Attempt: 1,
	}}
	registry := NewRegistry()
	if err := registry.Register(Spec{
		Ref: "test.terminal@v1", Type: "test.terminal",
		Executor: ExecutorFunc(func(context.Context, persistence.EffectRecord) (ExecutionResult, error) {
			return ExecutionResult{}, nil
		}),
		Reconciler: ReconcilerFunc(func(context.Context, persistence.EffectRecord) (ReconciliationResult, error) {
			return ReconciliationResult{Action: ReconcileFail, Reason: "external operation failed"}, nil
		}),
		TerminalFailure: TerminalFailureNotifierFunc(func(context.Context, persistence.EffectRecord, error) error {
			return fmt.Errorf("durable ingress unavailable")
		}),
	}); err != nil {
		t.Fatal(err)
	}
	worker, err := NewWorker(WorkerOptions{
		Plan: terminalTestPlan(t), Store: store, Registry: registry,
		Clock: terminalTestClock{now: time.Now().UTC()}, MaxAttempts: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := worker.DispatchNext(context.Background(), "owner")
	if err == nil || result.Status != persistence.EffectReconciliationRequired || store.terminal || !store.reconciliationRequired {
		t.Fatalf("result=%#v terminal=%v reconcile=%v err=%v", result, store.terminal, store.reconciliationRequired, err)
	}
}
