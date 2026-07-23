package webgateway

import (
	"agent/serviceruntime/effect"
	"agent/serviceruntime/persistence"
	"context"
	"encoding/json"
	"testing"
)

func TestPresentationExecutorAndReconcilerUseSamePresentationID(t *testing.T) {
	var delivered []string
	module, err := NewModule(ModuleOptions{
		Presenter: PresenterFunc(func(_ context.Context, presentation Presentation) error {
			delivered = append(delivered, presentation.PresentationID)
			return nil
		}),
		DefaultAgent: "agent.test", LegacyDefaultAgent: "agent.legacy",
	})
	if err != nil {
		t.Fatal(err)
	}
	presentation := Presentation{
		PresentationID: "web-task/create/request/success", RequestID: "request-1", Operation: OperationCreate,
		Created: &TaskCreatedPresentation{
			RequestID: "request-1",
			Task: TaskDTO{
				TaskID: "task-1", UserID: "user-1", Phase: "created", Input: "hello",
				CreatedAt: fixedTime(), UpdatedAt: fixedTime(),
			},
		},
	}
	payload, _ := json.Marshal(presentation)
	record := persistence.EffectRecord{
		EffectID: "effect-1", Type: PresentationEffectType, Version: ProtocolVersion,
		ExecutorRef: PresentationExecutorRef, IdempotencyKey: "web-task/presentation/" + presentation.PresentationID,
		Payload: payload,
	}
	executed, err := module.executePresentation(context.Background(), record)
	if err != nil {
		t.Fatalf("execute presentation: %v", err)
	}
	reconciled, err := module.reconcilePresentation(context.Background(), record)
	if err != nil {
		t.Fatalf("reconcile presentation: %v", err)
	}
	if reconciled.Action != effect.ReconcileComplete {
		t.Fatalf("unexpected reconcile action %q", reconciled.Action)
	}
	var executeResult, reconcileResult presentationResult
	if err := json.Unmarshal(executed.Payload, &executeResult); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(reconciled.Result, &reconcileResult); err != nil {
		t.Fatal(err)
	}
	if executeResult.PresentationID != presentation.PresentationID ||
		reconcileResult.PresentationID != presentation.PresentationID {
		t.Fatal("executor and reconciler changed presentation identity")
	}
	if len(delivered) != 2 || delivered[0] != delivered[1] {
		t.Fatalf("presenter did not receive the same deduplication id: %#v", delivered)
	}
}

func TestModuleRequiresPresenter(t *testing.T) {
	if _, err := NewModule(ModuleOptions{}); err == nil {
		t.Fatal("expected presenter requirement")
	}
}

func TestModuleRequiresDefaultAgent(t *testing.T) {
	if _, err := NewModule(ModuleOptions{Presenter: PresenterFunc(func(_ context.Context, _ Presentation) error { return nil })}); err == nil {
		t.Fatal("expected default agent requirement")
	}
}

func TestModuleRequiresLegacyDefaultAgent(t *testing.T) {
	if _, err := NewModule(ModuleOptions{
		Presenter:    PresenterFunc(func(_ context.Context, _ Presentation) error { return nil }),
		DefaultAgent: "agent.test",
	}); err == nil {
		t.Fatal("expected legacy default agent fallback requirement")
	}
}
