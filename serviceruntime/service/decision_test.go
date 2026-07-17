package service

import (
	"agent/serviceruntime/contract"
	"testing"
)

func TestDecisionRejectsQuerySideEffects(t *testing.T) {
	decision := Decision{Events: []NewEvent{{Key: "changed", Type: "state.changed", Version: 1}}}
	err := decision.Validate(contract.Message{Kind: contract.MessageQuery, Type: "state.get", Version: 1, ReplyTo: "caller"}, nil)
	if err == nil {
		t.Fatal("expected query side-effect validation error")
	}
}

func TestDecisionRequiresKnownEffectExecutor(t *testing.T) {
	decision := Decision{Effects: []PlannedEffect{{Key: "write", Type: "file.write", Version: 1, ExecutorRef: "filesystem", IdempotencyKey: "write-1"}}}
	err := decision.Validate(contract.Message{Kind: contract.MessageCommand, Type: "write", Version: 1}, func(string) bool { return false })
	if err == nil {
		t.Fatal("expected unknown effect executor validation error")
	}
}
