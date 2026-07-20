package service

import (
	"agent/serviceruntime/contract"
	"strings"
	"testing"
	"time"
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

func TestDecisionKeysAreUniqueAcrossAllOutputs(t *testing.T) {
	decision := Decision{
		Outgoing: []OutgoingMessage{{Key: "same", Kind: contract.MessageEvent, Type: "audit.created", Version: 1}},
		Reply:    &Reply{Key: "same", Type: "counter.reply", Version: 1},
	}
	message := contract.Message{Kind: contract.MessageCommand, Type: "counter.increment", ReplyTo: "caller"}
	if err := decision.ValidateAt(message, nil, time.Now().UTC()); err == nil || !strings.Contains(err.Error(), "decision key") {
		t.Fatalf("error=%v, want cross-output decision key rejection", err)
	}
}

func TestDecisionRejectsExpiredEffectDeadline(t *testing.T) {
	now := time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC)
	deadline := now.Add(-time.Second)
	decision := Decision{Effects: []PlannedEffect{{
		Key: "audit", Type: "audit.write", Version: 1, ExecutorRef: "audit", IdempotencyKey: "audit-1", Deadline: &deadline,
	}}}
	message := contract.Message{Kind: contract.MessageCommand, Type: "counter.increment"}
	if err := decision.ValidateAt(message, func(string) bool { return true }, now); err == nil || !strings.Contains(err.Error(), "deadline") {
		t.Fatalf("error=%v, want expired effect deadline rejection", err)
	}
}
