package orchestrator

import (
	"agent/internal/runtime/agents"
	"context"
	"encoding/json"
	"testing"
	"time"
)

type stubPlanner struct {
	decision Decision
}

func (p stubPlanner) Plan(_ context.Context, _ PlanRequest) (Decision, error) {
	return p.decision, nil
}

type stubAgent struct {
	name      string
	started   agents.AgentStartInput
	resumed   agents.AgentResumeInput
	startSeen bool
}

func (a *stubAgent) Name() string {
	return a.name
}

func (a *stubAgent) Start(_ context.Context, input agents.AgentStartInput) (agents.AgentResult, error) {
	a.started = input
	a.startSeen = true
	return agents.AgentResult{
		TaskID: input.TaskID,
		Agent:  a.name,
		Snapshot: agents.AgentSnapshot{
			TaskID:    input.TaskID,
			Agent:     a.name,
			Phase:     agents.BusinessPhaseCompleted,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
	}, nil
}

func (a *stubAgent) Resume(_ context.Context, input agents.AgentResumeInput) (agents.AgentResult, error) {
	a.resumed = input
	return agents.AgentResult{TaskID: input.TaskID, Agent: a.name}, nil
}

func (a *stubAgent) Snapshot(_ context.Context, _ string) (agents.AgentSnapshot, bool, error) {
	return agents.AgentSnapshot{}, false, nil
}

func TestAdvanceStartsSelectedAgent(t *testing.T) {
	agent := &stubAgent{name: "analyze"}
	registry, err := agents.NewRegistry(agent)
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	orchestrator, err := New(registry, stubPlanner{
		decision: Decision{
			Action: ActionStartAgent,
			Work: &AgentWork{
				TaskID: "task-1",
				Agent:  "analyze",
				Input:  "inspect repository",
				Metadata: map[string]string{
					"scope": "code",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("new orchestrator: %v", err)
	}

	result, err := orchestrator.Advance(context.Background(), AdvanceRequest{
		Goal: "understand code",
		Metadata: map[string]string{
			"request_id": "req-1",
		},
	})
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if result.Status != StatusDispatched {
		t.Fatalf("status = %q, want %q", result.Status, StatusDispatched)
	}
	if !agent.startSeen {
		t.Fatalf("expected selected agent to start")
	}
	if agent.started.TaskID != "task-1" {
		t.Fatalf("task id = %q, want task-1", agent.started.TaskID)
	}
	if agent.started.Metadata["request_id"] != "req-1" || agent.started.Metadata["scope"] != "code" {
		t.Fatalf("metadata was not merged: %#v", agent.started.Metadata)
	}
}

func TestDecodeDecisionAcceptsEnvelope(t *testing.T) {
	raw, err := json.Marshal(map[string]interface{}{
		"decision": map[string]interface{}{
			"action": "complete",
			"reason": "done",
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	decision, err := decodeDecision(string(raw))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decision.Action != ActionComplete {
		t.Fatalf("action = %q, want %q", decision.Action, ActionComplete)
	}
}
