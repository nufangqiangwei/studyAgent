package orchestrator

import (
	"agent/internal/runtime/agents"
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type AgentRegistry interface {
	Lookup(name string) (agents.Agent, bool)
	ListNames() []string
}

type Planner interface {
	Plan(ctx context.Context, request PlanRequest) (Decision, error)
}

type PlanRequest struct {
	Goal      string            `json:"goal"`
	TaskID    string            `json:"task_id,omitempty"`
	Agents    []AgentDescriptor `json:"agents,omitempty"`
	History   []StepRecord      `json:"history,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	MaxAgents int               `json:"max_agents,omitempty"`
}

type AgentDescriptor struct {
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	Tags        []string `json:"tags,omitempty"`
}

type StepRecord struct {
	TaskID  string          `json:"task_id,omitempty"`
	Agent   string          `json:"agent,omitempty"`
	Input   string          `json:"input,omitempty"`
	Status  string          `json:"status,omitempty"`
	Summary string          `json:"summary,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   string          `json:"error,omitempty"`
}

type DecisionAction string

const (
	ActionStartAgent  DecisionAction = "start_agent"
	ActionResumeAgent DecisionAction = "resume_agent"
	ActionWait        DecisionAction = "wait"
	ActionComplete    DecisionAction = "complete"
	ActionFail        DecisionAction = "fail"
)

type Decision struct {
	Action      DecisionAction    `json:"action"`
	Reason      string            `json:"reason,omitempty"`
	Work        *AgentWork        `json:"work,omitempty"`
	FinalAnswer string            `json:"final_answer,omitempty"`
	Result      json.RawMessage   `json:"result,omitempty"`
	Error       string            `json:"error,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

func (d Decision) Validate() error {
	switch d.Action {
	case ActionStartAgent:
		if d.Work == nil {
			return fmt.Errorf("start_agent decision requires work")
		}
		if strings.TrimSpace(d.Work.Agent) == "" {
			return fmt.Errorf("start_agent decision requires work.agent")
		}
		if strings.TrimSpace(d.Work.Input) == "" {
			return fmt.Errorf("start_agent decision requires work.input")
		}
	case ActionResumeAgent:
		if d.Work == nil {
			return fmt.Errorf("resume_agent decision requires work")
		}
		if strings.TrimSpace(d.Work.Agent) == "" {
			return fmt.Errorf("resume_agent decision requires work.agent")
		}
		if strings.TrimSpace(d.Work.TaskID) == "" {
			return fmt.Errorf("resume_agent decision requires work.task_id")
		}
	case ActionWait, ActionComplete, ActionFail:
		return nil
	default:
		return fmt.Errorf("unsupported orchestrator decision action %q", d.Action)
	}
	return nil
}

type AgentWork struct {
	TaskID   string            `json:"task_id,omitempty"`
	Agent    string            `json:"agent"`
	Input    string            `json:"input,omitempty"`
	Payload  json.RawMessage   `json:"payload,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type AdvanceRequest struct {
	Goal     string            `json:"goal"`
	TaskID   string            `json:"task_id,omitempty"`
	History  []StepRecord      `json:"history,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type AdvanceResult struct {
	Status      Status              `json:"status"`
	Decision    Decision            `json:"decision"`
	AgentResult *agents.AgentResult `json:"agent_result,omitempty"`
}

type Status string

const (
	StatusDispatched Status = "dispatched"
	StatusWaiting    Status = "waiting"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
)
