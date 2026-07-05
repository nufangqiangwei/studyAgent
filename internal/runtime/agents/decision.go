package agents

import (
	"encoding/json"
	"fmt"
	"strings"
)

type DecisionAction string

const (
	ActionUseTool        DecisionAction = "use_tool"
	ActionAskUser        DecisionAction = "ask_user"
	ActionCreateSubAgent DecisionAction = "create_sub_agent"
	ActionComplete       DecisionAction = "complete"
	ActionFail           DecisionAction = "fail"
)

type Decision struct {
	Action      DecisionAction   `json:"action"`
	Phase       BusinessPhase    `json:"phase,omitempty"`
	Thought     string           `json:"thought,omitempty"`
	Plan        []PlanStep       `json:"plan,omitempty"`
	StepIndex   *int             `json:"step_index,omitempty"`
	Scratchpad  string           `json:"scratchpad,omitempty"`
	Tool        *ToolIntent      `json:"tool,omitempty"`
	UserInput   *UserInputIntent `json:"user_input,omitempty"`
	SubAgent    *SubAgentIntent  `json:"sub_agent,omitempty"`
	FinalAnswer string           `json:"final_answer,omitempty"`
	Result      json.RawMessage  `json:"result,omitempty"`
	Error       string           `json:"error,omitempty"`
}

func (d Decision) Clone() Decision {
	cloned := d
	if len(d.Plan) > 0 {
		cloned.Plan = append([]PlanStep(nil), d.Plan...)
	}
	if d.StepIndex != nil {
		step := *d.StepIndex
		cloned.StepIndex = &step
	}
	if d.Tool != nil {
		tool := d.Tool.Clone()
		cloned.Tool = &tool
	}
	if d.UserInput != nil {
		input := *d.UserInput
		cloned.UserInput = &input
	}
	if d.SubAgent != nil {
		subAgent := *d.SubAgent
		cloned.SubAgent = &subAgent
	}
	cloned.Result = append(json.RawMessage(nil), d.Result...)
	return cloned
}

type ToolIntent struct {
	ToolCallID string          `json:"tool_call_id,omitempty"`
	ToolName   string          `json:"tool_name"`
	Arguments  json.RawMessage `json:"arguments,omitempty"`
}

func (i ToolIntent) Clone() ToolIntent {
	cloned := i
	cloned.Arguments = append(json.RawMessage(nil), i.Arguments...)
	return cloned
}

type UserInputIntent struct {
	RequestID string          `json:"request_id,omitempty"`
	Prompt    string          `json:"prompt"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
}

type SubAgentIntent struct {
	SubTaskID string `json:"sub_task_id,omitempty"`
	Agent     string `json:"agent"`
	Input     string `json:"input"`
}

func (d Decision) Validate() error {
	switch d.Action {
	case ActionUseTool:
		if d.Tool == nil {
			return fmt.Errorf("tool decision requires tool intent")
		}
		if strings.TrimSpace(d.Tool.ToolName) == "" {
			return fmt.Errorf("tool decision requires tool_name")
		}
	case ActionAskUser:
		if d.UserInput == nil {
			return fmt.Errorf("ask_user decision requires user_input intent")
		}
		if strings.TrimSpace(d.UserInput.Prompt) == "" {
			return fmt.Errorf("ask_user decision requires prompt")
		}
	case ActionCreateSubAgent:
		if d.SubAgent == nil {
			return fmt.Errorf("create_sub_agent decision requires sub_agent intent")
		}
		if strings.TrimSpace(d.SubAgent.Agent) == "" || strings.TrimSpace(d.SubAgent.Input) == "" {
			return fmt.Errorf("create_sub_agent decision requires agent and input")
		}
	case ActionComplete, ActionFail:
		return nil
	default:
		return fmt.Errorf("unsupported decision action %q", d.Action)
	}
	return nil
}
