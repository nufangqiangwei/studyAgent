package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimeevent "agent/internal/event"
	"agent/internal/foundation/llmClient"
	"agent/internal/foundation/policy"
	"agent/internal/llm"
	"agent/internal/state"
)

const runExtensionKey = "agent.runner"

type RunID string

type Task struct {
	RunID    string
	Input    string
	WorkDir  string
	Agent    llm.AgentProfile
	Messages []llmClient.Message
	MaxSteps int
}

type RunResult struct {
	RunID       string
	Status      state.RunPhase
	FinalAnswer string
	StepsUsed   int
	WorkDir     string
	State       state.RunState
	Events      []runtimeevent.Event
	Error       *state.ErrorState
}

type AdvanceStatus string

const (
	AdvanceStatusEventEnqueued    AdvanceStatus = "event_enqueued"
	AdvanceStatusEventProcessed   AdvanceStatus = "event_processed"
	AdvanceStatusEffectDispatched AdvanceStatus = "effect_dispatched"
	AdvanceStatusWaitingForEffect AdvanceStatus = "waiting_for_effect"
	AdvanceStatusSuspended        AdvanceStatus = "suspended"
	AdvanceStatusTerminal         AdvanceStatus = "terminal"
)

type LoopAdvanceResult struct {
	RunID  string               `json:"run_id"`
	Status AdvanceStatus        `json:"status"`
	State  state.RunState       `json:"state"`
	Event  *runtimeevent.Event  `json:"event,omitempty"`
	Effect *state.Effect        `json:"effect,omitempty"`
	Events []runtimeevent.Event `json:"events,omitempty"`
}

type RecoverResult struct {
	Runs []RecoverableRun `json:"runs"`
}

type WorkResult struct {
	Ran     bool              `json:"ran"`
	Advance LoopAdvanceResult `json:"advance,omitempty"`
}

type PendingWorkKind string

const (
	PendingWorkEvent  PendingWorkKind = "event"
	PendingWorkEffect PendingWorkKind = "effect"
)

type PendingWork struct {
	RunID string          `json:"run_id"`
	Kind  PendingWorkKind `json:"kind"`
}

type RecoverableRun struct {
	RunID          string         `json:"run_id"`
	State          state.RunState `json:"state"`
	PendingEvents  int            `json:"pending_events"`
	PendingEffects int            `json:"pending_effects"`
}

type UserInteraction interface {
	ReceiveInput(ctx context.Context, request UserInputRequestedPayload) (UserInputReceivedPayload, error)
	ReceiveApproval(ctx context.Context, request UserApprovalRequiredPayload) (UserApprovalReceivedPayload, error)
}

type toolCallStatus string

const (
	toolCallPending         toolCallStatus = "pending"
	toolCallRequested       toolCallStatus = "requested"
	toolCallDispatched      toolCallStatus = "dispatched"
	toolCallWaitingApproval toolCallStatus = "waiting_approval"
	toolCallWaitingInput    toolCallStatus = "waiting_input"
	toolCallCompleted       toolCallStatus = "completed"
	toolCallFailed          toolCallStatus = "failed"
)

type pendingToolCall struct {
	ToolCallID string          `json:"tool_call_id"`
	ToolName   string          `json:"tool_name"`
	Arguments  json.RawMessage `json:"arguments,omitempty"`
	Status     toolCallStatus  `json:"status"`
	Error      string          `json:"error,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

type runData struct {
	Task         string              `json:"task,omitempty"`
	WorkDir      string              `json:"work_dir,omitempty"`
	Agent        llm.AgentProfile    `json:"agent"`
	Messages     []llmClient.Message `json:"messages,omitempty"`
	PendingTools []pendingToolCall   `json:"pending_tools,omitempty"`
	FinalAnswer  string              `json:"final_answer,omitempty"`
	Usage        llmClient.Usage     `json:"usage,omitempty"`
}

type ModelResponseReceivedPayload struct {
	Response         llmClient.Response   `json:"response"`
	AssistantMessage *llmClient.Message   `json:"assistant_message,omitempty"`
	ToolCalls        []llmClient.ToolCall `json:"tool_calls,omitempty"`
	Usage            *llmClient.Usage     `json:"usage,omitempty"`
	StartedAt        time.Time            `json:"started_at"`
	CompletedAt      time.Time            `json:"completed_at"`
}

type ModelResponseFailedPayload struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type ModelRequestCreatedPayload struct {
	Step    int               `json:"step"`
	Request llmClient.Request `json:"request"`
}

type DispatchToolPayload struct {
	ToolCall llmClient.ToolCall `json:"tool_call"`
	Approved bool               `json:"approved,omitempty"`
}

type ToolCallEventPayload struct {
	ToolCallID string          `json:"tool_call_id"`
	ToolName   string          `json:"tool_name"`
	Arguments  json.RawMessage `json:"arguments,omitempty"`
	Result     llm.ToolResult  `json:"result,omitempty"`
	Error      string          `json:"error,omitempty"`
}

type EffectLifecyclePayload struct {
	EffectID   string `json:"effect_id"`
	EffectType string `json:"effect_type"`
	Status     string `json:"status"`
	Error      string `json:"error,omitempty"`
}

type UserInputRequestedPayload struct {
	ToolCallID string          `json:"tool_call_id"`
	ToolName   string          `json:"tool_name"`
	Arguments  json.RawMessage `json:"arguments,omitempty"`
	Question   string          `json:"question"`
	Default    string          `json:"default,omitempty"`
}

type UserInputReceivedPayload struct {
	ToolCallID  string `json:"tool_call_id"`
	ToolName    string `json:"tool_name,omitempty"`
	Answer      string `json:"answer"`
	UsedDefault bool   `json:"used_default,omitempty"`
}

type UserApprovalRequiredPayload struct {
	ToolCallID string          `json:"tool_call_id"`
	ToolName   string          `json:"tool_name"`
	Arguments  json.RawMessage `json:"arguments,omitempty"`
	Request    policy.Request  `json:"request"`
	Decision   policy.Result   `json:"decision"`
}

type UserApprovalReceivedPayload struct {
	ToolCallID string `json:"tool_call_id"`
	ToolName   string `json:"tool_name,omitempty"`
	Approved   bool   `json:"approved"`
	Reason     string `json:"reason,omitempty"`
}

type CompleteRunPayload struct {
	FinalAnswer string `json:"final_answer,omitempty"`
	StepsUsed   int    `json:"steps_used"`
}

func newInitialState(task Task, defaultMaxSteps int) (state.RunState, error) {
	task, err := normalizeTask(task, defaultMaxSteps)
	if err != nil {
		return state.RunState{}, err
	}

	runState := state.NewRunState(task.RunID, task.MaxSteps)
	data := runData{
		Task:     strings.TrimSpace(task.Input),
		WorkDir:  strings.TrimSpace(task.WorkDir),
		Agent:    cloneAgentProfile(task.Agent),
		Messages: cloneMessages(task.Messages),
	}
	if err := storeRunData(&runState, data); err != nil {
		return state.RunState{}, err
	}
	return runState, nil
}

func normalizeTask(task Task, defaultMaxSteps int) (Task, error) {
	if strings.TrimSpace(task.RunID) == "" {
		task.RunID = state.NewID("run")
	}
	if task.MaxSteps <= 0 {
		task.MaxSteps = defaultMaxSteps
	}
	if task.MaxSteps <= 0 {
		task.MaxSteps = 20
	}
	if strings.TrimSpace(task.Agent.Model) == "" {
		return Task{}, fmt.Errorf("runner: agent model is required")
	}
	if len(task.Messages) == 0 {
		input := strings.TrimSpace(task.Input)
		if input == "" {
			return Task{}, fmt.Errorf("runner: task input or messages are required")
		}
		task.Messages = []llmClient.Message{{Role: llmClient.RoleUser, Content: input}}
	}
	return task, nil
}

func loadRunData(runState state.RunState) (runData, error) {
	if runState.Extensions == nil {
		return runData{}, fmt.Errorf("runner: run data extension is missing")
	}
	raw := runState.Extensions[runExtensionKey]
	if len(raw) == 0 {
		return runData{}, fmt.Errorf("runner: run data extension is missing")
	}
	var data runData
	if err := json.Unmarshal(raw, &data); err != nil {
		return runData{}, fmt.Errorf("runner: decode run data: %w", err)
	}
	data.Agent = cloneAgentProfile(data.Agent)
	data.Messages = cloneMessages(data.Messages)
	data.PendingTools = clonePendingTools(data.PendingTools)
	return data, nil
}

func storeRunData(runState *state.RunState, data runData) error {
	if runState == nil {
		return fmt.Errorf("runner: run state is nil")
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("runner: encode run data: %w", err)
	}
	if runState.Extensions == nil {
		runState.Extensions = make(map[string]json.RawMessage)
	}
	runState.Extensions[runExtensionKey] = raw
	return nil
}

func cloneAgentProfile(profile llm.AgentProfile) llm.AgentProfile {
	cloned := profile
	if len(profile.Tools) > 0 {
		cloned.Tools = make([]llmClient.ToolDefinition, 0, len(profile.Tools))
		for _, tool := range profile.Tools {
			cloned.Tools = append(cloned.Tools, llmClient.ToolDefinition{
				Name:        tool.Name,
				Description: tool.Description,
				InputSchema: append(json.RawMessage(nil), tool.InputSchema...),
			})
		}
	}
	if len(profile.Metadata) > 0 {
		cloned.Metadata = make(map[string]string, len(profile.Metadata))
		for key, value := range profile.Metadata {
			cloned.Metadata[key] = value
		}
	}
	return cloned
}

func cloneMessages(messages []llmClient.Message) []llmClient.Message {
	if len(messages) == 0 {
		return nil
	}
	cloned := make([]llmClient.Message, 0, len(messages))
	for _, message := range messages {
		cloned = append(cloned, cloneMessage(message))
	}
	return cloned
}

func cloneMessage(message llmClient.Message) llmClient.Message {
	cloned := message
	if len(message.ToolCalls) > 0 {
		cloned.ToolCalls = make([]llmClient.ToolCall, 0, len(message.ToolCalls))
		for _, call := range message.ToolCalls {
			cloned.ToolCalls = append(cloned.ToolCalls, cloneToolCall(call))
		}
	}
	if message.Usage != nil {
		usage := *message.Usage
		cloned.Usage = &usage
	}
	return cloned
}

func cloneToolCall(call llmClient.ToolCall) llmClient.ToolCall {
	return llmClient.ToolCall{
		ID:    call.ID,
		Name:  call.Name,
		Input: append(json.RawMessage(nil), call.Input...),
	}
}

func clonePendingTools(calls []pendingToolCall) []pendingToolCall {
	if len(calls) == 0 {
		return nil
	}
	cloned := make([]pendingToolCall, 0, len(calls))
	for _, call := range calls {
		call.Arguments = append(json.RawMessage(nil), call.Arguments...)
		cloned = append(cloned, call)
	}
	return cloned
}
