package tools

import (
	"agent/internal/runtime"
	agents2 "agent/internal/runtime/agents"
	"agent/internal/runtime/agents/builtinagents"
	reactor2 "agent/internal/runtime/reactor"
	statemachine2 "agent/internal/runtime/statemachine"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"agent/internal/foundation/policy"
)

func TestAdapterExposesInternalCapabilityToolSpecs(t *testing.T) {
	adapter := mustAdapter(t, WithToolNames("list_files", "read_file"))

	specs := adapter.Specs()
	if len(specs) != 2 {
		t.Fatalf("specs = %#v, want two tool specs", specs)
	}
	if specs[0].Name != "list_files" || specs[1].Name != "read_file" {
		t.Fatalf("specs = %#v, want sorted list_files/read_file", specs)
	}
	if len(specs[0].InputSchema) == 0 || !strings.Contains(specs[0].Description, "List files") {
		t.Fatalf("spec = %#v, want description and input schema", specs[0])
	}
}

func TestAdapterExecutesInternalToolAsToolCompletedEvent(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module adaptertest\n"), 0644); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}
	adapter := mustAdapter(t, WithToolNames("read_file"), WithWorkDir(root))

	result, err := adapter.ExecuteEffect(context.Background(), reactor2.TaskRuntime{TaskID: "task_1"}, mustToolEffect(t, "task_1", statemachine2.ToolCallPayload{
		ToolCallID: "call_1",
		ToolName:   "read_file",
		Arguments:  json.RawMessage(`{"path":"go.mod"}`),
	}))
	if err != nil {
		t.Fatalf("ExecuteEffect returned error: %v", err)
	}
	if len(result.Events) != 1 || result.Events[0].Type != statemachine2.EventToolCompleted {
		t.Fatalf("events = %#v, want tool.completed", result.Events)
	}

	var payload statemachine2.ToolCallPayload
	mustUnmarshal(t, result.Events[0].Payload, &payload)
	if payload.ToolCallID != "call_1" || payload.ToolName != "read_file" {
		t.Fatalf("payload = %#v, want call_1 read_file", payload)
	}
	var toolResult ResultPayload
	mustUnmarshal(t, payload.Result, &toolResult)
	if !strings.Contains(toolResult.Content, "module adaptertest") {
		t.Fatalf("tool result content = %q, want file content", toolResult.Content)
	}
}

func TestAdapterMapsToolFailureToToolFailedEvent(t *testing.T) {
	adapter := mustAdapter(t, WithToolNames("read_file"), WithWorkDir(t.TempDir()))

	result, err := adapter.ExecuteEffect(context.Background(), reactor2.TaskRuntime{TaskID: "task_1"}, mustToolEffect(t, "task_1", statemachine2.ToolCallPayload{
		ToolCallID: "call_1",
		ToolName:   "read_file",
		Arguments:  json.RawMessage(`{"path":"missing.txt"}`),
	}))
	if err != nil {
		t.Fatalf("ExecuteEffect returned error: %v", err)
	}
	if len(result.Events) != 1 || result.Events[0].Type != statemachine2.EventToolFailed {
		t.Fatalf("events = %#v, want tool.failed", result.Events)
	}
	var payload statemachine2.ToolCallPayload
	mustUnmarshal(t, result.Events[0].Payload, &payload)
	if payload.Error == "" || !strings.Contains(payload.Error, "missing.txt") {
		t.Fatalf("payload error = %q, want missing file error", payload.Error)
	}
}

func TestAdapterCanBeWiredIntoNewRuntime(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module wired\n"), 0644); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}
	adapter := mustAdapter(t, WithToolNames("read_file"), WithWorkDir(root), WithPolicy(policy.New(policy.ModeReadOnly)))
	model := &scriptedModel{responses: []agents2.ModelResponse{
		{Decision: &agents2.Decision{
			Action: agents2.ActionUseTool,
			Tool: &agents2.ToolIntent{
				ToolCallID: "call_1",
				ToolName:   "read_file",
				Arguments:  json.RawMessage(`{"path":"go.mod"}`),
			},
		}},
		{Decision: &agents2.Decision{
			Action:      agents2.ActionComplete,
			FinalAnswer: "read complete",
		}},
	}}
	modelExecutor, err := agents2.NewModelExecutor(model)
	if err != nil {
		t.Fatalf("NewModelExecutor returned error: %v", err)
	}
	rt, err := runtime.New(
		runtime.WithEffectExecutor(reactor2.EffectModelCall, modelExecutor),
		runtime.WithEffectExecutor(reactor2.EffectToolDispatch, adapter),
		runtime.WithResultDelivery("sync"),
		runtime.WithSyncEffects(),
	)
	if err != nil {
		t.Fatalf("runtime.New returned error: %v", err)
	}
	defer rt.Close()

	agent, err := builtinagents.NewAnalyzeAgent(
		builtinagents.WithSnapshotStore(rt.SnapshotStore()),
		builtinagents.WithTools(adapter.Specs()),
	)
	if err != nil {
		t.Fatalf("NewAnalyzeAgent returned error: %v", err)
	}
	if err := rt.RegisterAgent(context.Background(), "task_1", agent); err != nil {
		t.Fatalf("RegisterAgent returned error: %v", err)
	}
	if _, err := rt.StartTask(context.Background(), runtime.Task{TaskID: "task_1", Input: "read go.mod"}); err != nil {
		t.Fatalf("StartTask returned error: %v", err)
	}
	state, ok, err := rt.State(context.Background(), "task_1")
	if err != nil || !ok {
		t.Fatalf("State ok=%v err=%v, want state", ok, err)
	}
	if state.Phase != statemachine2.PhaseCompleted {
		t.Fatalf("state = %#v, want completed", state)
	}
	snapshot, ok, err := rt.AgentSnapshot(context.Background(), builtinagents.AnalyzeAgentName, "task_1")
	if err != nil || !ok {
		t.Fatalf("AgentSnapshot ok=%v err=%v, want snapshot", ok, err)
	}
	if snapshot.LastToolResult == nil || !strings.Contains(string(snapshot.LastToolResult.Result), "module wired") {
		t.Fatalf("snapshot = %#v, want read_file result in last tool result", snapshot)
	}
}

func mustAdapter(t *testing.T, options ...Option) *Adapter {
	t.Helper()
	adapter, err := NewDefault(options...)
	if err != nil {
		t.Fatalf("NewDefault returned error: %v", err)
	}
	return adapter
}

func mustToolEffect(t *testing.T, taskID string, payload statemachine2.ToolCallPayload) reactor2.Effect {
	t.Helper()
	effect, err := reactor2.NewEffect(taskID, reactor2.EffectToolDispatch, payload)
	if err != nil {
		t.Fatalf("NewEffect returned error: %v", err)
	}
	return effect
}

func mustUnmarshal(t *testing.T, raw json.RawMessage, target any) {
	t.Helper()
	if err := json.Unmarshal(raw, target); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}
}

type scriptedModel struct {
	responses []agents2.ModelResponse
}

func (m *scriptedModel) Complete(_ context.Context, _ agents2.ModelRequest) (agents2.ModelResponse, error) {
	if len(m.responses) == 0 {
		return agents2.ModelResponse{}, nil
	}
	response := m.responses[0]
	m.responses = m.responses[1:]
	return response, nil
}
