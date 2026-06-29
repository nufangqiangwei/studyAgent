package agent

import (
	"agent/internal/capability/builtin/askUser"
	tools2 "agent/internal/capability/tool"
	"agent/internal/content"
	"agent/internal/foundation/llmClient"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"agent/internal/prompt"
	"agent/internal/session"
)

func TestNativeLoopExecutesToolCallAndContinues(t *testing.T) {
	model := &scriptedLLM{
		responses: []llmClient.Response{
			{
				Provider: "mock",
				Model:    "mock-native",
				Content:  "need target",
				ToolCalls: []llmClient.ToolCall{
					{Name: askUser.Name, Input: json.RawMessage(`{"question":"Which target?"}`)},
				},
			},
			{
				Provider: "mock",
				Model:    "mock-native",
				Content:  "final answer",
			},
		},
	}

	var promptOut strings.Builder
	store, err := session.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore returned error: %v", err)
	}
	toolManage := tools2.NewManage()
	if err := tools2.AddTool(askUser.Name, toolManage); err != nil {
		t.Fatalf("add ask_user: %v", err)
	}
	ctx := content.WithEnv(context.Background(), &content.Env{
		IO: content.IO{
			In:  strings.NewReader("web app\n"),
			Out: &promptOut,
		},
	})

	loop, err := NewNativeLoop(Options{
		LLM:           model,
		PromptBuilder: prompt.NewNativeBuilder(prompt.Options{}),
		Tools:         toolManage,
		MaxSteps:      2,
		Session:       store,
	})
	if err != nil {
		t.Fatalf("NewNativeLoop returned error: %v", err)
	}

	result, err := loop.Run(ctx, Task{
		Input:   "build a feature",
		WorkDir: "C:\\Code\\GO\\agent",
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if result.Content != "final answer" {
		t.Fatalf("Content = %q, want final answer", result.Content)
	}
	if len(model.requests) != 2 {
		t.Fatalf("requests = %d, want 2", len(model.requests))
	}
	if len(model.requests[0].Tools) != 1 || model.requests[0].Tools[0].Name != askUser.Name {
		t.Fatalf("first request tool = %#v, want ask_user", model.requests[0].Tools)
	}
	secondMessages := model.requests[1].Messages
	if len(secondMessages) < 2 {
		t.Fatalf("second request messages = %#v", secondMessages)
	}
	assistantMsg := secondMessages[len(secondMessages)-2]
	toolMsg := secondMessages[len(secondMessages)-1]
	if len(assistantMsg.ToolCalls) != 1 {
		t.Fatalf("second request missing assistant tool call: %#v", secondMessages)
	}
	if toolMsg.Role != llmClient.RoleTool || toolMsg.Name != askUser.Name || toolMsg.Content != "web app" {
		t.Fatalf("second request tool result = %#v", toolMsg)
	}
	if toolMsg.ToolCallID == "" || toolMsg.ToolCallID != assistantMsg.ToolCalls[0].ID {
		t.Fatalf("tool result id %q does not match assistant call %#v", toolMsg.ToolCallID, assistantMsg.ToolCalls[0])
	}
	if !strings.Contains(promptOut.String(), "? Which target?") {
		t.Fatalf("ask_user output missing question:\n%s", promptOut.String())
	}
	if len(result.Steps) != 2 {
		t.Fatalf("steps = %d, want 2", len(result.Steps))
	}
	if got := result.Steps[0].ToolResults[0].Content; got != "web app" {
		t.Fatalf("tool result content = %q, want web app", got)
	}
	if result.Steps[0].ToolResults[0].StartedAt.IsZero() || result.Steps[0].ToolResults[0].CompletedAt.IsZero() {
		t.Fatalf("tool result timestamps were not recorded: %#v", result.Steps[0].ToolResults[0])
	}

	records, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	eventCounts := countEventTypes(records)
	wantEventCounts := map[string]int{
		session.EventTypeLLMRequest:     2,
		session.EventTypeLLMResponse:    2,
		session.EventTypeToolCall:       1,
		session.EventTypePolicyDecision: 1,
		session.EventTypeToolResult:     1,
		session.EventTypeSummary:        1,
	}
	for eventType, want := range wantEventCounts {
		if got := eventCounts[eventType]; got != want {
			t.Fatalf("event count %s = %d, want %d: %#v", eventType, got, want, records)
		}
	}
	policyEvent := firstEvent(records, session.EventTypePolicyDecision)
	if policyEvent == nil {
		t.Fatalf("missing policy decision event: %#v", records)
	}
	var policyPayload struct {
		Request struct {
			ToolName string `json:"tool_name"`
		} `json:"request"`
		Result struct {
			Decision string `json:"decision"`
		} `json:"result"`
	}
	if err := json.Unmarshal(policyEvent.Payload, &policyPayload); err != nil {
		t.Fatalf("parse policy payload: %v", err)
	}
	if policyPayload.Request.ToolName != askUser.Name || policyPayload.Result.Decision != "allow" {
		t.Fatalf("policy payload = %#v", policyPayload)
	}
}

func TestNativeLoopRequiresToolRegistryForToolCalls(t *testing.T) {
	model := &scriptedLLM{
		responses: []llmClient.Response{
			{
				Provider:  "mock",
				Model:     "mock-native",
				ToolCalls: []llmClient.ToolCall{{Name: askUser.Name, Input: json.RawMessage(`{"question":"Which target?"}`)}},
			},
		},
	}

	loop, err := NewNativeLoop(Options{
		LLM:           model,
		PromptBuilder: prompt.NewNativeBuilder(prompt.Options{}),
		MaxSteps:      2,
	})
	if err != nil {
		t.Fatalf("NewNativeLoop returned error: %v", err)
	}

	_, err = loop.Run(context.Background(), Task{Input: "build a feature"})
	if err == nil {
		t.Fatal("Run returned nil error")
	}
	if !strings.Contains(err.Error(), "tool registry is required") {
		t.Fatalf("error = %q, want tool registry is required", err.Error())
	}
}

func TestNativeLoopCarriesConversationAcrossRuns(t *testing.T) {
	model := &scriptedLLM{
		responses: []llmClient.Response{
			{
				Provider: "mock",
				Model:    "mock-native",
				Content:  "first answer",
			},
			{
				Provider: "mock",
				Model:    "mock-native",
				Content:  "second answer",
			},
		},
	}

	loop, err := NewNativeLoop(Options{
		LLM:           model,
		PromptBuilder: prompt.NewNativeBuilder(prompt.Options{}),
	})
	if err != nil {
		t.Fatalf("NewNativeLoop returned error: %v", err)
	}

	first, err := loop.Run(context.Background(), Task{Input: "first question"})
	if err != nil {
		t.Fatalf("first Run returned error: %v", err)
	}
	if first.Content != "first answer" {
		t.Fatalf("first Content = %q, want first answer", first.Content)
	}

	second, err := loop.Run(context.Background(), Task{Input: "second question"})
	if err != nil {
		t.Fatalf("second Run returned error: %v", err)
	}
	if second.Content != "second answer" {
		t.Fatalf("second Content = %q, want second answer", second.Content)
	}

	if len(model.requests) != 2 {
		t.Fatalf("requests = %d, want 2", len(model.requests))
	}
	messages := model.requests[1].Messages
	if len(messages) != 4 {
		t.Fatalf("second request messages = %d, want 4: %#v", len(messages), messages)
	}
	if messages[0].Role != llmClient.RoleSystem {
		t.Fatalf("first second-request message role = %q, want system", messages[0].Role)
	}
	if messages[1].Role != llmClient.RoleUser || !strings.Contains(messages[1].Content, "first question") {
		t.Fatalf("second request missing first user message: %#v", messages)
	}
	if messages[2].Role != llmClient.RoleAssistant || messages[2].Content != "first answer" {
		t.Fatalf("second request missing first assistant response: %#v", messages)
	}
	if messages[3].Role != llmClient.RoleUser || !strings.Contains(messages[3].Content, "second question") {
		t.Fatalf("second request missing current user message: %#v", messages)
	}
}

func TestNativeLoopSavesSessionTurns(t *testing.T) {
	model := &scriptedLLM{
		responses: []llmClient.Response{
			{
				Provider: "mock",
				Model:    "mock-native",
				Content:  "saved answer",
				Usage: &llmClient.Usage{
					InputTokens:  11,
					OutputTokens: 7,
					TotalTokens:  18,
				},
			},
		},
	}
	store, err := session.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore returned error: %v", err)
	}

	loop, err := NewNativeLoop(Options{
		LLM:           model,
		PromptBuilder: prompt.NewNativeBuilder(prompt.Options{}),
		Session:       store,
	})
	if err != nil {
		t.Fatalf("NewNativeLoop returned error: %v", err)
	}

	_, err = loop.Run(context.Background(), Task{
		Input:     "save this conversation",
		WorkDir:   "C:\\Code\\GO\\agent",
		AgentName: DefaultAgentName,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	records, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if len(records) < 7 {
		t.Fatalf("records = %d, want at least message/event/summary records: %#v", len(records), records)
	}
	if eventCounts := countEventTypes(records); eventCounts[session.EventTypeLLMRequest] != 1 ||
		eventCounts[session.EventTypeLLMResponse] != 1 ||
		eventCounts[session.EventTypeSummary] != 1 {
		t.Fatalf("event counts = %#v, want llm request/response/summary events", eventCounts)
	}
	messages := messagesFromRecords(records)
	if len(messages) != 3 {
		t.Fatalf("messages = %d, want system/user/assistant: %#v", len(messages), messages)
	}
	if messages[0].Role != llmClient.RoleSystem {
		t.Fatalf("first message = %#v, want system message", messages[0])
	}
	if messages[1].Role != llmClient.RoleUser {
		t.Fatalf("second message = %#v, want user message", messages[1])
	}
	if messages[2].Role != llmClient.RoleAssistant || messages[2].Content != "saved answer" {
		t.Fatalf("third message = %#v, want assistant message", messages[2])
	}
	if messages[2].Usage == nil || messages[2].Usage.TotalTokens != 18 {
		t.Fatalf("assistant usage = %#v, want total tokens", messages[2].Usage)
	}
	usageSummary := usageSummaryRecord(records)
	if usageSummary == nil || usageSummary.UsageSummary == nil || usageSummary.UsageSummary.TotalTokens != 18 || usageSummary.LLMCalls != 1 {
		t.Fatalf("usage summary record = %#v", usageSummary)
	}
	for _, record := range records {
		if record.SessionID != store.ID() || record.AgentID != store.AgentID() || record.TurnID == "" {
			t.Fatalf("record identifiers missing: %#v", record)
		}
		if record.AgentName != DefaultAgentName {
			t.Fatalf("record agent name = %q, want %q", record.AgentName, DefaultAgentName)
		}
		if record.Timestamp.IsZero() {
			t.Fatalf("record timestamp missing: %#v", record)
		}
	}
}

func TestNativeLoopHandleEventSuspendsForToolResultAndResumes(t *testing.T) {
	toolManage := tools2.NewManage()
	if err := tools2.AddTool(askUser.Name, toolManage); err != nil {
		t.Fatalf("add ask_user: %v", err)
	}
	loop, err := NewNativeLoop(Options{
		LLM:           &scriptedLLM{},
		PromptBuilder: prompt.NewNativeBuilder(prompt.Options{}),
		Tools:         toolManage,
		MaxSteps:      3,
	})
	if err != nil {
		t.Fatalf("NewNativeLoop returned error: %v", err)
	}

	started, err := loop.HandleEvent(context.Background(), NewRunStartedEvent(Task{Input: "build a feature"}))
	if err != nil {
		t.Fatalf("HandleEvent RunStarted returned error: %v", err)
	}
	if started.Status != RunStatusCallingModel || len(started.Actions) != 1 || started.Actions[0].Kind != LoopActionCallModel {
		t.Fatalf("started advance = %#v, want one model action", started)
	}

	toolInput := json.RawMessage(`{"question":"Which target?"}`)
	waiting, err := loop.HandleEvent(context.Background(), ModelResponseReceivedEvent{
		RunIDValue: started.RunID,
		Response: llmClient.Response{
			Provider: "mock",
			Model:    "mock-native",
			Content:  "need target",
			ToolCalls: []llmClient.ToolCall{{
				ID:    "call_external_1",
				Name:  askUser.Name,
				Input: toolInput,
			}},
		},
	})
	if err != nil {
		t.Fatalf("HandleEvent ModelResponseReceived returned error: %v", err)
	}
	if waiting.Status != RunStatusWaitingForToolResult || !waiting.Suspended {
		t.Fatalf("waiting advance status = %s suspended=%v, want WaitingForToolResult suspended", waiting.Status, waiting.Suspended)
	}
	if len(waiting.Actions) != 1 || waiting.Actions[0].Kind != LoopActionDispatchTool {
		t.Fatalf("waiting actions = %#v, want one dispatch tool action", waiting.Actions)
	}
	if len(waiting.State.PendingTools) != 1 || waiting.State.PendingTools[0].ToolCallID != "call_external_1" {
		t.Fatalf("pending tool = %#v, want call_external_1", waiting.State.PendingTools)
	}

	continued, err := loop.HandleEvent(context.Background(), ToolCallCompletedEvent{
		RunIDValue: started.RunID,
		ToolCallID: "call_external_1",
		ToolName:   askUser.Name,
		Result: ToolResult{
			Name:    askUser.Name,
			Content: "web app",
		},
	})
	if err != nil {
		t.Fatalf("HandleEvent ToolCallCompleted returned error: %v", err)
	}
	if continued.Status != RunStatusCallingModel || len(continued.Actions) != 1 || continued.Actions[0].Kind != LoopActionCallModel {
		t.Fatalf("continued advance = %#v, want next model action", continued)
	}
	messages := continued.Actions[0].ModelRequest.Messages
	if len(messages) < 2 {
		t.Fatalf("continued request messages = %#v", messages)
	}
	assistantMsg := messages[len(messages)-2]
	toolMsg := messages[len(messages)-1]
	if len(assistantMsg.ToolCalls) != 1 || assistantMsg.ToolCalls[0].ID != "call_external_1" {
		t.Fatalf("assistant tool call message = %#v", assistantMsg)
	}
	if toolMsg.Role != llmClient.RoleTool || toolMsg.ToolCallID != "call_external_1" || toolMsg.Content != "web app" {
		t.Fatalf("tool result message = %#v, want external result", toolMsg)
	}
}

func TestNativeLoopResumesWaitingToolStateFromSession(t *testing.T) {
	store, err := session.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore returned error: %v", err)
	}
	toolManage := tools2.NewManage()
	if err := tools2.AddTool(askUser.Name, toolManage); err != nil {
		t.Fatalf("add ask_user: %v", err)
	}
	loop, err := NewNativeLoop(Options{
		LLM:           &scriptedLLM{},
		PromptBuilder: prompt.NewNativeBuilder(prompt.Options{}),
		Tools:         toolManage,
		MaxSteps:      3,
		Session:       store,
	})
	if err != nil {
		t.Fatalf("NewNativeLoop returned error: %v", err)
	}

	started, err := loop.HandleEvent(context.Background(), NewRunStartedEvent(Task{
		Input:     "build a feature",
		AgentName: DefaultAgentName,
	}))
	if err != nil {
		t.Fatalf("HandleEvent RunStarted returned error: %v", err)
	}
	waiting, err := loop.HandleEvent(context.Background(), ModelResponseReceivedEvent{
		RunIDValue: started.RunID,
		Response: llmClient.Response{
			Provider: "mock",
			Model:    "mock-native",
			ToolCalls: []llmClient.ToolCall{{
				ID:    "call_persisted_1",
				Name:  askUser.Name,
				Input: json.RawMessage(`{"question":"Which target?"}`),
			}},
		},
	})
	if err != nil {
		t.Fatalf("HandleEvent ModelResponseReceived returned error: %v", err)
	}
	if waiting.Status != RunStatusWaitingForToolResult {
		t.Fatalf("waiting status = %s, want WaitingForToolResult", waiting.Status)
	}

	resumedLoop, err := NewNativeLoop(Options{
		LLM:           &scriptedLLM{},
		PromptBuilder: prompt.NewNativeBuilder(prompt.Options{}),
		Tools:         toolManage,
		MaxSteps:      3,
		Session:       store,
	})
	if err != nil {
		t.Fatalf("NewNativeLoop resumed returned error: %v", err)
	}
	resumed, err := resumedLoop.HandleEvent(context.Background(), RunResumedEvent{RunIDValue: started.RunID})
	if err != nil {
		t.Fatalf("HandleEvent RunResumed returned error: %v", err)
	}
	if resumed.Status != RunStatusWaitingForToolResult || !resumed.Suspended {
		t.Fatalf("resumed status = %s suspended=%v, want waiting suspended", resumed.Status, resumed.Suspended)
	}
	if len(resumed.Actions) != 1 || resumed.Actions[0].Kind != LoopActionDispatchTool {
		t.Fatalf("resumed actions = %#v, want dispatch tool action", resumed.Actions)
	}
	if resumed.Actions[0].ToolCall == nil || resumed.Actions[0].ToolCall.ToolCallID != "call_persisted_1" {
		t.Fatalf("resumed tool action = %#v, want persisted call", resumed.Actions[0])
	}

	records, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	foundRunState := false
	for _, record := range records {
		if record.Kind == session.RecordKindRunState && len(record.RunState) > 0 {
			foundRunState = true
			break
		}
	}
	if !foundRunState {
		t.Fatalf("session records did not include persisted run_state: %#v", records)
	}
}
func countEventTypes(records []session.Record) map[string]int {
	counts := make(map[string]int)
	for _, record := range records {
		if record.Kind == session.RecordKindEvent && record.Event != nil {
			counts[record.Event.Type]++
		}
	}
	return counts
}

func firstEvent(records []session.Record, eventType string) *session.Event {
	for _, record := range records {
		if record.Kind == session.RecordKindEvent && record.Event != nil && record.Event.Type == eventType {
			return record.Event
		}
	}
	return nil
}

func messagesFromRecords(records []session.Record) []llmClient.Message {
	messages := make([]llmClient.Message, 0, len(records))
	for _, record := range records {
		if record.Kind == session.RecordKindMessage && record.Message != nil {
			messages = append(messages, *record.Message)
		}
	}
	return messages
}

func usageSummaryRecord(records []session.Record) *session.Record {
	for i := range records {
		if records[i].Kind == session.RecordKindUsageSummary {
			return &records[i]
		}
	}
	return nil
}

type scriptedLLM struct {
	requests  []llmClient.Request
	responses []llmClient.Response
}

func (c *scriptedLLM) Complete(_ context.Context, req llmClient.Request) (llmClient.Response, error) {
	c.requests = append(c.requests, req)
	if len(c.responses) == 0 {
		return llmClient.Response{}, nil
	}
	response := c.responses[0]
	c.responses = c.responses[1:]
	return response, nil
}
