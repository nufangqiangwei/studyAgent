package agent

import (
	"agent/internal/content"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"agent/internal/llm"
	"agent/internal/prompt"
	"agent/internal/session"
	"agent/internal/tools"
)

func TestNativeLoopExecutesToolCallAndContinues(t *testing.T) {
	model := &scriptedLLM{
		responses: []llm.Response{
			{
				Provider: "mock",
				Model:    "mock-native",
				Content:  "need target",
				ToolCalls: []llm.ToolCall{
					{Name: tools.AskUserToolName, Input: json.RawMessage(`{"question":"Which target?"}`)},
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
	toolRegistry := tools.NewRegistry()
	if err := toolRegistry.Register(tools.NewAskUserTool()); err != nil {
		t.Fatalf("register ask_user: %v", err)
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
		Tools:         toolRegistry,
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
	if len(model.requests[0].Tools) != 1 || model.requests[0].Tools[0].Name != tools.AskUserToolName {
		t.Fatalf("first request tools = %#v, want ask_user", model.requests[0].Tools)
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
	if toolMsg.Role != llm.RoleTool || toolMsg.Name != tools.AskUserToolName || toolMsg.Content != "web app" {
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
	if policyPayload.Request.ToolName != tools.AskUserToolName || policyPayload.Result.Decision != "allow" {
		t.Fatalf("policy payload = %#v", policyPayload)
	}
}

func TestNativeLoopRequiresToolRegistryForToolCalls(t *testing.T) {
	model := &scriptedLLM{
		responses: []llm.Response{
			{
				Provider:  "mock",
				Model:     "mock-native",
				ToolCalls: []llm.ToolCall{{Name: tools.AskUserToolName, Input: json.RawMessage(`{"question":"Which target?"}`)}},
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
		responses: []llm.Response{
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
	if messages[0].Role != llm.RoleSystem {
		t.Fatalf("first second-request message role = %q, want system", messages[0].Role)
	}
	if messages[1].Role != llm.RoleUser || !strings.Contains(messages[1].Content, "first question") {
		t.Fatalf("second request missing first user message: %#v", messages)
	}
	if messages[2].Role != llm.RoleAssistant || messages[2].Content != "first answer" {
		t.Fatalf("second request missing first assistant response: %#v", messages)
	}
	if messages[3].Role != llm.RoleUser || !strings.Contains(messages[3].Content, "second question") {
		t.Fatalf("second request missing current user message: %#v", messages)
	}
}

func TestNativeLoopSavesSessionTurns(t *testing.T) {
	model := &scriptedLLM{
		responses: []llm.Response{
			{
				Provider: "mock",
				Model:    "mock-native",
				Content:  "saved answer",
				Usage: &llm.Usage{
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
	if messages[0].Role != llm.RoleSystem {
		t.Fatalf("first message = %#v, want system message", messages[0])
	}
	if messages[1].Role != llm.RoleUser {
		t.Fatalf("second message = %#v, want user message", messages[1])
	}
	if messages[2].Role != llm.RoleAssistant || messages[2].Content != "saved answer" {
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

func TestNativeLoopCompressesContextBetweenToolSteps(t *testing.T) {
	model := &scriptedLLM{
		responses: []llm.Response{
			{
				Provider: "mock",
				Model:    "mock-native",
				Content:  "need target",
				ToolCalls: []llm.ToolCall{
					{Name: tools.AskUserToolName, Input: json.RawMessage(`{"question":"Which target?"}`)},
				},
				Usage: &llm.Usage{
					InputTokens:  20_000,
					OutputTokens: 50,
					TotalTokens:  20_050,
				},
			},
			{
				Provider: "mock",
				Model:    "mock-native",
				Content:  "compressed handoff",
				Usage: &llm.Usage{
					InputTokens:  100,
					OutputTokens: 10,
					TotalTokens:  110,
				},
			},
			{
				Provider: "mock",
				Model:    "mock-native",
				Content:  "final answer",
			},
		},
	}
	store, err := session.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore returned error: %v", err)
	}
	toolRegistry := tools.NewRegistry()
	if err := toolRegistry.Register(tools.NewAskUserTool()); err != nil {
		t.Fatalf("register ask_user: %v", err)
	}
	ctx := content.WithEnv(context.Background(), &content.Env{
		IO: content.IO{
			In:  strings.NewReader("web app\n"),
			Out: &strings.Builder{},
		},
	})

	loop, err := NewNativeLoop(Options{
		LLM:           model,
		PromptBuilder: prompt.NewNativeBuilder(prompt.Options{}),
		Tools:         toolRegistry,
		MaxSteps:      3,
		Session:       store,
	})
	if err != nil {
		t.Fatalf("NewNativeLoop returned error: %v", err)
	}

	result, err := loop.Run(ctx, Task{
		Input:     "build a feature",
		WorkDir:   "C:\\Code\\GO\\agent",
		AgentName: DefaultAgentName,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if result.Content != "final answer" {
		t.Fatalf("Content = %q, want final answer", result.Content)
	}
	if len(model.requests) != 3 {
		t.Fatalf("requests = %d, want business/compression/business", len(model.requests))
	}
	if model.requests[1].Metadata["purpose"] != "context_compression" {
		t.Fatalf("second request metadata = %#v, want compression request", model.requests[1].Metadata)
	}
	secondBusinessMessages := model.requests[2].Messages
	if len(secondBusinessMessages) != 2 {
		t.Fatalf("second business messages = %d, want compressed system+summary: %#v", len(secondBusinessMessages), secondBusinessMessages)
	}
	if secondBusinessMessages[0].Role != llm.RoleSystem {
		t.Fatalf("first compressed message = %#v, want system", secondBusinessMessages[0])
	}
	if secondBusinessMessages[1].Role != llm.RoleUser || !strings.Contains(secondBusinessMessages[1].Content, "compressed handoff") {
		t.Fatalf("second compressed message = %#v, want summary user message", secondBusinessMessages[1])
	}
	for _, msg := range secondBusinessMessages {
		if msg.Role == llm.RoleTool || strings.Contains(msg.Content, "Which target?") {
			t.Fatalf("compressed request leaked raw tool context: %#v", secondBusinessMessages)
		}
	}

	records, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	snapshot := firstContextSnapshot(records)
	if snapshot == nil || snapshot.ContextSnapshot == nil {
		t.Fatalf("missing context snapshot: %#v", records)
	}
	if snapshot.ContextSnapshot.TriggerTokens != 20_000 || snapshot.ContextSnapshot.ContextWindowTokens != defaultContextWindowTokens {
		t.Fatalf("snapshot token metadata = %#v", snapshot.ContextSnapshot)
	}
	if events := countEventTypes(records); events[session.EventTypeContextCompression] != 1 {
		t.Fatalf("context compression events = %#v, want one", events)
	}
	usageSummary := usageSummaryRecord(records)
	if usageSummary == nil || usageSummary.UsageSummary == nil || usageSummary.UsageSummary.TotalTokens != 20_160 || usageSummary.LLMCalls != 3 {
		t.Fatalf("usage summary = %#v, want business plus compression usage", usageSummary)
	}
}

func TestNativeLoopCompressesAtRunEndAndRestoresSnapshot(t *testing.T) {
	store, err := session.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore returned error: %v", err)
	}
	firstModel := &scriptedLLM{
		responses: []llm.Response{
			{
				Provider: "mock",
				Model:    "mock-native",
				Content:  "first final answer",
				Usage: &llm.Usage{
					InputTokens:  20_000,
					OutputTokens: 20,
					TotalTokens:  20_020,
				},
			},
			{
				Provider: "mock",
				Model:    "mock-native",
				Content:  "final summary",
				Usage: &llm.Usage{
					InputTokens:  90,
					OutputTokens: 10,
					TotalTokens:  100,
				},
			},
		},
	}
	firstLoop, err := NewNativeLoop(Options{
		LLM:           firstModel,
		PromptBuilder: prompt.NewNativeBuilder(prompt.Options{}),
		Session:       store,
	})
	if err != nil {
		t.Fatalf("NewNativeLoop returned error: %v", err)
	}
	if _, err := firstLoop.Run(context.Background(), Task{Input: "first task", AgentName: DefaultAgentName}); err != nil {
		t.Fatalf("first Run returned error: %v", err)
	}

	records, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if snapshot := firstContextSnapshot(records); snapshot == nil || snapshot.ContextSnapshot == nil {
		t.Fatalf("missing context snapshot after first run: %#v", records)
	}

	secondModel := &scriptedLLM{
		responses: []llm.Response{{Provider: "mock", Model: "mock-native", Content: "second answer"}},
	}
	secondLoop, err := NewNativeLoop(Options{
		LLM:           secondModel,
		PromptBuilder: prompt.NewNativeBuilder(prompt.Options{}),
		Session:       store,
	})
	if err != nil {
		t.Fatalf("NewNativeLoop returned error: %v", err)
	}
	if _, err := secondLoop.Run(context.Background(), Task{Input: "second task", AgentName: DefaultAgentName}); err != nil {
		t.Fatalf("second Run returned error: %v", err)
	}
	if len(secondModel.requests) != 1 {
		t.Fatalf("second model requests = %d, want 1", len(secondModel.requests))
	}
	messages := secondModel.requests[0].Messages
	if len(messages) != 3 {
		t.Fatalf("restored messages = %d, want snapshot plus current user: %#v", len(messages), messages)
	}
	if !strings.Contains(messages[1].Content, "final summary") {
		t.Fatalf("restored messages missing summary: %#v", messages)
	}
	if strings.Contains(messages[1].Content, "first task") {
		t.Fatalf("restored summary should not include raw old prompt unless summary chose it: %#v", messages[1])
	}
	if messages[2].Role != llm.RoleUser || !strings.Contains(messages[2].Content, "second task") {
		t.Fatalf("restored messages missing current task: %#v", messages)
	}
}

func TestNativeLoopContinuesWhenCompressionFails(t *testing.T) {
	model := &scriptedLLM{
		responses: []llm.Response{
			{
				Provider: "mock",
				Model:    "mock-native",
				Content:  "answer survives compression failure",
				Usage: &llm.Usage{
					InputTokens:  20_000,
					OutputTokens: 20,
					TotalTokens:  20_020,
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
		Compressor:    failingCompressor{err: errors.New("summary unavailable")},
		Session:       store,
	})
	if err != nil {
		t.Fatalf("NewNativeLoop returned error: %v", err)
	}

	result, err := loop.Run(context.Background(), Task{Input: "keep going", AgentName: DefaultAgentName})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if result.Content != "answer survives compression failure" {
		t.Fatalf("Content = %q, want original answer", result.Content)
	}
	if len(loop.history) != 3 {
		t.Fatalf("history = %d messages, want uncompressed system/user/assistant: %#v", len(loop.history), loop.history)
	}
	if strings.Contains(loop.history[1].Content, "Conversation summary") {
		t.Fatalf("history was compressed despite compressor failure: %#v", loop.history)
	}

	records, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if snapshot := firstContextSnapshot(records); snapshot != nil {
		t.Fatalf("unexpected snapshot after compression failure: %#v", snapshot)
	}
	event := firstEvent(records, session.EventTypeContextCompression)
	if event == nil {
		t.Fatalf("missing compression failure event: %#v", records)
	}
	var payload struct {
		Status string `json:"status"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		t.Fatalf("parse compression payload: %v", err)
	}
	if payload.Status != "failed" || payload.Reason != "compressor_error" {
		t.Fatalf("compression payload = %#v, want failed compressor_error", payload)
	}
}

func TestNativeLoopResumeReplaysLLMAfterSavedRequest(t *testing.T) {
	store, checkpoint := createInterruptedTurn(t, nil)
	if err := session.SaveEvent(context.Background(), store, session.EventScope{
		TurnID:    checkpoint.TurnID,
		Task:      checkpoint.Task,
		AgentName: checkpoint.AgentName,
		Step:      1,
	}, session.EventTypeLLMRequest, llmRequestEventPayload{Request: llm.Request{Model: "mock-native"}}); err != nil {
		t.Fatalf("SaveEvent request returned error: %v", err)
	}
	checkpoint = loadInterruptedCheckpoint(t, store)

	model := &scriptedLLM{responses: []llm.Response{{Provider: "mock", Model: "mock-native", Content: "resumed final"}}}
	var out strings.Builder
	loop := newResumeLoop(t, model, store, nil, &out)

	result, err := loop.Resume(context.Background(), checkpoint)
	if err != nil {
		t.Fatalf("Resume returned error: %v", err)
	}
	if result.Content != "resumed final" || len(model.requests) != 1 {
		t.Fatalf("result/model requests = %#v/%d", result, len(model.requests))
	}
	if !strings.Contains(out.String(), "resumed final") {
		t.Fatalf("output missing resumed final:\n%s", out.String())
	}
}

func TestNativeLoopResumeMaterializesLLMResponseAndFinalizes(t *testing.T) {
	store, checkpoint := createInterruptedTurn(t, nil)
	if err := session.SaveEvent(context.Background(), store, session.EventScope{
		TurnID:    checkpoint.TurnID,
		Task:      checkpoint.Task,
		AgentName: checkpoint.AgentName,
		Step:      1,
	}, session.EventTypeLLMRequest, llmRequestEventPayload{Request: llm.Request{Model: "mock-native"}}); err != nil {
		t.Fatalf("SaveEvent request returned error: %v", err)
	}
	if err := session.SaveEvent(context.Background(), store, session.EventScope{
		TurnID:    checkpoint.TurnID,
		Task:      checkpoint.Task,
		AgentName: checkpoint.AgentName,
		Step:      1,
	}, session.EventTypeLLMResponse, llmResponseEventPayload{
		Response: llm.Response{Provider: "mock", Model: "mock-native", Content: "saved final", Usage: &llm.Usage{TotalTokens: 9}},
	}); err != nil {
		t.Fatalf("SaveEvent response returned error: %v", err)
	}
	checkpoint = loadInterruptedCheckpoint(t, store)

	model := &scriptedLLM{}
	var out strings.Builder
	loop := newResumeLoop(t, model, store, nil, &out)

	result, err := loop.Resume(context.Background(), checkpoint)
	if err != nil {
		t.Fatalf("Resume returned error: %v", err)
	}
	if result.Content != "saved final" || len(model.requests) != 0 {
		t.Fatalf("result/model requests = %#v/%d", result, len(model.requests))
	}
	records, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	messages := messagesFromRecords(records)
	if messages[len(messages)-1].Role != llm.RoleAssistant || messages[len(messages)-1].Content != "saved final" {
		t.Fatalf("records did not materialize assistant message: %#v", messages)
	}
	if usageSummaryRecord(records) == nil {
		t.Fatalf("missing usage summary after resume: %#v", records)
	}
}

func TestNativeLoopResumeReexecutesMissingToolResult(t *testing.T) {
	call := llm.ToolCall{ID: "call_1", Name: tools.AskUserToolName, Input: json.RawMessage(`{"question":"Which target?"}`)}
	assistantMsg := llm.Message{Role: llm.RoleAssistant, Content: "need target", ToolCalls: []llm.ToolCall{call}}
	store, checkpoint := createInterruptedTurn(t, []session.Record{
		{Kind: session.RecordKindEvent, StepIndex: 1, Event: &session.Event{Type: session.EventTypeLLMRequest, Payload: mustJSON(t, llmRequestEventPayload{Request: llm.Request{Model: "mock-native"}})}},
		{Kind: session.RecordKindEvent, StepIndex: 1, Event: &session.Event{Type: session.EventTypeLLMResponse, Payload: mustJSON(t, llmResponseEventPayload{Response: llm.Response{Provider: "mock", Model: "mock-native", Content: "need target", ToolCalls: []llm.ToolCall{call}}})}},
		{Kind: session.RecordKindMessage, StepIndex: 1, Message: &assistantMsg},
		{Kind: session.RecordKindEvent, StepIndex: 1, Event: &session.Event{Type: session.EventTypeToolCall, Payload: mustJSON(t, toolCallEventPayload{ID: call.ID, Name: call.Name, Input: call.Input})}},
	})
	toolRegistry := tools.NewRegistry()
	if err := toolRegistry.Register(tools.NewAskUserTool()); err != nil {
		t.Fatalf("register ask_user: %v", err)
	}
	model := &scriptedLLM{responses: []llm.Response{{Provider: "mock", Model: "mock-native", Content: "final after tool"}}}
	var out strings.Builder
	loop := newResumeLoop(t, model, store, toolRegistry, &out)
	ctx := content.WithEnv(context.Background(), &content.Env{IO: content.IO{In: strings.NewReader("web app\n"), Out: &out}})

	result, err := loop.Resume(ctx, checkpoint)
	if err != nil {
		t.Fatalf("Resume returned error: %v", err)
	}
	if result.Content != "final after tool" || len(model.requests) != 1 {
		t.Fatalf("result/model requests = %#v/%d", result, len(model.requests))
	}
	if got := countEventTypes(mustLoadRecords(t, store))[session.EventTypeToolResult]; got != 1 {
		t.Fatalf("tool result events = %d, want 1", got)
	}
	if !strings.Contains(model.requests[0].Messages[len(model.requests[0].Messages)-1].Content, "web app") {
		t.Fatalf("resume request missing reexecuted tool result: %#v", model.requests[0].Messages)
	}
}

func TestNativeLoopResumeContinuesAfterMaterializingToolResultEvent(t *testing.T) {
	call := llm.ToolCall{ID: "call_1", Name: tools.AskUserToolName, Input: json.RawMessage(`{"question":"Which target?"}`)}
	assistantMsg := llm.Message{Role: llm.RoleAssistant, Content: "need target", ToolCalls: []llm.ToolCall{call}}
	store, checkpoint := createInterruptedTurn(t, []session.Record{
		{Kind: session.RecordKindEvent, StepIndex: 1, Event: &session.Event{Type: session.EventTypeLLMRequest, Payload: mustJSON(t, llmRequestEventPayload{Request: llm.Request{Model: "mock-native"}})}},
		{Kind: session.RecordKindEvent, StepIndex: 1, Event: &session.Event{Type: session.EventTypeLLMResponse, Payload: mustJSON(t, llmResponseEventPayload{Response: llm.Response{Provider: "mock", Model: "mock-native", Content: "need target", ToolCalls: []llm.ToolCall{call}}})}},
		{Kind: session.RecordKindMessage, StepIndex: 1, Message: &assistantMsg},
		{Kind: session.RecordKindEvent, StepIndex: 1, Event: &session.Event{Type: session.EventTypeToolCall, Payload: mustJSON(t, toolCallEventPayload{ID: call.ID, Name: call.Name, Input: call.Input})}},
		{Kind: session.RecordKindEvent, StepIndex: 1, Event: &session.Event{Type: session.EventTypeToolResult, Payload: mustJSON(t, toolResultEventPayload{ID: call.ID, Name: call.Name, Content: "saved tool result"})}},
	})
	model := &scriptedLLM{responses: []llm.Response{{Provider: "mock", Model: "mock-native", Content: "final after saved tool"}}}
	var out strings.Builder
	loop := newResumeLoop(t, model, store, nil, &out)

	result, err := loop.Resume(context.Background(), checkpoint)
	if err != nil {
		t.Fatalf("Resume returned error: %v", err)
	}
	if result.Content != "final after saved tool" || len(model.requests) != 1 {
		t.Fatalf("result/model requests = %#v/%d", result, len(model.requests))
	}
	last := model.requests[0].Messages[len(model.requests[0].Messages)-1]
	if last.Role != llm.RoleTool || last.Content != "saved tool result" {
		t.Fatalf("resume request missing materialized tool message: %#v", model.requests[0].Messages)
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

func firstContextSnapshot(records []session.Record) *session.Record {
	for i := range records {
		if records[i].Kind == session.RecordKindContextSnapshot {
			return &records[i]
		}
	}
	return nil
}

func messagesFromRecords(records []session.Record) []llm.Message {
	messages := make([]llm.Message, 0, len(records))
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

func createInterruptedTurn(t *testing.T, extra []session.Record) (*session.FileStore, session.ResumeCheckpoint) {
	t.Helper()
	store, err := session.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore returned error: %v", err)
	}
	base := []session.Record{
		{
			Kind:      session.RecordKindMessage,
			TurnID:    "turn-1",
			Task:      "resume task",
			WorkDir:   "C:\\Code\\GO\\agent",
			AgentName: DefaultAgentName,
			Message:   &llm.Message{Role: llm.RoleSystem, Content: "system prompt"},
		},
		{
			Kind:      session.RecordKindMessage,
			TurnID:    "turn-1",
			Task:      "resume task",
			WorkDir:   "C:\\Code\\GO\\agent",
			AgentName: DefaultAgentName,
			Message:   &llm.Message{Role: llm.RoleUser, Content: "Task:\nresume task"},
		},
	}
	for _, record := range append(base, extra...) {
		if record.TurnID == "" {
			record.TurnID = "turn-1"
		}
		if record.Task == "" {
			record.Task = "resume task"
		}
		if record.WorkDir == "" {
			record.WorkDir = "C:\\Code\\GO\\agent"
		}
		if record.AgentName == "" {
			record.AgentName = DefaultAgentName
		}
		if err := store.Save(context.Background(), record); err != nil {
			t.Fatalf("Save returned error: %v", err)
		}
	}
	return store, loadInterruptedCheckpoint(t, store)
}

func loadInterruptedCheckpoint(t *testing.T, store *session.FileStore) session.ResumeCheckpoint {
	t.Helper()
	records := mustLoadRecords(t, store)
	checkpoint, err := session.FindInterruptedTurn(records)
	if err != nil {
		t.Fatalf("FindInterruptedTurn returned error: %v", err)
	}
	return checkpoint
}

func mustLoadRecords(t *testing.T, store *session.FileStore) []session.Record {
	t.Helper()
	records, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	return records
}

func newResumeLoop(t *testing.T, model *scriptedLLM, store *session.FileStore, registry *tools.Registry, out *strings.Builder) *NativeLoop {
	t.Helper()
	loop, err := NewNativeLoop(Options{
		LLM:           model,
		PromptBuilder: prompt.NewNativeBuilder(prompt.Options{}),
		Tools:         registry,
		MaxSteps:      3,
		Session:       store,
		Out:           out,
	})
	if err != nil {
		t.Fatalf("NewNativeLoop returned error: %v", err)
	}
	return loop
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	return data
}

type scriptedLLM struct {
	requests  []llm.Request
	responses []llm.Response
}

func (c *scriptedLLM) Complete(_ context.Context, req llm.Request) (llm.Response, error) {
	c.requests = append(c.requests, req)
	if len(c.responses) == 0 {
		return llm.Response{}, nil
	}
	response := c.responses[0]
	c.responses = c.responses[1:]
	return response, nil
}

type failingCompressor struct {
	err error
}

func (c failingCompressor) Compress(context.Context, CompressionInput) (CompressionResult, error) {
	return CompressionResult{}, c.err
}
