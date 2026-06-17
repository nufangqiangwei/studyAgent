package agent

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"agent/internal/llm"
	"agent/internal/prompt"
	"agent/internal/session"
	"agent/internal/tools"
)

func TestNativeContextBuilderBuildsRequestFromHistoryAndToolResults(t *testing.T) {
	builder := NewNativeContextBuilder()
	llmContext, err := builder.Build(context.Background(), ContextInput{
		Prompt: prompt.Output{
			Model:       "mock-native",
			Temperature: 0.3,
			Messages: []llm.Message{
				{Role: llm.RoleSystem, Content: "system prompt"},
				{Role: llm.RoleUser, Content: "current task"},
			},
		},
		History: []llm.Message{
			{Role: llm.RoleSystem, Content: "system prompt"},
			{Role: llm.RoleUser, Content: "first task"},
			{Role: llm.RoleAssistant, Content: "first answer"},
		},
		Tools: []llm.ToolDefinition{
			{
				Name:        tools.AskUserToolName,
				Description: "ask user",
				InputSchema: json.RawMessage(`{"type":"object"}`),
			},
		},
	})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	initialMessages := llmContext.InitialMessages()
	if len(initialMessages) != 1 || initialMessages[0].Role != llm.RoleUser || initialMessages[0].Content != "current task" {
		t.Fatalf("initial messages = %#v, want only current user message", initialMessages)
	}

	request := llmContext.BuildRequest(RunState{StepIndex: 2})
	if request.Model != "mock-native" || request.Temperature != 0.3 {
		t.Fatalf("request model/temperature = %q/%v", request.Model, request.Temperature)
	}
	if request.Metadata["loop"] != "native" || request.Metadata["step"] != "2" {
		t.Fatalf("request metadata = %#v", request.Metadata)
	}
	if len(request.Messages) != 4 {
		t.Fatalf("request messages = %d, want history plus current user: %#v", len(request.Messages), request.Messages)
	}
	if request.Messages[3].Role != llm.RoleUser || request.Messages[3].Content != "current task" {
		t.Fatalf("request missing current user message: %#v", request.Messages)
	}
	if len(request.Tools) != 1 || request.Tools[0].Name != tools.AskUserToolName {
		t.Fatalf("request tools = %#v", request.Tools)
	}

	call := llm.ToolCall{
		ID:    "call_1",
		Name:  tools.AskUserToolName,
		Input: json.RawMessage(`{"question":"Which target?"}`),
	}
	assistantMessage, ok := llmContext.AddAssistantResponse(llm.Response{
		Content: "need target",
		Usage:   &llm.Usage{TotalTokens: 3},
	}, []llm.ToolCall{call})
	if !ok {
		t.Fatal("AddAssistantResponse returned ok=false")
	}
	if assistantMessage.Role != llm.RoleAssistant || len(assistantMessage.ToolCalls) != 1 {
		t.Fatalf("assistant message = %#v", assistantMessage)
	}

	toolMessage := llmContext.AddToolResult(call, ToolResult{
		Name:    tools.AskUserToolName,
		Content: "web app",
	})
	if toolMessage.Role != llm.RoleTool || toolMessage.ToolCallID != "call_1" || toolMessage.Content != "web app" {
		t.Fatalf("tool message = %#v", toolMessage)
	}

	nextRequest := llmContext.BuildRequest(RunState{StepIndex: 3})
	if len(nextRequest.Messages) != 6 {
		t.Fatalf("next request messages = %d, want 6: %#v", len(nextRequest.Messages), nextRequest.Messages)
	}
	if got := nextRequest.Messages[5]; got.Role != llm.RoleTool || got.Content != "web app" {
		t.Fatalf("next request last message = %#v", got)
	}
}

func TestNativeContextBuilderFallsBackToSessionRecords(t *testing.T) {
	builder := NewNativeContextBuilder()
	llmContext, err := builder.Build(context.Background(), ContextInput{
		Prompt: prompt.Output{
			Model: "mock-native",
			Messages: []llm.Message{
				{Role: llm.RoleSystem, Content: "new system prompt"},
				{Role: llm.RoleUser, Content: "second task"},
			},
		},
		SessionRecords: []session.Record{
			{
				Kind:    session.RecordKindMessage,
				Message: &llm.Message{Role: llm.RoleSystem, Content: "old system prompt"},
			},
			{
				Kind:    session.RecordKindMessage,
				Message: &llm.Message{Role: llm.RoleUser, Content: "first task"},
			},
			{
				Kind:    session.RecordKindMessage,
				Message: &llm.Message{Role: llm.RoleAssistant, Content: "first answer"},
			},
			{
				Kind: session.RecordKindEvent,
				Event: &session.Event{
					Type: session.EventTypeToolCall,
				},
			},
			{Kind: session.RecordKindUsageSummary, UsageSummary: &llm.Usage{TotalTokens: 12}},
		},
	})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	initialMessages := llmContext.InitialMessages()
	if len(initialMessages) != 1 || initialMessages[0].Content != "second task" {
		t.Fatalf("initial messages = %#v, want only second task", initialMessages)
	}

	request := llmContext.BuildRequest(RunState{StepIndex: 1})
	if len(request.Messages) != 4 {
		t.Fatalf("request messages = %d, want session history plus current user: %#v", len(request.Messages), request.Messages)
	}
	if request.Messages[0].Content != "old system prompt" {
		t.Fatalf("request did not use session history: %#v", request.Messages)
	}
	if request.Messages[3].Role != llm.RoleUser || request.Messages[3].Content != "second task" {
		t.Fatalf("request missing current task: %#v", request.Messages)
	}
}

func TestNativeContextBuilderRestoresFromLatestContextSnapshot(t *testing.T) {
	builder := NewNativeContextBuilder()
	llmContext, err := builder.Build(context.Background(), ContextInput{
		Prompt: prompt.Output{
			Model: "mock-native",
			Messages: []llm.Message{
				{Role: llm.RoleSystem, Content: "new system prompt"},
				{Role: llm.RoleUser, Content: "current task"},
			},
		},
		SessionRecords: []session.Record{
			{
				Kind:    session.RecordKindMessage,
				Message: &llm.Message{Role: llm.RoleSystem, Content: "old system prompt"},
			},
			{
				Kind:    session.RecordKindMessage,
				Message: &llm.Message{Role: llm.RoleUser, Content: "old raw user task"},
			},
			{
				Kind: session.RecordKindContextSnapshot,
				ContextSnapshot: &session.ContextSnapshot{
					Messages: []llm.Message{
						{Role: llm.RoleSystem, Content: "snapshot system prompt"},
						{Role: llm.RoleUser, Content: "Conversation summary:\ncompressed state"},
					},
					Summary: "compressed state",
				},
			},
			{
				Kind:    session.RecordKindMessage,
				Message: &llm.Message{Role: llm.RoleAssistant, Content: "post snapshot answer"},
			},
		},
	})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	request := llmContext.BuildRequest(RunState{StepIndex: 1})
	if len(request.Messages) != 4 {
		t.Fatalf("request messages = %d, want snapshot + post-snapshot + current: %#v", len(request.Messages), request.Messages)
	}
	if request.Messages[0].Content != "snapshot system prompt" ||
		!strings.Contains(request.Messages[1].Content, "compressed state") ||
		request.Messages[2].Content != "post snapshot answer" {
		t.Fatalf("request did not restore from snapshot: %#v", request.Messages)
	}
	for _, msg := range request.Messages {
		if strings.Contains(msg.Content, "old raw user task") {
			t.Fatalf("request leaked pre-snapshot raw history: %#v", request.Messages)
		}
	}
	if request.Messages[3].Role != llm.RoleUser || request.Messages[3].Content != "current task" {
		t.Fatalf("request missing current task: %#v", request.Messages)
	}
}

func TestNativeLoopLoadsSessionHistoryWhenMemoryHistoryIsEmpty(t *testing.T) {
	store, err := session.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore returned error: %v", err)
	}
	for _, record := range []session.Record{
		{
			Kind:    session.RecordKindMessage,
			TurnID:  "turn-1",
			Message: &llm.Message{Role: llm.RoleSystem, Content: "system prompt"},
		},
		{
			Kind:    session.RecordKindMessage,
			TurnID:  "turn-1",
			Message: &llm.Message{Role: llm.RoleUser, Content: "Task:\nfirst task"},
		},
		{
			Kind:    session.RecordKindMessage,
			TurnID:  "turn-1",
			Message: &llm.Message{Role: llm.RoleAssistant, Content: "first answer"},
		},
	} {
		if err := store.Save(context.Background(), record); err != nil {
			t.Fatalf("Save returned error: %v", err)
		}
	}

	model := &scriptedLLM{
		responses: []llm.Response{{Provider: "mock", Model: "mock-native", Content: "second answer"}},
	}
	loop, err := NewNativeLoop(Options{
		LLM:           model,
		PromptBuilder: prompt.NewNativeBuilder(prompt.Options{}),
		Session:       store,
	})
	if err != nil {
		t.Fatalf("NewNativeLoop returned error: %v", err)
	}

	_, err = loop.Run(context.Background(), Task{Input: "second task"})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if len(model.requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(model.requests))
	}
	messages := model.requests[0].Messages
	if len(messages) != 4 {
		t.Fatalf("request messages = %d, want previous session plus current user: %#v", len(messages), messages)
	}
	if messages[2].Role != llm.RoleAssistant || messages[2].Content != "first answer" {
		t.Fatalf("request missing previous assistant response: %#v", messages)
	}
	if messages[3].Role != llm.RoleUser || !strings.Contains(messages[3].Content, "second task") {
		t.Fatalf("request missing current user message: %#v", messages)
	}
}
