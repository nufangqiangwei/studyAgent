package agent

import (
	"agent/internal/content"
	"context"
	"encoding/json"
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
	if len(records) != 4 {
		t.Fatalf("records = %d, want system/user/assistant/usage_summary: %#v", len(records), records)
	}
	if records[0].Kind != session.RecordKindMessage || records[0].Message == nil || records[0].Message.Role != llm.RoleSystem {
		t.Fatalf("first record = %#v, want system message", records[0])
	}
	if records[1].Kind != session.RecordKindMessage || records[1].Message == nil || records[1].Message.Role != llm.RoleUser {
		t.Fatalf("second record = %#v, want user message", records[1])
	}
	if records[2].Kind != session.RecordKindMessage || records[2].Message == nil || records[2].Message.Role != llm.RoleAssistant || records[2].Message.Content != "saved answer" {
		t.Fatalf("third record = %#v, want assistant message", records[2])
	}
	if records[2].Message.Usage == nil || records[2].Message.Usage.TotalTokens != 18 {
		t.Fatalf("assistant usage = %#v, want total tokens", records[2].Message.Usage)
	}
	if records[3].Kind != session.RecordKindUsageSummary || records[3].UsageSummary == nil || records[3].UsageSummary.TotalTokens != 18 || records[3].LLMCalls != 1 {
		t.Fatalf("usage summary record = %#v", records[3])
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
