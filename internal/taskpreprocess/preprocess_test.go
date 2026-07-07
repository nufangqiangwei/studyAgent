package taskpreprocess

import (
	"agent/internal/foundation/llmClient"
	"context"
	"strings"
	"testing"
)

func TestProcessorProceedBuildsAgentTask(t *testing.T) {
	client := &recordingClient{
		response: `{
  "action": "proceed",
  "normalized_task": "Add task preprocessing before interactive agent runs.",
  "summary": "The user wants a preprocessing stage for non-command input.",
  "steps": [
    {"id": "1", "goal": "Analyze the input"},
    {"id": "2", "goal": "Ask the user when required information is missing"}
  ]
}`,
	}
	analyzer, err := NewModelAnalyzer(client, "mock-native")
	if err != nil {
		t.Fatalf("NewModelAnalyzer returned error: %v", err)
	}
	processor, err := NewProcessor(analyzer)
	if err != nil {
		t.Fatalf("NewProcessor returned error: %v", err)
	}

	result, err := processor.Preprocess(context.Background(), Request{
		Input:   "add task preprocessing",
		WorkDir: "C:\\Code\\GO\\agent",
	})
	if err != nil {
		t.Fatalf("Preprocess returned error: %v", err)
	}

	if result.Action != ActionProceed {
		t.Fatalf("action = %q, want proceed", result.Action)
	}
	if len(result.Steps) != 2 {
		t.Fatalf("steps = %#v, want two steps", result.Steps)
	}
	task := result.AgentTask()
	for _, want := range []string{
		"Original input:",
		"add task preprocessing",
		"Normalized task:",
		"Add task preprocessing",
		"Do not invent missing requirements",
	} {
		if !strings.Contains(task, want) {
			t.Fatalf("agent task missing %q:\n%s", want, task)
		}
	}
	if len(client.requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(client.requests))
	}
	if client.requests[0].Metadata["purpose"] != "task_preprocess" {
		t.Fatalf("metadata = %#v, want task_preprocess purpose", client.requests[0].Metadata)
	}
}

func TestProcessorAskUserRequiresQuestion(t *testing.T) {
	client := &recordingClient{
		response: `{
  "action": "ask_user",
  "missing_information": ["target package"],
  "questions": [
    {"id": "q1", "prompt": "Which package should be changed?"},
    {"id": "q2", "prompt": "Which tests should pass?"}
  ]
}`,
	}
	analyzer, err := NewModelAnalyzer(client, "mock-native")
	if err != nil {
		t.Fatalf("NewModelAnalyzer returned error: %v", err)
	}
	processor, err := NewProcessor(analyzer)
	if err != nil {
		t.Fatalf("NewProcessor returned error: %v", err)
	}

	result, err := processor.Preprocess(context.Background(), Request{
		Input:        "fix this issue",
		MaxQuestions: 1,
	})
	if err != nil {
		t.Fatalf("Preprocess returned error: %v", err)
	}
	if result.Action != ActionAskUser {
		t.Fatalf("action = %q, want ask_user", result.Action)
	}
	if len(result.Questions) != 1 {
		t.Fatalf("questions = %#v, want one question", result.Questions)
	}
	if result.Questions[0].Prompt != "Which package should be changed?" {
		t.Fatalf("question = %q", result.Questions[0].Prompt)
	}
}

func TestProcessorIncludesClarificationsInModelRequest(t *testing.T) {
	client := &recordingClient{
		response: `{"action":"proceed","normalized_task":"Fix parser tests in internal/foundation/startup."}`,
	}
	analyzer, err := NewModelAnalyzer(client, "mock-native")
	if err != nil {
		t.Fatalf("NewModelAnalyzer returned error: %v", err)
	}
	processor, err := NewProcessor(analyzer)
	if err != nil {
		t.Fatalf("NewProcessor returned error: %v", err)
	}

	_, err = processor.Preprocess(context.Background(), Request{
		Input: "fix tests",
		Clarifications: []Clarification{{
			QuestionID: "q1",
			Question:   "Which package?",
			Answer:     "internal/foundation/startup",
		}},
	})
	if err != nil {
		t.Fatalf("Preprocess returned error: %v", err)
	}
	if len(client.requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(client.requests))
	}
	if !strings.Contains(client.requests[0].Messages[1].Content, "internal/foundation/startup") {
		t.Fatalf("request missing clarification:\n%s", client.requests[0].Messages[1].Content)
	}
}

type recordingClient struct {
	requests []llmClient.Request
	response string
}

func (c *recordingClient) Complete(_ context.Context, request llmClient.Request) (llmClient.Response, error) {
	c.requests = append(c.requests, request)
	return llmClient.Response{
		Provider: "mock",
		Model:    request.Model,
		Content:  c.response,
	}, nil
}
