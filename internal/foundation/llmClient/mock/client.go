package mock

import (
	"agent/internal/foundation/llmClient"
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type Client struct {
	model string
}

func New(model string) *Client {
	if model == "" {
		model = "mock-native"
	}
	return &Client{model: model}
}

func (c *Client) ModelName() string {
	return c.model
}

func (c *Client) Complete(_ context.Context, req llmClient.Request) (llmClient.Response, error) {
	model := req.Model
	if model == "" {
		model = c.model
	}
	if req.Metadata != nil && req.Metadata["purpose"] == "task_preprocess" {
		return mockPreprocessResponse(model, req), nil
	}

	lastUser := ""
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == llmClient.RoleUser {
			lastUser = req.Messages[i].Content
			break
		}
	}

	content := strings.TrimSpace(fmt.Sprintf(`Mock LLM response

Provider: mock
Model: %s
Messages: %d

Last user prompt:
%s`, model, len(req.Messages), lastUser))

	return llmClient.Response{
		Provider:   "mock",
		Model:      model,
		Content:    content,
		StopReason: "stop",
	}, nil
}

func mockPreprocessResponse(model string, req llmClient.Request) llmClient.Response {
	lastUser := ""
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == llmClient.RoleUser {
			lastUser = req.Messages[i].Content
			break
		}
	}

	var payload struct {
		OriginalInput string `json:"original_input"`
	}
	if err := json.Unmarshal([]byte(lastUser), &payload); err != nil || strings.TrimSpace(payload.OriginalInput) == "" {
		payload.OriginalInput = lastUser
	}
	task := strings.TrimSpace(payload.OriginalInput)
	if task == "" {
		task = "mock task"
	}

	raw, _ := json.Marshal(map[string]any{
		"action":          "proceed",
		"normalized_task": task,
		"summary":         "Mock preprocessing passed the input through.",
		"steps": []map[string]string{{
			"id":   "1",
			"goal": task,
		}},
	})
	return llmClient.Response{
		Provider:   "mock",
		Model:      model,
		Content:    string(raw),
		StopReason: "stop",
	}
}
