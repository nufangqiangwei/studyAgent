package mock

import (
	"agent/internal/foundation/llmClient"
	"context"
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
