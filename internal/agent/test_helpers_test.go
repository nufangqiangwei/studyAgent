package agent

import (
	"agent/internal/foundation/llmClient"
	"context"
)

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
