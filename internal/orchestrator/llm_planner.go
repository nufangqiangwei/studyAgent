package orchestrator

import (
	"agent/internal/foundation/llmClient"
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type LLMPlannerOption func(*llmPlannerConfig)

type llmPlannerConfig struct {
	model       string
	temperature float64
}

func WithModel(model string) LLMPlannerOption {
	return func(config *llmPlannerConfig) {
		config.model = strings.TrimSpace(model)
	}
}

func WithTemperature(temperature float64) LLMPlannerOption {
	return func(config *llmPlannerConfig) {
		config.temperature = temperature
	}
}

type LLMPlanner struct {
	client      llmClient.Client
	model       string
	temperature float64
}

func NewLLMPlanner(client llmClient.Client, options ...LLMPlannerOption) (*LLMPlanner, error) {
	if client == nil {
		return nil, fmt.Errorf("llm planner: client is required")
	}
	config := llmPlannerConfig{
		model:       client.ModelName(),
		temperature: 0,
	}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	if strings.TrimSpace(config.model) == "" {
		return nil, fmt.Errorf("llm planner: model is required")
	}
	return &LLMPlanner{
		client:      client,
		model:       strings.TrimSpace(config.model),
		temperature: config.temperature,
	}, nil
}

func (p *LLMPlanner) Plan(ctx context.Context, request PlanRequest) (Decision, error) {
	if p == nil {
		return Decision{}, fmt.Errorf("llm planner is nil")
	}
	if p.client == nil {
		return Decision{}, fmt.Errorf("llm planner: client is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	payload, err := json.Marshal(request)
	if err != nil {
		return Decision{}, fmt.Errorf("llm planner: marshal plan request: %w", err)
	}
	response, err := p.client.Complete(ctx, llmClient.Request{
		Model: p.model,
		Messages: []llmClient.Message{
			{Role: llmClient.RoleSystem, Content: plannerSystemPrompt()},
			{Role: llmClient.RoleUser, Content: string(payload)},
		},
		Temperature: p.temperature,
		Metadata: map[string]string{
			"component": "orchestrator",
			"purpose":   "agent_scheduling",
		},
	})
	if err != nil {
		return Decision{}, err
	}
	decision, err := decodeDecision(response.Content)
	if err != nil {
		return Decision{}, err
	}
	if err := decision.Validate(); err != nil {
		return Decision{}, err
	}
	return decision, nil
}

func plannerSystemPrompt() string {
	return strings.TrimSpace(`You are an agent orchestrator.
Select the next agent action for the goal using only the available agents.
Return strict JSON only. The JSON shape is:
{
  "action": "start_agent|resume_agent|wait|complete|fail",
  "reason": "short routing reason",
  "work": {
    "task_id": "optional stable task id",
    "agent": "agent name",
    "input": "task input for start_agent",
    "payload": {},
    "metadata": {}
  },
  "final_answer": "required when complete",
  "error": "required when fail",
  "metadata": {}
}`)
}

func decodeDecision(content string) (Decision, error) {
	content = stripJSONFence(strings.TrimSpace(content))
	var decision Decision
	if err := json.Unmarshal([]byte(content), &decision); err == nil && decision.Action != "" {
		return decision, nil
	}
	var envelope struct {
		Decision Decision `json:"decision"`
	}
	if err := json.Unmarshal([]byte(content), &envelope); err != nil {
		return Decision{}, fmt.Errorf("decode orchestrator decision: %w", err)
	}
	if envelope.Decision.Action == "" {
		return Decision{}, fmt.Errorf("orchestrator decision action is required")
	}
	return envelope.Decision, nil
}

func stripJSONFence(content string) string {
	if !strings.HasPrefix(content, "```") {
		return content
	}
	lines := strings.Split(content, "\n")
	if len(lines) < 2 {
		return content
	}
	lines = lines[1:]
	if len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "```" {
		lines = lines[:len(lines)-1]
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}
