package app

import (
	"agent/internal/foundation/llmClient"
	"agent/internal/runtime/agents"
	"agent/internal/runtime/statemachine"
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type appLLMClient interface {
	Complete(ctx context.Context, req llmClient.Request) (llmClient.Response, error)
}

// agentModelAdapter is an outer adapter between provider-neutral HTTP clients
// and the model protocol consumed by runtime agents.
type agentModelAdapter struct {
	client appLLMClient
}

func (m *agentModelAdapter) Complete(ctx context.Context, request agents.ModelRequest) (agents.ModelResponse, error) {
	if m == nil || m.client == nil {
		return agents.ModelResponse{}, fmt.Errorf("app model client is not configured")
	}
	response, err := m.client.Complete(ctx, llmClient.Request{
		Model: request.Model, Messages: agentMessagesToLLM(request.Messages),
		Tools: agentToolsToLLM(request.Tools), Temperature: request.Temperature,
		Metadata: cloneStringMap(request.Metadata),
	})
	if err != nil {
		return agents.ModelResponse{}, err
	}
	metadata := map[string]string{"provider": response.Provider, "model": response.Model}
	if len(response.ToolCalls) > 0 {
		call := response.ToolCalls[0]
		arguments := append(json.RawMessage(nil), call.Input...)
		if strings.TrimSpace(string(arguments)) == "" {
			arguments = json.RawMessage(`{}`)
		}
		if call.Name == "ask_user" {
			return agents.ModelResponse{
				Content: strings.TrimSpace(response.Content),
				Decision: &agents.Decision{Action: agents.ActionAskUser, UserInput: &agents.UserInputIntent{
					RequestID: call.ID, Prompt: askUserPrompt(arguments),
				}},
				Metadata: metadata,
			}, nil
		}
		return agents.ModelResponse{
			Content: strings.TrimSpace(response.Content),
			Decision: &agents.Decision{Action: agents.ActionUseTool, Tool: &agents.ToolIntent{
				ToolCallID: call.ID, ToolName: call.Name, Arguments: arguments,
			}},
			Metadata: metadata,
		}, nil
	}
	content := strings.TrimSpace(response.Content)
	if content == "" {
		return agents.ModelResponse{Decision: &agents.Decision{Action: agents.ActionComplete}, Metadata: metadata}, nil
	}
	modelResponse := agents.ModelResponse{Content: content, Metadata: metadata}
	if _, err := modelResponse.ResolveDecision(); err == nil {
		return modelResponse, nil
	}
	modelResponse.Decision = &agents.Decision{Action: agents.ActionComplete, FinalAnswer: content}
	return modelResponse, nil
}

func agentMessagesToLLM(messages []agents.Message) []llmClient.Message {
	if len(messages) == 0 {
		return nil
	}
	out := make([]llmClient.Message, 0, len(messages))
	for _, message := range messages {
		role := agentRoleToLLM(message.Role)
		content, name, toolCallID := message.Content, "", ""
		if strings.TrimSpace(message.Role) == string(llmClient.RoleTool) {
			role = llmClient.RoleTool
			content, name, toolCallID = toolObservationToLLM(message)
		} else if strings.TrimSpace(message.Role) == string(llmClient.RoleUser) && len(message.Data) > 0 {
			if toolContent, toolName, id, ok := userInputToLLMTool(message); ok {
				role, content, name, toolCallID = llmClient.RoleTool, toolContent, toolName, id
			} else if strings.TrimSpace(content) == "" {
				content = string(message.Data)
			}
		} else if strings.TrimSpace(content) == "" && len(message.Data) > 0 {
			content = string(message.Data)
		}
		out = append(out, llmClient.Message{Role: role, Content: content, Name: name, ToolCallID: toolCallID})
	}
	return out
}

func agentRoleToLLM(role string) llmClient.Role {
	switch strings.TrimSpace(role) {
	case string(llmClient.RoleSystem):
		return llmClient.RoleSystem
	case string(llmClient.RoleAssistant):
		return llmClient.RoleAssistant
	case string(llmClient.RoleTool):
		return llmClient.RoleTool
	default:
		return llmClient.RoleUser
	}
}

func toolObservationToLLM(message agents.Message) (string, string, string) {
	var payload statemachine.ToolCallPayload
	if len(message.Data) > 0 && json.Unmarshal(message.Data, &payload) == nil && payload.ToolCallID != "" {
		if payload.Error != "" {
			return "error: " + payload.Error, payload.ToolName, payload.ToolCallID
		}
		if len(payload.Result) > 0 {
			return string(payload.Result), payload.ToolName, payload.ToolCallID
		}
		return "{}", payload.ToolName, payload.ToolCallID
	}
	if strings.TrimSpace(message.Content) != "" {
		return message.Content, "", ""
	}
	if len(message.Data) > 0 {
		return string(message.Data), "", ""
	}
	return "{}", "", ""
}

func userInputToLLMTool(message agents.Message) (string, string, string, bool) {
	var payload statemachine.UserInputPayload
	if len(message.Data) == 0 || json.Unmarshal(message.Data, &payload) != nil || payload.RequestID == "" || strings.TrimSpace(payload.Answer) == "" {
		return "", "", "", false
	}
	return payload.Answer, "ask_user", payload.RequestID, true
}

func askUserPrompt(arguments json.RawMessage) string {
	var input struct {
		Question string `json:"question"`
		Prompt   string `json:"prompt"`
	}
	if len(arguments) > 0 {
		_ = json.Unmarshal(arguments, &input)
	}
	prompt := strings.TrimSpace(input.Question)
	if prompt == "" {
		prompt = strings.TrimSpace(input.Prompt)
	}
	if prompt == "" {
		prompt = "Input requested"
	}
	return prompt
}

func agentToolsToLLM(specs []agents.ToolSpec) []llmClient.ToolDefinition {
	if len(specs) == 0 {
		return nil
	}
	out := make([]llmClient.ToolDefinition, 0, len(specs))
	for _, spec := range specs {
		out = append(out, llmClient.ToolDefinition{
			Name: spec.Name, Description: spec.Description,
			InputSchema: append(json.RawMessage(nil), spec.InputSchema...),
		})
	}
	return out
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}
