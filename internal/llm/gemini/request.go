package gemini

import (
	"encoding/json"
	"strings"

	"agent/internal/llm"
)

type Builder struct{}

type GenerateContentRequest struct {
	Contents          []Content        `json:"contents"`
	SystemInstruction *Content         `json:"systemInstruction,omitempty"`
	Tools             []Tool           `json:"tools,omitempty"`
	GenerationConfig  GenerationConfig `json:"generationConfig,omitempty"`
}

type Content struct {
	Role  string `json:"role,omitempty"`
	Parts []Part `json:"parts"`
}

type Part struct {
	Text             string            `json:"text,omitempty"`
	FunctionCall     *FunctionCall     `json:"functionCall,omitempty"`
	FunctionResponse *FunctionResponse `json:"functionResponse,omitempty"`
}

type FunctionCall struct {
	ID   string          `json:"id,omitempty"`
	Name string          `json:"name"`
	Args json.RawMessage `json:"args,omitempty"`
}

type FunctionResponse struct {
	ID       string         `json:"id,omitempty"`
	Name     string         `json:"name"`
	Response map[string]any `json:"response"`
}

type GenerationConfig struct {
	Temperature float64 `json:"temperature,omitempty"`
}

type Tool struct {
	FunctionDeclarations []FunctionDeclaration `json:"functionDeclarations,omitempty"`
}

type FunctionDeclaration struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

func (Builder) Build(req llm.Request) ([]byte, error) {
	contents := make([]Content, 0, len(req.Messages))
	var systemParts []Part
	for _, msg := range req.Messages {
		if msg.Role == llm.RoleSystem {
			systemParts = append(systemParts, Part{Text: strings.TrimSpace(msg.Content)})
			continue
		}

		contents = append(contents, Content{
			Role:  geminiRole(msg.Role),
			Parts: geminiParts(msg),
		})
	}

	var systemInstruction *Content
	if len(systemParts) > 0 {
		systemInstruction = &Content{Parts: systemParts}
	}

	return json.MarshalIndent(GenerateContentRequest{
		Contents:          contents,
		SystemInstruction: systemInstruction,
		Tools:             contentTools(req.Tools),
		GenerationConfig: GenerationConfig{
			Temperature: req.Temperature,
		},
	}, "", "  ")
}

func geminiRole(role llm.Role) string {
	if role == llm.RoleAssistant {
		return "model"
	}
	return "user"
}

func geminiParts(msg llm.Message) []Part {
	switch {
	case msg.Role == llm.RoleTool:
		return []Part{{
			FunctionResponse: &FunctionResponse{
				ID:   msg.ToolCallID,
				Name: msg.Name,
				Response: map[string]any{
					"content": msg.Content,
				},
			},
		}}
	case len(msg.ToolCalls) > 0:
		parts := make([]Part, 0, len(msg.ToolCalls)+1)
		if strings.TrimSpace(msg.Content) != "" {
			parts = append(parts, Part{Text: msg.Content})
		}
		for _, call := range msg.ToolCalls {
			parts = append(parts, Part{
				FunctionCall: &FunctionCall{
					ID:   call.ID,
					Name: call.Name,
					Args: rawArgs(call.Input),
				},
			})
		}
		return parts
	default:
		return []Part{{Text: msg.Content}}
	}
}

func rawArgs(input json.RawMessage) json.RawMessage {
	if len(strings.TrimSpace(string(input))) == 0 {
		return json.RawMessage(`{}`)
	}
	return input
}

func contentTools(defs []llm.ToolDefinition) []Tool {
	if len(defs) == 0 {
		return nil
	}

	functions := make([]FunctionDeclaration, 0, len(defs))
	for _, def := range defs {
		functions = append(functions, FunctionDeclaration{
			Name:        def.Name,
			Description: def.Description,
			Parameters:  def.InputSchema,
		})
	}
	return []Tool{{FunctionDeclarations: functions}}
}
