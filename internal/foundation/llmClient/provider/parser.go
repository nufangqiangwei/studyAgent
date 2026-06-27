package provider

import (
	"agent/internal/foundation/llmClient"
	"encoding/json"
	"fmt"
	"strings"
)

type openAIParser struct {
	provider string
}

type openAIChatResponse struct {
	Model   string `json:"model"`
	Choices []struct {
		FinishReason string `json:"finish_reason"`
		Message      struct {
			Content   string `json:"content"`
			ToolCalls []struct {
				ID       string `json:"id"`
				Type     string `json:"type"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"message"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
		TotalTokens      int `json:"total_tokens"`
	} `json:"usage,omitempty"`
	Error *apiError `json:"error,omitempty"`
}

type anthropicParser struct {
	provider string
}

type anthropicMessagesResponse struct {
	Model      string `json:"model"`
	StopReason string `json:"stop_reason"`
	Content    []struct {
		Type  string          `json:"type"`
		Text  string          `json:"text"`
		ID    string          `json:"id"`
		Name  string          `json:"name"`
		Input json.RawMessage `json:"input"`
	} `json:"content"`
	Usage *struct {
		InputTokens              int `json:"input_tokens"`
		OutputTokens             int `json:"output_tokens"`
		CacheCreationInputTokens int `json:"cache_creation_input_tokens"`
		CacheReadInputTokens     int `json:"cache_read_input_tokens"`
	} `json:"usage,omitempty"`
	Error *apiError `json:"error,omitempty"`
}

type geminiParser struct{}

type geminiGenerateContentResponse struct {
	Candidates []struct {
		FinishReason string `json:"finishReason"`
		Content      struct {
			Parts []struct {
				Text         string `json:"text"`
				FunctionCall *struct {
					ID   string          `json:"id,omitempty"`
					Name string          `json:"name"`
					Args json.RawMessage `json:"args"`
				} `json:"functionCall,omitempty"`
			} `json:"parts"`
		} `json:"content"`
	} `json:"candidates"`
	UsageMetadata *struct {
		PromptTokenCount     int `json:"promptTokenCount"`
		CandidatesTokenCount int `json:"candidatesTokenCount"`
		TotalTokenCount      int `json:"totalTokenCount"`
	} `json:"usageMetadata,omitempty"`
	Error *apiError `json:"error,omitempty"`
}

type apiError struct {
	Message string `json:"message"`
	Type    string `json:"type,omitempty"`
	Code    any    `json:"code,omitempty"`
}

func (p openAIParser) Parse(req llmClient.Request, body []byte) (llmClient.Response, error) {
	var parsed openAIChatResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return llmClient.Response{}, err
	}
	if parsed.Error != nil {
		return llmClient.Response{}, parsed.Error
	}
	if len(parsed.Choices) == 0 {
		return llmClient.Response{}, fmt.Errorf("response has no choices")
	}

	choice := parsed.Choices[0]
	toolCalls := make([]llmClient.ToolCall, 0, len(choice.Message.ToolCalls))
	for _, call := range choice.Message.ToolCalls {
		if call.Function.Name == "" {
			continue
		}
		toolCalls = append(toolCalls, llmClient.ToolCall{
			ID:    call.ID,
			Name:  call.Function.Name,
			Input: rawJSONOrString(call.Function.Arguments),
		})
	}

	provider := p.provider
	if provider == "" {
		provider = req.Provider
	}
	model := parsed.Model
	if model == "" {
		model = req.Model
	}

	return llmClient.Response{
		Provider:   provider,
		Model:      model,
		Content:    choice.Message.Content,
		StopReason: choice.FinishReason,
		ToolCalls:  toolCalls,
		Usage:      openAIUsage(parsed.Usage),
		Raw:        body,
	}, nil
}

func (p anthropicParser) Parse(req llmClient.Request, body []byte) (llmClient.Response, error) {
	var parsed anthropicMessagesResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return llmClient.Response{}, err
	}
	if parsed.Error != nil {
		return llmClient.Response{}, parsed.Error
	}

	var texts []string
	var toolCalls []llmClient.ToolCall
	for _, block := range parsed.Content {
		switch block.Type {
		case "text":
			if block.Text != "" {
				texts = append(texts, block.Text)
			}
		case "tool_use":
			if block.Name != "" {
				toolCalls = append(toolCalls, llmClient.ToolCall{
					ID:    block.ID,
					Name:  block.Name,
					Input: block.Input,
				})
			}
		}
	}

	model := parsed.Model
	if model == "" {
		model = req.Model
	}

	return llmClient.Response{
		Provider:   p.provider,
		Model:      model,
		Content:    strings.Join(texts, "\n"),
		StopReason: parsed.StopReason,
		ToolCalls:  toolCalls,
		Usage:      anthropicUsage(parsed.Usage),
		Raw:        body,
	}, nil
}

func (geminiParser) Parse(req llmClient.Request, body []byte) (llmClient.Response, error) {
	var parsed geminiGenerateContentResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return llmClient.Response{}, err
	}
	if parsed.Error != nil {
		return llmClient.Response{}, parsed.Error
	}
	if len(parsed.Candidates) == 0 {
		return llmClient.Response{}, fmt.Errorf("response has no candidates")
	}

	candidate := parsed.Candidates[0]
	var texts []string
	var toolCalls []llmClient.ToolCall
	for _, part := range candidate.Content.Parts {
		if part.Text != "" {
			texts = append(texts, part.Text)
		}
		if part.FunctionCall != nil {
			toolCalls = append(toolCalls, llmClient.ToolCall{
				ID:    part.FunctionCall.ID,
				Name:  part.FunctionCall.Name,
				Input: part.FunctionCall.Args,
			})
		}
	}

	return llmClient.Response{
		Provider:   "gemini",
		Model:      req.Model,
		Content:    strings.Join(texts, "\n"),
		StopReason: candidate.FinishReason,
		ToolCalls:  toolCalls,
		Usage:      geminiUsage(parsed.UsageMetadata),
		Raw:        body,
	}, nil
}

func openAIUsage(usage *struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}) *llmClient.Usage {
	if usage == nil {
		return nil
	}
	return &llmClient.Usage{
		InputTokens:  usage.PromptTokens,
		OutputTokens: usage.CompletionTokens,
		TotalTokens:  usage.TotalTokens,
	}
}

func anthropicUsage(usage *struct {
	InputTokens              int `json:"input_tokens"`
	OutputTokens             int `json:"output_tokens"`
	CacheCreationInputTokens int `json:"cache_creation_input_tokens"`
	CacheReadInputTokens     int `json:"cache_read_input_tokens"`
}) *llmClient.Usage {
	if usage == nil {
		return nil
	}
	total := usage.InputTokens + usage.OutputTokens + usage.CacheCreationInputTokens + usage.CacheReadInputTokens
	return &llmClient.Usage{
		InputTokens:              usage.InputTokens,
		OutputTokens:             usage.OutputTokens,
		TotalTokens:              total,
		CacheCreationInputTokens: usage.CacheCreationInputTokens,
		CacheReadInputTokens:     usage.CacheReadInputTokens,
	}
}

func geminiUsage(usage *struct {
	PromptTokenCount     int `json:"promptTokenCount"`
	CandidatesTokenCount int `json:"candidatesTokenCount"`
	TotalTokenCount      int `json:"totalTokenCount"`
}) *llmClient.Usage {
	if usage == nil {
		return nil
	}
	return &llmClient.Usage{
		InputTokens:  usage.PromptTokenCount,
		OutputTokens: usage.CandidatesTokenCount,
		TotalTokens:  usage.TotalTokenCount,
	}
}

func (e *apiError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message == "" {
		return "api error"
	}
	return e.Message
}

func rawJSONOrString(value string) json.RawMessage {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return json.RawMessage(`{}`)
	}
	var js json.RawMessage
	if json.Unmarshal([]byte(trimmed), &js) == nil {
		return js
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`""`)
	}
	return encoded
}
