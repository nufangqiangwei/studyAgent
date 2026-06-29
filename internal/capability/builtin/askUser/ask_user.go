package askUser

import (
	"agent/internal/capability/builtin"
	"agent/internal/content"
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

const Name = "ask_user"

type Question struct {
}

func NewAskUserTool() *Question {
	return &Question{}
}

func (t *Question) Name() string {
	return Name
}

func (t *Question) Description() string {
	return "Ask the user a concise clarifying question and return their answer."
}

func (t *Question) InputSchema() json.RawMessage {
	return json.RawMessage(`{
  "type": "object",
  "properties": {
    "question": {
      "type": "string",
      "description": "The concise question to show to the user."
    },
    "default": {
      "type": "string",
      "description": "Optional default answer used when the user submits an empty line."
    }
  },
  "required": ["question"],
  "additionalProperties": false
}`)
}

func (t *Question) Execute(ctx context.Context, input json.RawMessage) (builtin.Result, error) {
	if err := ctx.Err(); err != nil {
		return builtin.Result{}, err
	}
	if t == nil {
		return builtin.Result{}, errors.New("ask_user: tool is nil")
	}
	env, ok := content.EnvFromContext(ctx)
	if !ok {
		return builtin.Result{}, errors.New("ask_user: env is required")
	}
	if env.IO.In == nil {
		return builtin.Result{}, errors.New("ask_user: input reader is required")
	}
	if env.IO.Out == nil {
		return builtin.Result{}, errors.New("ask_user: output writer is required")
	}
	out := env.IO.Out
	in := env.IO.In

	req, err := decodeAskUserInput(input)
	if err != nil {
		return builtin.Result{}, err
	}

	if req.Default != "" {
		if _, err := fmt.Fprintf(out, "? %s [%s]\n> ", req.Question, req.Default); err != nil {
			return builtin.Result{}, fmt.Errorf("ask_user: write question: %w", err)
		}
	} else {
		if _, err := fmt.Fprintf(out, "? %s\n> ", req.Question); err != nil {
			return builtin.Result{}, fmt.Errorf("ask_user: write question: %w", err)
		}
	}

	line, readErr := bufio.NewReader(in).ReadString('\n')
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return builtin.Result{}, fmt.Errorf("ask_user: read answer: %w", readErr)
	}
	if err := ctx.Err(); err != nil {
		return builtin.Result{}, err
	}
	if errors.Is(readErr, io.EOF) && line == "" {
		if req.Default == "" {
			return builtin.Result{}, fmt.Errorf("ask_user: no answer received")
		}
	}

	answer := strings.TrimRight(line, "\r\n")
	usedDefault := false
	if answer == "" && req.Default != "" {
		answer = req.Default
		usedDefault = true
	}

	raw, err := json.Marshal(map[string]any{
		"question":     req.Question,
		"answer":       answer,
		"used_default": usedDefault,
	})
	if err != nil {
		return builtin.Result{}, fmt.Errorf("ask_user: marshal result: %w", err)
	}

	return builtin.Result{
		Content: answer,
		Metadata: map[string]any{
			"question":     req.Question,
			"used_default": usedDefault,
		},
		Raw: raw,
	}, nil
}

type askUserInput struct {
	Question string `json:"question"`
	Prompt   string `json:"prompt"`
	Default  string `json:"default"`
}

func decodeAskUserInput(input json.RawMessage) (askUserInput, error) {
	trimmed := strings.TrimSpace(string(input))
	if trimmed == "" {
		return askUserInput{}, fmt.Errorf("ask_user: input is required")
	}

	var req askUserInput
	if err := json.Unmarshal(input, &req); err != nil {
		var question string
		if stringErr := json.Unmarshal(input, &question); stringErr != nil {
			return askUserInput{}, fmt.Errorf("ask_user: decode input: %w", err)
		}
		req.Question = question
	}

	if strings.TrimSpace(req.Question) == "" {
		req.Question = req.Prompt
	}
	req.Question = strings.TrimSpace(req.Question)
	req.Default = strings.TrimSpace(req.Default)
	if req.Question == "" {
		return askUserInput{}, fmt.Errorf("ask_user: question is required")
	}

	return req, nil
}
