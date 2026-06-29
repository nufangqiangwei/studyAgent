package askUser

import (
	"agent/internal/content"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
)

func TestAskUserToolReadsAnswer(t *testing.T) {
	var out strings.Builder
	tool := NewAskUserTool()
	ctx := askUserContext(strings.NewReader("blue\n"), &out)

	result, err := tool.Execute(ctx, json.RawMessage(`{"question":"Favorite color?"}`))
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if result.Content != "blue" {
		t.Fatalf("Content = %q, want blue", result.Content)
	}
	if !strings.Contains(out.String(), "? Favorite color?") {
		t.Fatalf("output missing question:\n%s", out.String())
	}
}

func TestAskUserToolUsesDefaultForBlankAnswer(t *testing.T) {
	var out strings.Builder
	tool := NewAskUserTool()
	ctx := askUserContext(strings.NewReader("\n"), &out)

	result, err := tool.Execute(ctx, json.RawMessage(`{"question":"Branch?","default":"main"}`))
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if result.Content != "main" {
		t.Fatalf("Content = %q, want main", result.Content)
	}
	if usedDefault, ok := result.Metadata["used_default"].(bool); !ok || !usedDefault {
		t.Fatalf("used_default metadata = %#v, want true", result.Metadata["used_default"])
	}
}

func TestAskUserToolRequiresQuestion(t *testing.T) {
	tool := NewAskUserTool()
	ctx := askUserContext(strings.NewReader("answer\n"), &strings.Builder{})

	_, err := tool.Execute(ctx, json.RawMessage(`{}`))
	if err == nil {
		t.Fatal("Execute returned nil error")
	}
	if !strings.Contains(err.Error(), "question is required") {
		t.Fatalf("error = %q, want question is required", err.Error())
	}
}

func TestAskUserToolRequiresEnvFromContext(t *testing.T) {
	tool := NewAskUserTool()

	_, err := tool.Execute(context.Background(), json.RawMessage(`{"question":"Favorite color?"}`))
	if err == nil {
		t.Fatal("Execute returned nil error")
	}
	if !strings.Contains(err.Error(), "env is required") {
		t.Fatalf("error = %q, want env is required", err.Error())
	}
}

func askUserContext(in io.Reader, out io.Writer) context.Context {
	return content.WithEnv(context.Background(), &content.Env{
		IO: content.IO{
			In:  in,
			Out: out,
		},
	})
}
