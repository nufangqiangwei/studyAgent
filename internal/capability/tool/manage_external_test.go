package tool_test

import (
	"agent/internal/capability/builtin/workspace"
	"agent/internal/capability/tool"
	"agent/internal/content"
	"agent/internal/foundation/policy"
	"context"
	"encoding/json"
	"testing"
)

func TestExternalCallerAddsRegisteredToolByName(t *testing.T) {
	if _, err := tool.NewDefaultManage(tool.WithPolicy(policy.New(policy.ModeModify))); err != nil {
		t.Fatalf("NewDefaultManage returned error: %v", err)
	}

	registry := tool.NewManage(tool.WithPolicy(policy.New(policy.ModeModify)))
	if err := tool.AddTool(workspace.ListFilesToolName, registry); err != nil {
		t.Fatalf("AddTool returned error: %v", err)
	}

	tools := registry.List()
	if len(tools) != 1 || tools[0].Name() != workspace.ListFilesToolName {
		t.Fatalf("tools = %#v, want only %q", tools, workspace.ListFilesToolName)
	}

	ctx := content.WithEnv(context.Background(), &content.Env{
		Config: content.Config{WorkDir: t.TempDir()},
	})
	if _, err := registry.Execute(ctx, workspace.ListFilesToolName, json.RawMessage(`{}`)); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
}
