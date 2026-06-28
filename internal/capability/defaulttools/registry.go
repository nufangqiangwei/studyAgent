package defaulttools

import (
	"agent/internal/capability/builtin/askUser"
	"agent/internal/capability/builtin/workspace"
	"agent/internal/capability/tool"
	"encoding/json"
	"fmt"
)

func NewRegistry(options ...tool.RegistryOption) (*tool.Registry, error) {
	registry := tool.NewRegistry(options...)
	if err := Register(registry); err != nil {
		return nil, err
	}
	tool.SetCurrentRegistry(registry)
	return registry, nil
}

func Register(registry *tool.Registry) error {
	if registry == nil {
		return fmt.Errorf("register default tool: nil registry")
	}
	defaults := []struct {
		tool     tool.Tool
		analyzer tool.PolicyAnalyzer
	}{
		{tool: workspace.NewApplyPatchTool(), analyzer: tool.PolicyAnalyzerFunc(workspace.AnalyzeApplyPatchPolicy)},
		{tool: askUser.NewAskUserTool(), analyzer: tool.PolicyAnalyzerFunc(analyzeAskUserPolicy)},
		{tool: workspace.NewListFilesTool(), analyzer: tool.PolicyAnalyzerFunc(workspace.AnalyzeListFilesPolicy)},
		{tool: workspace.NewReadFileTool(), analyzer: tool.PolicyAnalyzerFunc(workspace.AnalyzeReadFilePolicy)},
		{tool: workspace.NewSearchTextTool(), analyzer: tool.PolicyAnalyzerFunc(workspace.AnalyzeSearchTextPolicy)},
		{tool: workspace.NewGetWorkspaceSummaryTool(), analyzer: tool.PolicyAnalyzerFunc(workspace.AnalyzeGetWorkspaceSummaryPolicy)},
		{tool: workspace.NewWriteFileTool(), analyzer: tool.PolicyAnalyzerFunc(workspace.AnalyzeWriteFilePolicy)},
	}
	for _, entry := range defaults {
		if err := registry.RegisterWithPolicyAnalyzer(entry.tool, entry.analyzer); err != nil {
			return fmt.Errorf("register default tool %q: %w", entry.tool.Name(), err)
		}
	}
	return nil
}

func analyzeAskUserPolicy(json.RawMessage) (tool.PolicyFacts, error) {
	return tool.PolicyFacts{Read: true, Operation: askUser.Name}, nil
}
