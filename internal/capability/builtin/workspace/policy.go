package workspace

import (
	"agent/internal/capability/tool"
	"encoding/json"
)

func (t *ListFilesTool) AnalyzePolicy(input json.RawMessage) (tool.PolicyFacts, error) {
	return AnalyzeListFilesPolicy(input)
}

func (t *ReadFileTool) AnalyzePolicy(input json.RawMessage) (tool.PolicyFacts, error) {
	return AnalyzeReadFilePolicy(input)
}

func (t *SearchTextTool) AnalyzePolicy(input json.RawMessage) (tool.PolicyFacts, error) {
	return AnalyzeSearchTextPolicy(input)
}

func (t *GetWorkspaceSummaryTool) AnalyzePolicy(input json.RawMessage) (tool.PolicyFacts, error) {
	return AnalyzeGetWorkspaceSummaryPolicy(input)
}

func (t *WriteFileTool) AnalyzePolicy(input json.RawMessage) (tool.PolicyFacts, error) {
	return AnalyzeWriteFilePolicy(input)
}

func (t *ApplyPatchTool) AnalyzePolicy(input json.RawMessage) (tool.PolicyFacts, error) {
	return AnalyzeApplyPatchPolicy(input)
}

func AnalyzeListFilesPolicy(input json.RawMessage) (tool.PolicyFacts, error) {
	var req listFilesInput
	if err := decodeWorkspaceToolInput(ListFilesToolName, input, &req); err != nil {
		return tool.PolicyFacts{}, err
	}
	normalized, err := req.normalize()
	if err != nil {
		return tool.PolicyFacts{}, err
	}
	return readPolicyFacts(ListFilesToolName, normalized.Path), nil
}

func AnalyzeReadFilePolicy(input json.RawMessage) (tool.PolicyFacts, error) {
	var req readFileInput
	if err := decodeWorkspaceToolInput(ReadFileToolName, input, &req); err != nil {
		return tool.PolicyFacts{}, err
	}
	normalized, err := req.normalize()
	if err != nil {
		return tool.PolicyFacts{}, err
	}
	return readPolicyFacts(ReadFileToolName, normalized.Path), nil
}

func AnalyzeSearchTextPolicy(input json.RawMessage) (tool.PolicyFacts, error) {
	var req searchTextInput
	if err := decodeWorkspaceToolInput(SearchTextToolName, input, &req); err != nil {
		return tool.PolicyFacts{}, err
	}
	normalized, err := req.normalize()
	if err != nil {
		return tool.PolicyFacts{}, err
	}
	return readPolicyFacts(SearchTextToolName, normalized.Path), nil
}

func AnalyzeGetWorkspaceSummaryPolicy(input json.RawMessage) (tool.PolicyFacts, error) {
	var req workspaceSummaryInput
	if err := decodeWorkspaceToolInput(GetWorkspaceSummaryToolName, input, &req); err != nil {
		return tool.PolicyFacts{}, err
	}
	normalized := req.normalize()
	return readPolicyFacts(GetWorkspaceSummaryToolName, normalized.Path), nil
}

func AnalyzeWriteFilePolicy(input json.RawMessage) (tool.PolicyFacts, error) {
	var req writeFileInput
	if err := decodeWorkspaceToolInput(WriteFileToolName, input, &req); err != nil {
		return tool.PolicyFacts{}, err
	}
	if err := req.validate(); err != nil {
		return tool.PolicyFacts{}, err
	}
	rel := normalizeToolPath(req.Path)
	highRisk, _ := highRiskWritePath(rel)
	facts := tool.PolicyFacts{
		Paths:     []string{rel},
		DryRun:    req.dryRun(),
		Write:     true,
		HighRisk:  highRisk,
		Operation: "write",
	}
	if facts.DryRun {
		facts.Operation = "dry-run write"
	} else if facts.HighRisk {
		facts.Operation = "high-risk write"
	}
	return facts, nil
}

func AnalyzeApplyPatchPolicy(input json.RawMessage) (tool.PolicyFacts, error) {
	var req applyPatchInput
	if err := decodeWorkspaceToolInput(ApplyPatchToolName, input, &req); err != nil {
		return tool.PolicyFacts{}, err
	}
	if err := req.validate(); err != nil {
		return tool.PolicyFacts{}, err
	}
	filePatches, err := parseUnifiedPatch(req.Patch)
	if err != nil {
		return tool.PolicyFacts{}, err
	}

	facts := tool.PolicyFacts{
		DryRun:    req.dryRun(),
		Write:     true,
		Operation: ApplyPatchToolName,
	}
	if facts.DryRun {
		facts.Operation = "dry-run patch"
	}
	for _, filePatch := range filePatches {
		target := filePatch.targetPath()
		if target != "" {
			facts.Paths = append(facts.Paths, target)
		}
		if filePatch.operation() == "delete" {
			facts.Delete = true
		}
		if highRisk, _ := highRiskWritePath(target); highRisk {
			facts.HighRisk = true
		}
	}
	if facts.Delete {
		facts.Operation = "delete"
	} else if facts.HighRisk && !facts.DryRun {
		facts.Operation = "high-risk write"
	}
	return facts, nil
}

func readPolicyFacts(operation, path string) tool.PolicyFacts {
	return tool.PolicyFacts{
		Paths:     []string{path},
		Read:      true,
		Operation: operation,
	}
}
