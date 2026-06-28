package workspace

import (
	"agent/internal/foundation/policy"
	"encoding/json"
	"strings"
)

func (t *ListFilesTool) PolicyRequest(input json.RawMessage) policy.Request {
	return readPolicyRequest(ListFilesToolName, inputStringField(input, "path"))
}

func (t *ReadFileTool) PolicyRequest(input json.RawMessage) policy.Request {
	return readPolicyRequest(ReadFileToolName, inputStringField(input, "path"))
}

func (t *SearchTextTool) PolicyRequest(input json.RawMessage) policy.Request {
	return readPolicyRequest(SearchTextToolName, inputStringField(input, "path"))
}

func (t *GetWorkspaceSummaryTool) PolicyRequest(input json.RawMessage) policy.Request {
	return readPolicyRequest(GetWorkspaceSummaryToolName, inputStringField(input, "path"))
}

func (t *WriteFileTool) PolicyRequest(input json.RawMessage) policy.Request {
	request := policy.Request{
		ToolName:  WriteFileToolName,
		Operation: WriteFileToolName,
		Risk:      policy.RiskWrite,
		Path:      inputStringField(input, "path"),
		DryRun:    inputBoolField(input, "dry_run", true),
	}
	if request.DryRun {
		request.Operation = "dry-run write"
	} else if highRisk, _ := highRiskWritePath(request.Path); highRisk {
		request.Operation = "high-risk write"
	}
	return request
}

func (t *ApplyPatchTool) PolicyRequest(input json.RawMessage) policy.Request {
	request := policy.Request{
		ToolName:  ApplyPatchToolName,
		Risk:      policy.RiskWrite,
		Operation: ApplyPatchToolName,
		DryRun:    inputBoolField(input, "dry_run", true),
	}
	if request.DryRun {
		request.Operation = "dry-run patch"
	}

	var req applyPatchInput
	if err := decodeWorkspaceToolInput(ApplyPatchToolName, input, &req); err != nil {
		return request
	}
	filePatches, err := parseUnifiedPatch(req.Patch)
	if err != nil {
		return request
	}

	paths := make([]string, 0, len(filePatches))
	highRisk := false
	for _, filePatch := range filePatches {
		target := filePatch.targetPath()
		if target != "" {
			paths = append(paths, target)
		}
		if filePatch.operation() == "delete" {
			request.Risk = policy.RiskDelete
			request.Operation = "delete"
		}
		if isHighRisk, _ := highRiskWritePath(target); isHighRisk {
			highRisk = true
		}
	}
	request.Path = strings.Join(paths, ",")
	if highRisk && request.Operation != "delete" {
		request.Operation = "high-risk write"
	}
	return request
}

func readPolicyRequest(toolName, path string) policy.Request {
	return policy.Request{
		ToolName:  toolName,
		Operation: toolName,
		Risk:      policy.RiskRead,
		Path:      path,
	}
}

func inputBoolField(input json.RawMessage, field string, fallback bool) bool {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(input, &raw); err != nil {
		return fallback
	}
	value, ok := raw[field]
	if !ok {
		return fallback
	}
	var parsed bool
	if err := json.Unmarshal(value, &parsed); err != nil {
		return fallback
	}
	return parsed
}

func inputStringField(input json.RawMessage, field string) string {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(input, &raw); err != nil {
		return ""
	}
	var value string
	if err := json.Unmarshal(raw[field], &value); err != nil {
		return ""
	}
	return value
}
