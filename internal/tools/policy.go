package tools

import (
	"agent/internal/content"
	"agent/internal/policy"
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

func policyRequestForToolCall(name string, input json.RawMessage) policy.Request {
	request := policy.Request{
		ToolName:  name,
		Operation: name,
	}
	switch name {
	case AskUserToolName:
		request.Risk = policy.RiskRead
	case ListFilesToolName:
		request.Risk = policy.RiskRead
		request.Path = inputStringField(input, "path")
	case ReadFileToolName:
		request.Risk = policy.RiskRead
		request.Path = inputStringField(input, "path")
	case SearchTextToolName:
		request.Risk = policy.RiskRead
		request.Path = inputStringField(input, "path")
	case GetWorkspaceSummaryToolName:
		request.Risk = policy.RiskRead
		request.Path = inputStringField(input, "path")
	case WriteFileToolName:
		request.Risk = policy.RiskWrite
		request.Path = inputStringField(input, "path")
		request.DryRun = inputBoolField(input, "dry_run", true)
		if request.DryRun {
			request.Operation = "dry-run write"
		}
	case ApplyPatchToolName:
		request = applyPatchPolicyRequest(input)
	default:
		request.Risk = inferredPolicyRiskForToolName(name)
		if name == "run_command" {
			request.Command = firstInputStringSliceField(input, "command", "cmd", "args")
		}
	}
	return request
}

func applyPatchPolicyRequest(input json.RawMessage) policy.Request {
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

func firstInputStringSliceField(input json.RawMessage, fields ...string) []string {
	for _, field := range fields {
		values := inputStringSliceField(input, field)
		if len(values) > 0 {
			return values
		}
	}
	return nil
}

func inputStringSliceField(input json.RawMessage, field string) []string {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(input, &raw); err != nil {
		return nil
	}
	var values []string
	if err := json.Unmarshal(raw[field], &values); err == nil {
		return values
	}
	var value string
	if err := json.Unmarshal(raw[field], &value); err == nil {
		return strings.Fields(value)
	}
	return nil
}

func inferredPolicyRiskForToolName(name string) policy.RiskLevel {
	switch name {
	case "git_status", "git_diff":
		return policy.RiskRead
	case "run_command", "run_tests":
		return policy.RiskExec
	case "network":
		return policy.RiskNet
	case "delete":
		return policy.RiskDelete
	default:
		return ""
	}
}

func confirmPolicyDecision(ctx context.Context, request policy.Request, decision policy.Result) (bool, error) {
	env, ok := content.EnvFromContext(ctx)
	if !ok {
		return false, errors.New("env is required")
	}
	if env.IO.In == nil {
		return false, errors.New("input reader is required")
	}
	if env.IO.Out == nil {
		return false, errors.New("output writer is required")
	}
	if _, err := fmt.Fprintf(env.IO.Out, "! Policy confirmation required: %s\nExecute %s? [y/N] ", decision.Reason, policyRequestSummary(request)); err != nil {
		return false, fmt.Errorf("write confirmation prompt: %w", err)
	}
	line, readErr := bufio.NewReader(env.IO.In).ReadString('\n')
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return false, fmt.Errorf("read confirmation: %w", readErr)
	}
	answer := strings.ToLower(strings.TrimSpace(line))
	return answer == "y" || answer == "yes", nil
}

func policyRequestSummary(request policy.Request) string {
	parts := []string{fmt.Sprintf("tool %q", request.ToolName)}
	if request.Path != "" {
		parts = append(parts, "path "+request.Path)
	}
	if len(request.Command) > 0 {
		parts = append(parts, "command "+strings.Join(request.Command, " "))
	}
	return strings.Join(parts, ", ")
}
