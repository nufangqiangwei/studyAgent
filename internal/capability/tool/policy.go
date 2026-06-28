package tool

import (
	"agent/internal/content"
	"agent/internal/foundation/policy"
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
		Risk:      inferredPolicyRiskForToolName(name),
	}
	if name == "run_command" {
		request.Command = firstInputStringSliceField(input, "command", "cmd", "args")
	}
	return request
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
