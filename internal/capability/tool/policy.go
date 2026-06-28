package tool

import (
	"agent/internal/content"
	"agent/internal/foundation/policy"
	"agent/internal/session"
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

type policyTool struct {
	inner    Tool
	analyzer PolicyAnalyzer
	checker  policy.Checker
}

type policyDecisionEventPayload struct {
	Request           policy.Request `json:"request"`
	Result            policy.Result  `json:"result"`
	Confirmed         *bool          `json:"confirmed,omitempty"`
	ConfirmationError string         `json:"confirmation_error,omitempty"`
}

func newPolicyTool(inner Tool, analyzer PolicyAnalyzer, checker policy.Checker) Tool {
	return &policyTool{inner: inner, analyzer: analyzer, checker: checker}
}

func (t *policyTool) Name() string {
	if t == nil || t.inner == nil {
		return ""
	}
	return t.inner.Name()
}

func (t *policyTool) Description() string {
	if t == nil || t.inner == nil {
		return ""
	}
	return t.inner.Description()
}

func (t *policyTool) InputSchema() json.RawMessage {
	if t == nil || t.inner == nil {
		return nil
	}
	return t.inner.InputSchema()
}

func (t *policyTool) Execute(ctx context.Context, input json.RawMessage) (Result, error) {
	if t == nil || t.inner == nil {
		return Result{}, errors.New("policy tool: inner tool is nil")
	}
	request, err := t.policyRequest(input)
	if err != nil {
		return Result{}, err
	}
	checker := t.checker
	if checker == nil {
		checker = policy.Default()
	}
	decision := checker.Check(request)
	switch decision.Decision {
	case policy.Allow:
		if err := recordPolicyDecisionEvent(ctx, request, decision, nil, ""); err != nil {
			return Result{}, err
		}
	case policy.Ask:
		confirmed, err := confirmPolicyDecision(ctx, request, decision)
		if err != nil {
			if recordErr := recordPolicyDecisionEvent(ctx, request, decision, nil, err.Error()); recordErr != nil {
				return Result{}, recordErr
			}
			return Result{}, fmt.Errorf("policy confirmation for tool %q: %w", t.Name(), err)
		}
		if err := recordPolicyDecisionEvent(ctx, request, decision, &confirmed, ""); err != nil {
			return Result{}, err
		}
		if !confirmed {
			return Result{}, fmt.Errorf("policy denied tool %q: user declined confirmation: %s", t.Name(), decision.Reason)
		}
	case policy.Deny:
		if err := recordPolicyDecisionEvent(ctx, request, decision, nil, ""); err != nil {
			return Result{}, err
		}
		return Result{}, fmt.Errorf("policy denied tool %q: %s", t.Name(), decision.Reason)
	default:
		if err := recordPolicyDecisionEvent(ctx, request, decision, nil, ""); err != nil {
			return Result{}, err
		}
		return Result{}, fmt.Errorf("policy returned unknown decision %q for tool %q", decision.Decision, t.Name())
	}
	return t.inner.Execute(ctx, input)
}

func (t *policyTool) policyRequest(input json.RawMessage) (policy.Request, error) {
	var facts PolicyFacts
	if t.analyzer != nil {
		analyzed, err := t.analyzer.AnalyzePolicy(input)
		if err != nil {
			return policy.Request{}, fmt.Errorf("policy analysis for tool %q: %w", t.Name(), err)
		}
		facts = analyzed
	}
	return policyRequestForToolCall(t.Name(), facts), nil
}

func policyRequestForToolCall(name string, facts PolicyFacts) policy.Request {
	operation := strings.TrimSpace(facts.Operation)
	if operation == "" {
		operation = name
	}
	if facts.HighRisk && facts.Write && !facts.Delete && !facts.DryRun && !strings.Contains(strings.ToLower(operation), "high-risk") {
		operation = "high-risk write"
	}
	return policy.Request{
		ToolName:  name,
		Risk:      policyRiskForFacts(facts),
		Operation: operation,
		Path:      strings.Join(normalizedPolicyFactPaths(facts.Paths), ","),
		Command:   normalizedPolicyFactCommand(facts.Command),
		DryRun:    facts.DryRun,
	}
}

func policyRiskForFacts(facts PolicyFacts) policy.RiskLevel {
	switch {
	case facts.Delete:
		return policy.RiskDelete
	case facts.Network:
		return policy.RiskNet
	case facts.Exec:
		return policy.RiskExec
	case facts.Write:
		return policy.RiskWrite
	case facts.Read:
		return policy.RiskRead
	default:
		return ""
	}
}

func normalizedPolicyFactPaths(paths []string) []string {
	if len(paths) == 0 {
		return nil
	}
	cleaned := make([]string, 0, len(paths))
	for _, path := range paths {
		path = strings.TrimSpace(path)
		if path != "" {
			cleaned = append(cleaned, path)
		}
	}
	return cleaned
}

func normalizedPolicyFactCommand(command []string) []string {
	if len(command) == 0 {
		return nil
	}
	cleaned := make([]string, 0, len(command))
	for _, arg := range command {
		arg = strings.TrimSpace(arg)
		if arg != "" {
			cleaned = append(cleaned, arg)
		}
	}
	return cleaned
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

func recordPolicyDecisionEvent(ctx context.Context, request policy.Request, result policy.Result, confirmed *bool, confirmationError string) error {
	env, ok := content.EnvFromContext(ctx)
	if !ok || env.Session == nil {
		return nil
	}
	scope := env.EventScope
	if scope.AgentName == "" {
		scope.AgentName = env.Config.AgentName
	}
	err := session.SaveEvent(ctx, env.Session, scope, session.EventTypePolicyDecision, policyDecisionEventPayload{
		Request:           request,
		Result:            result,
		Confirmed:         confirmed,
		ConfirmationError: confirmationError,
	})
	if err != nil {
		return fmt.Errorf("record policy decision event: %w", err)
	}
	return nil
}
