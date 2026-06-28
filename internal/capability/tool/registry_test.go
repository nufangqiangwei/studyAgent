package tool

import (
	"agent/internal/content"
	"agent/internal/foundation/policy"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestRegistryRegisterReturnsPolicyWrappedTool(t *testing.T) {
	inner := &recordingTool{name: "read_file", result: Result{Content: "ok"}}
	registry := NewRegistry()
	if err := registry.RegisterWithPolicyAnalyzer(inner, PolicyAnalyzerFunc(func(json.RawMessage) (PolicyFacts, error) {
		return PolicyFacts{Read: true, Paths: []string{"notes.txt"}, Operation: "read_file"}, nil
	})); err != nil {
		t.Fatalf("RegisterWithPolicyAnalyzer returned error: %v", err)
	}

	wrapped, ok := registry.Lookup("read_file")
	if !ok {
		t.Fatal("Lookup returned ok=false")
	}
	result, err := wrapped.Execute(context.Background(), json.RawMessage(`{"path":"notes.txt"}`))
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if result.Content != "ok" || !inner.called {
		t.Fatalf("result/called = %q/%v, want ok/true", result.Content, inner.called)
	}
	if got := registry.List(); len(got) != 1 || got[0].Name() != "read_file" {
		t.Fatalf("List = %#v, want wrapped read_file", got)
	}
}

func TestPolicyFactsForWriteDryRunBuildsDryRunWriteRequest(t *testing.T) {
	checker := &recordingChecker{result: policy.Result{Decision: policy.Allow, Reason: "ok"}}
	registry := NewRegistry(WithPolicy(checker))
	if err := registry.RegisterWithPolicyAnalyzer(&recordingTool{name: "write_file"}, PolicyAnalyzerFunc(func(json.RawMessage) (PolicyFacts, error) {
		return PolicyFacts{Paths: []string{"notes.txt"}, DryRun: true, Write: true, Operation: "dry-run write"}, nil
	})); err != nil {
		t.Fatalf("RegisterWithPolicyAnalyzer returned error: %v", err)
	}

	wrapped, _ := registry.Lookup("write_file")
	if _, err := wrapped.Execute(context.Background(), json.RawMessage(`{}`)); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	request := checker.lastRequest()
	if request.Risk != policy.RiskWrite || !request.DryRun || request.Operation != "dry-run write" || request.Path != "notes.txt" {
		t.Fatalf("request = %#v, want dry-run write risk", request)
	}
}

func TestPolicyFactsForWriteBuildsWriteRequest(t *testing.T) {
	checker := &recordingChecker{result: policy.Result{Decision: policy.Allow, Reason: "ok"}}
	registry := NewRegistry(WithPolicy(checker))
	if err := registry.RegisterWithPolicyAnalyzer(&recordingTool{name: "write_file"}, PolicyAnalyzerFunc(func(json.RawMessage) (PolicyFacts, error) {
		return PolicyFacts{Paths: []string{"notes.txt"}, Write: true, Operation: "write"}, nil
	})); err != nil {
		t.Fatalf("RegisterWithPolicyAnalyzer returned error: %v", err)
	}

	wrapped, _ := registry.Lookup("write_file")
	if _, err := wrapped.Execute(context.Background(), json.RawMessage(`{}`)); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	request := checker.lastRequest()
	if request.Risk != policy.RiskWrite || request.DryRun || request.Operation != "write" || request.Path != "notes.txt" {
		t.Fatalf("request = %#v, want write risk", request)
	}
}

func TestPolicyFactsForApplyPatchDeleteBuildsDeleteRequest(t *testing.T) {
	request := policyRequestForToolCall("apply_patch", PolicyFacts{
		Paths:     []string{"notes.txt"},
		Write:     true,
		Delete:    true,
		Operation: "delete",
	})
	if request.Risk != policy.RiskDelete || request.Operation != "delete" || request.Path != "notes.txt" {
		t.Fatalf("request = %#v, want delete risk", request)
	}
}

func TestPolicyFactsForHighRiskWriteBuildsHighRiskOperation(t *testing.T) {
	request := policyRequestForToolCall("apply_patch", PolicyFacts{
		Paths:    []string{"go.mod"},
		Write:    true,
		HighRisk: true,
	})
	if request.Risk != policy.RiskWrite || request.Operation != "high-risk write" || request.Path != "go.mod" {
		t.Fatalf("request = %#v, want high-risk write", request)
	}
}

func TestPolicyDenyDoesNotExecuteInnerTool(t *testing.T) {
	inner := &recordingTool{name: "write_file"}
	registry := NewRegistry(WithPolicy(policy.New(policy.ModeReadOnly)))
	if err := registry.RegisterWithPolicyAnalyzer(inner, PolicyAnalyzerFunc(func(json.RawMessage) (PolicyFacts, error) {
		return PolicyFacts{Write: true, Paths: []string{"notes.txt"}, Operation: "write"}, nil
	})); err != nil {
		t.Fatalf("RegisterWithPolicyAnalyzer returned error: %v", err)
	}

	wrapped, _ := registry.Lookup("write_file")
	_, err := wrapped.Execute(context.Background(), json.RawMessage(`{}`))
	if err == nil {
		t.Fatal("Execute returned nil error")
	}
	if !strings.Contains(err.Error(), "policy denied tool") {
		t.Fatalf("error = %q, want policy denied tool", err.Error())
	}
	if inner.called {
		t.Fatal("inner tool executed even though policy denied it")
	}
}

func TestPolicyAskUserDeclineDoesNotExecuteInnerTool(t *testing.T) {
	inner := &recordingTool{name: "network"}
	registry := NewRegistry(WithPolicy(policy.New(policy.ModeModify)))
	if err := registry.RegisterWithPolicyAnalyzer(inner, PolicyAnalyzerFunc(func(json.RawMessage) (PolicyFacts, error) {
		return PolicyFacts{Network: true, Operation: "network"}, nil
	})); err != nil {
		t.Fatalf("RegisterWithPolicyAnalyzer returned error: %v", err)
	}
	var out strings.Builder
	ctx := content.WithEnv(context.Background(), &content.Env{
		IO: content.IO{
			In:  strings.NewReader("no\n"),
			Out: &out,
		},
	})

	wrapped, _ := registry.Lookup("network")
	_, err := wrapped.Execute(ctx, json.RawMessage(`{}`))
	if err == nil {
		t.Fatal("Execute returned nil error")
	}
	if !strings.Contains(err.Error(), "user declined confirmation") {
		t.Fatalf("error = %q, want declined confirmation", err.Error())
	}
	if inner.called {
		t.Fatal("inner tool executed even though user declined policy confirmation")
	}
	if !strings.Contains(out.String(), "Policy confirmation required") {
		t.Fatalf("prompt missing confirmation text:\n%s", out.String())
	}
}

func TestLookupToolExecuteStillTriggersPolicy(t *testing.T) {
	inner := &recordingTool{name: "write_file"}
	registry := NewRegistry(WithPolicy(policy.New(policy.ModeReadOnly)))
	if err := registry.RegisterWithPolicyAnalyzer(inner, PolicyAnalyzerFunc(func(json.RawMessage) (PolicyFacts, error) {
		return PolicyFacts{Write: true, Paths: []string{"notes.txt"}, Operation: "write"}, nil
	})); err != nil {
		t.Fatalf("RegisterWithPolicyAnalyzer returned error: %v", err)
	}

	wrapped, ok := registry.Lookup("write_file")
	if !ok {
		t.Fatal("Lookup returned ok=false")
	}
	_, err := wrapped.Execute(context.Background(), json.RawMessage(`{}`))
	if err == nil {
		t.Fatal("direct wrapped Execute returned nil error")
	}
	if inner.called {
		t.Fatal("inner tool executed through direct Lookup result despite policy denial")
	}
}

type recordingChecker struct {
	requests []policy.Request
	result   policy.Result
}

func (c *recordingChecker) Check(request policy.Request) policy.Result {
	c.requests = append(c.requests, request)
	if c.result.Decision == "" {
		return policy.Result{Decision: policy.Allow, Reason: "ok"}
	}
	return c.result
}

func (c *recordingChecker) lastRequest() policy.Request {
	if len(c.requests) == 0 {
		return policy.Request{}
	}
	return c.requests[len(c.requests)-1]
}

type recordingTool struct {
	name   string
	result Result
	called bool
}

func (t *recordingTool) Name() string {
	return t.name
}

func (t *recordingTool) Description() string {
	return "record calls"
}

func (t *recordingTool) InputSchema() json.RawMessage {
	return json.RawMessage(`{"type":"object"}`)
}

func (t *recordingTool) Execute(context.Context, json.RawMessage) (Result, error) {
	t.called = true
	return t.result, nil
}
