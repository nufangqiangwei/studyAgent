package policy

import "testing"

func TestReadOnlyModeAllowsOnlyReadTools(t *testing.T) {
	checker := New(ModeReadOnly)

	allowed := checker.Check(Request{ToolName: "read_file", Risk: RiskRead, Path: "main.go"})
	if allowed.Decision != Allow {
		t.Fatalf("read_file decision = %q, want allow: %s", allowed.Decision, allowed.Reason)
	}

	dryRunWrite := checker.Check(Request{ToolName: "write_file", Risk: RiskWrite, Path: "main.go", DryRun: true})
	if dryRunWrite.Decision != Allow {
		t.Fatalf("write_file dry-run decision = %q, want allow: %s", dryRunWrite.Decision, dryRunWrite.Reason)
	}

	for _, req := range []Request{
		{ToolName: "write_file", Risk: RiskWrite, Path: "main.go"},
		{ToolName: "apply_patch", Risk: RiskWrite, Path: "main.go"},
		{ToolName: "run_command", Risk: RiskExec, Command: []string{"go", "test", "./..."}},
		{ToolName: "network", Risk: RiskNet},
		{ToolName: "delete", Risk: RiskDelete, Path: "main.go"},
	} {
		result := checker.Check(req)
		if result.Decision != Deny {
			t.Fatalf("%s decision = %q, want deny: %s", req.ToolName, result.Decision, result.Reason)
		}
	}
}

func TestValidateModeAllowsLimitedVerificationCommands(t *testing.T) {
	checker := New(ModeValidate)

	dryRunPatch := checker.Check(Request{ToolName: "apply_patch", Risk: RiskWrite, Path: "main.go", DryRun: true})
	if dryRunPatch.Decision != Allow {
		t.Fatalf("apply_patch dry-run decision = %q, want allow: %s", dryRunPatch.Decision, dryRunPatch.Reason)
	}

	for _, command := range [][]string{
		{"go", "test", "./..."},
		{"go", "vet", "./..."},
		{"go", "list", "./..."},
		{"git", "status", "--short"},
		{"git", "diff", "--", "main.go"},
	} {
		result := checker.Check(Request{ToolName: "run_command", Risk: RiskExec, Command: command})
		if result.Decision != Allow {
			t.Fatalf("%v decision = %q, want allow: %s", command, result.Decision, result.Reason)
		}
	}

	for _, req := range []Request{
		{ToolName: "write_file", Risk: RiskWrite, Path: "main.go"},
		{ToolName: "run_command", Risk: RiskExec, Command: []string{"go", "build", "./..."}},
		{ToolName: "run_command", Risk: RiskExec, Command: []string{"sh", "-c", "go test ./..."}},
	} {
		result := checker.Check(req)
		if result.Decision != Deny {
			t.Fatalf("%#v decision = %q, want deny: %s", req, result.Decision, result.Reason)
		}
	}
}

func TestModifyModeAllowsPatchesAndAsksForHighRisk(t *testing.T) {
	checker := New(ModeModify)

	for _, req := range []Request{
		{ToolName: "apply_patch", Risk: RiskWrite, Path: "internal/app/app.go"},
		{ToolName: "write_file", Risk: RiskWrite, Path: "docs/report.md"},
		{ToolName: "run_tests", Risk: RiskExec, Command: []string{"go", "test", "./..."}},
		{ToolName: "git_diff", Risk: RiskRead},
	} {
		result := checker.Check(req)
		if result.Decision != Allow {
			t.Fatalf("%#v decision = %q, want allow: %s", req, result.Decision, result.Reason)
		}
	}

	for _, req := range []Request{
		{ToolName: "apply_patch", Risk: RiskWrite, Path: "go.mod"},
		{ToolName: "apply_patch", Risk: RiskDelete, Path: "notes.txt", Operation: "delete"},
		{ToolName: "run_command", Risk: RiskExec, Command: []string{"go", "build", "./..."}},
		{ToolName: "network", Risk: RiskNet},
	} {
		result := checker.Check(req)
		if result.Decision != Ask {
			t.Fatalf("%#v decision = %q, want ask: %s", req, result.Decision, result.Reason)
		}
	}

	result := checker.Check(Request{ToolName: "write_file", Risk: RiskWrite, Path: "go.mod"})
	if result.Decision != Ask {
		t.Fatalf("write_file high-risk decision = %q, want ask: %s", result.Decision, result.Reason)
	}
}

func TestParseModeAcceptsAliases(t *testing.T) {
	tests := map[string]Mode{
		"":             ModeReadOnly,
		"read-only":    ModeReadOnly,
		"verification": ModeValidate,
		"write":        ModeModify,
	}
	for input, want := range tests {
		got, err := ParseMode(input)
		if err != nil {
			t.Fatalf("ParseMode(%q) returned error: %v", input, err)
		}
		if got != want {
			t.Fatalf("ParseMode(%q) = %q, want %q", input, got, want)
		}
	}
}
