package policy

import (
	"fmt"
	"path/filepath"
	"strings"
)

type RiskLevel string

const (
	RiskRead   RiskLevel = "read"
	RiskWrite  RiskLevel = "write"
	RiskExec   RiskLevel = "exec"
	RiskNet    RiskLevel = "net"
	RiskDelete RiskLevel = "delete"
)

type Decision string

const (
	Allow Decision = "allow"
	Deny  Decision = "deny"
	Ask   Decision = "ask"
)

type Mode string

const (
	ModeReadOnly Mode = "read"
	ModeValidate Mode = "validate"
	ModeModify   Mode = "modify"
)

type Request struct {
	ToolName  string    `json:"tool_name"`
	Risk      RiskLevel `json:"risk,omitempty"`
	Operation string    `json:"operation,omitempty"`
	Path      string    `json:"path,omitempty"`
	Command   []string  `json:"command,omitempty"`
	DryRun    bool      `json:"dry_run,omitempty"`
}

type Result struct {
	Decision Decision `json:"decision"`
	Reason   string   `json:"reason"`
}

type Checker interface {
	Check(Request) Result
}

type Static struct {
	Mode Mode
}

func New(mode Mode) Static {
	return Static{Mode: normalizeMode(mode)}
}

func Default() Static {
	return New(ModeReadOnly)
}

func ParseMode(value string) (Mode, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "read", "readonly", "read-only":
		return ModeReadOnly, nil
	case "validate", "validation", "verify", "verification":
		return ModeValidate, nil
	case "modify", "write", "edit":
		return ModeModify, nil
	default:
		return "", fmt.Errorf("unknown policy mode %q; want read, validate, or modify", value)
	}
}

func (p Static) Check(req Request) Result {
	mode := normalizeMode(p.Mode)
	req = normalizeRequest(req)

	switch mode {
	case ModeReadOnly:
		return checkReadOnly(req)
	case ModeValidate:
		return checkValidate(req)
	case ModeModify:
		return checkModify(req)
	default:
		return deny(req, "unknown policy mode")
	}
}

func normalizeMode(mode Mode) Mode {
	parsed, err := ParseMode(string(mode))
	if err != nil {
		return ModeReadOnly
	}
	return parsed
}

func normalizeRequest(req Request) Request {
	req.ToolName = strings.TrimSpace(req.ToolName)
	req.Operation = strings.TrimSpace(req.Operation)
	req.Path = normalizePolicyPaths(req.Path)
	req.Command = normalizeCommand(req.Command)
	if req.Operation == "" {
		req.Operation = req.ToolName
	}
	if req.Risk == "" {
		req.Risk = inferRisk(req)
	}
	return req
}

func checkReadOnly(req Request) Result {
	if isReadAllowed(req) {
		return allow(req, "read-only mode allows read-only tool")
	}
	if isDryRunWriteAllowed(req) {
		return allow(req, "read-only mode allows dry-run write validation")
	}
	return deny(req, "read-only mode blocks writes, command execution, network, and delete operations")
}

func checkValidate(req Request) Result {
	if isReadAllowed(req) {
		return allow(req, "validation mode allows read-only tool")
	}
	if isDryRunWriteAllowed(req) {
		return allow(req, "validation mode allows dry-run write validation")
	}
	if req.Risk == RiskExec && isValidationCommand(req.Command) {
		return allow(req, "validation mode allows limited verification commands")
	}
	return deny(req, "validation mode allows reads and limited verification commands only")
}

func checkModify(req Request) Result {
	if isReadAllowed(req) {
		return allow(req, "modify mode allows read-only tool")
	}
	if isDryRunWriteAllowed(req) {
		return allow(req, "modify mode allows dry-run write validation")
	}
	if req.Risk == RiskDelete {
		return ask(req, "modify mode requires user confirmation before delete operations")
	}
	if req.Risk == RiskNet {
		return ask(req, "modify mode requires user confirmation before network operations")
	}
	if (req.ToolName == "apply_patch" || req.ToolName == "write_file") && req.Risk == RiskWrite {
		if isHighRiskPath(req.Path) || strings.Contains(strings.ToLower(req.Operation), "high-risk") {
			return ask(req, "modify mode requires user confirmation for high-risk write paths")
		}
		return allow(req, "modify mode allows workspace writes")
	}
	if req.ToolName == "run_tests" {
		return allow(req, "modify mode allows run_tests")
	}
	if req.ToolName == "run_command" && req.Risk == RiskExec {
		if isValidationCommand(req.Command) {
			return allow(req, "modify mode allows verification commands")
		}
		return ask(req, "modify mode requires user confirmation before arbitrary commands")
	}
	return deny(req, "modify mode allows apply_patch, write_file, run_tests, git_diff, and read-only tool by default")
}

func allow(req Request, reason string) Result {
	return Result{Decision: Allow, Reason: reasonFor(req, reason)}
}

func deny(req Request, reason string) Result {
	return Result{Decision: Deny, Reason: reasonFor(req, reason)}
}

func ask(req Request, reason string) Result {
	return Result{Decision: Ask, Reason: reasonFor(req, reason)}
}

func reasonFor(req Request, reason string) string {
	details := make([]string, 0, 4)
	if req.ToolName != "" {
		details = append(details, "tool="+req.ToolName)
	}
	if req.Risk != "" {
		details = append(details, "risk="+string(req.Risk))
	}
	if req.Path != "" && req.Path != "." {
		details = append(details, "path="+req.Path)
	}
	if len(req.Command) > 0 {
		details = append(details, "command="+strings.Join(req.Command, " "))
	}
	if req.DryRun {
		details = append(details, "dry_run=true")
	}
	if len(details) == 0 {
		return reason
	}
	return reason + " (" + strings.Join(details, ", ") + ")"
}

func isReadAllowed(req Request) bool {
	if req.Risk != RiskRead {
		return false
	}
	switch req.ToolName {
	case "ask_user", "list_files", "read_file", "search_text", "get_workspace_summary", "git_status", "git_diff":
		return true
	default:
		return false
	}
}

func isDryRunWriteAllowed(req Request) bool {
	if !req.DryRun {
		return false
	}
	switch req.ToolName {
	case "write_file", "apply_patch":
		return true
	default:
		return false
	}
}

func inferRisk(req Request) RiskLevel {
	switch req.ToolName {
	case "ask_user", "list_files", "read_file", "search_text", "get_workspace_summary", "git_status", "git_diff":
		return RiskRead
	case "write_file", "apply_patch":
		return RiskWrite
	case "run_command", "run_tests":
		return RiskExec
	case "network":
		return RiskNet
	case "delete":
		return RiskDelete
	default:
		switch strings.ToLower(req.Operation) {
		case "read":
			return RiskRead
		case "write":
			return RiskWrite
		case "exec":
			return RiskExec
		case "network", "net":
			return RiskNet
		case "delete":
			return RiskDelete
		default:
			return ""
		}
	}
}

func isValidationCommand(command []string) bool {
	if len(command) < 2 {
		return false
	}
	name := strings.ToLower(filepath.Base(command[0]))
	name = strings.TrimSuffix(name, ".exe")
	subcommand := strings.ToLower(command[1])
	switch name {
	case "go":
		return subcommand == "test" || subcommand == "vet" || subcommand == "list"
	case "git":
		return subcommand == "status" || subcommand == "diff"
	default:
		return false
	}
}

func isHighRiskPath(pathValue string) bool {
	for _, clean := range splitPolicyPaths(pathValue) {
		if clean == "" || clean == "." {
			continue
		}
		base := filepath.Base(clean)
		if strings.HasPrefix(base, ".env") {
			return true
		}
		if strings.HasPrefix(clean, ".github/workflows/") || clean == ".github/workflows" {
			return true
		}
		if _, ok := highRiskFileNames[base]; ok {
			return true
		}
	}
	return false
}

func normalizePolicyPaths(value string) string {
	return strings.Join(splitPolicyPaths(value), ",")
}

func splitPolicyPaths(value string) []string {
	value = strings.TrimSpace(strings.ReplaceAll(value, "\\", "/"))
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	paths := make([]string, 0, len(parts))
	for _, part := range parts {
		clean := strings.Trim(strings.TrimSpace(part), `"'`)
		if clean == "" {
			continue
		}
		paths = append(paths, strings.TrimPrefix(filepath.ToSlash(filepath.Clean(clean)), "./"))
	}
	return paths
}

func normalizeCommand(command []string) []string {
	if len(command) == 0 {
		return nil
	}
	normalized := make([]string, 0, len(command))
	for _, arg := range command {
		arg = strings.TrimSpace(arg)
		if arg != "" {
			normalized = append(normalized, arg)
		}
	}
	return normalized
}

var highRiskFileNames = map[string]struct{}{
	".npmrc":              {},
	".pypirc":             {},
	"AGENTS.md":           {},
	"Cargo.lock":          {},
	"Cargo.toml":          {},
	"Dockerfile":          {},
	"docker-compose.yaml": {},
	"docker-compose.yml":  {},
	"go.mod":              {},
	"go.sum":              {},
	"id_rsa":              {},
	"package-lock.json":   {},
	"package.json":        {},
	"pnpm-lock.yaml":      {},
	"yarn.lock":           {},
}
