package workspace

import (
	"agent/internal/capability/tool"
	"agent/internal/foundation/workspace"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

type Result = tool.Result

const (
	ApplyPatchToolName = "apply_patch"
	WriteFileToolName  = "write_file"

	maxPatchBytes       = 256 * 1024
	maxPatchTargetBytes = 4 * 1024 * 1024
	maxWriteFileBytes   = 1024 * 1024
)

var unifiedHunkHeaderRE = regexp.MustCompile(`^@@ -([0-9]+)(?:,([0-9]+))? \+([0-9]+)(?:,([0-9]+))? @@`)

type ApplyPatchTool struct{}

func NewApplyPatchTool() *ApplyPatchTool {
	return &ApplyPatchTool{}
}

func (t *ApplyPatchTool) Name() string {
	return ApplyPatchToolName
}

func (t *ApplyPatchTool) Description() string {
	return "Validate and apply a small unified diff patch to workspace text files. Prefer this over write_file for code edits; dry_run defaults to true."
}

func (t *ApplyPatchTool) InputSchema() json.RawMessage {
	return json.RawMessage(`{
  "type": "object",
  "properties": {
    "patch": {
      "type": "string",
      "description": "Unified diff text. Use small, reviewable patches."
    },
    "dry_run": {
      "type": "boolean",
      "description": "Validate only when true. Defaults to true; set false only after review."
    },
    "confirm_high_risk": {
      "type": "boolean",
      "description": "Must be true to modify high-risk paths such as dependency manifests, lockfiles, env files, Dockerfiles, or CI workflows after user confirmation."
    },
    "allow_delete": {
      "type": "boolean",
      "description": "Must be true to allow file deletion patches."
    }
  },
  "required": ["patch"],
  "additionalProperties": false
}`)
}

func (t *ApplyPatchTool) Execute(ctx context.Context, input json.RawMessage) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if t == nil {
		return Result{}, errors.New("apply_patch: tool is nil")
	}

	var req applyPatchInput
	if err := decodeWorkspaceToolInput(ApplyPatchToolName, input, &req); err != nil {
		return Result{}, err
	}
	if err := req.validate(); err != nil {
		return Result{}, err
	}

	filePatches, err := parseUnifiedPatch(req.Patch)
	if err != nil {
		return Result{}, fmt.Errorf("apply_patch: %w", err)
	}
	ws, err := workspaceFromContext(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("apply_patch: %w", err)
	}
	changes, err := preparePatchChanges(ctx, ws, filePatches, req)
	if err != nil {
		return Result{}, fmt.Errorf("apply_patch: %w", err)
	}

	dryRun := req.dryRun()
	if !dryRun {
		if err := writePatchChanges(ctx, changes); err != nil {
			return Result{}, fmt.Errorf("apply_patch: %w", err)
		}
	}

	out := applyPatchOutput{
		DryRun:     dryRun,
		Applied:    !dryRun,
		PatchBytes: len([]byte(req.Patch)),
		Files:      summarizePatchChanges(changes),
	}
	raw, err := json.Marshal(out)
	if err != nil {
		return Result{}, fmt.Errorf("apply_patch: marshal result: %w", err)
	}
	return Result{
		Content:  formatApplyPatchOutput(out),
		Metadata: map[string]any{"dry_run": out.DryRun, "applied": out.Applied, "files": len(out.Files)},
		Raw:      raw,
	}, nil
}

type WriteFileTool struct{}

func NewWriteFileTool() *WriteFileTool {
	return &WriteFileTool{}
}

func (t *WriteFileTool) Name() string {
	return WriteFileToolName
}

func (t *WriteFileTool) Description() string {
	return "Write a complete small text file in the workspace. Prefer apply_patch for edits; dry_run defaults to true and existing files require overwrite=true."
}

func (t *WriteFileTool) InputSchema() json.RawMessage {
	return json.RawMessage(`{
  "type": "object",
  "properties": {
    "path": {
      "type": "string",
      "description": "Workspace-relative file path to write."
    },
    "content": {
      "type": "string",
      "description": "Complete UTF-8 text content to write. Binary content is not allowed."
    },
    "dry_run": {
      "type": "boolean",
      "description": "Validate only when true. Defaults to true."
    },
    "overwrite": {
      "type": "boolean",
      "description": "Must be true to replace an existing file."
    },
    "confirm_high_risk": {
      "type": "boolean",
      "description": "Must be true to modify high-risk paths after user confirmation."
    }
  },
  "required": ["path", "content"],
  "additionalProperties": false
}`)
}

func (t *WriteFileTool) Execute(ctx context.Context, input json.RawMessage) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if t == nil {
		return Result{}, errors.New("write_file: tool is nil")
	}

	var req writeFileInput
	if err := decodeWorkspaceToolInput(WriteFileToolName, input, &req); err != nil {
		return Result{}, err
	}
	if err := req.validate(); err != nil {
		return Result{}, err
	}

	ws, err := workspaceFromContext(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("write_file: %w", err)
	}
	rel, abs, err := resolveWritablePath(ws, req.Path, WriteFileToolName, req.ConfirmHighRisk)
	if err != nil {
		return Result{}, fmt.Errorf("write_file: %w", err)
	}

	exists, oldBytes, mode, err := inspectWritableTarget(rel, abs)
	if err != nil {
		return Result{}, fmt.Errorf("write_file: %w", err)
	}
	if exists && !req.Overwrite {
		return Result{}, fmt.Errorf("write_file: %q already exists; set overwrite=true or use apply_patch for edits", rel)
	}

	dryRun := req.dryRun()
	if !dryRun {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		if err := os.MkdirAll(filepath.Dir(abs), 0755); err != nil {
			return Result{}, fmt.Errorf("create parent directories for %q: %w", rel, err)
		}
		if err := os.WriteFile(abs, []byte(req.Content), mode); err != nil {
			return Result{}, fmt.Errorf("write %q: %w", rel, err)
		}
	}

	out := writeFileOutput{
		Path:      rel,
		DryRun:    dryRun,
		Applied:   !dryRun,
		Created:   !exists,
		OldBytes:  oldBytes,
		NewBytes:  len([]byte(req.Content)),
		Overwrite: req.Overwrite,
	}
	raw, err := json.Marshal(out)
	if err != nil {
		return Result{}, fmt.Errorf("write_file: marshal result: %w", err)
	}
	return Result{
		Content:  formatWriteFileOutput(out),
		Metadata: map[string]any{"path": out.Path, "dry_run": out.DryRun, "applied": out.Applied, "created": out.Created},
		Raw:      raw,
	}, nil
}

type applyPatchInput struct {
	Patch           string `json:"patch"`
	DryRun          *bool  `json:"dry_run"`
	ConfirmHighRisk bool   `json:"confirm_high_risk"`
	AllowDelete     bool   `json:"allow_delete"`
}

func (i applyPatchInput) dryRun() bool {
	if i.DryRun == nil {
		return true
	}
	return *i.DryRun
}

func (i applyPatchInput) validate() error {
	if strings.TrimSpace(i.Patch) == "" {
		return fmt.Errorf("apply_patch: patch is required")
	}
	if len([]byte(i.Patch)) > maxPatchBytes {
		return fmt.Errorf("apply_patch: patch size %d exceeds limit %d", len([]byte(i.Patch)), maxPatchBytes)
	}
	if containsBinaryText(i.Patch) || strings.Contains(i.Patch, "GIT binary patch") || strings.Contains(i.Patch, "Binary files ") {
		return fmt.Errorf("apply_patch: binary patches are not allowed")
	}
	return nil
}

type writeFileInput struct {
	Path            string `json:"path"`
	Content         string `json:"content"`
	DryRun          *bool  `json:"dry_run"`
	Overwrite       bool   `json:"overwrite"`
	ConfirmHighRisk bool   `json:"confirm_high_risk"`
}

func (i writeFileInput) dryRun() bool {
	if i.DryRun == nil {
		return true
	}
	return *i.DryRun
}

func (i writeFileInput) validate() error {
	if normalizeToolPath(i.Path) == "." {
		return fmt.Errorf("write_file: path is required")
	}
	if len([]byte(i.Content)) > maxWriteFileBytes {
		return fmt.Errorf("write_file: content size %d exceeds limit %d", len([]byte(i.Content)), maxWriteFileBytes)
	}
	if containsBinaryText(i.Content) {
		return fmt.Errorf("write_file: binary content is not allowed")
	}
	return nil
}

type unifiedFilePatch struct {
	OldPath string
	NewPath string
	Hunks   []unifiedHunk
}

type unifiedHunk struct {
	OldStart int
	OldCount int
	NewStart int
	NewCount int
	Lines    []unifiedHunkLine
}

type unifiedHunkLine struct {
	Kind byte
	Text string
}

type patchChange struct {
	Path      string
	AbsPath   string
	Operation string
	Content   []byte
	Mode      os.FileMode
	OldBytes  int64
	NewBytes  int
	Hunks     int
}

type applyPatchOutput struct {
	DryRun     bool              `json:"dry_run"`
	Applied    bool              `json:"applied"`
	PatchBytes int               `json:"patch_bytes"`
	Files      []patchFileOutput `json:"files"`
}

type patchFileOutput struct {
	Path      string `json:"path"`
	Operation string `json:"operation"`
	Hunks     int    `json:"hunks"`
	OldBytes  int64  `json:"old_bytes"`
	NewBytes  int    `json:"new_bytes"`
}

type writeFileOutput struct {
	Path      string `json:"path"`
	DryRun    bool   `json:"dry_run"`
	Applied   bool   `json:"applied"`
	Created   bool   `json:"created"`
	OldBytes  int64  `json:"old_bytes"`
	NewBytes  int    `json:"new_bytes"`
	Overwrite bool   `json:"overwrite"`
}

func parseUnifiedPatch(patch string) ([]unifiedFilePatch, error) {
	normalized := strings.ReplaceAll(patch, "\r\n", "\n")
	normalized = strings.ReplaceAll(normalized, "\r", "\n")
	lines := strings.Split(normalized, "\n")

	files := make([]unifiedFilePatch, 0)
	for i := 0; i < len(lines); {
		if !strings.HasPrefix(lines[i], "--- ") {
			i++
			continue
		}

		oldPath := parseUnifiedPath(strings.TrimPrefix(lines[i], "--- "))
		i++
		if i >= len(lines) || !strings.HasPrefix(lines[i], "+++ ") {
			return nil, fmt.Errorf("expected +++ path after --- path")
		}
		newPath := parseUnifiedPath(strings.TrimPrefix(lines[i], "+++ "))
		i++

		filePatch := unifiedFilePatch{OldPath: oldPath, NewPath: newPath}
		for i < len(lines) {
			if strings.HasPrefix(lines[i], "diff --git ") {
				break
			}
			if strings.HasPrefix(lines[i], "--- ") && i+1 < len(lines) && strings.HasPrefix(lines[i+1], "+++ ") {
				break
			}
			if strings.HasPrefix(lines[i], "@@ ") {
				hunk, next, err := parseUnifiedHunk(lines, i)
				if err != nil {
					return nil, err
				}
				filePatch.Hunks = append(filePatch.Hunks, hunk)
				i = next
				continue
			}
			i++
		}
		if len(filePatch.Hunks) == 0 {
			return nil, fmt.Errorf("patch for %q has no hunks", filePatch.targetPath())
		}
		files = append(files, filePatch)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no unified diff file patches found")
	}
	return files, nil
}

func parseUnifiedPath(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "/dev/null" {
		return ""
	}
	if tab := strings.IndexByte(raw, '\t'); tab >= 0 {
		raw = raw[:tab]
	} else if fields := strings.Fields(raw); len(fields) > 0 {
		raw = fields[0]
	}
	if strings.HasPrefix(raw, "a/") || strings.HasPrefix(raw, "b/") {
		raw = raw[2:]
	}
	return normalizeToolPath(raw)
}

func parseUnifiedHunk(lines []string, start int) (unifiedHunk, int, error) {
	matches := unifiedHunkHeaderRE.FindStringSubmatch(lines[start])
	if matches == nil {
		return unifiedHunk{}, start, fmt.Errorf("invalid hunk header %q", lines[start])
	}
	oldStart, err := parsePositiveInt(matches[1])
	if err != nil {
		return unifiedHunk{}, start, fmt.Errorf("invalid old hunk start: %w", err)
	}
	oldCount, err := parseOptionalCount(matches[2])
	if err != nil {
		return unifiedHunk{}, start, fmt.Errorf("invalid old hunk count: %w", err)
	}
	newStart, err := parsePositiveInt(matches[3])
	if err != nil {
		return unifiedHunk{}, start, fmt.Errorf("invalid new hunk start: %w", err)
	}
	newCount, err := parseOptionalCount(matches[4])
	if err != nil {
		return unifiedHunk{}, start, fmt.Errorf("invalid new hunk count: %w", err)
	}

	hunk := unifiedHunk{
		OldStart: oldStart,
		OldCount: oldCount,
		NewStart: newStart,
		NewCount: newCount,
	}

	oldSeen := 0
	newSeen := 0
	i := start + 1
	for i < len(lines) && (oldSeen < oldCount || newSeen < newCount) {
		line := lines[i]
		if strings.HasPrefix(line, `\ No newline at end of file`) {
			i++
			continue
		}
		if line == "" {
			return unifiedHunk{}, start, fmt.Errorf("invalid empty patch line in hunk %q", lines[start])
		}

		kind := line[0]
		text := line[1:]
		switch kind {
		case ' ':
			oldSeen++
			newSeen++
		case '-':
			oldSeen++
		case '+':
			newSeen++
		default:
			return unifiedHunk{}, start, fmt.Errorf("invalid patch line prefix %q in hunk %q", string(kind), lines[start])
		}
		hunk.Lines = append(hunk.Lines, unifiedHunkLine{Kind: kind, Text: text})
		i++
	}
	for i < len(lines) && strings.HasPrefix(lines[i], `\ No newline at end of file`) {
		i++
	}
	if oldSeen != oldCount || newSeen != newCount {
		return unifiedHunk{}, start, fmt.Errorf("hunk %q line counts old=%d/%d new=%d/%d", lines[start], oldSeen, oldCount, newSeen, newCount)
	}
	return hunk, i, nil
}

func parsePositiveInt(value string) (int, error) {
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, err
	}
	if parsed < 0 {
		return 0, fmt.Errorf("must be >= 0")
	}
	return parsed, nil
}

func parseOptionalCount(value string) (int, error) {
	if value == "" {
		return 1, nil
	}
	return parsePositiveInt(value)
}

func preparePatchChanges(ctx context.Context, ws *workspace.LocalWorkspace, filePatches []unifiedFilePatch, req applyPatchInput) ([]patchChange, error) {
	changes := make([]patchChange, 0, len(filePatches))
	seen := make(map[string]struct{}, len(filePatches))
	for _, filePatch := range filePatches {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if filePatch.OldPath != "" && filePatch.NewPath != "" && filePatch.OldPath != filePatch.NewPath {
			return nil, fmt.Errorf("renames are not supported: %q -> %q", filePatch.OldPath, filePatch.NewPath)
		}

		operation := filePatch.operation()
		if operation == "delete" && !req.AllowDelete {
			return nil, fmt.Errorf("delete patch for %q requires allow_delete=true", filePatch.targetPath())
		}

		rel, abs, err := resolveWritablePath(ws, filePatch.targetPath(), ApplyPatchToolName, req.ConfirmHighRisk)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[rel]; ok {
			return nil, fmt.Errorf("duplicate patch target %q", rel)
		}
		seen[rel] = struct{}{}

		change, err := prepareSinglePatchChange(ctx, rel, abs, operation, filePatch)
		if err != nil {
			return nil, err
		}
		changes = append(changes, change)
	}
	return changes, nil
}

func prepareSinglePatchChange(ctx context.Context, rel, abs, operation string, filePatch unifiedFilePatch) (patchChange, error) {
	mode := os.FileMode(0644)
	oldBytes := int64(0)
	originalLines := []string{}
	originalFinalNewline := false
	eol := "\n"

	info, statErr := os.Stat(abs)
	exists := statErr == nil
	if statErr != nil && !os.IsNotExist(statErr) {
		return patchChange{}, fmt.Errorf("stat %q: %w", rel, statErr)
	}

	switch operation {
	case "add":
		if exists {
			return patchChange{}, fmt.Errorf("add patch target %q already exists", rel)
		}
	case "update", "delete":
		if !exists {
			return patchChange{}, fmt.Errorf("%s patch target %q does not exist", operation, rel)
		}
		if info.IsDir() {
			return patchChange{}, fmt.Errorf("%s patch target %q is a directory", operation, rel)
		}
		if info.Size() > maxPatchTargetBytes {
			return patchChange{}, fmt.Errorf("%s patch target %q size %d exceeds limit %d", operation, rel, info.Size(), maxPatchTargetBytes)
		}
		if err := rejectExistingBinary(rel, abs); err != nil {
			return patchChange{}, err
		}
		data, err := os.ReadFile(abs)
		if err != nil {
			return patchChange{}, fmt.Errorf("read %q: %w", rel, err)
		}
		if err := ctx.Err(); err != nil {
			return patchChange{}, err
		}
		originalLines, originalFinalNewline, eol = splitFileText(string(data))
		mode = info.Mode()
		oldBytes = info.Size()
	default:
		return patchChange{}, fmt.Errorf("unsupported patch operation %q", operation)
	}

	newLines, err := applyUnifiedHunks(originalLines, filePatch.Hunks)
	if err != nil {
		return patchChange{}, fmt.Errorf("%s: %w", rel, err)
	}
	if operation == "delete" && len(newLines) != 0 {
		return patchChange{}, fmt.Errorf("delete patch for %q did not remove all content", rel)
	}

	finalNewline := originalFinalNewline
	if operation == "add" && len(newLines) > 0 {
		finalNewline = true
	}
	content := []byte(nil)
	if operation != "delete" {
		content = []byte(joinFileText(newLines, finalNewline, eol))
		if containsBinaryBytes(content) {
			return patchChange{}, fmt.Errorf("patch result for %q is binary; binary files are not allowed", rel)
		}
	}

	return patchChange{
		Path:      rel,
		AbsPath:   abs,
		Operation: operation,
		Content:   content,
		Mode:      mode,
		OldBytes:  oldBytes,
		NewBytes:  len(content),
		Hunks:     len(filePatch.Hunks),
	}, nil
}

func writePatchChanges(ctx context.Context, changes []patchChange) error {
	for _, change := range changes {
		if err := ctx.Err(); err != nil {
			return err
		}
		switch change.Operation {
		case "delete":
			if err := os.Remove(change.AbsPath); err != nil {
				return fmt.Errorf("delete %q: %w", change.Path, err)
			}
		case "add", "update":
			if err := os.MkdirAll(filepath.Dir(change.AbsPath), 0755); err != nil {
				return fmt.Errorf("create parent directories for %q: %w", change.Path, err)
			}
			if err := os.WriteFile(change.AbsPath, change.Content, change.Mode); err != nil {
				return fmt.Errorf("write %q: %w", change.Path, err)
			}
		default:
			return fmt.Errorf("unsupported patch operation %q", change.Operation)
		}
	}
	return nil
}

func applyUnifiedHunks(original []string, hunks []unifiedHunk) ([]string, error) {
	result := make([]string, 0, len(original))
	oldIndex := 0

	for _, hunk := range hunks {
		start := hunkStartIndex(hunk)
		if start < oldIndex {
			return nil, fmt.Errorf("hunk starts before previous hunk ended")
		}
		if start > len(original) {
			return nil, fmt.Errorf("hunk start line %d exceeds file length %d", hunk.OldStart, len(original))
		}
		result = append(result, original[oldIndex:start]...)
		oldIndex = start

		oldConsumed := 0
		newProduced := 0
		for _, line := range hunk.Lines {
			switch line.Kind {
			case ' ':
				if oldIndex >= len(original) {
					return nil, fmt.Errorf("context line %q exceeds file length", line.Text)
				}
				if original[oldIndex] != line.Text {
					return nil, fmt.Errorf("context mismatch at line %d: got %q want %q", oldIndex+1, original[oldIndex], line.Text)
				}
				result = append(result, line.Text)
				oldIndex++
				oldConsumed++
				newProduced++
			case '-':
				if oldIndex >= len(original) {
					return nil, fmt.Errorf("remove line %q exceeds file length", line.Text)
				}
				if original[oldIndex] != line.Text {
					return nil, fmt.Errorf("remove mismatch at line %d: got %q want %q", oldIndex+1, original[oldIndex], line.Text)
				}
				oldIndex++
				oldConsumed++
			case '+':
				result = append(result, line.Text)
				newProduced++
			default:
				return nil, fmt.Errorf("unsupported hunk line kind %q", string(line.Kind))
			}
		}
		if oldConsumed != hunk.OldCount || newProduced != hunk.NewCount {
			return nil, fmt.Errorf("hunk count mismatch old=%d/%d new=%d/%d", oldConsumed, hunk.OldCount, newProduced, hunk.NewCount)
		}
	}

	result = append(result, original[oldIndex:]...)
	return result, nil
}

func hunkStartIndex(hunk unifiedHunk) int {
	if hunk.OldStart == 0 {
		return 0
	}
	if hunk.OldCount == 0 {
		return hunk.OldStart
	}
	return hunk.OldStart - 1
}

func (p unifiedFilePatch) targetPath() string {
	if p.NewPath != "" {
		return p.NewPath
	}
	return p.OldPath
}

func (p unifiedFilePatch) operation() string {
	switch {
	case p.OldPath == "" && p.NewPath != "":
		return "add"
	case p.OldPath != "" && p.NewPath == "":
		return "delete"
	default:
		return "update"
	}
}

func resolveWritablePath(ws *workspace.LocalWorkspace, inputPath, toolName string, confirmHighRisk bool) (string, string, error) {
	rel := normalizeToolPath(inputPath)
	if rel == "." {
		return "", "", fmt.Errorf("%s: path is required", toolName)
	}
	abs, err := ws.Resolve(rel)
	if err != nil {
		return "", "", err
	}
	if err := validateWritableRelPath(toolName, rel, confirmHighRisk); err != nil {
		return "", "", err
	}
	return rel, abs, nil
}

func validateWritableRelPath(toolName, rel string, confirmHighRisk bool) error {
	if strings.ContainsRune(rel, 0) {
		return fmt.Errorf("%s: path contains NUL", toolName)
	}
	for _, segment := range strings.Split(rel, "/") {
		if segment == ".git" {
			return fmt.Errorf("%s: modifying .git is not allowed", toolName)
		}
	}
	if highRisk, reason := highRiskWritePath(rel); highRisk && !confirmHighRisk {
		return fmt.Errorf("%s: path %q is high-risk (%s); rerun with confirm_high_risk=true after user confirmation", toolName, rel, reason)
	}
	return nil
}

func highRiskWritePath(rel string) (bool, string) {
	clean := normalizeToolPath(rel)
	base := path.Base(clean)
	if strings.HasPrefix(base, ".env") {
		return true, "environment file"
	}
	if strings.HasPrefix(clean, ".github/workflows/") || clean == ".github/workflows" {
		return true, "CI workflow"
	}
	if reason, ok := highRiskFileNames[base]; ok {
		return true, reason
	}
	return false, ""
}

var highRiskFileNames = map[string]string{
	".npmrc":              "credential-bearing config",
	".pypirc":             "credential-bearing config",
	"AGENTS.md":           "agent instructions",
	"Cargo.lock":          "dependency lockfile",
	"Cargo.toml":          "dependency manifest",
	"Dockerfile":          "container build config",
	"docker-compose.yaml": "container orchestration config",
	"docker-compose.yml":  "container orchestration config",
	"go.mod":              "dependency manifest",
	"go.sum":              "dependency lockfile",
	"id_rsa":              "private key",
	"package-lock.json":   "dependency lockfile",
	"package.json":        "dependency manifest",
	"pnpm-lock.yaml":      "dependency lockfile",
	"yarn.lock":           "dependency lockfile",
}

func inspectWritableTarget(rel, abs string) (bool, int64, os.FileMode, error) {
	mode := os.FileMode(0644)
	info, err := os.Stat(abs)
	if err != nil {
		if os.IsNotExist(err) {
			return false, 0, mode, nil
		}
		return false, 0, mode, fmt.Errorf("stat %q: %w", rel, err)
	}
	if info.IsDir() {
		return true, 0, mode, fmt.Errorf("%q is a directory", rel)
	}
	if err := rejectExistingBinary(rel, abs); err != nil {
		return true, info.Size(), info.Mode(), err
	}
	return true, info.Size(), info.Mode(), nil
}

func rejectExistingBinary(rel, abs string) error {
	file, err := os.Open(abs)
	if err != nil {
		return fmt.Errorf("open %q: %w", rel, err)
	}
	defer file.Close()
	binary, err := isBinarySample(file)
	if err != nil {
		return fmt.Errorf("inspect %q: %w", rel, err)
	}
	if binary {
		return fmt.Errorf("%q is binary; binary files are not allowed", rel)
	}
	return nil
}

func splitFileText(content string) ([]string, bool, string) {
	eol := "\n"
	if strings.Contains(content, "\r\n") {
		eol = "\r\n"
	}
	normalized := strings.ReplaceAll(content, "\r\n", "\n")
	if normalized == "" {
		return []string{}, false, eol
	}
	finalNewline := strings.HasSuffix(normalized, "\n")
	parts := strings.Split(normalized, "\n")
	if finalNewline {
		parts = parts[:len(parts)-1]
	}
	return parts, finalNewline, eol
}

func joinFileText(lines []string, finalNewline bool, eol string) string {
	if eol == "" {
		eol = "\n"
	}
	content := strings.Join(lines, eol)
	if finalNewline {
		content += eol
	}
	return content
}

func containsBinaryText(value string) bool {
	return strings.ContainsRune(value, 0)
}

func containsBinaryBytes(value []byte) bool {
	return bytes.Contains(value, []byte{0})
}

func summarizePatchChanges(changes []patchChange) []patchFileOutput {
	files := make([]patchFileOutput, 0, len(changes))
	for _, change := range changes {
		files = append(files, patchFileOutput{
			Path:      change.Path,
			Operation: change.Operation,
			Hunks:     change.Hunks,
			OldBytes:  change.OldBytes,
			NewBytes:  change.NewBytes,
		})
	}
	return files
}

func formatApplyPatchOutput(out applyPatchOutput) string {
	action := "dry-run: patch is valid"
	if out.Applied {
		action = "applied patch"
	}
	var builder strings.Builder
	fmt.Fprintf(&builder, "%s for %d file(s)\n", action, len(out.Files))
	for _, file := range out.Files {
		fmt.Fprintf(&builder, "- %s %s (%d hunk(s), %d -> %d bytes)\n", file.Operation, file.Path, file.Hunks, file.OldBytes, file.NewBytes)
	}
	return strings.TrimRight(builder.String(), "\n")
}

func formatWriteFileOutput(out writeFileOutput) string {
	action := "dry-run: write is valid"
	if out.Applied {
		action = "wrote file"
	}
	operation := "create"
	if !out.Created {
		operation = "overwrite"
	}
	return fmt.Sprintf("%s: %s %s (%d -> %d bytes)", action, operation, out.Path, out.OldBytes, out.NewBytes)
}
