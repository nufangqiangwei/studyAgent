package workspace

import (
	"agent/internal/capability/builtin"
	"agent/internal/content"
	"agent/internal/foundation/workspace"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	ListFilesToolName           = "list_files"
	ReadFileToolName            = "read_file"
	SearchTextToolName          = "search_text"
	GetWorkspaceSummaryToolName = "get_workspace_summary"

	defaultListMaxDepth = 3
	defaultListLimit    = 200
	maxListLimit        = 1000

	defaultReadLineCount = 200
	maxReadLineCount     = 400

	defaultSearchLimit = 50
	maxSearchLimit     = 200
)

type ListFilesTool struct{}

func NewListFilesTool() *ListFilesTool {
	return &ListFilesTool{}
}

func (t *ListFilesTool) Name() string {
	return ListFilesToolName
}

func (t *ListFilesTool) Description() string {
	return "List files and directories under a workspace path with depth and result limits."
}

func (t *ListFilesTool) InputSchema() json.RawMessage {
	return json.RawMessage(`{
  "type": "object",
  "properties": {
    "path": {
      "type": "string",
      "description": "Workspace-relative path to list. Defaults to the workspace root."
    },
    "max_depth": {
      "type": "integer",
      "minimum": 0,
      "description": "Maximum depth below path to include. Defaults to 3."
    },
    "limit": {
      "type": "integer",
      "minimum": 1,
      "description": "Maximum number of entries to return. Defaults to 200."
    }
  },
  "additionalProperties": false
}`)
}

func (t *ListFilesTool) Execute(ctx context.Context, input json.RawMessage) (builtin.Result, error) {
	if err := ctx.Err(); err != nil {
		return builtin.Result{}, err
	}
	if t == nil {
		return builtin.Result{}, errors.New("list_files: tool is nil")
	}

	var req listFilesInput
	if err := decodeWorkspaceToolInput(ListFilesToolName, input, &req); err != nil {
		return builtin.Result{}, err
	}
	normalized, err := req.normalize()
	if err != nil {
		return builtin.Result{}, err
	}

	ws, err := workspaceFromContext(ctx)
	if err != nil {
		return builtin.Result{}, fmt.Errorf("list_files: %w", err)
	}
	entries, truncated, err := listWorkspaceEntries(ctx, ws, normalized.Path, normalized.MaxDepth, normalized.Limit)
	if err != nil {
		return builtin.Result{}, fmt.Errorf("list_files: %w", err)
	}

	out := listFilesOutput{
		Root:      normalized.Path,
		Entries:   entries,
		Truncated: truncated,
	}
	return jsonToolResult(out, map[string]any{
		"root":        out.Root,
		"entries":     len(out.Entries),
		"truncated":   out.Truncated,
		"max_depth":   normalized.MaxDepth,
		"entry_limit": normalized.Limit,
	})
}

type ReadFileTool struct{}

func NewReadFileTool() *ReadFileTool {
	return &ReadFileTool{}
}

func (t *ReadFileTool) Name() string {
	return ReadFileToolName
}

func (t *ReadFileTool) Description() string {
	return "Read a bounded line range from a workspace file. Defaults to 200 lines, not the entire file."
}

func (t *ReadFileTool) InputSchema() json.RawMessage {
	return json.RawMessage(`{
  "type": "object",
  "properties": {
    "path": {
      "type": "string",
      "description": "Workspace-relative file path to read."
    },
    "start_line": {
      "type": "integer",
      "minimum": 1,
      "description": "1-based starting line. Defaults to 1."
    },
    "end_line": {
      "type": "integer",
      "minimum": 1,
      "description": "1-based ending line. Defaults to start_line + 199 and is capped to 400 lines."
    }
  },
  "required": ["path"],
  "additionalProperties": false
}`)
}

func (t *ReadFileTool) Execute(ctx context.Context, input json.RawMessage) (builtin.Result, error) {
	if err := ctx.Err(); err != nil {
		return builtin.Result{}, err
	}
	if t == nil {
		return builtin.Result{}, errors.New("read_file: tool is nil")
	}

	var req readFileInput
	if err := decodeWorkspaceToolInput(ReadFileToolName, input, &req); err != nil {
		return builtin.Result{}, err
	}
	normalized, err := req.normalize()
	if err != nil {
		return builtin.Result{}, err
	}

	ws, err := workspaceFromContext(ctx)
	if err != nil {
		return builtin.Result{}, fmt.Errorf("read_file: %w", err)
	}
	rawContent, numberedContent, actualEndLine, truncated, err := readWorkspaceLineRange(ctx, ws, normalized.Path, normalized.StartLine, normalized.EndLine)
	if err != nil {
		return builtin.Result{}, fmt.Errorf("read_file: %w", err)
	}
	out := readFileOutput{
		Path:      normalized.Path,
		StartLine: normalized.StartLine,
		EndLine:   actualEndLine,
		Content:   rawContent,
		Truncated: truncated,
	}

	raw, err := json.Marshal(out)
	if err != nil {
		return builtin.Result{}, fmt.Errorf("read_file: marshal result: %w", err)
	}
	header := fmt.Sprintf("path: %s\nlines: %d-%d\ntruncated: %t\n\n", out.Path, out.StartLine, out.EndLine, out.Truncated)
	return builtin.Result{
		Content: header + numberedContent,
		Metadata: map[string]any{
			"path":       out.Path,
			"start_line": out.StartLine,
			"end_line":   out.EndLine,
			"truncated":  out.Truncated,
		},
		Raw: raw,
	}, nil
}

type SearchTextTool struct{}

func NewSearchTextTool() *SearchTextTool {
	return &SearchTextTool{}
}

func (t *SearchTextTool) Name() string {
	return SearchTextToolName
}

func (t *SearchTextTool) Description() string {
	return "Search for literal text in workspace files and return matching lines."
}

func (t *SearchTextTool) InputSchema() json.RawMessage {
	return json.RawMessage(`{
  "type": "object",
  "properties": {
    "query": {
      "type": "string",
      "description": "Literal text to search for."
    },
    "path": {
      "type": "string",
      "description": "Workspace-relative file or directory to search. Defaults to the workspace root."
    },
    "limit": {
      "type": "integer",
      "minimum": 1,
      "description": "Maximum number of matches to return. Defaults to 50."
    }
  },
  "required": ["query"],
  "additionalProperties": false
}`)
}

func (t *SearchTextTool) Execute(ctx context.Context, input json.RawMessage) (builtin.Result, error) {
	if err := ctx.Err(); err != nil {
		return builtin.Result{}, err
	}
	if t == nil {
		return builtin.Result{}, errors.New("search_text: tool is nil")
	}

	var req searchTextInput
	if err := decodeWorkspaceToolInput(SearchTextToolName, input, &req); err != nil {
		return builtin.Result{}, err
	}
	normalized, err := req.normalize()
	if err != nil {
		return builtin.Result{}, err
	}

	ws, err := workspaceFromContext(ctx)
	if err != nil {
		return builtin.Result{}, fmt.Errorf("search_text: %w", err)
	}
	matches, err := ws.Search(ctx, normalized.Query, workspace.SearchOptions{
		Dir:        normalized.Path,
		MaxMatches: normalized.Limit + 1,
	})
	if err != nil {
		return builtin.Result{}, fmt.Errorf("search_text: %w", err)
	}
	truncated := len(matches) > normalized.Limit
	if truncated {
		matches = matches[:normalized.Limit]
	}

	out := searchTextOutput{
		Query:     normalized.Query,
		Path:      normalized.Path,
		Matches:   convertSearchMatches(matches),
		Truncated: truncated,
	}
	return jsonToolResult(out, map[string]any{
		"query":       out.Query,
		"path":        out.Path,
		"matches":     len(out.Matches),
		"truncated":   out.Truncated,
		"match_limit": normalized.Limit,
	})
}

type GetWorkspaceSummaryTool struct{}

func NewGetWorkspaceSummaryTool() *GetWorkspaceSummaryTool {
	return &GetWorkspaceSummaryTool{}
}

func (t *GetWorkspaceSummaryTool) Name() string {
	return GetWorkspaceSummaryToolName
}

func (t *GetWorkspaceSummaryTool) Description() string {
	return "Summarize the workspace for LLM context: module, root, key directories, commands, languages, tests, and git status."
}

func (t *GetWorkspaceSummaryTool) InputSchema() json.RawMessage {
	return json.RawMessage(`{
  "type": "object",
  "properties": {
    "path": {
      "type": "string",
      "description": "Workspace-relative directory to summarize. Defaults to the workspace root."
    }
  },
  "additionalProperties": false
}`)
}

func (t *GetWorkspaceSummaryTool) Execute(ctx context.Context, input json.RawMessage) (builtin.Result, error) {
	if err := ctx.Err(); err != nil {
		return builtin.Result{}, err
	}
	if t == nil {
		return builtin.Result{}, errors.New("get_workspace_summary: tool is nil")
	}

	var req workspaceSummaryInput
	if err := decodeWorkspaceToolInput(GetWorkspaceSummaryToolName, input, &req); err != nil {
		return builtin.Result{}, err
	}
	normalized := req.normalize()
	ws, err := workspaceFromContext(ctx)
	if err != nil {
		return builtin.Result{}, fmt.Errorf("get_workspace_summary: %w", err)
	}
	if normalized.Path != "." {
		abs, err := ws.Resolve(normalized.Path)
		if err != nil {
			return builtin.Result{}, fmt.Errorf("get_workspace_summary: %w", err)
		}
		ws, err = workspace.New(abs)
		if err != nil {
			return builtin.Result{}, fmt.Errorf("get_workspace_summary: %w", err)
		}
	}

	summary := buildWorkspaceSummary(ctx, ws)
	raw, err := json.Marshal(summary)
	if err != nil {
		return builtin.Result{}, fmt.Errorf("get_workspace_summary: marshal result: %w", err)
	}

	return builtin.Result{
		Content:  formatWorkspaceSummary(summary),
		Metadata: map[string]any{"root_path": summary.RootPath, "module_name": summary.ModuleName},
		Raw:      raw,
	}, nil
}

type listFilesInput struct {
	Path     string `json:"path"`
	MaxDepth *int   `json:"max_depth"`
	Limit    *int   `json:"limit"`
}

type normalizedListFilesInput struct {
	Path     string
	MaxDepth int
	Limit    int
}

func (i listFilesInput) normalize() (normalizedListFilesInput, error) {
	path := normalizeToolPath(i.Path)
	maxDepth := defaultListMaxDepth
	if i.MaxDepth != nil {
		maxDepth = *i.MaxDepth
	}
	if maxDepth < 0 {
		return normalizedListFilesInput{}, fmt.Errorf("list_files: max_depth must be >= 0")
	}
	limit := defaultListLimit
	if i.Limit != nil {
		limit = *i.Limit
	}
	if limit <= 0 {
		return normalizedListFilesInput{}, fmt.Errorf("list_files: limit must be > 0")
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}
	return normalizedListFilesInput{Path: path, MaxDepth: maxDepth, Limit: limit}, nil
}

type readFileInput struct {
	Path      string `json:"path"`
	StartLine *int   `json:"start_line"`
	EndLine   *int   `json:"end_line"`
}

type normalizedReadFileInput struct {
	Path      string
	StartLine int
	EndLine   int
}

func (i readFileInput) normalize() (normalizedReadFileInput, error) {
	path := normalizeToolPath(i.Path)
	if path == "." {
		return normalizedReadFileInput{}, fmt.Errorf("read_file: path is required")
	}
	startLine := 1
	if i.StartLine != nil {
		startLine = *i.StartLine
	}
	if startLine < 1 {
		return normalizedReadFileInput{}, fmt.Errorf("read_file: start_line must be >= 1")
	}
	endLine := startLine + defaultReadLineCount - 1
	if i.EndLine != nil {
		endLine = *i.EndLine
	}
	if endLine < startLine {
		return normalizedReadFileInput{}, fmt.Errorf("read_file: end_line must be >= start_line")
	}
	if endLine-startLine+1 > maxReadLineCount {
		endLine = startLine + maxReadLineCount - 1
	}
	return normalizedReadFileInput{Path: path, StartLine: startLine, EndLine: endLine}, nil
}

type searchTextInput struct {
	Query string `json:"query"`
	Path  string `json:"path"`
	Limit *int   `json:"limit"`
}

type normalizedSearchTextInput struct {
	Query string
	Path  string
	Limit int
}

func (i searchTextInput) normalize() (normalizedSearchTextInput, error) {
	if strings.TrimSpace(i.Query) == "" {
		return normalizedSearchTextInput{}, fmt.Errorf("search_text: query is required")
	}
	limit := defaultSearchLimit
	if i.Limit != nil {
		limit = *i.Limit
	}
	if limit <= 0 {
		return normalizedSearchTextInput{}, fmt.Errorf("search_text: limit must be > 0")
	}
	if limit > maxSearchLimit {
		limit = maxSearchLimit
	}
	return normalizedSearchTextInput{Query: i.Query, Path: normalizeToolPath(i.Path), Limit: limit}, nil
}

type workspaceSummaryInput struct {
	Path string `json:"path"`
}

func (i workspaceSummaryInput) normalize() workspaceSummaryInput {
	return workspaceSummaryInput{Path: normalizeToolPath(i.Path)}
}

type workspaceEntry struct {
	Path string `json:"path"`
	Type string `json:"type"`
}

type listFilesOutput struct {
	Root      string           `json:"root"`
	Entries   []workspaceEntry `json:"entries"`
	Truncated bool             `json:"truncated"`
}

type readFileOutput struct {
	Path      string `json:"path"`
	StartLine int    `json:"start_line"`
	EndLine   int    `json:"end_line"`
	Content   string `json:"content"`
	Truncated bool   `json:"truncated"`
}

type searchMatch struct {
	Path       string `json:"path"`
	LineNumber int    `json:"line_number"`
	Line       string `json:"line"`
}

type searchTextOutput struct {
	Query     string        `json:"query"`
	Path      string        `json:"path"`
	Matches   []searchMatch `json:"matches"`
	Truncated bool          `json:"truncated"`
}

type workspaceSummary struct {
	ModuleName           string   `json:"module_name"`
	RootPath             string   `json:"root_path"`
	ImportantDirectories []string `json:"important_directories"`
	KnownCommands        []string `json:"known_commands"`
	DetectedLanguages    []string `json:"detected_languages"`
	TestCommand          string   `json:"test_command"`
	GitStatusSummary     string   `json:"git_status_summary"`
}

var errWorkspaceToolLimit = errors.New("workspace tool limit reached")

func decodeWorkspaceToolInput(toolName string, input json.RawMessage, target any) error {
	if strings.TrimSpace(string(input)) == "" {
		input = json.RawMessage(`{}`)
	}
	if err := json.Unmarshal(input, target); err != nil {
		return fmt.Errorf("%s: decode input: %w", toolName, err)
	}
	return nil
}

func workspaceFromContext(ctx context.Context) (*workspace.LocalWorkspace, error) {
	root := "."
	if env, ok := content.EnvFromContext(ctx); ok && strings.TrimSpace(env.Config.WorkDir) != "" {
		root = strings.TrimSpace(env.Config.WorkDir)
	}
	return workspace.New(root)
}

func normalizeToolPath(input string) string {
	input = strings.TrimSpace(input)
	if input == "" {
		return "."
	}
	cleaned := path.Clean(strings.ReplaceAll(input, "\\", "/"))
	if cleaned == "" {
		return "."
	}
	return cleaned
}

func listWorkspaceEntries(ctx context.Context, ws *workspace.LocalWorkspace, rootRel string, maxDepth int, limit int) ([]workspaceEntry, bool, error) {
	abs, err := ws.Resolve(rootRel)
	if err != nil {
		return nil, false, err
	}
	info, err := os.Stat(abs)
	if err != nil {
		return nil, false, err
	}
	if !info.IsDir() {
		return []workspaceEntry{{Path: rootRel, Type: "file"}}, false, nil
	}

	entries := make([]workspaceEntry, 0, limit)
	truncated := false
	var walk func(dir string, depth int) error
	walk = func(dir string, depth int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if depth >= maxDepth {
			return nil
		}
		children, err := ws.List(ctx, workspace.ListOptions{
			Dir:         dir,
			Recursive:   false,
			MaxEntries:  workspace.DefaultMaxEntries,
			IncludeDirs: true,
		})
		if err != nil {
			return err
		}
		for _, child := range children {
			if len(entries) >= limit {
				truncated = true
				return errWorkspaceToolLimit
			}
			entryType := "file"
			if child.IsDir {
				entryType = "directory"
			}
			entries = append(entries, workspaceEntry{
				Path: child.Path,
				Type: entryType,
			})
			if child.IsDir {
				if err := walk(child.Path, depth+1); err != nil {
					return err
				}
			}
		}
		return nil
	}

	if err := walk(rootRel, 0); err != nil && !errors.Is(err, errWorkspaceToolLimit) {
		return nil, false, err
	}
	return entries, truncated, nil
}

func jsonToolResult(value any, metadata map[string]any) (builtin.Result, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return builtin.Result{}, fmt.Errorf("marshal result: %w", err)
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, raw, "", "  "); err != nil {
		return builtin.Result{}, fmt.Errorf("format result: %w", err)
	}
	return builtin.Result{
		Content:  pretty.String(),
		Metadata: metadata,
		Raw:      raw,
	}, nil
}

func readWorkspaceLineRange(ctx context.Context, ws *workspace.LocalWorkspace, filePath string, startLine, endLine int) (string, string, int, bool, error) {
	abs, err := ws.Resolve(filePath)
	if err != nil {
		return "", "", 0, false, err
	}
	info, err := os.Stat(abs)
	if err != nil {
		return "", "", 0, false, err
	}
	if info.IsDir() {
		return "", "", 0, false, fmt.Errorf("read %q: is a directory", filePath)
	}

	file, err := os.Open(abs)
	if err != nil {
		return "", "", 0, false, err
	}
	defer file.Close()

	binary, err := isBinarySample(file)
	if err != nil {
		return "", "", 0, false, err
	}
	if binary {
		return "", "", 0, false, fmt.Errorf("read %q: binary file is ignored", filePath)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", "", 0, false, err
	}

	return readLineRange(ctx, file, startLine, endLine)
}

func isBinarySample(file *os.File) (bool, error) {
	const binarySampleBytes = 8000
	buf := make([]byte, binarySampleBytes)
	n, err := file.Read(buf)
	if err != nil && !errors.Is(err, io.EOF) {
		return false, err
	}
	return bytes.Contains(buf[:n], []byte{0}), nil
}

func readLineRange(ctx context.Context, reader io.Reader, startLine, endLine int) (string, string, int, bool, error) {
	buffered := bufio.NewReader(reader)
	var raw strings.Builder
	var numbered strings.Builder
	actualEndLine := startLine - 1
	truncated := false

	for lineNumber := 1; ; lineNumber++ {
		if err := ctx.Err(); err != nil {
			return "", "", 0, false, err
		}
		line, err := buffered.ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return "", "", 0, false, err
		}
		if errors.Is(err, io.EOF) && line == "" {
			break
		}
		if lineNumber > endLine {
			truncated = true
			break
		}
		if lineNumber >= startLine {
			raw.WriteString(line)
			fmt.Fprintf(&numbered, "%d: %s\n", lineNumber, strings.TrimRight(line, "\r\n"))
			actualEndLine = lineNumber
		}
		if errors.Is(err, io.EOF) {
			break
		}
	}
	return raw.String(), numbered.String(), actualEndLine, truncated, nil
}

func convertSearchMatches(matches []workspace.SearchMatch) []searchMatch {
	result := make([]searchMatch, 0, len(matches))
	for _, match := range matches {
		result = append(result, searchMatch{
			Path:       match.Path,
			LineNumber: match.Line,
			Line:       trimSearchLine(match.LineText),
		})
	}
	return result
}

func trimSearchLine(line string) string {
	const maxLineLength = 500
	if len(line) <= maxLineLength {
		return line
	}
	return line[:maxLineLength] + "..."
}

func buildWorkspaceSummary(ctx context.Context, ws *workspace.LocalWorkspace) workspaceSummary {
	snapshot, err := ws.Snapshot(ctx, workspace.SnapshotOptions{
		MaxEntries: workspace.DefaultSnapshotEntries,
		MaxDepth:   workspace.DefaultSnapshotDepth,
	})
	if err != nil {
		snapshot = workspace.Snapshot{Root: ws.Root()}
	}
	knownCommands, testCommand := detectCommands(ws.Root())
	return workspaceSummary{
		ModuleName:           detectModuleName(ws.Root()),
		RootPath:             ws.Root(),
		ImportantDirectories: detectImportantDirectories(snapshot),
		KnownCommands:        knownCommands,
		DetectedLanguages:    detectLanguages(snapshot),
		TestCommand:          testCommand,
		GitStatusSummary:     gitStatusSummary(ctx, ws.Root()),
	}
}

func detectModuleName(root string) string {
	goModPath := filepath.Join(root, "go.mod")
	file, err := os.Open(goModPath)
	if err == nil {
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if strings.HasPrefix(line, "module ") {
				return strings.TrimSpace(strings.TrimPrefix(line, "module "))
			}
		}
	}

	packageJSONPath := filepath.Join(root, "package.json")
	data, err := os.ReadFile(packageJSONPath)
	if err == nil {
		var packageJSON struct {
			Name string `json:"name"`
		}
		if json.Unmarshal(data, &packageJSON) == nil && strings.TrimSpace(packageJSON.Name) != "" {
			return strings.TrimSpace(packageJSON.Name)
		}
	}
	return "unknown"
}

func detectImportantDirectories(snapshot workspace.Snapshot) []string {
	important := make([]string, 0)
	for _, entry := range snapshot.Entries {
		if entry.IsDir {
			important = append(important, entry.Path)
		}
	}
	sort.Strings(important)
	const maxImportantDirs = 40
	if len(important) > maxImportantDirs {
		return important[:maxImportantDirs]
	}
	return important
}

func detectCommands(root string) ([]string, string) {
	commands := make([]string, 0)
	testCommand := "not detected"
	if fileExists(filepath.Join(root, "go.mod")) {
		commands = append(commands, "go run .", "go test ./...")
		testCommand = "go test ./..."
	}

	packageJSONPath := filepath.Join(root, "package.json")
	if data, err := os.ReadFile(packageJSONPath); err == nil {
		var packageJSON struct {
			Scripts map[string]string `json:"scripts"`
		}
		if json.Unmarshal(data, &packageJSON) == nil {
			names := make([]string, 0, len(packageJSON.Scripts))
			for name := range packageJSON.Scripts {
				names = append(names, name)
			}
			sort.Strings(names)
			for _, name := range names {
				commands = append(commands, "npm run "+name)
			}
			if _, ok := packageJSON.Scripts["test"]; ok {
				testCommand = "npm test"
			}
		}
	}

	makefile := filepath.Join(root, "Makefile")
	if fileExists(makefile) {
		commands = append(commands, detectMakeTargets(makefile)...)
	}

	if len(commands) == 0 {
		return []string{"none detected"}, testCommand
	}
	return commands, testCommand
}

func detectMakeTargets(filePath string) []string {
	file, err := os.Open(filePath)
	if err != nil {
		return nil
	}
	defer file.Close()

	targets := make([]string, 0)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "\t") || strings.HasPrefix(line, " ") || strings.HasPrefix(line, ".") {
			continue
		}
		colon := strings.Index(line, ":")
		if colon <= 0 {
			continue
		}
		target := strings.TrimSpace(line[:colon])
		if target == "" || strings.ContainsAny(target, "$#/") {
			continue
		}
		targets = append(targets, "make "+target)
		if len(targets) >= 10 {
			break
		}
	}
	return targets
}

func detectLanguages(snapshot workspace.Snapshot) []string {
	byExt := map[string]string{
		".go":   "Go",
		".js":   "JavaScript",
		".jsx":  "JavaScript",
		".json": "JSON",
		".md":   "Markdown",
		".ps1":  "PowerShell",
		".py":   "Python",
		".sh":   "Shell",
		".ts":   "TypeScript",
		".tsx":  "TypeScript",
		".yaml": "YAML",
		".yml":  "YAML",
	}
	found := make(map[string]bool)
	for _, entry := range snapshot.Entries {
		if entry.IsDir {
			continue
		}
		if language, ok := byExt[strings.ToLower(filepath.Ext(entry.Path))]; ok {
			found[language] = true
		}
	}

	languages := make([]string, 0, len(found))
	for language := range found {
		languages = append(languages, language)
	}
	sort.Strings(languages)
	if len(languages) == 0 {
		return []string{"unknown"}
	}
	return languages
}

func gitStatusSummary(ctx context.Context, root string) string {
	cmd := exec.CommandContext(ctx, "git", "-C", root, "status", "--short", "--branch")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "not available"
	}
	text := strings.TrimSpace(string(output))
	if text == "" {
		return "clean"
	}
	lines := strings.Split(text, "\n")
	branch := strings.TrimSpace(lines[0])
	if len(lines) == 1 {
		return branch + "; clean"
	}
	return branch + "; " + strconv.Itoa(len(lines)-1) + " changed entries"
}

func fileExists(filePath string) bool {
	info, err := os.Stat(filePath)
	return err == nil && !info.IsDir()
}

func formatWorkspaceSummary(summary workspaceSummary) string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "module name: %s\n", summary.ModuleName)
	fmt.Fprintf(&builder, "root path: %s\n", summary.RootPath)
	writeSummaryList(&builder, "important directories", summary.ImportantDirectories)
	writeSummaryList(&builder, "known commands", summary.KnownCommands)
	writeSummaryList(&builder, "detected languages", summary.DetectedLanguages)
	fmt.Fprintf(&builder, "test command: %s\n", summary.TestCommand)
	fmt.Fprintf(&builder, "git status summary: %s", summary.GitStatusSummary)
	return builder.String()
}

func writeSummaryList(builder *strings.Builder, label string, values []string) {
	fmt.Fprintf(builder, "%s:\n", label)
	for _, value := range values {
		fmt.Fprintf(builder, "- %s\n", value)
	}
}
