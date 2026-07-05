package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestRunDefaultsToCLI(t *testing.T) {
	configureTestHome(t)

	var out bytes.Buffer
	var errOut bytes.Buffer

	err := Run(context.Background(), nil, strings.NewReader("/exit\n"), &out, &errOut)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if !strings.Contains(out.String(), "Agent CLI") {
		t.Fatalf("output missing CLI banner:\n%s", out.String())
	}
}

func TestRunExplicitCLICommand(t *testing.T) {
	configureTestHome(t)

	var out bytes.Buffer
	var errOut bytes.Buffer

	err := Run(context.Background(), []string{"cli"}, strings.NewReader("/exit\n"), &out, &errOut)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if !strings.Contains(out.String(), "Agent CLI") {
		t.Fatalf("output missing CLI banner:\n%s", out.String())
	}
}

func TestRunCommandUsesStartupWrapper(t *testing.T) {
	configureTestHome(t)

	workDir := t.TempDir()

	out := runAppCommand(t, nil, workDir, "", "run", "hello")
	runID := requireSubmittedRunID(t, out)
	if !strings.Contains(out, "Submitted run: "+runID) {
		t.Fatalf("output missing submitted run:\n%s", out)
	}

	result := driveRunToCompletion(t, nil, workDir, runID, nil)
	if !strings.Contains(result, "Mock LLM response") {
		t.Fatalf("result missing mock response:\n%s", result)
	}
}

func TestRunWorkCommandDrivesSubmittedRunWithoutRunID(t *testing.T) {
	configureTestHome(t)

	workDir := t.TempDir()
	out := runAppCommand(t, nil, workDir, "", "run", "hello worker")
	runID := requireSubmittedRunID(t, out)

	result := driveRunToCompletionWithWork(t, nil, workDir, runID, nil)
	if !strings.Contains(result, "Mock LLM response") {
		t.Fatalf("result missing mock response:\n%s", result)
	}
}

func TestRunDoesNotCreateLegacySessionDirectory(t *testing.T) {
	homeDir := configureTestHome(t)
	workDir := t.TempDir()
	var out bytes.Buffer
	var errOut bytes.Buffer

	err := Run(context.Background(), []string{"--workdir", workDir, "cli"}, strings.NewReader("first\ndefault\nsecond\n/exit\n"), &out, &errOut)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	sessionRoot := filepath.Join(homeDir, ".testAgent", "sessions")
	if _, err := os.Stat(sessionRoot); !os.IsNotExist(err) {
		t.Fatalf("legacy session directory exists or stat failed unexpectedly: %v", err)
	}
}

func TestRunExecutesToolCallsThroughRunner(t *testing.T) {
	homeDir := configureTestHome(t)
	workDir := t.TempDir()
	var requestCount int
	var sawToolResultMessage bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Tools    []any `json:"tool"`
			Messages []struct {
				Role string `json:"role"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if len(payload.Tools) == 0 {
			t.Fatal("request missing tool definitions")
		}
		requestCount++
		hasToolResult := false
		for _, message := range payload.Messages {
			if message.Role == "tool" {
				sawToolResultMessage = true
				hasToolResult = true
			}
		}

		w.Header().Set("Content-Type", "application/json")
		if hasToolResult {
			_, _ = w.Write([]byte(`{
  "model": "gpt-test",
  "choices": [{"finish_reason": "stop", "message": {"content": "tool flow complete"}}]
}`))
			return
		}
		_, _ = w.Write([]byte(`{
  "model": "gpt-test",
  "choices": [{
    "finish_reason": "tool_calls",
    "message": {
      "content": "",
      "tool_calls": [{
        "id": "call_ask_user_1",
        "type": "function",
        "function": {
          "name": "ask_user",
          "arguments": "{\"question\":\"Which target?\"}"
        }
      }]
    }
  }]
}`))
	}))
	defer server.Close()

	writeDefaultConfig(t, homeDir, []byte(`{
  "model_url": "`+server.URL+`",
  "model_name": "gpt-test",
  "api_key": "secret-token"
}`))

	out := runAppCommand(t, nil, workDir, "", "run", "build a feature")
	runID := requireSubmittedRunID(t, out)
	inputSent := false
	result := driveRunToCompletion(t, nil, workDir, runID, func(stepOutput string) {
		if inputSent || !strings.Contains(stepOutput, "Waiting: user_input") {
			return
		}
		runAppCommand(t, nil, workDir, "", "input", runID, "backend")
		inputSent = true
	})
	if requestCount != 2 {
		t.Fatalf("requests = %d, want 2", requestCount)
	}
	if !sawToolResultMessage {
		t.Fatal("llm did not receive tool result message")
	}
	if !inputSent {
		t.Fatal("run never requested user input")
	}
	if !strings.Contains(result, "tool flow complete") {
		t.Fatalf("result missing final answer:\n%s", result)
	}
	wantAgents := []string{analyzeAgentName, defaultAgentName, toolsTesterAgentName}
	if got := registeredAgentNames(); !reflect.DeepEqual(got, wantAgents) {
		t.Fatalf("registered agent names = %#v, want %#v", got, wantAgents)
	}
}

func TestRunRestoresSubmittedWorkDirWhenSteppingFromAnotherDirectory(t *testing.T) {
	homeDir := configureTestHome(t)
	workDirA := t.TempDir()
	workDirB := t.TempDir()
	if err := os.WriteFile(filepath.Join(workDirA, "marker.txt"), []byte("from submitted workspace\n"), 0600); err != nil {
		t.Fatalf("write marker A: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workDirB, "marker.txt"), []byte("from step workspace\n"), 0600); err != nil {
		t.Fatalf("write marker B: %v", err)
	}

	var requestCount int
	var sawSubmittedWorkspace bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Messages []struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		requestCount++
		hasToolResult := false
		for _, message := range payload.Messages {
			if message.Role != "tool" {
				continue
			}
			hasToolResult = true
			if strings.Contains(message.Content, "from submitted workspace") {
				sawSubmittedWorkspace = true
			}
			if strings.Contains(message.Content, "from step workspace") {
				t.Fatalf("tool used step command workspace instead of submitted workspace: %q", message.Content)
			}
		}

		w.Header().Set("Content-Type", "application/json")
		if hasToolResult {
			_, _ = w.Write([]byte(`{
  "model": "gpt-test",
  "choices": [{"finish_reason": "stop", "message": {"content": "workspace restored"}}]
}`))
			return
		}
		_, _ = w.Write([]byte(`{
  "model": "gpt-test",
  "choices": [{
    "finish_reason": "tool_calls",
    "message": {
      "content": "",
      "tool_calls": [{
        "id": "call_read_marker_1",
        "type": "function",
        "function": {
          "name": "read_file",
          "arguments": "{\"path\":\"marker.txt\"}"
        }
      }]
    }
  }]
}`))
	}))
	defer server.Close()

	writeDefaultConfig(t, homeDir, []byte(`{
  "model_url": "`+server.URL+`",
  "model_name": "gpt-test",
  "api_key": "secret-token"
}`))

	out := runAppCommand(t, nil, workDirA, "", "run", "read marker")
	runID := requireSubmittedRunID(t, out)
	result := driveRunToCompletion(t, nil, workDirB, runID, nil)
	if requestCount != 2 {
		t.Fatalf("requests = %d, want 2", requestCount)
	}
	if !sawSubmittedWorkspace {
		t.Fatal("model did not receive tool result from submitted workspace")
	}
	if !strings.Contains(result, "Workspace: "+workDirA) {
		t.Fatalf("result missing submitted workspace:\n%s", result)
	}
	if !strings.Contains(result, "workspace restored") {
		t.Fatalf("result missing final answer:\n%s", result)
	}
}

func TestRunDebugWritesLLMBodyJSONL(t *testing.T) {
	homeDir := configureTestHome(t)
	workDir := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
  "model": "gpt-test",
  "choices": [{"finish_reason": "stop", "message": {"content": "debug app response"}}]
}`))
	}))
	defer server.Close()

	writeDefaultConfig(t, homeDir, []byte(`{
  "model_url": "`+server.URL+`",
  "model_name": "gpt-test",
  "api_key": "secret-token"
}`))

	out := runAppCommand(t, []string{"--debug"}, workDir, "", "run", "hello debug")
	runID := requireSubmittedRunID(t, out)
	_ = driveRunToCompletion(t, []string{"--debug"}, workDir, runID, nil)

	debugRoot := filepath.Join(homeDir, ".testAgent", "debug")
	debugPath := findFirstFile(t, debugRoot, "llm.jsonl")
	data, err := os.ReadFile(debugPath)
	if err != nil {
		t.Fatalf("read debug jsonl: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 {
		t.Fatalf("debug lines = %d, want 1:\n%s", len(lines), string(data))
	}

	var entry struct {
		RequestBody  map[string]any `json:"request_body"`
		ResponseBody struct {
			Choices []struct {
				Message struct {
					Content string `json:"content"`
				} `json:"message"`
			} `json:"choices"`
		} `json:"response_body"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatalf("parse debug jsonl: %v\n%s", err, lines[0])
	}
	if entry.ResponseBody.Choices[0].Message.Content != "debug app response" {
		t.Fatalf("response body = %#v", entry.ResponseBody)
	}
	if _, ok := entry.RequestBody["messages"]; !ok {
		t.Fatalf("request body missing messages: %#v", entry.RequestBody)
	}
}

func TestRunLoadsDefaultConfigFile(t *testing.T) {
	homeDir := configureTestHome(t)
	workDir := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("path = %q, want /v1/chat/completions", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer secret-token" {
			t.Fatalf("Authorization = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
  "model": "gpt-test",
  "choices": [{"finish_reason": "stop", "message": {"content": "openai response"}}]
}`))
	}))
	defer server.Close()

	writeDefaultConfig(t, homeDir, []byte(`{
  "model_url": "`+server.URL+`",
  "model_name": "gpt-test",
  "api_key": "secret-token"
}`))

	out := runAppCommand(t, nil, workDir, "", "run", "hello")
	runID := requireSubmittedRunID(t, out)
	result := driveRunToCompletion(t, nil, workDir, runID, nil)

	got := result
	if !strings.Contains(got, "openai response") {
		t.Fatalf("output missing model response:\n%s", got)
	}
	if strings.Contains(got, "secret-token") {
		t.Fatalf("api key leaked in output:\nout=%s", got)
	}
}

func TestRunInfersDeepSeekProviderFromConfigFileModelName(t *testing.T) {
	homeDir := configureTestHome(t)
	workDir := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/anthropic/v1/messages" {
			t.Fatalf("path = %q, want /anthropic/v1/messages", r.URL.Path)
		}
		if got := r.Header.Get("x-api-key"); got != "secret-token" {
			t.Fatalf("x-api-key = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
  "model": "deepseek-v4-pro",
  "stop_reason": "end_turn",
  "content": [{"type": "text", "text": "deepseek response"}]
}`))
	}))
	defer server.Close()

	writeDefaultConfig(t, homeDir, []byte(`{
  "model_url": "`+server.URL+`",
  "model_name": "deepseek-v4-pro",
  "api_key": "secret-token"
}`))

	out := runAppCommand(t, nil, workDir, "", "run", "hello")
	runID := requireSubmittedRunID(t, out)
	result := driveRunToCompletion(t, nil, workDir, runID, nil)

	got := result
	if !strings.Contains(got, "deepseek response") {
		t.Fatalf("output missing model response:\n%s", got)
	}
	if strings.Contains(got, "secret-token") {
		t.Fatalf("api key leaked in output:\nout=%s", got)
	}
}

func TestRunModelNameOverridesProviderFlag(t *testing.T) {
	homeDir := configureTestHome(t)
	workDir := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			t.Fatalf("path = %q, want /chat/completions", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer secret-token" {
			t.Fatalf("Authorization = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
  "model": "deepseek-chat",
  "choices": [{"finish_reason": "stop", "message": {"content": "deepseek chat response"}}]
}`))
	}))
	defer server.Close()

	writeDefaultConfig(t, homeDir, []byte(`{
  "model_url": "`+server.URL+`",
  "model_name": "deepseek-chat",
  "api_key": "secret-token"
}`))

	out := runAppCommand(t, []string{"--provider", "openai"}, workDir, "", "run", "hello")
	runID := requireSubmittedRunID(t, out)
	result := driveRunToCompletion(t, []string{"--provider", "openai"}, workDir, runID, nil)

	got := result
	if !strings.Contains(got, "deepseek chat response") {
		t.Fatalf("output missing model response:\n%s", got)
	}
}

func runAppCommand(t *testing.T, flags []string, workDir string, input string, args ...string) string {
	t.Helper()

	var out bytes.Buffer
	var errOut bytes.Buffer
	allArgs := make([]string, 0, len(flags)+len(args)+2)
	allArgs = append(allArgs, flags...)
	if workDir != "" {
		allArgs = append(allArgs, "--workdir", workDir)
	}
	allArgs = append(allArgs, args...)
	if err := Run(context.Background(), allArgs, strings.NewReader(input), &out, &errOut); err != nil {
		t.Fatalf("Run(%v) returned error: %v\nstdout:\n%s\nstderr:\n%s", allArgs, err, out.String(), errOut.String())
	}
	if strings.Contains(errOut.String(), "secret-token") {
		t.Fatalf("api key leaked in stderr:\n%s", errOut.String())
	}
	return out.String()
}

func requireSubmittedRunID(t *testing.T, output string) string {
	t.Helper()

	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "Submitted run: ") {
			runID := strings.TrimSpace(strings.TrimPrefix(line, "Submitted run: "))
			if runID != "" {
				return runID
			}
		}
	}
	t.Fatalf("submitted run id not found in output:\n%s", output)
	return ""
}

func driveRunToCompletion(t *testing.T, flags []string, workDir string, runID string, afterStep func(string)) string {
	t.Helper()

	for i := 0; i < 40; i++ {
		stepOutput := runAppCommand(t, flags, workDir, "", "step", runID)
		if afterStep != nil {
			afterStep(stepOutput)
		}
		result := runAppCommand(t, flags, workDir, "", "result", runID)
		if strings.Contains(result, "Phase: completed") {
			return result
		}
	}
	t.Fatalf("run %s did not complete after 40 async steps", runID)
	return ""
}

func driveRunToCompletionWithWork(t *testing.T, flags []string, workDir string, runID string, afterStep func(string)) string {
	t.Helper()

	for i := 0; i < 40; i++ {
		stepOutput := runAppCommand(t, flags, workDir, "", "work")
		if afterStep != nil {
			afterStep(stepOutput)
		}
		result := runAppCommand(t, flags, workDir, "", "result", runID)
		if strings.Contains(result, "Phase: completed") {
			return result
		}
	}
	t.Fatalf("run %s did not complete after 40 async worker ticks", runID)
	return ""
}

func findFirstFile(t *testing.T, root string, name string) string {
	t.Helper()

	var found string
	stopWalk := errors.New("stop walk")
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || entry.Name() != name {
			return nil
		}
		found = path
		return stopWalk
	})
	if err != nil && !errors.Is(err, stopWalk) {
		t.Fatalf("walk %s: %v", root, err)
	}
	if found == "" {
		t.Fatalf("%s not found under %s", name, root)
	}
	return found
}

func configureTestHome(t *testing.T) string {
	t.Helper()

	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	t.Setenv("USERPROFILE", homeDir)
	return homeDir
}

func writeDefaultConfig(t *testing.T, homeDir string, data []byte) {
	t.Helper()

	configPath := filepath.Join(homeDir, ".testAgent", "config.json")
	if err := os.MkdirAll(filepath.Dir(configPath), 0700); err != nil {
		t.Fatalf("create config dir: %v", err)
	}
	if err := os.WriteFile(configPath, data, 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}
}
