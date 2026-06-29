package app

import (
	"agent/internal/agent"
	agentsession "agent/internal/session"
	"bytes"
	"context"
	"encoding/json"
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
	var out bytes.Buffer
	var errOut bytes.Buffer

	err := Run(context.Background(), []string{"--workdir", workDir, "run", "hello"}, strings.NewReader(""), &out, &errOut)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if !strings.Contains(out.String(), "Mock LLM response") {
		t.Fatalf("output missing mock response:\n%s", out.String())
	}
}

func TestRunUsesOneAgentFileForStartupConversation(t *testing.T) {
	homeDir := configureTestHome(t)
	workDir := t.TempDir()
	var out bytes.Buffer
	var errOut bytes.Buffer

	err := Run(context.Background(), []string{"--workdir", workDir, "cli"}, strings.NewReader("first\ndefault\nsecond\n/exit\n"), &out, &errOut)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	sessionRoot := filepath.Join(homeDir, ".testAgent", "sessions")
	sessionEntries, err := os.ReadDir(sessionRoot)
	if err != nil {
		t.Fatalf("read session dir: %v", err)
	}
	if len(sessionEntries) != 1 || !sessionEntries[0].IsDir() {
		t.Fatalf("session entries = %#v, want one session directory", sessionEntries)
	}

	sessionDir := filepath.Join(sessionRoot, sessionEntries[0].Name())
	manifestData, err := os.ReadFile(filepath.Join(sessionDir, "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var manifest agentsession.Manifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	if manifest.Layout.AgentsDir != "agents" || len(manifest.Agents) != 1 {
		t.Fatalf("manifest = %#v, want one agent file", manifest)
	}

	agentData, err := os.ReadFile(filepath.Join(sessionDir, filepath.FromSlash(manifest.Agents[0].Path)))
	if err != nil {
		t.Fatalf("read agent session file: %v", err)
	}
	records := parseSessionRecords(t, agentData)
	if len(records) < 6 {
		t.Fatalf("records = %d, want at least 6: %#v", len(records), records)
	}

	var firstUser, secondUser bool
	usageSummaries := 0
	for _, record := range records {
		if record.Timestamp.IsZero() {
			t.Fatalf("record timestamp missing: %#v", record)
		}
		if record.Kind == agentsession.RecordKindMessage && record.Message != nil && record.Message.Role == "user" {
			if record.AgentName == agent.AnalyzeAgentName && strings.Contains(record.Message.Content, "first") {
				firstUser = true
			}
			if record.AgentName == agent.DefaultAgentName && strings.Contains(record.Message.Content, "second") {
				secondUser = true
			}
		}
		if record.Kind == agentsession.RecordKindUsageSummary {
			usageSummaries++
		}
	}
	if !firstUser || !secondUser {
		t.Fatalf("missing expected user message records: %#v", records)
	}
	if usageSummaries != 2 {
		t.Fatalf("usage summaries = %d, want 2: %#v", usageSummaries, records)
	}
}

func TestRunBindsEnvToToolContext(t *testing.T) {
	homeDir := configureTestHome(t)
	workDir := t.TempDir()
	var seenToolResult bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Tools    []any `json:"tool"`
			Messages []struct {
				Role       string `json:"role"`
				Content    string `json:"content"`
				ToolCallID string `json:"tool_call_id"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode request: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		if len(payload.Tools) == 0 {
			t.Fatal("request missing tool")
		}
		if len(payload.Messages) > 0 {
			lastMessage := payload.Messages[len(payload.Messages)-1]
			if lastMessage.Role == "tool" && lastMessage.ToolCallID == "call_ask_user_1" && lastMessage.Content == "web app" {
				seenToolResult = true
				_, _ = w.Write([]byte(`{
  "model": "gpt-test",
  "choices": [{"finish_reason": "stop", "message": {"content": "final answer"}}]
}`))
				return
			}
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

	var out bytes.Buffer
	var errOut bytes.Buffer

	err := Run(context.Background(), []string{"--workdir", workDir, "run", "build a feature"}, strings.NewReader("web app\n"), &out, &errOut)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if !seenToolResult {
		t.Fatal("model did not receive ask_user tool result")
	}
	wantAgents := []string{agent.AnalyzeAgentName, agent.DefaultAgentName, agent.ToolsTesterAgentName}
	if got := agent.RegisteredAgentNames(); !reflect.DeepEqual(got, wantAgents) {
		t.Fatalf("registered agent names = %#v, want %#v", got, wantAgents)
	}
	if got := out.String(); !strings.Contains(got, "? Which target?") || !strings.Contains(got, "final answer") {
		t.Fatalf("output missing tool prompt or final answer:\n%s", got)
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

	var out bytes.Buffer
	var errOut bytes.Buffer
	err := Run(context.Background(), []string{"--debug", "--workdir", workDir, "run", "hello debug"}, strings.NewReader(""), &out, &errOut)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	sessionRoot := filepath.Join(homeDir, ".testAgent", "sessions")
	sessionEntries, err := os.ReadDir(sessionRoot)
	if err != nil {
		t.Fatalf("read session root: %v", err)
	}
	if len(sessionEntries) != 1 || !sessionEntries[0].IsDir() {
		t.Fatalf("session entries = %#v, want one session directory", sessionEntries)
	}

	debugPath := filepath.Join(sessionRoot, sessionEntries[0].Name(), "llm.jsonl")
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

	var out bytes.Buffer
	var errOut bytes.Buffer
	err := Run(context.Background(), []string{"--workdir", workDir, "run", "hello"}, strings.NewReader(""), &out, &errOut)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	got := out.String()
	if !strings.Contains(got, "openai response") {
		t.Fatalf("output missing model response:\n%s", got)
	}
	if strings.Contains(got, "secret-token") || strings.Contains(errOut.String(), "secret-token") {
		t.Fatalf("api key leaked in output:\nout=%s\nerr=%s", got, errOut.String())
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

	var out bytes.Buffer
	var errOut bytes.Buffer
	err := Run(context.Background(), []string{"--workdir", workDir, "run", "hello"}, strings.NewReader(""), &out, &errOut)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	got := out.String()
	if !strings.Contains(got, "deepseek response") {
		t.Fatalf("output missing model response:\n%s", got)
	}
	if strings.Contains(got, "secret-token") || strings.Contains(errOut.String(), "secret-token") {
		t.Fatalf("api key leaked in output:\nout=%s\nerr=%s", got, errOut.String())
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

	var out bytes.Buffer
	var errOut bytes.Buffer
	err := Run(context.Background(), []string{"--workdir", workDir, "--provider", "openai", "run", "hello"}, strings.NewReader(""), &out, &errOut)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	got := out.String()
	if !strings.Contains(got, "deepseek chat response") {
		t.Fatalf("output missing model response:\n%s", got)
	}
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

func parseSessionRecords(t *testing.T, data []byte) []agentsession.Record {
	t.Helper()

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	records := make([]agentsession.Record, 0, len(lines))
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var record agentsession.Record
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("parse session jsonl: %v\n%s", err, line)
		}
		records = append(records, record)
	}
	return records
}
