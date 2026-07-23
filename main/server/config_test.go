package main

import (
	"bytes"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestParseServerConfigValues(t *testing.T) {
	tests := []struct {
		name string
		args []string
		env  map[string]string
		want serverConfig
	}{
		{
			name: "defaults",
			env:  map[string]string{"AGENT_MODEL": "deepseek-chat"},
			want: serverConfig{
				Address:           "127.0.0.1:8080",
				ReadHeaderTimeout: 5 * time.Second,
				IdleTimeout:       60 * time.Second,
				ShutdownTimeout:   10 * time.Second,
				DataDir:           ".agent/runtime",
				RuntimeID:         "agent-server",
				Provider:          "deepseek",
				Model:             "deepseek-chat",
				BaseURL:           "https://api.deepseek.com",
				ModelTimeout:      2 * time.Minute,
				AgentSystemPrompt: defaultServerAgentSystemPrompt,
				AgentMaxTurns:     8,
			},
		},
		{
			name: "environment",
			env: map[string]string{
				"AGENT_SERVER_ADDRESS":             " 127.0.0.1:9090 ",
				"AGENT_SERVER_READ_HEADER_TIMEOUT": " 7s ",
				"AGENT_SERVER_IDLE_TIMEOUT":        " 80s ",
				"AGENT_SERVER_SHUTDOWN_TIMEOUT":    " 12s ",
				"AGENT_DATA_DIR":                   " data/runtime ",
				"AGENT_RUNTIME_ID":                 " web-runtime ",
				"AGENT_PROVIDER":                   " OpenAI ",
				"AGENT_MODEL":                      " gpt-test ",
				"AGENT_BASE_URL":                   " https://models.example/v1 ",
				"AGENT_API_KEY":                    " secret-key ",
				"AGENT_MODEL_TIMEOUT":              " 45s ",
				"AGENT_SYSTEM_PROMPT":              " Be accurate. ",
				"AGENT_MAX_TURNS":                  " 6 ",
				"AGENT_MAX_TOKENS":                 " 1024 ",
			},
			want: serverConfig{
				Address:           "127.0.0.1:9090",
				ReadHeaderTimeout: 7 * time.Second,
				IdleTimeout:       80 * time.Second,
				ShutdownTimeout:   12 * time.Second,
				DataDir:           "data/runtime",
				RuntimeID:         "web-runtime",
				Provider:          "openai",
				Model:             "gpt-test",
				BaseURL:           "https://models.example/v1",
				APIKey:            "secret-key",
				ModelTimeout:      45 * time.Second,
				AgentSystemPrompt: "Be accurate.",
				AgentMaxTurns:     6,
				AgentMaxTokens:    1024,
			},
		},
		{
			name: "command line overrides environment",
			args: []string{
				"-address=127.0.0.1:7070",
				"-read-header-timeout=8s",
				"-idle-timeout=90s",
				"-shutdown-timeout=15s",
				"-data-dir=flag/runtime",
				"-runtime-id=flag-runtime",
				"-provider= Anthropic ",
				"-model= flag-model ",
				"-base-url= https://flag.example ",
				"-model-timeout=50s",
				"-system-prompt= Flag prompt. ",
				"-max-turns=9",
				"-max-tokens=2048",
			},
			env: map[string]string{
				"AGENT_SERVER_ADDRESS":             "127.0.0.1:9090",
				"AGENT_SERVER_READ_HEADER_TIMEOUT": "7s",
				"AGENT_SERVER_IDLE_TIMEOUT":        "80s",
				"AGENT_SERVER_SHUTDOWN_TIMEOUT":    "12s",
				"AGENT_DATA_DIR":                   "env/runtime",
				"AGENT_RUNTIME_ID":                 "env-runtime",
				"AGENT_PROVIDER":                   "openai",
				"AGENT_MODEL":                      "env-model",
				"AGENT_BASE_URL":                   "https://env.example",
				"AGENT_API_KEY":                    " env-secret ",
				"AGENT_MODEL_TIMEOUT":              "45s",
				"AGENT_SYSTEM_PROMPT":              "Environment prompt.",
				"AGENT_MAX_TURNS":                  "6",
				"AGENT_MAX_TOKENS":                 "1024",
			},
			want: serverConfig{
				Address:           "127.0.0.1:7070",
				ReadHeaderTimeout: 8 * time.Second,
				IdleTimeout:       90 * time.Second,
				ShutdownTimeout:   15 * time.Second,
				DataDir:           "flag/runtime",
				RuntimeID:         "flag-runtime",
				Provider:          "anthropic",
				Model:             "flag-model",
				BaseURL:           "https://flag.example",
				APIKey:            "env-secret",
				ModelTimeout:      50 * time.Second,
				AgentSystemPrompt: "Flag prompt.",
				AgentMaxTurns:     9,
				AgentMaxTokens:    2048,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config, err := parseServerConfig(test.args, io.Discard, mapGetenv(test.env))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(config, test.want) {
				t.Fatalf("config mismatch\n got: %#v\nwant: %#v", config, test.want)
			}
		})
	}
}

func TestParseServerConfigRejectsMissingRequiredValues(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{name: "model", wantErr: "provider, model, and base URL are required"},
		{name: "data directory", args: []string{"-model=test-model", "-data-dir= "}, wantErr: "data directory and runtime id are required"},
		{name: "runtime id", args: []string{"-model=test-model", "-runtime-id= "}, wantErr: "data directory and runtime id are required"},
		{name: "provider", args: []string{"-model=test-model", "-provider= "}, wantErr: "provider, model, and base URL are required"},
		{name: "base URL", args: []string{"-model=test-model", "-base-url= "}, wantErr: "provider, model, and base URL are required"},
		{name: "system prompt", args: []string{"-model=test-model", "-system-prompt= "}, wantErr: "agent system prompt is required"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseServerConfig(test.args, io.Discard, mapGetenv(nil))
			if err == nil || err.Error() != test.wantErr {
				t.Fatalf("err=%v, want %q", err, test.wantErr)
			}
		})
	}
}

func TestParseServerConfigRejectsInvalidTimeoutsAndLimits(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		env         map[string]string
		wantErr     string
		errContains string
	}{
		{name: "read header timeout", args: []string{"-model=test-model", "-read-header-timeout=0s"}, wantErr: "server timeouts must be positive"},
		{name: "idle timeout", args: []string{"-model=test-model", "-idle-timeout=-1s"}, wantErr: "server timeouts must be positive"},
		{name: "shutdown timeout", args: []string{"-model=test-model", "-shutdown-timeout=0s"}, wantErr: "server timeouts must be positive"},
		{name: "model timeout", args: []string{"-model=test-model", "-model-timeout=0s"}, wantErr: "model timeout and agent max turns must be positive, and agent max tokens cannot be negative"},
		{name: "max turns", args: []string{"-model=test-model", "-max-turns=0"}, wantErr: "model timeout and agent max turns must be positive, and agent max tokens cannot be negative"},
		{name: "max tokens", args: []string{"-model=test-model", "-max-tokens=-1"}, wantErr: "model timeout and agent max turns must be positive, and agent max tokens cannot be negative"},
		{name: "invalid duration environment", env: map[string]string{"AGENT_MODEL": "test-model", "AGENT_MODEL_TIMEOUT": "later"}, errContains: "parse AGENT_MODEL_TIMEOUT"},
		{name: "invalid integer environment", env: map[string]string{"AGENT_MODEL": "test-model", "AGENT_MAX_TURNS": "many"}, errContains: "parse AGENT_MAX_TURNS"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseServerConfig(test.args, io.Discard, mapGetenv(test.env))
			switch {
			case err == nil:
				t.Fatal("expected an error")
			case test.wantErr != "" && err.Error() != test.wantErr:
				t.Fatalf("err=%v, want %q", err, test.wantErr)
			case test.errContains != "" && !strings.Contains(err.Error(), test.errContains):
				t.Fatalf("err=%v, want error containing %q", err, test.errContains)
			}
		})
	}
}

func TestParseServerConfigDoesNotAcceptAPIKeyFlag(t *testing.T) {
	_, err := parseServerConfig(
		[]string{"-model=test-model", "-api-key=visible-secret"},
		io.Discard,
		mapGetenv(nil),
	)
	if err == nil || !strings.Contains(err.Error(), "flag provided but not defined: -api-key") {
		t.Fatalf("err=%v", err)
	}
	if strings.Contains(err.Error(), "visible-secret") {
		t.Fatalf("error exposed API key: %v", err)
	}
}

func TestParseServerConfigErrorHasNoFilesystemSideEffects(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "runtime")
	_, err := parseServerConfig(
		[]string{"-data-dir=" + dataDir},
		io.Discard,
		mapGetenv(nil),
	)
	if err == nil {
		t.Fatal("expected missing model error")
	}
	if _, statErr := os.Stat(dataDir); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("data directory was created or stat failed unexpectedly: %v", statErr)
	}
}

func TestParseServerConfigHelpDescribesLocalRuntime(t *testing.T) {
	for _, arg := range []string{"-h", "--help"} {
		t.Run(arg, func(t *testing.T) {
			var output bytes.Buffer
			_, err := parseServerConfig([]string{arg}, &output, mapGetenv(nil))
			if !errors.Is(err, flag.ErrHelp) {
				t.Fatalf("err=%v, want flag.ErrHelp", err)
			}
			help := output.String()
			if !strings.Contains(help, "local, persistent service Runtime") {
				t.Fatalf("help does not describe the local Runtime:\n%s", help)
			}
			if strings.Contains(help, "Runtime integration is not connected yet") {
				t.Fatalf("help contains obsolete Runtime description:\n%s", help)
			}
			if strings.Contains(help, "-api-key") {
				t.Fatalf("help advertises an API key flag:\n%s", help)
			}
		})
	}
}

func mapGetenv(values map[string]string) func(string) string {
	return func(key string) string {
		return values[key]
	}
}
