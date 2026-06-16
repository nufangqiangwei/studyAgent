package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	err := os.WriteFile(path, []byte(`{
  "model_url": " https://example.test/v1/chat/completions ",
  "model_name": " test-model ",
  "api_key": " secret "
}`), 0600)
	if err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.ModelURL != "https://example.test/v1/chat/completions" {
		t.Fatalf("ModelURL = %q", cfg.ModelURL)
	}
	if cfg.ModelName != "test-model" {
		t.Fatalf("ModelName = %q", cfg.ModelName)
	}
	if cfg.APIKey != "secret" {
		t.Fatalf("APIKey = %q", cfg.APIKey)
	}
}

func TestLoadOptionalMissingConfigFile(t *testing.T) {
	_, found, err := LoadOptional(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatalf("LoadOptional returned error: %v", err)
	}
	if found {
		t.Fatal("found = true, want false")
	}
}
