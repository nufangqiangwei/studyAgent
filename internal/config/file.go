package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

type File struct {
	ModelURL  string `json:"model_url"`
	ModelName string `json:"model_name"`
	APIKey    string `json:"api_key"`
}

func LoadOptional(path string) (File, bool, error) {
	cfg, err := Load(path)
	if err == nil {
		return cfg, true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return File{}, false, nil
	}
	return File{}, false, err
}

func Load(path string) (File, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return File{}, fmt.Errorf("read config file %s: %w", path, err)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	var cfg File
	if err := decoder.Decode(&cfg); err != nil {
		return File{}, fmt.Errorf("parse config file %s: %w", path, err)
	}

	cfg.ModelURL = strings.TrimSpace(cfg.ModelURL)
	cfg.ModelName = strings.TrimSpace(cfg.ModelName)
	cfg.APIKey = strings.TrimSpace(cfg.APIKey)

	return cfg, nil
}
