package main

import (
	"io"
	"testing"
	"time"
)

func TestParseServerConfig(t *testing.T) {
	config, err := parseServerConfig([]string{"-shutdown-timeout=3s"}, io.Discard, func(key string) string {
		if key == "AGENT_SERVER_ADDRESS" {
			return "127.0.0.1:9090"
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	if config.Address != "127.0.0.1:9090" || config.ShutdownTimeout != 3*time.Second {
		t.Fatalf("config=%#v", config)
	}
}
