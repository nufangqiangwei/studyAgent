package config

import (
	"agent/internal/content"
	"context"
	"testing"
)

func TestWithEnvBindsEnvToContext(t *testing.T) {
	env := &content.Env{
		Config: content.Config{
			WorkDir: "root",
		},
	}

	ctx := content.WithEnv(context.Background(), env)

	got, ok := content.EnvFromContext(ctx)
	if !ok {
		t.Fatal("EnvFromContext returned ok=false")
	}
	if got != env {
		t.Fatal("EnvFromContext returned a different env")
	}
}

func TestWithEnvAllowsChildContextOverride(t *testing.T) {
	parentEnv := &content.Env{
		Config: content.Config{
			WorkDir: "parent",
		},
	}
	childEnv := &content.Env{
		Config: content.Config{
			WorkDir: "child",
		},
	}

	parentCtx := content.WithEnv(context.Background(), parentEnv)
	childCtx := content.WithEnv(parentCtx, childEnv)

	gotParent, ok := content.EnvFromContext(parentCtx)
	if !ok {
		t.Fatal("parent context env missing")
	}
	if gotParent != parentEnv {
		t.Fatal("parent context env was changed")
	}

	gotChild, ok := content.EnvFromContext(childCtx)
	if !ok {
		t.Fatal("child context env missing")
	}
	if gotChild != childEnv {
		t.Fatal("child context did not use child env")
	}
}
