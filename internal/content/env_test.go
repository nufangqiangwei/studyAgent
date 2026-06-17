package content

import (
	"context"
	"strings"
	"testing"
)

func TestEnvWithMethodsReturnUpdatedCopy(t *testing.T) {
	var out strings.Builder
	registry := testRegistry{}
	base := Env{
		Config: Config{
			Model:   "old-model",
			WorkDir: "workspace",
		},
		RunModel: "cmd",
	}

	next := base.
		WithIO(IO{Out: &out}).
		WithRegistry(registry).
		WithRunModel("cli").
		WithConfigUpdate(func(config *Config) {
			config.Model = "new-model"
		})

	if base.Config.Model != "old-model" {
		t.Fatalf("base model = %q, want old-model", base.Config.Model)
	}
	if base.Registry != nil {
		t.Fatal("base registry was changed")
	}
	if base.RunModel != "cmd" {
		t.Fatalf("base run model = %q, want cmd", base.RunModel)
	}
	if next.Config.Model != "new-model" {
		t.Fatalf("next model = %q, want new-model", next.Config.Model)
	}
	if next.Config.WorkDir != "workspace" {
		t.Fatalf("next workdir = %q, want workspace", next.Config.WorkDir)
	}
	if next.IO.Out != &out {
		t.Fatal("next IO.Out was not updated")
	}
	if next.Registry != registry {
		t.Fatal("next registry was not updated")
	}
	if next.RunModel != "cli" {
		t.Fatalf("next run model = %q, want cli", next.RunModel)
	}
}

func TestEnvUpdateMethodsMutateExistingEnv(t *testing.T) {
	env := &Env{
		Config: Config{
			Model:   "old-model",
			WorkDir: "workspace",
		},
		RunModel: "cmd",
	}

	env.Update(func(env *Env) {
		env.RunModel = "cli"
	})
	env.UpdateConfig(func(config *Config) {
		config.Model = "new-model"
	})

	if env.RunModel != "cli" {
		t.Fatalf("run model = %q, want cli", env.RunModel)
	}
	if env.Config.Model != "new-model" {
		t.Fatalf("model = %q, want new-model", env.Config.Model)
	}
	if env.Config.WorkDir != "workspace" {
		t.Fatalf("workdir = %q, want workspace", env.Config.WorkDir)
	}
}

func TestWithUpdatedEnvDerivesChildContext(t *testing.T) {
	parent := &Env{
		Config: Config{
			Model:   "old-model",
			WorkDir: "workspace",
		},
		RunModel: "cmd",
	}
	parentCtx := WithEnv(context.Background(), parent)

	childCtx, child := WithUpdatedEnv(parentCtx, func(env *Env) {
		env.RunModel = "cli"
		env.Config.Model = "new-model"
	})

	gotParent, ok := EnvFromContext(parentCtx)
	if !ok {
		t.Fatal("parent context env missing")
	}
	if gotParent != parent {
		t.Fatal("parent context env pointer changed")
	}
	if gotParent.Config.Model != "old-model" {
		t.Fatalf("parent model = %q, want old-model", gotParent.Config.Model)
	}

	gotChild, ok := EnvFromContext(childCtx)
	if !ok {
		t.Fatal("child context env missing")
	}
	if gotChild != child {
		t.Fatal("child context did not bind returned env")
	}
	if gotChild == parent {
		t.Fatal("child context reused parent env pointer")
	}
	if gotChild.Config.Model != "new-model" {
		t.Fatalf("child model = %q, want new-model", gotChild.Config.Model)
	}
	if gotChild.Config.WorkDir != "workspace" {
		t.Fatalf("child workdir = %q, want workspace", gotChild.Config.WorkDir)
	}
	if gotChild.RunModel != "cli" {
		t.Fatalf("child run model = %q, want cli", gotChild.RunModel)
	}
}

func TestWithUpdatedConfigCreatesEnvWhenMissing(t *testing.T) {
	ctx, env := WithUpdatedConfig(context.Background(), func(config *Config) {
		config.Model = "new-model"
	})

	got, ok := EnvFromContext(ctx)
	if !ok {
		t.Fatal("context env missing")
	}
	if got != env {
		t.Fatal("context did not bind returned env")
	}
	if got.Config.Model != "new-model" {
		t.Fatalf("model = %q, want new-model", got.Config.Model)
	}
}

type testRegistry struct{}

func (testRegistry) List() []CommandInfo {
	return nil
}
