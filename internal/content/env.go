package content

import (
	"context"
	systemIO "io"
)

type IO struct {
	In  systemIO.Reader
	Out systemIO.Writer
	Err systemIO.Writer
}
type Config struct {
	ConfigPath       string
	Provider         string
	Model            string
	ModelURL         string
	APIKeyConfigured bool
	AgentName        string
	WorkDir          string
	Debug            bool
}

type AgentRunner interface {
	Run(context.Context, string) error
}

type AgentSelector interface {
	ActiveAgentName() string
	ListAgentNames() []string
	SelectAgent(name string) error
}

type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

type CommandInfo interface {
	Name() string
	Description() string
}

type CommandRegistry interface {
	List() []CommandInfo
}

type Env struct {
	IO       IO
	Agent    AgentRunner
	Registry CommandRegistry
	Logger   Logger
	Config   Config
	RunModel string // cli 或者 cmd
}

type envContextKey struct{}

func WithEnv(ctx context.Context, env *Env) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, envContextKey{}, env)
}

func EnvFromContext(ctx context.Context) (*Env, bool) {
	if ctx == nil {
		return nil, false
	}
	env, ok := ctx.Value(envContextKey{}).(*Env)
	if !ok || env == nil {
		return nil, false
	}
	return env, true
}
