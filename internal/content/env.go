package content

import (
	"agent/internal/session"
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
	PolicyMode       string
}

type AgentRunner interface {
	Run(context.Context, string) error
}

type AsyncRunStatus struct {
	RunID              string
	AdvanceStatus      string
	Phase              string
	FinalAnswer        string
	StepsUsed          int
	WorkDir            string
	WaitingReason      string
	WaitingTarget      string
	PendingEvents      int
	PendingEffects     int
	EventType          string
	EffectType         string
	ProducedEventTypes []string
	Error              string
}

type AsyncRecoverResult struct {
	Runs []AsyncRunStatus
}

type AsyncWorkResult struct {
	Ran    bool
	Status AsyncRunStatus
}

type AsyncAgentRunner interface {
	Submit(context.Context, string) (AsyncRunStatus, error)
	Recover(context.Context) (AsyncRecoverResult, error)
	Work(context.Context) (AsyncWorkResult, error)
	Advance(context.Context, string) (AsyncRunStatus, error)
	DispatchNextEffect(context.Context, string) (AsyncRunStatus, error)
	SubmitUserInput(context.Context, string, string) (AsyncRunStatus, error)
	SubmitUserApproval(context.Context, string, bool, string) (AsyncRunStatus, error)
	Result(context.Context, string) (AsyncRunStatus, error)
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
	IO         IO
	Agent      AgentRunner
	Registry   CommandRegistry
	Logger     Logger
	Config     Config
	Session    session.Recorder
	EventScope session.EventScope
	RunModel   string // cli 或者 cmd
}

type envContextKey struct{}

// Clone returns a shallow copy of the environment.
func (e Env) Clone() Env {
	return e
}

// WithIO returns an environment copy with IO replaced.
func (e Env) WithIO(io IO) Env {
	e.IO = io
	return e
}

// WithAgent returns an environment copy with Agent replaced.
func (e Env) WithAgent(agent AgentRunner) Env {
	e.Agent = agent
	return e
}

// WithRegistry returns an environment copy with Registry replaced.
func (e Env) WithRegistry(registry CommandRegistry) Env {
	e.Registry = registry
	return e
}

// WithLogger returns an environment copy with Logger replaced.
func (e Env) WithLogger(logger Logger) Env {
	e.Logger = logger
	return e
}

// WithSession returns an environment copy with the session recorder replaced.
func (e Env) WithSession(recorder session.Recorder) Env {
	e.Session = recorder
	return e
}

// WithEventScope returns an environment copy with event metadata replaced.
func (e Env) WithEventScope(scope session.EventScope) Env {
	e.EventScope = scope
	return e
}

// WithConfig returns an environment copy with Config replaced.
func (e Env) WithConfig(config Config) Env {
	e.Config = config
	return e
}

// WithConfigUpdate returns an environment copy with a partial Config update.
func (e Env) WithConfigUpdate(update func(*Config)) Env {
	if update != nil {
		update(&e.Config)
	}
	return e
}

// WithRunModel returns an environment copy with RunModel replaced.
func (e Env) WithRunModel(runModel string) Env {
	e.RunModel = runModel
	return e
}

// WithUpdate returns an environment copy with a partial Env update.
func (e Env) WithUpdate(update func(*Env)) Env {
	if update != nil {
		update(&e)
	}
	return e
}

// Update applies a partial update to the existing environment.
func (e *Env) Update(update func(*Env)) {
	if e == nil || update == nil {
		return
	}
	update(e)
}

// UpdateConfig applies a partial Config update to the existing environment.
func (e *Env) UpdateConfig(update func(*Config)) {
	if e == nil || update == nil {
		return
	}
	update(&e.Config)
}

func WithEnv(ctx context.Context, env *Env) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, envContextKey{}, env)
}

// WithUpdatedEnv copies the current context Env, applies a local update, and
// returns a child context bound to that copy.
func WithUpdatedEnv(ctx context.Context, update func(*Env)) (context.Context, *Env) {
	var next Env
	if env, ok := EnvFromContext(ctx); ok {
		next = env.Clone()
	}
	next.Update(update)
	return WithEnv(ctx, &next), &next
}

// WithUpdatedConfig copies the current context Env and applies a local Config update.
func WithUpdatedConfig(ctx context.Context, update func(*Config)) (context.Context, *Env) {
	return WithUpdatedEnv(ctx, func(env *Env) {
		env.UpdateConfig(update)
	})
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
