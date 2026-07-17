package app

import (
	"agent/internal/content"
	"agent/internal/foundation/policy"
	appruntime "agent/internal/runtime"
	"agent/internal/runtime/agents"
	"agent/internal/runtime/eventbus"
	"agent/internal/runtime/persistence"
	"agent/internal/runtime/reactor"
	"agent/internal/runtime/runservice"
	"agent/internal/runtime/tools"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
)

type runtimeSetupOptions struct {
	LLM              appLLMClient
	Model            string
	MaxSteps         int
	WorkDir          string
	In               io.Reader
	Out              io.Writer
	RuntimeStoreRoot string
	Policy           policy.Checker
	Agents           *agents.FactoryRegistry
}

// runtimeSetupBuilder is application composition only: it connects provider,
// tools, persistence and registered agent modules into a runtime object graph.
type runtimeSetupBuilder struct {
	opts runtimeSetupOptions
}

func newRuntimeSetupBuilder(options runtimeSetupOptions) (*runtimeSetupBuilder, error) {
	if options.Agents == nil {
		return nil, fmt.Errorf("runtime setup builder: agent factory registry is required")
	}
	if strings.TrimSpace(options.RuntimeStoreRoot) == "" {
		return nil, fmt.Errorf("runtime setup builder: runtime store root is required")
	}
	return &runtimeSetupBuilder{opts: options}, nil
}

func (b *runtimeSetupBuilder) DefaultWorkDir() string {
	if b == nil {
		return ""
	}
	return strings.TrimSpace(b.opts.WorkDir)
}

func (b *runtimeSetupBuilder) OpenStorage(_ context.Context) (persistence.RuntimeStorage, *persistence.WorkQueue, func(), error) {
	if b == nil {
		return nil, nil, nil, fmt.Errorf("runtime setup builder is nil")
	}
	root := strings.TrimSpace(b.opts.RuntimeStoreRoot)
	if root == "" {
		return nil, nil, nil, fmt.Errorf("runtime store root is required")
	}
	storage, err := persistence.NewFileStore(root)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create runtime storage: %w", err)
	}
	queue, err := persistence.NewWorkQueue(filepath.Join(root, "queue"))
	if err != nil {
		_ = storage.Close()
		return nil, nil, nil, err
	}
	return storage, queue, func() { _ = storage.Close() }, nil
}

func (b *runtimeSetupBuilder) Build(ctx context.Context, taskID string, requestedAgent string) (*runservice.Setup, error) {
	storage, queue, closeStore, err := b.OpenStorage(ctx)
	if err != nil {
		return nil, err
	}
	owned := false
	defer func() {
		if !owned {
			closeStore()
		}
	}()

	agentName := b.agentForTask(ctx, storage, taskID, requestedAgent)
	workDir := b.workDirForTask(ctx, storage, taskID)
	source := "app." + agentName
	env := content.Env{
		IO:     content.IO{In: b.opts.In, Out: b.opts.Out},
		Config: content.Config{Model: b.opts.Model, AgentName: agentName, WorkDir: workDir},
	}
	toolAdapter, err := tools.NewDefault(
		tools.WithPolicy(b.opts.Policy), tools.WithWorkDir(workDir), tools.WithEnv(env), tools.WithSource(source+".tools"),
	)
	if err != nil {
		return nil, err
	}
	modelExecutor, err := agents.NewModelExecutor(&agentModelAdapter{client: b.opts.LLM}, agents.WithModelExecutorSource(source+".model"))
	if err != nil {
		return nil, err
	}
	rt, err := appruntime.New(
		appruntime.WithOwnedStorage(storage), appruntime.WithSource(source),
		appruntime.WithEffectDispatcher(queue), appruntime.WithResultDelivery(eventbus.DeliverySync),
		appruntime.WithEffectExecutor(reactor.EffectModelCall, modelExecutor),
		appruntime.WithEffectExecutor(reactor.EffectToolDispatch, toolAdapter),
		appruntime.WithEffectExecutor(reactor.EffectUserInputRequest, userInputRequestExecutor{}),
	)
	if err != nil {
		return nil, err
	}
	owned = true

	maxTurns := b.opts.MaxSteps
	if maxTurns <= 0 {
		maxTurns = 20
	}
	agent, err := b.opts.Agents.Create(agentName, agents.FactoryConfig{
		ModelName: b.opts.Model, SnapshotStore: rt.SnapshotStore(), Tools: toolAdapter.Specs(),
		Source: source + ".agent", MaxTurns: maxTurns,
	})
	if err != nil {
		_ = rt.Close()
		return nil, err
	}
	taskRuntime, err := rt.CreateTaskRuntime(ctx, taskID, agent)
	if err != nil {
		_ = rt.Close()
		return nil, err
	}
	return &runservice.Setup{
		Runtime: rt, Storage: storage, Queue: queue, TaskRuntime: taskRuntime, WorkDir: workDir,
	}, nil
}

func (b *runtimeSetupBuilder) agentForTask(ctx context.Context, storage persistence.RuntimeStorage, taskID string, fallback string) string {
	if storage != nil {
		if snapshot, ok, err := storage.Runtimes().Load(ctx, taskID); err == nil && ok && strings.TrimSpace(snapshot.Agent) != "" {
			return strings.TrimSpace(snapshot.Agent)
		}
		if state, ok, err := storage.TaskStates().Load(ctx, taskID); err == nil && ok && strings.TrimSpace(state.Agent.Name) != "" {
			return strings.TrimSpace(state.Agent.Name)
		}
	}
	if b != nil && b.opts.Agents != nil {
		if canonical, ok := b.opts.Agents.CanonicalName(fallback); ok {
			return canonical
		}
	}
	return strings.TrimSpace(fallback)
}

func (b *runtimeSetupBuilder) workDirForTask(ctx context.Context, storage persistence.RuntimeStorage, taskID string) string {
	if storage != nil {
		if state, ok, err := storage.TaskStates().Load(ctx, taskID); err == nil && ok {
			if workDir := strings.TrimSpace(state.Metadata["work_dir"]); workDir != "" {
				return workDir
			}
		}
		if snapshot, ok, err := storage.Runtimes().Load(ctx, taskID); err == nil && ok {
			if workDir := strings.TrimSpace(snapshot.Metadata["work_dir"]); workDir != "" {
				return workDir
			}
		}
	}
	return b.DefaultWorkDir()
}

type userInputRequestExecutor struct{}

func (userInputRequestExecutor) ExecuteEffect(_ context.Context, _ reactor.TaskRuntime, effect reactor.Effect) (reactor.EffectResult, error) {
	if effect.Type != reactor.EffectUserInputRequest {
		return reactor.EffectResult{}, fmt.Errorf("user input executor: unsupported effect type %q", effect.Type)
	}
	return reactor.EffectResult{}, nil
}
