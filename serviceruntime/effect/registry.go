package effect

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/persistence"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

type ExecutionResult struct {
	Payload  json.RawMessage
	Metadata map[string]string
}

type Executor interface {
	ExecuteEffect(ctx context.Context, effect persistence.EffectRecord) (ExecutionResult, error)
}

type ExecutorFunc func(ctx context.Context, effect persistence.EffectRecord) (ExecutionResult, error)

func (f ExecutorFunc) ExecuteEffect(ctx context.Context, effect persistence.EffectRecord) (ExecutionResult, error) {
	return f(ctx, effect)
}

type ReconciliationAction string

const (
	ReconcileComplete   ReconciliationAction = "complete"
	ReconcileRetry      ReconciliationAction = "retry"
	ReconcileCompensate ReconciliationAction = "compensate"
	ReconcileAskUser    ReconciliationAction = "ask_user"
	ReconcileFail       ReconciliationAction = "fail"
)

type ReconciliationResult struct {
	Action  ReconciliationAction
	Result  json.RawMessage
	RetryAt *time.Time
	Reason  string
	status  persistence.EffectStatus
}

type Reconciler interface {
	ReconcileEffect(ctx context.Context, effect persistence.EffectRecord) (ReconciliationResult, error)
}

type ReconcilerFunc func(ctx context.Context, effect persistence.EffectRecord) (ReconciliationResult, error)

func (f ReconcilerFunc) ReconcileEffect(ctx context.Context, effect persistence.EffectRecord) (ReconciliationResult, error) {
	return f(ctx, effect)
}

// TerminalFailureNotifier lets an explicitly installed module turn a final
// Effect failure into a durable business message. The notifier must be
// idempotent because it can run again after a crash between notification and
// the terminal Effect-store update.
type TerminalFailureNotifier interface {
	NotifyTerminalFailure(ctx context.Context, effect persistence.EffectRecord, cause error) error
}

type TerminalFailureNotifierFunc func(ctx context.Context, effect persistence.EffectRecord, cause error) error

func (f TerminalFailureNotifierFunc) NotifyTerminalFailure(ctx context.Context, effect persistence.EffectRecord, cause error) error {
	return f(ctx, effect, cause)
}

type Spec struct {
	Ref             string
	Type            contract.EffectType
	Executor        Executor
	Reconciler      Reconciler
	TerminalFailure TerminalFailureNotifier
}

type Registry struct {
	mu    sync.RWMutex
	specs map[string]Spec
}

func NewRegistry() *Registry {
	return &Registry{specs: make(map[string]Spec)}
}

func (r *Registry) Register(spec Spec) error {
	if r == nil {
		return fmt.Errorf("effect registry is nil")
	}
	spec.Ref = strings.TrimSpace(spec.Ref)
	if spec.Ref == "" || spec.Type == "" || spec.Executor == nil {
		return fmt.Errorf("effect ref, type and executor are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.specs[spec.Ref]; exists {
		return fmt.Errorf("effect executor %q is already registered", spec.Ref)
	}
	r.specs[spec.Ref] = spec
	return nil
}

func (r *Registry) ResolveExecutor(ref string) (Executor, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.RLock()
	spec, ok := r.specs[strings.TrimSpace(ref)]
	r.mu.RUnlock()
	return spec.Executor, ok
}

func (r *Registry) ResolveReconciler(ref string) (Reconciler, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.RLock()
	spec, ok := r.specs[strings.TrimSpace(ref)]
	r.mu.RUnlock()
	return spec.Reconciler, ok && spec.Reconciler != nil
}

func (r *Registry) ResolveTerminalFailure(ref string) (TerminalFailureNotifier, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.RLock()
	spec, ok := r.specs[strings.TrimSpace(ref)]
	r.mu.RUnlock()
	return spec.TerminalFailure, ok && spec.TerminalFailure != nil
}

func (r *Registry) Refs() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	refs := make([]string, 0, len(r.specs))
	for ref := range r.specs {
		refs = append(refs, ref)
	}
	r.mu.RUnlock()
	sort.Strings(refs)
	return refs
}
