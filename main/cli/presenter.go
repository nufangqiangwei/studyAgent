package main

import (
	"agent/services/interaction"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
)

const presenterHistoryLimit = 5

type terminalPresenter struct {
	output io.Writer

	mu             sync.Mutex
	seen           map[string]struct{}
	seenOrder      []string
	completed      map[string]struct{}
	completedOrder []string
	notify         chan struct{}
}

func newTerminalPresenter(output io.Writer) *terminalPresenter {
	return &terminalPresenter{
		output: output, seen: make(map[string]struct{}), seenOrder: make([]string, 0, presenterHistoryLimit),
		completed: make(map[string]struct{}), completedOrder: make([]string, 0, presenterHistoryLimit),
		notify: make(chan struct{}, 1),
	}
}

func (p *terminalPresenter) Present(ctx context.Context, presentation interaction.Presentation) error {
	p.mu.Lock()
	_, duplicate := p.seen[presentation.ID]
	if !duplicate {
		if err := p.write(presentation); err != nil {
			p.mu.Unlock()
			return err
		}
		p.rememberSeen(presentation.ID)
	}
	terminal := presentation.RequestID != "" && presentation.Kind != interaction.PresentationApproval
	if terminal {
		p.rememberCompleted(presentation.RequestID)
	}
	p.mu.Unlock()
	if terminal {
		select {
		case p.notify <- struct{}{}:
		default:
		}
	}
	return nil
}

func (p *terminalPresenter) write(presentation interaction.Presentation) error {
	switch presentation.Kind {
	case interaction.PresentationAnswer:
		_, err := fmt.Fprintf(p.output, "\nassistant> %s\n", strings.TrimSpace(presentation.Content))
		return err
	case interaction.PresentationError:
		if presentation.ErrorCode == "" {
			_, err := fmt.Fprintf(p.output, "\nerror> %s\n", presentation.ErrorMessage)
			return err
		}
		_, err := fmt.Fprintf(p.output, "\nerror> %s (%s)\n", presentation.ErrorMessage, presentation.ErrorCode)
		return err
	case interaction.PresentationApproval:
		if presentation.Approval == nil {
			return fmt.Errorf("approval presentation is missing approval details")
		}
		_, err := fmt.Fprintf(
			p.output,
			"\napproval> %s@%s: %s\n%s\n",
			presentation.Approval.CapabilityRef,
			presentation.Approval.CapabilityVersion,
			presentation.Approval.RiskSummary,
			presentation.ErrorMessage,
		)
		return err
	default:
		return fmt.Errorf("unsupported presentation kind %q", presentation.Kind)
	}
}

func (p *terminalPresenter) wait(ctx context.Context, requestID string, serveErrors <-chan error) error {
	for {
		p.mu.Lock()
		_, completed := p.completed[requestID]
		if completed {
			delete(p.completed, requestID)
			p.completedOrder = removeID(p.completedOrder, requestID)
		}
		p.mu.Unlock()
		if completed {
			return nil
		}
		select {
		case <-p.notify:
		case err := <-serveErrors:
			if err == nil {
				return fmt.Errorf("Runtime stopped while request %q was running", requestID)
			}
			return fmt.Errorf("Runtime stopped while request %q was running: %w", requestID, err)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (p *terminalPresenter) rememberSeen(id string) {
	p.seen[id] = struct{}{}
	p.seenOrder = append(p.seenOrder, id)
	for len(p.seenOrder) > presenterHistoryLimit {
		oldest := p.seenOrder[0]
		p.seenOrder = p.seenOrder[1:]
		delete(p.seen, oldest)
	}
}

func (p *terminalPresenter) rememberCompleted(requestID string) {
	if _, found := p.completed[requestID]; found {
		return
	}
	p.completed[requestID] = struct{}{}
	p.completedOrder = append(p.completedOrder, requestID)
	for len(p.completedOrder) > presenterHistoryLimit {
		oldest := p.completedOrder[0]
		p.completedOrder = p.completedOrder[1:]
		delete(p.completed, oldest)
	}
}

func removeID(values []string, target string) []string {
	for index, value := range values {
		if value == target {
			return append(values[:index], values[index+1:]...)
		}
	}
	return values
}

func (p *terminalPresenter) printf(format string, args ...interface{}) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, err := fmt.Fprintf(p.output, format, args...)
	return err
}

func (p *terminalPresenter) println(args ...interface{}) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, err := fmt.Fprintln(p.output, args...)
	return err
}
