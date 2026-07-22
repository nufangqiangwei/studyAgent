package agent

import (
	"agent/serviceruntime/artifact"
	"agent/serviceruntime/contract"
	"agent/services/capability"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
)

const modelProtocolInstruction = `Return exactly one JSON object and no markdown.
To call a capability:
{"action":"capability","capability_ref":"<ref>","capability_version":"<version>","arguments":{...}}
To finish:
{"action":"finish","answer":"<final answer>"}
Use only capabilities listed in the prompt. Never claim a capability succeeded before its result appears in the transcript.`

type promptPreparation struct {
	Operation     artifactOperation       `json:"operation"`
	AgentAddress  contract.ServiceAddress `json:"agent_address"`
	RunID         string                  `json:"run_id"`
	Turn          int                     `json:"turn"`
	UserID        string                  `json:"user_id,omitempty"`
	GoalID        string                  `json:"goal_id,omitempty"`
	CorrelationID string                  `json:"correlation_id"`
	Spec          AgentSpec               `json:"spec"`
	Input         string                  `json:"input,omitempty"`
	InputArtifact *contract.ArtifactRef   `json:"input_artifact,omitempty"`
	Capabilities  []ResolvedCapability    `json:"capabilities,omitempty"`
	History       []TurnRecord            `json:"history,omitempty"`
	Source        *contract.ArtifactRef   `json:"source,omitempty"`
}

type artifactPrepared struct {
	Operation artifactOperation    `json:"operation"`
	RunID     string               `json:"run_id"`
	Turn      int                  `json:"turn"`
	Artifact  contract.ArtifactRef `json:"artifact"`
}

type artifactFailed struct {
	Operation artifactOperation `json:"operation"`
	RunID     string            `json:"run_id"`
	Turn      int               `json:"turn"`
	ErrorCode string            `json:"error_code"`
}

func resolveCapabilities(spec AgentSpec, response capability.ListResponse) ([]ResolvedCapability, error) {
	available := make(map[string]capability.CapabilityDescriptor, len(response.Descriptors))
	for _, descriptor := range response.Descriptors {
		available[descriptor.Ref+"@"+descriptor.Version] = descriptor.Clone()
	}
	if len(spec.Capabilities) == 0 {
		keys := make([]string, 0, len(available))
		for key := range available {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		resolved := make([]ResolvedCapability, 0, len(keys))
		for _, key := range keys {
			descriptor := available[key]
			resolved = append(resolved, ResolvedCapability{
				Descriptor:      descriptor,
				Description:     "Capability " + descriptor.Ref,
				ArgumentsSchema: json.RawMessage(`{}`),
			})
		}
		return resolved, nil
	}
	resolved := make([]ResolvedCapability, 0, len(spec.Capabilities))
	for _, prompt := range spec.Capabilities {
		key := prompt.Ref + "@" + prompt.Version
		descriptor, found := available[key]
		if !found {
			return nil, fmt.Errorf("required capability %q is not available", key)
		}
		resolved = append(resolved, ResolvedCapability{
			Descriptor: descriptor.Clone(), Description: prompt.Description,
			ArgumentsSchema: contract.CloneRaw(prompt.ArgumentsSchema),
		})
	}
	return resolved, nil
}

func findCapability(values []ResolvedCapability, ref, version string) (ResolvedCapability, bool) {
	for _, value := range values {
		if value.Descriptor.Ref == ref && value.Descriptor.Version == version {
			return value.clone(), true
		}
	}
	return ResolvedCapability{}, false
}

func parseModelAction(data []byte) (ModelAction, error) {
	trimmed := bytes.TrimSpace(data)
	if bytes.HasPrefix(trimmed, []byte("```")) {
		firstLine := bytes.IndexByte(trimmed, '\n')
		if firstLine < 0 || !bytes.HasSuffix(trimmed, []byte("```")) {
			return ModelAction{}, fmt.Errorf("model response has an incomplete code fence")
		}
		trimmed = bytes.TrimSpace(trimmed[firstLine+1 : len(trimmed)-3])
	}
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	var action ModelAction
	if err := decoder.Decode(&action); err != nil {
		return ModelAction{}, fmt.Errorf("model response is not a JSON action: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return ModelAction{}, fmt.Errorf("model response contains trailing content")
	}
	action.Action = strings.ToLower(strings.TrimSpace(action.Action))
	action.CapabilityRef = strings.TrimSpace(action.CapabilityRef)
	action.CapabilityVersion = strings.TrimSpace(action.CapabilityVersion)
	switch action.Action {
	case "finish":
		if strings.TrimSpace(action.Answer) == "" || action.CapabilityRef != "" || action.CapabilityVersion != "" || len(action.Arguments) > 0 {
			return ModelAction{}, fmt.Errorf("finish action requires only a non-empty answer")
		}
	case "capability":
		if action.CapabilityRef == "" || action.CapabilityVersion == "" || len(action.Arguments) == 0 || !json.Valid(action.Arguments) || action.Answer != "" {
			return ModelAction{}, fmt.Errorf("capability action requires ref, version, and valid arguments")
		}
	default:
		return ModelAction{}, fmt.Errorf("model action %q is unsupported", action.Action)
	}
	return action.clone(), nil
}

func readArtifact(ctx context.Context, reader artifact.Reader, ref contract.ArtifactRef, limit int64) ([]byte, error) {
	if reader == nil {
		return nil, artifact.ErrUnavailable
	}
	if err := artifact.ValidateRef(ref); err != nil {
		return nil, err
	}
	stream, _, err := reader.Open(ctx, ref)
	if err != nil {
		return nil, err
	}
	defer stream.Close()
	data, err := io.ReadAll(io.LimitReader(stream, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("artifact %q exceeds %d bytes", ref.Key, limit)
	}
	return data, nil
}

type boundedWriter struct {
	writer io.Writer
	limit  int64
	wrote  int64
}

func (w *boundedWriter) Write(value []byte) (int, error) {
	if int64(len(value)) > w.limit-w.wrote {
		return 0, fmt.Errorf("prepared prompt exceeds %d bytes", w.limit)
	}
	n, err := w.writer.Write(value)
	w.wrote += int64(n)
	return n, err
}

func writePrompt(ctx context.Context, destination io.Writer, store artifact.Store, input promptPreparation) error {
	buffered := bufio.NewWriter(destination)
	limited := &boundedWriter{writer: buffered, limit: input.Spec.MaxPromptBytes}
	write := func(format string, values ...any) error {
		_, err := fmt.Fprintf(limited, format, values...)
		return err
	}
	if err := write("# Agent\n%s@%s\n\n# System instructions\n%s\n\n# Response protocol\n%s\n\n", input.Spec.Ref, input.Spec.Version, input.Spec.SystemPrompt, modelProtocolInstruction); err != nil {
		return err
	}
	if err := write("# Available capabilities\n"); err != nil {
		return err
	}
	if len(input.Capabilities) == 0 {
		if err := write("No capabilities are available.\n"); err != nil {
			return err
		}
	}
	for _, resolved := range input.Capabilities {
		schema := resolved.ArgumentsSchema
		if len(schema) == 0 {
			schema = json.RawMessage(`{}`)
		}
		if err := write("- %s@%s (descriptor %s)\n  %s\n  arguments_schema: %s\n", resolved.Descriptor.Ref, resolved.Descriptor.Version, resolved.Descriptor.DescriptorRevision, resolved.Description, schema); err != nil {
			return err
		}
	}
	if err := write("\n# Goal\n%s", input.Input); err != nil {
		return err
	}
	if input.InputArtifact != nil {
		if err := write("\n"); err != nil {
			return err
		}
		if err := copyArtifact(ctx, limited, store, *input.InputArtifact, input.Spec.MaxArtifactBytes); err != nil {
			return fmt.Errorf("append goal artifact: %w", err)
		}
	}
	if err := write("\n\n# Transcript\n"); err != nil {
		return err
	}
	for _, turn := range input.History {
		if err := write("\n## Turn %d model response\n", turn.Number); err != nil {
			return err
		}
		if turn.ModelResponseRef != nil {
			if err := copyArtifact(ctx, limited, store, *turn.ModelResponseRef, input.Spec.MaxArtifactBytes); err != nil {
				return fmt.Errorf("append model response for turn %d: %w", turn.Number, err)
			}
		}
		if turn.Feedback != "" {
			if err := write("\nRuntime feedback: %s\n", turn.Feedback); err != nil {
				return err
			}
		}
		if turn.Capability != nil {
			if err := write("\nCapability result for %s: phase=%s error_code=%s\n", turn.Capability.CallID, turn.Capability.Phase, turn.Capability.ErrorCode); err != nil {
				return err
			}
			if turn.Capability.ResultRef != nil {
				if err := copyArtifact(ctx, limited, store, *turn.Capability.ResultRef, input.Spec.MaxArtifactBytes); err != nil {
					return fmt.Errorf("append capability result for turn %d: %w", turn.Number, err)
				}
			} else if len(turn.Capability.Result) > 0 {
				if _, err := limited.Write(turn.Capability.Result); err != nil {
					return err
				}
			}
			if turn.Capability.ErrorMessage != "" {
				if err := write("\n%s\n", turn.Capability.ErrorMessage); err != nil {
					return err
				}
			}
		}
	}
	if err := write("\nProduce the next JSON action now.\n"); err != nil {
		return err
	}
	return buffered.Flush()
}

func copyArtifact(ctx context.Context, destination io.Writer, store artifact.Reader, ref contract.ArtifactRef, limit int64) error {
	if err := artifact.ValidateRef(ref); err != nil {
		return err
	}
	stream, _, err := store.Open(ctx, ref)
	if err != nil {
		return err
	}
	defer stream.Close()
	limited := &io.LimitedReader{R: stream, N: limit + 1}
	written, err := io.Copy(destination, limited)
	if err != nil {
		return err
	}
	if written > limit {
		return fmt.Errorf("artifact %q exceeds %d bytes", ref.Key, limit)
	}
	return nil
}

func writeFinalOutput(ctx context.Context, destination io.Writer, store artifact.Store, input promptPreparation) error {
	if input.Source == nil {
		return fmt.Errorf("final output source is required")
	}
	data, err := readArtifact(ctx, store, *input.Source, input.Spec.MaxArtifactBytes)
	if err != nil {
		return err
	}
	action, err := parseModelAction(data)
	if err != nil {
		return err
	}
	if action.Action != "finish" {
		return fmt.Errorf("final output source does not contain a finish action")
	}
	_, err = io.WriteString(destination, action.Answer)
	return err
}
