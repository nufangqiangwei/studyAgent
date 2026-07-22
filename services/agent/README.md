# Single Agent Service

`agent` is an explicitly installed business module on top of `serviceruntime`.
It implements one mounted Agent Service and deliberately has no spawn, child
Agent, supervisor, task, or orchestrator protocol.

The service owns recoverable `RunState` and advances this durable loop:

```text
agent.execute
  -> capability.list (freeze the capability view for this Run)
  -> prepare immutable prompt Artifact
  -> model.complete
  -> capability.invoke, when requested by the model
  -> append the capability result to the next prompt
  -> model.complete
  -> prepare immutable final-output Artifact
  -> agent.completed
```

`Handle` never calls a model, tool, Artifact writer, or another Service. Prompt
and output materialization are persisted Effects. Model and capability work is
requested through durable messages. Every large input, prompt, model response,
capability result, and final answer is represented by `ArtifactRef` when it
crosses the Service boundary.

## Model response protocol

The current MVP uses a provider-neutral JSON action protocol. The model must
return exactly one of these objects:

```json
{"action":"capability","capability_ref":"workspace.read","capability_version":"v1","arguments":{"path":"README.md"}}
```

```json
{"action":"finish","answer":"The task is complete."}
```

Invalid model output is recorded as a rejected turn and fed back to the next
model round. The Run fails safely when `MaxTurns` is exhausted. Capability
calls use a stable Agent-owned `CallID`; the model cannot choose an execution
identity or invoke a capability that was not frozen into the Run.

## Installation

```go
agents, err := agent.NewModule(agent.AgentSpec{
    Ref:          "coding-agent",
    Version:      "v1",
    SystemPrompt: "Complete the requested coding work and verify it.",
    Capabilities: []agent.CapabilityPrompt{
        {
            Ref:             "workspace.read",
            Version:         "v1",
            Description:     "Read a file from the active workspace.",
            ArgumentsSchema: json.RawMessage(`{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}`),
        },
    },
}, clock)
if err != nil {
    return err
}
if err := agents.Register(builder); err != nil {
    return err
}

manifest.Services = append(manifest.Services,
    agents.Mount(agent.DefaultAddress, llmClient.DefaultAddress, capability.DefaultAddress),
)
```

The application must also install and mount `llmClient`, `capability`, and its
required `approval` dependency, and must inject an Artifact Store. Use SQLite
plus a local Artifact Store when Runs must survive process restarts.

Start a Run with an explicit final reply target:

```go
payload, _ := json.Marshal(agent.ExecuteRequest{
    RunID: "run-42",
    Input: "Implement the requested change and run the tests.",
})
_, err := runtime.Publish(ctx, contract.Message{
    Kind:    contract.MessageCommand,
    Type:    agent.ExecuteMessageType,
    Version: agent.ProtocolVersion,
    From:    "gateway.main",
    To:      agent.DefaultAddress,
    ReplyTo: "gateway.main",
    RunID:   "run-42",
    Payload: payload,
})
```

The terminal `agent.completed` Reply contains an `ExecuteResult`. On success,
`ExecuteResult.Output` points to a plain-text final answer Artifact. On failure
or cancellation it contains a stable error code and safe message.

## Current boundary

- One statically mounted Agent Service; no child Agent or `agent.spawn`.
- Multiple Run records may be in flight, but the single durable mailbox owns
  their serialized state transitions.
- No TaskService or Orchestrator; callers submit Runs directly and own Goal
  aggregation outside this module.
- Capability descriptions and argument schemas are AgentSpec prompt metadata.
  The runtime CapabilityService catalog remains authoritative for availability,
  version, descriptor revision, authorization, approval, and execution.
- Inline capability arguments/results are bounded. Large results must use an
  ArtifactRef supplied by the Capability executor.
