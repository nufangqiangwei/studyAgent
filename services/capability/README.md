# Capability Service

`capability` is an explicitly installed event-sourced business module. It owns
`CallState`, runs a deterministic authorization evaluator, asks the separate
Approval Service through durable messages when required, and records an Effect
or downstream business command only after the corresponding state transition
is committed.

Providers only describe and plan work. Tool, MCP, HTTP, and local adapters are
registered as Effect executors and reconcilers; they are not Services. The
module wraps their outcomes as stable `capability.execution.*` messages back to
the Capability mailbox. Terminal Effect failures use the Runtime's generic
terminal-failure notifier, so calls cannot remain permanently waiting merely
because the Effect exhausted its retry policy.

Completed calls are retained in full for `TerminalRetention`, compacted to
idempotency tombstones, and may be removed after `IdempotencyWindow` by a
trusted durable `capability.prune` command.

Applications install both independent modules before `Builder.Build` and bind
their logical addresses through the manifest:

```go
approvals, _ := approval.NewModule(approval.ModuleOptions{
    TrustedRequesters: []contract.ServiceAddress{capability.DefaultAddress},
})
capabilities, _ := capability.NewModule(capability.ModuleOptions{
    Evaluator: evaluator,
})
_ = capabilities.RegisterProvider(provider)
_ = capabilities.RegisterExecutor(executorSpec)
_ = approvals.Register(builder)
_ = capabilities.Register(builder)

manifest.Services = append(manifest.Services,
    approvals.Mount(approval.DefaultAddress, interactionAddress, schedulerAddress),
    capabilities.Mount(capability.DefaultAddress, approval.DefaultAddress, schedulerAddress),
)
```

The dependency values are addresses only. Neither factory receives the other
Service object.
