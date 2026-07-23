# Interaction Service

`interaction` is the durable gateway between an external user interface and
the event-sourced Runtime. The UI publishes `interaction.submit` commands; the
Service persists request identity, sends `agent.execute`, receives
`agent.completed`, and records the terminal request state.

The Service never writes to a terminal or calls a UI object from `Handle`.
After the terminal state and presentation plan are committed atomically, the
registered presentation Effect reads any output Artifact and calls the
process-local `Presenter` port. Presentations carry a stable ID, so adapters
should deduplicate retries.

The Journal retains the complete request history. The materialized Service
state retains every active request plus only the five most recently persisted
terminal requests. The same deterministic projection is rebuilt during
Replay, so the memory bound does not delete durable facts.

The target Agent address is persisted in the Interaction mount config rather
than declared as a static object dependency. This avoids a false dependency
cycle in the normal message path `Agent -> Capability -> Approval ->
Interaction -> Agent`; Plan validation still requires the configured address
to resolve to an Agent component.

The first CLI integration intentionally has an empty Capability catalog.
Approval notifications can be displayed, but resolving them is deferred until
interactive approval commands are implemented.
