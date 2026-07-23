# CLI entry

This executable is the process composition root for the current
`serviceruntime` implementation. It owns terminal input/output and local
infrastructure configuration. User input is published as an
`interaction.submit` command; terminal output is received through the
`services/interaction` Presenter port after the corresponding presentation
Effect is committed.

Configure a model endpoint, then run:

```powershell
$env:AGENT_MODEL = "your-model"
$env:AGENT_API_KEY = "..."
go run ./main/cli
```

The default provider is `deepseek` and the default OpenAI-compatible endpoint
is `https://api.deepseek.com`. Override them with `AGENT_PROVIDER` and
`AGENT_BASE_URL` when needed. Runtime facts are stored in
`.agent/runtime/runtime.db`; immutable inputs, prompts, model responses, and
answers are stored below `.agent/runtime/artifacts`. Use `-data-dir` to select
another location.

The current entry deliberately registers an empty Capability catalog. Each
line starts an independent Agent run; workspace tools, approval resolution,
conversation history, and streaming output remain later work.

Durable request events remain in the Journal. Interaction's materialized state
and the terminal Presenter's process-local caches retain only the five most
recent persisted terminal entries, while active requests are never evicted.
