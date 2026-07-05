package command

import (
	"agent/internal/content"
	"context"
	"fmt"
)

type Help struct{}

func (Help) Name() string {
	return "help"
}

func (Help) Description() string {
	return "show commands and global flags"
}

func (Help) Execute(_ context.Context, env content.Env, _ []string) error {
	fmt.Fprintln(env.IO.Out, "Usage:")
	fmt.Fprintln(env.IO.Out, "  agent [flags] [command] [args]")
	fmt.Fprintln(env.IO.Out, "  agent")
	fmt.Fprintln(env.IO.Out)
	fmt.Fprintln(env.IO.Out, "Commands:")
	fmt.Fprintln(env.IO.Out, "  run <task>       submit an async agent run")
	fmt.Fprintln(env.IO.Out, "  recover          enqueue one resume event for a persisted run")
	fmt.Fprintln(env.IO.Out, "  work             process one global event or effect and stop")
	fmt.Fprintln(env.IO.Out, "  step <run_id>    process one event or one effect and stop")
	fmt.Fprintln(env.IO.Out, "  advance <run_id> process one queued event")
	fmt.Fprintln(env.IO.Out, "  effect <run_id>  dispatch one queued effect")
	fmt.Fprintln(env.IO.Out, "  input <run_id> <answer> submit a user input event")
	fmt.Fprintln(env.IO.Out, "  approve <run_id> yes|no submit a user approval event")
	fmt.Fprintln(env.IO.Out, "  result <run_id>  show run state and final answer")
	fmt.Fprintln(env.IO.Out, "  status           show provider, model, and workspace")
	fmt.Fprintln(env.IO.Out, "  agent            show active and available agents")
	fmt.Fprintln(env.IO.Out)
	fmt.Fprintln(env.IO.Out, "Global flags:")
	fmt.Fprintln(env.IO.Out, "  --config string    config file path, default: ~/.testAgent/config.json if it exists")
	fmt.Fprintln(env.IO.Out, "  --provider string   deprecated; provider is inferred from --model")
	fmt.Fprintln(env.IO.Out, "  --model string      llm model name, default: mock-native")
	fmt.Fprintln(env.IO.Out, "  --workdir string    workspace directory, default: current directory")
	fmt.Fprintln(env.IO.Out, "  --log-level string  debug, info, warn, error, silent")
	fmt.Fprintln(env.IO.Out, "  --policy-mode string tool permission mode: read, validate, modify; default: read")
	fmt.Fprintln(env.IO.Out, "  --debug             write llm request and response bodies to debug llm.jsonl")
	fmt.Fprintln(env.IO.Out, "  --help, -h          show help")
	fmt.Fprintln(env.IO.Out, "  --version, -v       show version")
	fmt.Fprintln(env.IO.Out)
	fmt.Fprintln(env.IO.Out, "Interactive mode:")
	fmt.Fprintln(env.IO.Out, "  Run agent with no command to start interactive CLI mode.")
	fmt.Fprintln(env.IO.Out, "  In CLI mode, prefix registered command names with /, for example /status.")
	fmt.Fprintln(env.IO.Out, "  Plain input is sent to the current runtime task as user dialogue or guidance.")
	fmt.Fprintln(env.IO.Out, "  Mistyped slash commands may prompt to run the closest command.")
	fmt.Fprintln(env.IO.Out, "  Unknown slash input without a close command match reports an error.")
	fmt.Fprintln(env.IO.Out, "  Use /exit or /quit to leave CLI mode.")
	fmt.Fprintln(env.IO.Out)
	fmt.Fprintln(env.IO.Out, "Examples:")
	fmt.Fprintln(env.IO.Out, "  agent")
	fmt.Fprintln(env.IO.Out, "  agent run \"summarize this project\"")
	fmt.Fprintln(env.IO.Out, "  agent work")
	fmt.Fprintln(env.IO.Out, "  agent step run_123")
	fmt.Fprintln(env.IO.Out, "  agent result run_123")
	fmt.Fprintln(env.IO.Out, "  agent --model=gpt-4.1 run \"plan the next module\"")
	return nil
}
