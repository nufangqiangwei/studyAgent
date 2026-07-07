package cli

import (
	"agent/internal/capability/command"
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"agent/internal/content"
)

type PlainInputHandler func(ctx context.Context, env content.Env, line string) error

func Run(ctx context.Context, env content.Env, registry *command.Registry, handlePlainInput PlainInputHandler) error {
	if registry == nil {
		registry = command.Manage
	}
	if handlePlainInput == nil {
		return fmt.Errorf("plain input handler is not configured")
	}
	if err := printBanner(env); err != nil {
		return err
	}

	scanner := bufio.NewScanner(env.IO.In)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := fmt.Fprint(env.IO.Out, "agent> "); err != nil {
			return err
		}
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var err error
		if strings.HasPrefix(line, "/") {
			err = executeUserCommand(ctx, env, registry, line, func() (string, bool) {
				if !scanner.Scan() {
					return "", false
				}
				return scanner.Text(), true
			})
		} else {
			err = handlePlainInput(ctx, env, line)
		}
		if err != nil {
			if errors.Is(err, errExit) {
				fmt.Fprintln(env.IO.Out)
				return nil
			}
			fmt.Fprintf(errorWriter(env), "error: %v\n", err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read cli input: %w", err)
	}
	return nil
}

var errExit = errors.New("cli exit")

func executeUserCommand(ctx context.Context, env content.Env, registry *command.Registry, line string, readLine func() (string, bool)) error {
	name, arg, _ := strings.Cut(line[1:], " ")
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return unknownCommandError(line)
	}

	if err, ok := executeBuiltinCommand(ctx, env, name, arg); ok {
		return err
	}

	if _, ok := registry.Lookup(name); !ok {
		if isAgentName(env.Agent, name) {
			return registry.Execute(ctx, "set-agent", env, []string{name})
		}
		if suggestion, ok := suggestCommandName(registry, name); ok {
			confirmed, err := confirmSuggestedCommand(env, name, suggestion, readLine)
			if err != nil {
				return err
			}
			if confirmed {
				return executeSuggestedCommand(ctx, env, registry, suggestion, arg)
			}
			_, err = fmt.Fprintln(env.IO.Out, "Command not executed.")
			return err
		}
		return unknownCommandError("/" + name)
	}

	args := commandArgs(arg)
	return registry.Execute(ctx, name, env, args)
}

func executeBuiltinCommand(ctx context.Context, env content.Env, name string, arg string) (error, bool) {
	switch name {
	case "exit", "quit":
		return errExit, true
	default:
		return nil, false
	}
}

func executeSuggestedCommand(ctx context.Context, env content.Env, registry *command.Registry, name string, arg string) error {
	if err, ok := executeBuiltinCommand(ctx, env, name, arg); ok {
		return err
	}
	args := commandArgs(arg)
	return registry.Execute(ctx, name, env, args)
}

func unknownCommandError(input string) error {
	return fmt.Errorf("unknown command %q", input)
}

func commandArgs(arg string) []string {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil
	}
	return []string{arg}
}

func confirmSuggestedCommand(env content.Env, original string, suggestion string, readLine func() (string, bool)) (bool, error) {
	if _, err := fmt.Fprintf(env.IO.Out, "Unknown command \"/%s\". Did you mean \"/%s\"? Execute it? [y/N] ", original, suggestion); err != nil {
		return false, err
	}
	if readLine == nil {
		return false, nil
	}
	answer, ok := readLine()
	if !ok {
		if _, err := fmt.Fprintln(env.IO.Out); err != nil {
			return false, err
		}
		return false, nil
	}
	switch strings.ToLower(strings.TrimSpace(answer)) {
	case "y", "yes":
		return true, nil
	default:
		return false, nil
	}
}

func suggestCommandName(registry *command.Registry, input string) (string, bool) {
	input = strings.ToLower(strings.TrimSpace(input))
	if input == "" {
		return "", false
	}

	names := []string{"exit", "quit"}
	if registry != nil {
		for _, cmd := range registry.List() {
			names = append(names, cmd.Name())
		}
	}

	prefixMatch := ""
	for _, name := range names {
		if strings.HasPrefix(name, input) {
			if prefixMatch != "" {
				return "", false
			}
			prefixMatch = name
		}
	}
	if prefixMatch != "" {
		return prefixMatch, true
	}

	bestName := ""
	bestDistance := 0
	tied := false
	for _, name := range names {
		distance := levenshteinDistance(input, name)
		if bestName == "" || distance < bestDistance {
			bestName = name
			bestDistance = distance
			tied = false
			continue
		}
		if distance == bestDistance {
			tied = true
		}
	}
	if bestName == "" || tied || bestDistance > maxCommandSuggestionDistance(input) {
		return "", false
	}
	return bestName, true
}

func maxCommandSuggestionDistance(input string) int {
	switch l := len(input); {
	case l <= 2:
		return 1
	case l <= 6:
		return 2
	default:
		return 3
	}
}

func levenshteinDistance(a string, b string) int {
	if a == b {
		return 0
	}
	if a == "" {
		return len(b)
	}
	if b == "" {
		return len(a)
	}

	previous := make([]int, len(b)+1)
	for j := range previous {
		previous[j] = j
	}

	for i := 1; i <= len(a); i++ {
		current := make([]int, len(b)+1)
		current[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			current[j] = minInt(
				current[j-1]+1,
				minInt(previous[j]+1, previous[j-1]+cost),
			)
		}
		previous = current
	}
	return previous[len(b)]
}

func minInt(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func printBanner(env content.Env) error {
	if _, err := fmt.Fprintln(env.IO.Out, "Agent CLI"); err != nil {
		return err
	}
	if env.Config.AgentName != "" {
		if _, err := fmt.Fprintf(env.IO.Out, "Agent: %s\n", env.Config.AgentName); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(env.IO.Out, "Provider: %s  Model: %s\n", env.Config.Provider, env.Config.Model); err != nil {
		return err
	}
	if env.Config.ConfigPath != "" {
		if _, err := fmt.Fprintf(env.IO.Out, "Config: %s\n", env.Config.ConfigPath); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(env.IO.Out, "Workspace: %s\n", env.Config.WorkDir); err != nil {
		return err
	}
	_, err := fmt.Fprintln(env.IO.Out, "Type /help for commands, /exit to quit. Other input is sent to the model.")
	return err
}

func errorWriter(env content.Env) io.Writer {
	if env.IO.Err != nil {
		return env.IO.Err
	}
	return env.IO.Out
}

func isAgentName(runner content.AgentRunner, name string) bool {
	switcher, ok := runner.(content.AgentSwitcher)
	if !ok || switcher == nil || name == "" {
		return false
	}
	for _, agentName := range switcher.ListAgentNames() {
		if strings.EqualFold(agentName, name) {
			return true
		}
	}
	return false
}
