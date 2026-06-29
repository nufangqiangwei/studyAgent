package command

import (
	"agent/internal/content"
	"context"
	"fmt"
)

type Model struct{}

func (Model) Name() string {
	return "model"
}
func (Model) Description() string {
	return "show current model"
}
func (Model) Execute(_ context.Context, env content.Env, _ []string) error {
	_, err := fmt.Fprintf(env.IO.Out, " %s\n", env.Config.Model)
	return err
}
