# LLM Client Service

`llmClient` is an explicitly installed `serviceruntime` business module. It
receives `model.complete` commands, persists an Effect, calls the configured
provider only after the command commit, stores the completion in the Runtime
Artifact Store, and sends `model.completed` to the command's `ReplyTo` address.

```go
module, err := llmClient.NewModule(llmClient.Config{
    BaseURL:   "https://api.openai.com/v1",
    APIKey:    os.Getenv("OPENAI_API_KEY"),
    Provider:  llmClient.ProviderOpenAI,
    ModelName: "gpt-4.1-mini",
})
if err != nil {
    return err
}
if err := module.Register(builder); err != nil {
    return err
}

manifest.Services = append(manifest.Services, module.Mount(llmClient.DefaultAddress))
```

The API key is process-local module configuration and is never persisted. An
Artifact Store must be supplied through `serviceruntime.BuilderOptions`.

Callers send a command with an explicit `ReplyTo`:

```go
payload, _ := json.Marshal(llmClient.CompletionRequest{Prompt: "Hello"})
message := contract.Message{
    Kind: contract.MessageCommand,
    Type: llmClient.CompleteMessageType,
    Version: llmClient.ProtocolVersion,
    To: llmClient.DefaultAddress,
    ReplyTo: callerAddress,
    Payload: payload,
}
```

The `CompletionReply` contains both `ArtifactKey` and the full immutable
`ArtifactRef`. OpenAI-compatible, DeepSeek/OpenRouter-style, Anthropic, and
Gemini HTTP shapes are supported. Unknown provider names use the
OpenAI-compatible wire format.
