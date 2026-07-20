package request

import "context"

// AwaitDriver lets a higher-level durable workflow intercept a synchronous
// request before Client sends it. The driver may return a previously recorded
// response or cooperatively suspend the workflow. Client itself stays unaware
// of workflow persistence and scheduling.
type AwaitDriver interface {
	AwaitRequest(ctx context.Context, spec CallSpec) (Response, error)
}

type awaitDriverContextKey struct{}

func WithAwaitDriver(ctx context.Context, driver AwaitDriver) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, awaitDriverContextKey{}, driver)
}

func awaitDriverFromContext(ctx context.Context) (AwaitDriver, bool) {
	if ctx == nil {
		return nil, false
	}
	driver, ok := ctx.Value(awaitDriverContextKey{}).(AwaitDriver)
	return driver, ok && driver != nil
}

func awaitCall(ctx context.Context, spec CallSpec, output interface{}) (bool, error) {
	driver, ok := awaitDriverFromContext(ctx)
	if !ok {
		return false, nil
	}
	response, err := driver.AwaitRequest(ctx, spec)
	if err != nil {
		return true, err
	}
	if response.Error != nil {
		return true, response.Error
	}
	return true, decodePayload(response.Message.Payload, output)
}
