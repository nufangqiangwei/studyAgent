package builtin

import "encoding/json"

type Result struct {
	Content  string
	Metadata map[string]any
	Raw      json.RawMessage
}
