package builtin

import "encoding/json"

type Result struct {
	Content  string          `json:"content"`
	Metadata map[string]any  `json:"metadata,omitempty"`
	Raw      json.RawMessage `json:"raw,omitempty"`
}
