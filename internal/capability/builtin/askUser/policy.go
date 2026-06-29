package askUser

import (
	"agent/internal/foundation/policy"
	"encoding/json"
)

func (t *Question) PolicyRequest(input json.RawMessage) policy.Request {
	return policy.Request{
		ToolName:  Name,
		Operation: Name,
		Risk:      policy.RiskRead,
	}
}
