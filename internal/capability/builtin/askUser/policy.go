package askUser

import (
	"agent/internal/foundation/policy"
	"encoding/json"
)

func (t *AskUserTool) PolicyRequest(input json.RawMessage) policy.Request {
	return policy.Request{
		ToolName:  Name,
		Operation: Name,
		Risk:      policy.RiskRead,
	}
}
