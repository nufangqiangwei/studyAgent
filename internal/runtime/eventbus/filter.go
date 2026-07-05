package eventbus

const Any = "*"

type Filter struct {
	Topic  string    `json:"topic,omitempty"`
	Type   EventType `json:"type,omitempty"`
	TaskID string    `json:"task_id,omitempty"`
}

func (f Filter) Match(event Event) bool {
	return matchString(f.Topic, event.Topic) &&
		matchString(string(f.Type), string(event.Type)) &&
		matchString(f.TaskID, event.TaskID)
}

func (f Filter) Overlaps(other Filter) bool {
	return patternOverlaps(f.Topic, other.Topic) &&
		patternOverlaps(string(f.Type), string(other.Type)) &&
		patternOverlaps(f.TaskID, other.TaskID)
}

func patternOverlaps(a, b string) bool {
	return isAny(a) || isAny(b) || a == b
}

func isAny(value string) bool {
	return value == "" || value == Any
}

func matchString(pattern, value string) bool {
	return pattern == "" || pattern == Any || pattern == value
}
