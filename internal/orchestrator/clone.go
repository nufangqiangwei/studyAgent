package orchestrator

import "encoding/json"

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func mergeMetadata(base map[string]string, overlay map[string]string) map[string]string {
	if len(base) == 0 && len(overlay) == 0 {
		return nil
	}
	merged := cloneStringMap(base)
	if merged == nil {
		merged = make(map[string]string, len(overlay))
	}
	for key, value := range overlay {
		merged[key] = value
	}
	return merged
}

func cloneRaw(raw json.RawMessage) json.RawMessage {
	return append(json.RawMessage(nil), raw...)
}

func cloneStepRecords(records []StepRecord) []StepRecord {
	if len(records) == 0 {
		return nil
	}
	cloned := make([]StepRecord, 0, len(records))
	for _, record := range records {
		record.Result = cloneRaw(record.Result)
		cloned = append(cloned, record)
	}
	return cloned
}
