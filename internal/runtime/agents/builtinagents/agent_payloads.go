package builtinagents

import (
	agents2 "agent/internal/runtime/agents"
	"agent/internal/runtime/statemachine"
	"encoding/json"
	"strings"
)

func completionResult(decision agents2.Decision, snapshot agents2.AgentSnapshot) (json.RawMessage, error) {
	if len(decision.Result) > 0 {
		return append(json.RawMessage(nil), decision.Result...), nil
	}
	payload := struct {
		Answer     string             `json:"answer,omitempty"`
		Plan       []agents2.PlanStep `json:"plan,omitempty"`
		Scratchpad string             `json:"scratchpad,omitempty"`
	}{
		Answer:     decision.FinalAnswer,
		Plan:       append([]agents2.PlanStep(nil), snapshot.Plan...),
		Scratchpad: snapshot.Scratchpad,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(raw), nil
}

type modelResponseObservation struct {
	ModelCallID string
	Response    agents2.ModelResponse
}

type modelFailureObservation struct {
	ModelCallID string
	Error       string
}

func decodeModelResponse(raw json.RawMessage) (modelResponseObservation, bool, error) {
	var payload statemachine.ModelCallPayload
	if len(raw) == 0 || json.Unmarshal(raw, &payload) != nil || payload.ModelCallID == "" {
		return modelResponseObservation{}, false, nil
	}
	if len(payload.Response) == 0 {
		return modelResponseObservation{}, false, nil
	}
	response, err := agents2.UnmarshalModelResponse(payload.Response)
	if err != nil {
		return modelResponseObservation{}, true, err
	}
	return modelResponseObservation{
		ModelCallID: payload.ModelCallID,
		Response:    response,
	}, true, nil
}

func decodeModelFailure(raw json.RawMessage) (modelFailureObservation, bool, error) {
	var payload statemachine.ModelCallPayload
	if len(raw) == 0 || json.Unmarshal(raw, &payload) != nil || payload.ModelCallID == "" {
		return modelFailureObservation{}, false, nil
	}
	if strings.TrimSpace(payload.Error) == "" {
		return modelFailureObservation{}, false, nil
	}
	return modelFailureObservation{
		ModelCallID: payload.ModelCallID,
		Error:       payload.Error,
	}, true, nil
}

func decodeToolObservation(raw json.RawMessage) (agents2.ToolObservation, bool) {
	var payload statemachine.ToolCallPayload
	if len(raw) == 0 || json.Unmarshal(raw, &payload) != nil || payload.ToolCallID == "" {
		return agents2.ToolObservation{}, false
	}
	if len(payload.Result) == 0 && payload.Error == "" {
		return agents2.ToolObservation{}, false
	}
	return agents2.ToolObservation{
		ToolCallID: payload.ToolCallID,
		ToolName:   payload.ToolName,
		Result:     append(json.RawMessage(nil), payload.Result...),
		Error:      payload.Error,
	}, true
}

func decodeUserInput(raw json.RawMessage) (statemachine.UserInputPayload, bool) {
	var payload statemachine.UserInputPayload
	if len(raw) == 0 || json.Unmarshal(raw, &payload) != nil || payload.RequestID == "" {
		return statemachine.UserInputPayload{}, false
	}
	if payload.Answer == "" {
		return statemachine.UserInputPayload{}, false
	}
	return payload, true
}

func decodeSubAgent(raw json.RawMessage) (statemachine.SubAgentPayload, bool) {
	var payload statemachine.SubAgentPayload
	if len(raw) == 0 || json.Unmarshal(raw, &payload) != nil || payload.SubTaskID == "" {
		return statemachine.SubAgentPayload{}, false
	}
	if len(payload.Result) == 0 && payload.Error == "" {
		return statemachine.SubAgentPayload{}, false
	}
	return payload, true
}

func updateSubTask(snapshot *agents2.AgentSnapshot, payload statemachine.SubAgentPayload) {
	if snapshot == nil {
		return
	}
	for i := range snapshot.SubTasks {
		if snapshot.SubTasks[i].SubTaskID != payload.SubTaskID {
			continue
		}
		snapshot.SubTasks[i].Result = append(json.RawMessage(nil), payload.Result...)
		snapshot.SubTasks[i].Error = payload.Error
		if payload.Error != "" {
			snapshot.SubTasks[i].Status = "failed"
		} else {
			snapshot.SubTasks[i].Status = "completed"
		}
		return
	}
	status := "completed"
	if payload.Error != "" {
		status = "failed"
	}
	snapshot.SubTasks = append(snapshot.SubTasks, agents2.SubTaskSnapshot{
		SubTaskID: payload.SubTaskID,
		Agent:     payload.Agent,
		Status:    status,
		Result:    append(json.RawMessage(nil), payload.Result...),
		Error:     payload.Error,
	})
}
