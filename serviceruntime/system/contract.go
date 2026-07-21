package system

import (
	"agent/serviceruntime/assembly"
	"agent/serviceruntime/contract"
	"agent/serviceruntime/instance"
	"encoding/json"
)

var Component = contract.ComponentRef{Type: "runtime.system", Version: "v1"}

const (
	Address           contract.ServiceAddress = "system.runtime"
	CallMessageType   contract.MessageType    = "system.call"
	ResultMessageType contract.MessageType    = "system.result"
	CallVersion                               = 1
	ExecutorRef       string                  = "runtime.system.call"
	EffectType        contract.EffectType     = "runtime.system.call"

	DeclareInstanceOperation = assembly.SystemOperationDeclareInstance
)

const (
	MetadataCallID    = "system.call_id"
	MetadataOperation = "system.operation"
)

// Call is the stable, versioned envelope for an asynchronous Runtime control
// operation. Payload is decoded according to Operation and OperationVersion.
type Call struct {
	CallID           string          `json:"call_id"`
	Operation        string          `json:"operation"`
	OperationVersion int             `json:"operation_version"`
	Payload          json.RawMessage `json:"payload,omitempty"`
}

type Result struct {
	CallID           string          `json:"call_id"`
	Operation        string          `json:"operation"`
	OperationVersion int             `json:"operation_version"`
	Result           json.RawMessage `json:"result,omitempty"`
}

type DeclareInstanceRequest = instance.Declaration

type DeclareInstanceResult struct {
	Instance instance.Record `json:"instance"`
}
