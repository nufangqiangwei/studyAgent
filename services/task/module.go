package task

import (
	"agent/serviceruntime/building"
	"agent/serviceruntime/contract"
	"fmt"
	"time"
)

type Registrar interface {
	RegisterService(building.ServiceDefinition) error
}

type Module struct {
	factory ServiceFactory
}

func NewModule(clock contract.Clock) *Module {
	if clock == nil {
		clock = systemClock{}
	}
	return &Module{factory: ServiceFactory{clock: clock}}
}

func (m *Module) Register(registrar Registrar) error {
	if m == nil || registrar == nil {
		return fmt.Errorf("task module and registrar are required")
	}
	return registrar.RegisterService(Definition(m.factory))
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }
