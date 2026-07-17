package host

import (
	"agent/serviceruntime/contract"
	"sync"
)

type lockPool struct {
	mu    sync.Mutex
	locks map[contract.ServiceInstanceID]*instanceLock
}

type instanceLock struct {
	mu   sync.Mutex
	refs int
}

func newLockPool() *lockPool {
	return &lockPool{locks: make(map[contract.ServiceInstanceID]*instanceLock)}
}

func (p *lockPool) lock(id contract.ServiceInstanceID) func() {
	p.mu.Lock()
	entry := p.locks[id]
	if entry == nil {
		entry = &instanceLock{}
		p.locks[id] = entry
	}
	entry.refs++
	p.mu.Unlock()
	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		p.mu.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(p.locks, id)
		}
		p.mu.Unlock()
	}
}
