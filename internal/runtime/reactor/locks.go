package reactor

import "sync"

type taskLockPool struct {
	mu    sync.Mutex
	locks map[string]*taskLock
}

type taskLock struct {
	mu   sync.Mutex
	refs int
}

func newTaskLockPool() *taskLockPool {
	return &taskLockPool{locks: make(map[string]*taskLock)}
}

func (p *taskLockPool) lock(taskID string) func() {
	p.mu.Lock()
	lock := p.locks[taskID]
	if lock == nil {
		lock = &taskLock{}
		p.locks[taskID] = lock
	}
	lock.refs++
	p.mu.Unlock()

	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()

		p.mu.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(p.locks, taskID)
		}
		p.mu.Unlock()
	}
}
