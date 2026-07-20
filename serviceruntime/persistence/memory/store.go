package memory

import (
	"agent/serviceruntime/contract"
	"agent/serviceruntime/instance"
	"agent/serviceruntime/persistence"
	"context"
	"fmt"
	"sync"
	"time"
)

var _ persistence.RuntimeStorage = (*Store)(nil)

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

type Store struct {
	mu     sync.Mutex
	clock  contract.Clock
	closed bool
	serial uint64

	events           map[contract.StreamID][]contract.StoredEvent
	eventIDs         map[string]contract.StreamID
	snapshots        map[contract.StreamID]contract.Snapshot
	inbox            map[string]persistence.InboxRecord
	inboxOrder       map[contract.MailboxID][]string
	inboxDedupe      map[string]string
	outbox           map[string]persistence.OutboxRecord
	outboxOrder      []string
	effects          map[string]persistence.EffectRecord
	effectOrder      []string
	instances        map[contract.ServiceInstanceID]instance.Record
	addresses        map[addressKey]contract.ServiceInstanceID
	leases           map[contract.ServiceInstanceID]instance.ActivationLease
	plans            map[planKey]persistence.PlanRecord
	connections      map[string]persistence.ConnectionRecord
	connectionKeys   map[connectionKey]string
	messageSequences map[messageSequenceKey]messageSequence
	messageHeads     map[messageStreamKey]uint64
	inboxHeads       map[inboxStreamKey]uint64
}

type messageSequence struct {
	runtime  contract.RuntimeID
	stream   contract.StreamID
	sequence uint64
}

type messageSequenceKey struct {
	scope   string
	message string
}

type messageStreamKey struct {
	scope   string
	runtime contract.RuntimeID
	stream  contract.StreamID
}

type inboxStreamKey struct {
	mailbox contract.MailboxID
	stream  contract.StreamID
}

type planKey struct {
	runtime  contract.RuntimeID
	revision contract.PlanRevision
}

type connectionKey struct {
	runtime  contract.RuntimeID
	revision contract.PlanRevision
	owner    contract.ServiceInstanceID
	key      string
}

type addressKey struct {
	runtime  contract.RuntimeID
	revision contract.PlanRevision
	address  contract.ServiceAddress
}

func New(clock contract.Clock) *Store {
	if clock == nil {
		clock = systemClock{}
	}
	return &Store{
		clock:            clock,
		events:           make(map[contract.StreamID][]contract.StoredEvent),
		eventIDs:         make(map[string]contract.StreamID),
		snapshots:        make(map[contract.StreamID]contract.Snapshot),
		inbox:            make(map[string]persistence.InboxRecord),
		inboxOrder:       make(map[contract.MailboxID][]string),
		inboxDedupe:      make(map[string]string),
		outbox:           make(map[string]persistence.OutboxRecord),
		effects:          make(map[string]persistence.EffectRecord),
		instances:        make(map[contract.ServiceInstanceID]instance.Record),
		addresses:        make(map[addressKey]contract.ServiceInstanceID),
		leases:           make(map[contract.ServiceInstanceID]instance.ActivationLease),
		plans:            make(map[planKey]persistence.PlanRecord),
		connections:      make(map[string]persistence.ConnectionRecord),
		connectionKeys:   make(map[connectionKey]string),
		messageSequences: make(map[messageSequenceKey]messageSequence),
		messageHeads:     make(map[messageStreamKey]uint64),
		inboxHeads:       make(map[inboxStreamKey]uint64),
	}
}

func (s *Store) Journal() persistence.JournalStore           { return s }
func (s *Store) Snapshots() persistence.SnapshotStore        { return s }
func (s *Store) Inbox() persistence.InboxStore               { return &inboxStore{owner: s} }
func (s *Store) Outbox() persistence.OutboxStore             { return &outboxStore{owner: s} }
func (s *Store) Effects() persistence.EffectStore            { return &effectStore{owner: s} }
func (s *Store) Instances() instance.Store                   { return s }
func (s *Store) Leases() instance.ActivationLeaseStore       { return s }
func (s *Store) Committer() persistence.MessageCommitStore   { return s }
func (s *Store) Plans() persistence.PlanStore                { return &planStore{owner: s} }
func (s *Store) Sequences() persistence.MessageSequenceStore { return &sequenceStore{owner: s} }
func (s *Store) Connections() persistence.ConnectionStore    { return &connectionStore{owner: s} }

func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

func (s *Store) check(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("memory runtime store is nil")
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if s.closed {
		return persistence.ErrClosed
	}
	return nil
}

func (s *Store) now() time.Time {
	return s.clock.Now().UTC()
}

func (s *Store) token(prefix string) string {
	s.serial++
	return fmt.Sprintf("%s-%d", prefix, s.serial)
}
