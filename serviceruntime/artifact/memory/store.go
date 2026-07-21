package memory

import (
	"agent/serviceruntime/artifact"
	"agent/serviceruntime/contract"
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"
)

var _ artifact.Store = (*Store)(nil)

type object struct {
	data []byte
	info artifact.Info
}

type Store struct {
	mu      sync.RWMutex
	name    string
	objects map[string]object
	closed  bool
}

func New(name string) (*Store, error) {
	if name == "" {
		return nil, fmt.Errorf("artifact store name is required")
	}
	return &Store{name: name, objects: make(map[string]object)}, nil
}

func (s *Store) Name() string {
	if s == nil {
		return ""
	}
	return s.name
}

func (s *Store) Begin(ctx context.Context, request artifact.WriteRequest) (artifact.WriteSession, error) {
	if err := s.check(ctx); err != nil {
		return nil, err
	}
	if err := artifact.ValidateWriteRequest(request); err != nil {
		return nil, err
	}
	return &session{store: s, request: request}, nil
}

func (s *Store) Open(ctx context.Context, ref contract.ArtifactRef) (io.ReadCloser, artifact.Info, error) {
	if err := s.check(ctx); err != nil {
		return nil, artifact.Info{}, err
	}
	if err := s.validateRef(ref); err != nil {
		return nil, artifact.Info{}, err
	}
	s.mu.RLock()
	value, ok := s.objects[ref.Key]
	s.mu.RUnlock()
	if !ok {
		return nil, artifact.Info{}, fmt.Errorf("%w: %s/%s", artifact.ErrNotFound, ref.Store, ref.Key)
	}
	if err := matchRef(ref, value.info.Ref); err != nil {
		return nil, artifact.Info{}, err
	}
	return io.NopCloser(bytes.NewReader(value.data)), value.info, nil
}

func (s *Store) Stat(ctx context.Context, ref contract.ArtifactRef) (artifact.Info, error) {
	reader, info, err := s.Open(ctx, ref)
	if reader != nil {
		_ = reader.Close()
	}
	return info, err
}

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
		return artifact.ErrUnavailable
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return artifact.ErrClosed
	}
	return nil
}

func (s *Store) validateRef(ref contract.ArtifactRef) error {
	if err := artifact.ValidateRef(ref); err != nil {
		return err
	}
	if ref.Store != s.name {
		return fmt.Errorf("%w: artifact store %q is not %q", artifact.ErrNotFound, ref.Store, s.name)
	}
	return nil
}

type session struct {
	mu      sync.Mutex
	store   *Store
	request artifact.WriteRequest
	buffer  bytes.Buffer
	done    bool
}

func (s *session) Write(data []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done {
		return 0, artifact.ErrClosed
	}
	return s.buffer.Write(data)
}

func (s *session) Commit(ctx context.Context) (contract.ArtifactRef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done {
		return contract.ArtifactRef{}, artifact.ErrClosed
	}
	if err := s.store.check(ctx); err != nil {
		return contract.ArtifactRef{}, err
	}
	data := append([]byte(nil), s.buffer.Bytes()...)
	hasher := artifact.NewChecksum()
	_, _ = hasher.Write(data)
	checksum := artifact.FormatChecksum(hasher.Sum(nil))
	size := int64(len(data))
	if err := validateExpected(s.request, checksum, size); err != nil {
		s.done = true
		return contract.ArtifactRef{}, err
	}
	ref := contract.ArtifactRef{Store: s.store.name, Key: s.request.Key, ContentType: s.request.ContentType, Checksum: checksum, Size: size}
	now := time.Now().UTC()
	s.store.mu.Lock()
	if s.store.closed {
		s.store.mu.Unlock()
		return contract.ArtifactRef{}, artifact.ErrClosed
	}
	if existing, ok := s.store.objects[s.request.Key]; ok {
		s.store.mu.Unlock()
		s.done = true
		if err := matchRef(ref, existing.info.Ref); err != nil {
			return contract.ArtifactRef{}, fmt.Errorf("%w: key %q already contains different content", artifact.ErrConflict, s.request.Key)
		}
		return existing.info.Ref, nil
	}
	s.store.objects[s.request.Key] = object{data: data, info: artifact.Info{Ref: ref, CreatedAt: now}}
	s.store.mu.Unlock()
	s.done = true
	return ref, nil
}

func (s *session) Abort(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done {
		return nil
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	s.done = true
	s.buffer.Reset()
	return nil
}

func validateExpected(request artifact.WriteRequest, checksum string, size int64) error {
	if request.ExpectedChecksum != "" && request.ExpectedChecksum != checksum {
		return fmt.Errorf("%w: checksum is %q, want %q", artifact.ErrCorrupt, checksum, request.ExpectedChecksum)
	}
	if request.ExpectedSize != nil && *request.ExpectedSize != size {
		return fmt.Errorf("%w: size is %d, want %d", artifact.ErrCorrupt, size, *request.ExpectedSize)
	}
	return nil
}

func matchRef(expected, actual contract.ArtifactRef) error {
	if expected.Checksum != "" && expected.Checksum != actual.Checksum {
		return fmt.Errorf("%w: checksum is %q, want %q", artifact.ErrCorrupt, actual.Checksum, expected.Checksum)
	}
	if expected.Size != 0 && expected.Size != actual.Size {
		return fmt.Errorf("%w: size is %d, want %d", artifact.ErrCorrupt, actual.Size, expected.Size)
	}
	if expected.ContentType != "" && actual.ContentType != "" && expected.ContentType != actual.ContentType {
		return fmt.Errorf("%w: content type is %q, want %q", artifact.ErrCorrupt, actual.ContentType, expected.ContentType)
	}
	return nil
}
