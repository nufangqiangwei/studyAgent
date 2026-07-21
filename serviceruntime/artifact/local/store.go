package local

import (
	"agent/serviceruntime/artifact"
	"agent/serviceruntime/contract"
	"context"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var _ artifact.Store = (*Store)(nil)

type Options struct {
	Name string
}

// Store keeps immutable artifact bytes below root. Staging files and finalized
// objects live on the same filesystem so Commit can publish atomically.
type Store struct {
	mu     sync.RWMutex
	name   string
	root   string
	closed bool
}

func Open(root string, options Options) (*Store, error) {
	if options.Name == "" {
		return nil, fmt.Errorf("artifact store name is required")
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve artifact root: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(absolute, "staging"), 0o700); err != nil {
		return nil, fmt.Errorf("create artifact staging directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(absolute, "objects"), 0o700); err != nil {
		return nil, fmt.Errorf("create artifact object directory: %w", err)
	}
	return &Store{name: options.Name, root: absolute}, nil
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
	file, err := os.CreateTemp(filepath.Join(s.root, "staging"), "artifact-*.tmp")
	if err != nil {
		return nil, fmt.Errorf("create artifact staging file: %w", err)
	}
	return &session{store: s, request: request, file: file, path: file.Name(), checksum: artifact.NewChecksum()}, nil
}

func (s *Store) Open(ctx context.Context, ref contract.ArtifactRef) (io.ReadCloser, artifact.Info, error) {
	if err := s.check(ctx); err != nil {
		return nil, artifact.Info{}, err
	}
	if err := s.validateRef(ref); err != nil {
		return nil, artifact.Info{}, err
	}
	file, err := os.Open(s.objectPath(ref.Key))
	if errors.Is(err, os.ErrNotExist) {
		return nil, artifact.Info{}, fmt.Errorf("%w: %s/%s", artifact.ErrNotFound, ref.Store, ref.Key)
	}
	if err != nil {
		return nil, artifact.Info{}, fmt.Errorf("open artifact %q: %w", ref.Key, err)
	}
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, artifact.Info{}, fmt.Errorf("stat artifact %q: %w", ref.Key, err)
	}
	if ref.Size != 0 && ref.Size != stat.Size() {
		_ = file.Close()
		return nil, artifact.Info{}, fmt.Errorf("%w: artifact %q size is %d, want %d", artifact.ErrCorrupt, ref.Key, stat.Size(), ref.Size)
	}
	actual := ref
	actual.Size = stat.Size()
	reader := &verifiedReader{file: file, checksum: artifact.NewChecksum(), expectedChecksum: ref.Checksum, expectedSize: stat.Size()}
	return reader, artifact.Info{Ref: actual, CreatedAt: stat.ModTime().UTC()}, nil
}

func (s *Store) Stat(ctx context.Context, ref contract.ArtifactRef) (artifact.Info, error) {
	if err := s.check(ctx); err != nil {
		return artifact.Info{}, err
	}
	if err := s.validateRef(ref); err != nil {
		return artifact.Info{}, err
	}
	info, found, err := inspectObject(s.objectPath(ref.Key), ref)
	if err != nil {
		return artifact.Info{}, err
	}
	if !found {
		return artifact.Info{}, fmt.Errorf("%w: %s/%s", artifact.ErrNotFound, ref.Store, ref.Key)
	}
	if ref.Checksum != "" && ref.Checksum != info.Ref.Checksum {
		return artifact.Info{}, fmt.Errorf("%w: checksum is %q, want %q", artifact.ErrCorrupt, info.Ref.Checksum, ref.Checksum)
	}
	if ref.Size != 0 && ref.Size != info.Ref.Size {
		return artifact.Info{}, fmt.Errorf("%w: size is %d, want %d", artifact.ErrCorrupt, info.Ref.Size, ref.Size)
	}
	return info, nil
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

func (s *Store) objectPath(key string) string {
	return filepath.Join(s.root, "objects", filepath.FromSlash(key))
}

type session struct {
	mu       sync.Mutex
	store    *Store
	request  artifact.WriteRequest
	file     *os.File
	path     string
	checksum hash.Hash
	size     int64
	done     bool
}

func (s *session) Write(data []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done || s.file == nil {
		return 0, artifact.ErrClosed
	}
	written, err := s.file.Write(data)
	if written > 0 {
		_, _ = s.checksum.Write(data[:written])
		s.size += int64(written)
	}
	return written, err
}

func (s *session) Commit(ctx context.Context) (contract.ArtifactRef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done || s.file == nil {
		return contract.ArtifactRef{}, artifact.ErrClosed
	}
	if err := s.store.check(ctx); err != nil {
		return contract.ArtifactRef{}, err
	}
	checksum := artifact.FormatChecksum(s.checksum.Sum(nil))
	if s.request.ExpectedChecksum != "" && s.request.ExpectedChecksum != checksum {
		_ = s.discard()
		return contract.ArtifactRef{}, fmt.Errorf("%w: checksum is %q, want %q", artifact.ErrCorrupt, checksum, s.request.ExpectedChecksum)
	}
	if s.request.ExpectedSize != nil && *s.request.ExpectedSize != s.size {
		_ = s.discard()
		return contract.ArtifactRef{}, fmt.Errorf("%w: size is %d, want %d", artifact.ErrCorrupt, s.size, *s.request.ExpectedSize)
	}
	if err := s.file.Sync(); err != nil {
		_ = s.discard()
		return contract.ArtifactRef{}, fmt.Errorf("sync artifact %q: %w", s.request.Key, err)
	}
	if err := s.file.Close(); err != nil {
		s.file = nil
		_ = s.discard()
		return contract.ArtifactRef{}, fmt.Errorf("close artifact %q: %w", s.request.Key, err)
	}
	s.file = nil
	ref := contract.ArtifactRef{Store: s.store.name, Key: s.request.Key, ContentType: s.request.ContentType, Checksum: checksum, Size: s.size}
	destination := s.store.objectPath(s.request.Key)
	if existing, ok, err := inspectObject(destination, ref); err != nil {
		_ = s.discard()
		return contract.ArtifactRef{}, err
	} else if ok {
		_ = s.discard()
		s.done = true
		if existing.Ref.Checksum != ref.Checksum || existing.Ref.Size != ref.Size {
			return contract.ArtifactRef{}, fmt.Errorf("%w: key %q already contains different content", artifact.ErrConflict, ref.Key)
		}
		return ref, nil
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		_ = s.discard()
		return contract.ArtifactRef{}, fmt.Errorf("create artifact object directory: %w", err)
	}
	// Link publishes without overwriting a concurrent writer's immutable object.
	if err := os.Link(s.path, destination); err != nil {
		existing, ok, inspectErr := inspectObject(destination, ref)
		if inspectErr == nil && ok {
			_ = s.discard()
			s.done = true
			if existing.Ref.Checksum == ref.Checksum && existing.Ref.Size == ref.Size {
				return ref, nil
			}
			return contract.ArtifactRef{}, fmt.Errorf("%w: key %q already contains different content", artifact.ErrConflict, ref.Key)
		}
		_ = s.discard()
		return contract.ArtifactRef{}, fmt.Errorf("finalize artifact %q: %w", s.request.Key, err)
	}
	if err := os.Remove(s.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		s.done = true
		return contract.ArtifactRef{}, fmt.Errorf("remove finalized artifact staging file: %w", err)
	}
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
	return s.discard()
}

func (s *session) discard() error {
	if s.file != nil {
		_ = s.file.Close()
		s.file = nil
	}
	s.done = true
	if err := os.Remove(s.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func inspectObject(objectPath string, ref contract.ArtifactRef) (artifact.Info, bool, error) {
	file, err := os.Open(objectPath)
	if errors.Is(err, os.ErrNotExist) {
		return artifact.Info{}, false, nil
	}
	if err != nil {
		return artifact.Info{}, false, err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return artifact.Info{}, false, err
	}
	hasher := artifact.NewChecksum()
	size, err := io.Copy(hasher, file)
	if err != nil {
		return artifact.Info{}, false, err
	}
	actual := ref
	actual.Size = size
	actual.Checksum = artifact.FormatChecksum(hasher.Sum(nil))
	return artifact.Info{Ref: actual, CreatedAt: stat.ModTime().UTC()}, true, nil
}

type verifiedReader struct {
	file             *os.File
	checksum         hash.Hash
	expectedChecksum string
	expectedSize     int64
	read             int64
	verified         bool
}

func (r *verifiedReader) Read(data []byte) (int, error) {
	read, err := r.file.Read(data)
	if read > 0 {
		_, _ = r.checksum.Write(data[:read])
		r.read += int64(read)
	}
	if errors.Is(err, io.EOF) && !r.verified {
		r.verified = true
		if r.read != r.expectedSize {
			return read, fmt.Errorf("%w: read %d bytes, want %d", artifact.ErrCorrupt, r.read, r.expectedSize)
		}
		if r.expectedChecksum != "" {
			actual := artifact.FormatChecksum(r.checksum.Sum(nil))
			if actual != r.expectedChecksum {
				return read, fmt.Errorf("%w: checksum is %q, want %q", artifact.ErrCorrupt, actual, r.expectedChecksum)
			}
		}
	}
	return read, err
}

func (r *verifiedReader) Close() error {
	if r == nil || r.file == nil {
		return nil
	}
	return r.file.Close()
}
