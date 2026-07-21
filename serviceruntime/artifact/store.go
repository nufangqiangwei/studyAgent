package artifact

import (
	"agent/serviceruntime/contract"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"
)

var (
	ErrUnavailable = errors.New("artifact store is unavailable")
	ErrNotFound    = errors.New("artifact not found")
	ErrConflict    = errors.New("artifact conflicts with an existing object")
	ErrCorrupt     = errors.New("artifact content is corrupt")
	ErrClosed      = errors.New("artifact store is closed")
)

// WriteRequest describes a stable immutable artifact. Key is a logical key
// chosen by the caller, normally derived from a durable EffectID or business
// operation ID. Retrying the same key with the same bytes is idempotent.
type WriteRequest struct {
	Key              string
	ContentType      string
	ExpectedChecksum string
	ExpectedSize     *int64
}

type Info struct {
	Ref       contract.ArtifactRef
	CreatedAt time.Time
}

// Reader is the narrow capability exposed to Services. Implementations must
// return immutable content and validate any checksum/size present in ref.
type Reader interface {
	Open(ctx context.Context, ref contract.ArtifactRef) (io.ReadCloser, Info, error)
	Stat(ctx context.Context, ref contract.ArtifactRef) (Info, error)
}

// WriteSession streams bytes to a staging object. Commit is the only operation
// that makes the artifact visible; Abort removes the staging object.
type WriteSession interface {
	io.Writer
	Commit(ctx context.Context) (contract.ArtifactRef, error)
	Abort(ctx context.Context) error
}

type Writer interface {
	Begin(ctx context.Context, request WriteRequest) (WriteSession, error)
}

type Store interface {
	Reader
	Writer
	Name() string
	Close() error
}

func WriteAll(ctx context.Context, writer Writer, request WriteRequest, source io.Reader) (contract.ArtifactRef, error) {
	if writer == nil {
		return contract.ArtifactRef{}, ErrUnavailable
	}
	if source == nil {
		return contract.ArtifactRef{}, fmt.Errorf("artifact source is required")
	}
	session, err := writer.Begin(ctx, request)
	if err != nil {
		return contract.ArtifactRef{}, err
	}
	if _, err := copyContext(ctx, session, source); err != nil {
		_ = session.Abort(context.Background())
		return contract.ArtifactRef{}, fmt.Errorf("write artifact %q: %w", request.Key, err)
	}
	ref, err := session.Commit(ctx)
	if err != nil {
		_ = session.Abort(context.Background())
		return contract.ArtifactRef{}, err
	}
	return ref, nil
}

func copyContext(ctx context.Context, destination io.Writer, source io.Reader) (int64, error) {
	buffer := make([]byte, 32*1024)
	var total int64
	for {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return total, err
			}
		}
		read, readErr := source.Read(buffer)
		if read > 0 {
			written, writeErr := destination.Write(buffer[:read])
			total += int64(written)
			if writeErr != nil {
				return total, writeErr
			}
			if written != read {
				return total, io.ErrShortWrite
			}
		}
		if errors.Is(readErr, io.EOF) {
			return total, nil
		}
		if readErr != nil {
			return total, readErr
		}
	}
}

func ValidateWriteRequest(request WriteRequest) error {
	if err := ValidateKey(request.Key); err != nil {
		return err
	}
	if request.ExpectedSize != nil && *request.ExpectedSize < 0 {
		return fmt.Errorf("artifact expected size cannot be negative")
	}
	if request.ExpectedChecksum != "" {
		if err := ValidateChecksum(request.ExpectedChecksum); err != nil {
			return err
		}
	}
	return nil
}

func ValidateRef(ref contract.ArtifactRef) error {
	if strings.TrimSpace(ref.Store) == "" {
		return fmt.Errorf("artifact store is required")
	}
	if err := ValidateKey(ref.Key); err != nil {
		return err
	}
	if ref.Size < 0 {
		return fmt.Errorf("artifact size cannot be negative")
	}
	if ref.Checksum != "" {
		if err := ValidateChecksum(ref.Checksum); err != nil {
			return err
		}
	}
	return nil
}

func ValidateKey(key string) error {
	if strings.TrimSpace(key) == "" {
		return fmt.Errorf("artifact key is required")
	}
	if key != strings.TrimSpace(key) || strings.Contains(key, "\\") || strings.HasPrefix(key, "/") || key == "." || key == ".." || strings.HasPrefix(key, "../") || path.Clean(key) != key {
		return fmt.Errorf("artifact key %q is not a clean relative path", key)
	}
	for _, value := range key {
		if value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9' {
			continue
		}
		switch value {
		case '/', '-', '_', '.':
		default:
			return fmt.Errorf("artifact key %q contains unsupported character %q", key, value)
		}
	}
	return nil
}

func ValidateChecksum(checksum string) error {
	const prefix = "sha256:"
	if !strings.HasPrefix(checksum, prefix) || len(checksum) != len(prefix)+64 {
		return fmt.Errorf("artifact checksum must use sha256:<64 hex characters>")
	}
	for _, value := range checksum[len(prefix):] {
		if value >= '0' && value <= '9' || value >= 'a' && value <= 'f' {
			continue
		}
		return fmt.Errorf("artifact checksum contains non-lowercase-hex character %q", value)
	}
	return nil
}
