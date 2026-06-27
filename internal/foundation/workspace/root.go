package workspace

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
)

const (
	DefaultMaxEntries       = 1000
	DefaultMaxReadBytes     = 1 << 20
	DefaultMaxFileBytes     = 1 << 20
	DefaultMaxSearchMatches = 100
	MaxSearchContextLines   = 5
	DefaultSnapshotEntries  = 200
	DefaultSnapshotDepth    = 3
)

var (
	ErrPathEscapesRoot = errors.New("path escapes workspace root")
	errLimitReached    = errors.New("workspace result limit reached")
)

type Workspace interface {
	Root() string
	Resolve(rel string) (abs string, err error)
	List(ctx context.Context, opts ListOptions) ([]FileEntry, error)
	Read(ctx context.Context, filePath string, opts ReadOptions) (FileContent, error)
	Search(ctx context.Context, query string, opts SearchOptions) ([]SearchMatch, error)
}

type LocalWorkspace struct {
	root   string
	ignore *ignoreMatcher
}

type ListOptions struct {
	Dir            string
	Recursive      bool
	MaxEntries     int
	MaxFileBytes   int64
	IncludeDirs    bool
	IncludeIgnored bool
}

type ReadOptions struct {
	MaxBytes       int64
	AllowBinary    bool
	IncludeIgnored bool
}

type SearchOptions struct {
	Dir            string
	MaxMatches     int
	ContextLines   int
	IgnoreCase     bool
	MaxFileBytes   int64
	IncludeIgnored bool
}

type SnapshotOptions struct {
	MaxEntries     int
	MaxDepth       int
	MaxFileBytes   int64
	IncludeIgnored bool
}

type FileContent struct {
	Path    string
	Content string
	Size    int64
}

type SearchMatch struct {
	Path     string
	Line     int
	Column   int
	LineText string
	Before   []string
	After    []string
}

type Snapshot struct {
	Root      string
	Files     int
	Dirs      int
	TotalSize int64
	Entries   []FileEntry
	Tree      string
	Truncated bool
}

func New(root string) (*LocalWorkspace, error) {
	if root == "" {
		root = "."
	}

	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve workspace root %q: %w", root, err)
	}
	abs = filepath.Clean(abs)

	actual, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return nil, fmt.Errorf("resolve workspace root %q: %w", root, err)
	}
	actual = filepath.Clean(actual)

	info, err := os.Stat(actual)
	if err != nil {
		return nil, fmt.Errorf("stat workspace root %q: %w", actual, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("workspace root %q is not a directory", actual)
	}

	ignore, err := newIgnoreMatcher(actual)
	if err != nil {
		return nil, err
	}

	return &LocalWorkspace{
		root:   actual,
		ignore: ignore,
	}, nil
}

func (w *LocalWorkspace) Root() string {
	if w == nil {
		return ""
	}
	return w.root
}

func (w *LocalWorkspace) Resolve(rel string) (string, error) {
	if w == nil {
		return "", fmt.Errorf("resolve %q: nil workspace", rel)
	}

	cleanRel, err := normalizeRel(rel)
	if err != nil {
		return "", err
	}

	abs := w.root
	if cleanRel != "." {
		abs = filepath.Join(w.root, filepath.FromSlash(cleanRel))
	}
	abs = filepath.Clean(abs)

	if err := ensureInsideRoot(w.root, abs); err != nil {
		return "", fmt.Errorf("resolve %q: %w", rel, err)
	}
	if err := w.ensureExistingPrefixInside(abs); err != nil {
		return "", fmt.Errorf("resolve %q: %w", rel, err)
	}
	return abs, nil
}

func normalizeRel(rel string) (string, error) {
	if strings.ContainsRune(rel, 0) {
		return "", fmt.Errorf("invalid workspace path %q: contains NUL", rel)
	}
	if rel == "" {
		return ".", nil
	}
	if filepath.IsAbs(rel) || filepath.VolumeName(rel) != "" {
		return "", fmt.Errorf("workspace path %q must be relative", rel)
	}

	slashRel := strings.ReplaceAll(rel, "\\", "/")
	clean := path.Clean(slashRel)
	if clean == "." {
		return ".", nil
	}
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("%w: %q", ErrPathEscapesRoot, rel)
	}
	return clean, nil
}

func ensureInsideRoot(root, abs string) error {
	rel, err := filepath.Rel(root, abs)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrPathEscapesRoot, err)
	}
	if rel == "." {
		return nil
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || filepath.IsAbs(rel) {
		return fmt.Errorf("%w: %s", ErrPathEscapesRoot, abs)
	}
	return nil
}

func (w *LocalWorkspace) ensureExistingPrefixInside(abs string) error {
	candidate := filepath.Clean(abs)
	for {
		if _, err := os.Lstat(candidate); err == nil {
			break
		} else if os.IsNotExist(err) {
			parent := filepath.Dir(candidate)
			if parent == candidate {
				return err
			}
			candidate = parent
		} else {
			return err
		}
	}

	actual, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return err
	}
	return ensureInsideRoot(w.root, filepath.Clean(actual))
}

func (w *LocalWorkspace) relative(abs string) (string, error) {
	rel, err := filepath.Rel(w.root, abs)
	if err != nil {
		return "", err
	}
	rel = filepath.ToSlash(rel)
	if rel == "." {
		return ".", nil
	}
	return path.Clean(rel), nil
}

func contextOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func checkContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func defaultInt(value, fallback int) int {
	if value <= 0 {
		return fallback
	}
	return value
}

func defaultInt64(value, fallback int64) int64 {
	if value <= 0 {
		return fallback
	}
	return value
}
