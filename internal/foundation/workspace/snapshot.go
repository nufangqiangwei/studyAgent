package workspace

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
)

func (w *LocalWorkspace) Snapshot(ctx context.Context, opts SnapshotOptions) (Snapshot, error) {
	if w == nil {
		return Snapshot{}, fmt.Errorf("snapshot workspace: nil workspace")
	}
	ctx = contextOrBackground(ctx)

	maxEntries := defaultInt(opts.MaxEntries, DefaultSnapshotEntries)
	maxDepth := defaultInt(opts.MaxDepth, DefaultSnapshotDepth)
	maxFileBytes := defaultInt64(opts.MaxFileBytes, DefaultMaxFileBytes)

	snapshot := Snapshot{
		Root:    w.root,
		Entries: make([]FileEntry, 0),
	}

	err := filepath.WalkDir(w.root, func(abs string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := checkContext(ctx); err != nil {
			return err
		}
		if abs == w.root {
			return nil
		}

		rel, err := w.relative(abs)
		if err != nil {
			return err
		}
		depth := pathDepth(rel)
		if depth > maxDepth {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if !opts.IncludeIgnored && w.ignore.ignored(rel, d.IsDir()) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Type()&fs.ModeSymlink != 0 {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		entry := FileEntry{
			Path:    rel,
			IsDir:   d.IsDir(),
			ModTime: info.ModTime(),
		}
		if d.IsDir() {
			snapshot.Dirs++
		} else {
			if info.Size() > maxFileBytes {
				return nil
			}
			binary, err := isBinaryFile(abs)
			if err != nil {
				return err
			}
			if binary {
				return nil
			}
			entry.Size = info.Size()
			snapshot.Files++
			snapshot.TotalSize += info.Size()
		}

		if len(snapshot.Entries) >= maxEntries {
			snapshot.Truncated = true
			return errLimitReached
		}
		snapshot.Entries = append(snapshot.Entries, entry)
		return nil
	})
	if err == errLimitReached {
		err = nil
	}
	if err != nil && !os.IsNotExist(err) {
		return Snapshot{}, err
	}

	snapshot.Tree = renderTree(snapshot.Entries, snapshot.Truncated)
	return snapshot, nil
}

func pathDepth(rel string) int {
	if rel == "." || rel == "" {
		return 0
	}
	return strings.Count(rel, "/") + 1
}

func renderTree(entries []FileEntry, truncated bool) string {
	lines := []string{"."}
	for _, entry := range entries {
		name := path.Base(entry.Path)
		if entry.IsDir {
			name += "/"
		}
		lines = append(lines, strings.Repeat("  ", pathDepth(entry.Path))+name)
	}
	if truncated {
		lines = append(lines, "  ...")
	}
	return strings.Join(lines, "\n")
}
