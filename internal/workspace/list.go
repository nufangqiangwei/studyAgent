package workspace

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

type FileEntry struct {
	Path    string
	IsDir   bool
	Size    int64
	ModTime time.Time
}

func (w *LocalWorkspace) List(ctx context.Context, opts ListOptions) ([]FileEntry, error) {
	if w == nil {
		return nil, fmt.Errorf("list workspace: nil workspace")
	}
	ctx = contextOrBackground(ctx)

	startRel, err := normalizeRel(opts.Dir)
	if err != nil {
		return nil, err
	}
	startAbs, err := w.Resolve(opts.Dir)
	if err != nil {
		return nil, err
	}

	info, err := os.Stat(startAbs)
	if err != nil {
		return nil, fmt.Errorf("stat list root %q: %w", opts.Dir, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("list root %q is not a directory", opts.Dir)
	}
	if !opts.IncludeIgnored && startRel != "." && w.ignore.ignored(startRel, true) {
		return nil, nil
	}

	maxEntries := defaultInt(opts.MaxEntries, DefaultMaxEntries)
	maxFileBytes := defaultInt64(opts.MaxFileBytes, DefaultMaxFileBytes)
	entries := make([]FileEntry, 0)

	if opts.Recursive {
		err = filepath.WalkDir(startAbs, func(abs string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if err := checkContext(ctx); err != nil {
				return err
			}
			if abs == startAbs {
				return nil
			}

			rel, err := w.relative(abs)
			if err != nil {
				return err
			}
			stop, err := w.appendListEntry(rel, abs, d, opts, maxFileBytes, maxEntries, &entries)
			if err != nil {
				return err
			}
			if stop {
				if d.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			if len(entries) >= maxEntries {
				return errLimitReached
			}
			return nil
		})
		if err == errLimitReached {
			err = nil
		}
		return entries, err
	}

	children, err := os.ReadDir(startAbs)
	if err != nil {
		return nil, fmt.Errorf("read list root %q: %w", opts.Dir, err)
	}
	for _, child := range children {
		if err := checkContext(ctx); err != nil {
			return nil, err
		}

		abs := filepath.Join(startAbs, child.Name())
		rel, err := w.relative(abs)
		if err != nil {
			return nil, err
		}
		if _, err := w.appendListEntry(rel, abs, child, opts, maxFileBytes, maxEntries, &entries); err != nil {
			return nil, err
		}
		if len(entries) >= maxEntries {
			break
		}
	}
	return entries, nil
}

func (w *LocalWorkspace) appendListEntry(rel, abs string, d fs.DirEntry, opts ListOptions, maxFileBytes int64, maxEntries int, entries *[]FileEntry) (bool, error) {
	if !opts.IncludeIgnored && w.ignore.ignored(rel, d.IsDir()) {
		return true, nil
	}
	if d.Type()&fs.ModeSymlink != 0 {
		return true, nil
	}

	info, err := d.Info()
	if err != nil {
		return false, err
	}

	if d.IsDir() {
		if opts.IncludeDirs {
			if len(*entries) >= maxEntries {
				return false, errLimitReached
			}
			*entries = append(*entries, FileEntry{
				Path:    rel,
				IsDir:   true,
				ModTime: info.ModTime(),
			})
		}
		return false, nil
	}

	if info.Size() > maxFileBytes {
		return true, nil
	}
	binary, err := isBinaryFile(abs)
	if err != nil {
		return false, err
	}
	if binary {
		return true, nil
	}

	if len(*entries) >= maxEntries {
		return false, errLimitReached
	}
	*entries = append(*entries, FileEntry{
		Path:    rel,
		IsDir:   false,
		Size:    info.Size(),
		ModTime: info.ModTime(),
	})
	return false, nil
}
