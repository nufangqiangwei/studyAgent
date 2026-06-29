package workspace

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

func (w *LocalWorkspace) Search(ctx context.Context, query string, opts SearchOptions) ([]SearchMatch, error) {
	if w == nil {
		return nil, fmt.Errorf("search workspace: nil workspace")
	}
	if query == "" {
		return nil, fmt.Errorf("search query is empty")
	}
	if strings.ContainsAny(query, "\r\n") {
		return nil, fmt.Errorf("search query must be a single line")
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
		return nil, fmt.Errorf("stat search root %q: %w", opts.Dir, err)
	}
	if !opts.IncludeIgnored && startRel != "." && w.ignore.ignored(startRel, info.IsDir()) {
		return nil, nil
	}

	maxMatches := defaultInt(opts.MaxMatches, DefaultMaxSearchMatches)
	contextLines := opts.ContextLines
	if contextLines < 0 {
		contextLines = 0
	}
	if contextLines > MaxSearchContextLines {
		contextLines = MaxSearchContextLines
	}
	maxFileBytes := defaultInt64(opts.MaxFileBytes, DefaultMaxFileBytes)
	matches := make([]SearchMatch, 0)

	if !info.IsDir() {
		if err := w.searchFile(ctx, startRel, startAbs, query, opts, maxFileBytes, contextLines, maxMatches, &matches); err != nil && err != errLimitReached {
			return nil, err
		}
		return matches, nil
	}

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
		if d.IsDir() {
			if !opts.IncludeIgnored && w.ignore.ignored(rel, true) {
				return filepath.SkipDir
			}
			if d.Type()&fs.ModeSymlink != 0 {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Type()&fs.ModeSymlink != 0 {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if info.Size() > maxFileBytes {
			return nil
		}
		if err := w.searchFile(ctx, rel, abs, query, opts, maxFileBytes, contextLines, maxMatches, &matches); err != nil {
			return err
		}
		if len(matches) >= maxMatches {
			return errLimitReached
		}
		return nil
	})
	if err == errLimitReached {
		err = nil
	}
	return matches, err
}

func (w *LocalWorkspace) searchFile(ctx context.Context, rel, abs, query string, opts SearchOptions, maxFileBytes int64, contextLines int, maxMatches int, matches *[]SearchMatch) error {
	if !opts.IncludeIgnored && w.ignore.ignored(rel, false) {
		return nil
	}

	info, err := os.Stat(abs)
	if err != nil {
		return fmt.Errorf("stat %q: %w", rel, err)
	}
	if info.IsDir() || info.Size() > maxFileBytes {
		return nil
	}

	binary, err := isBinaryFile(abs)
	if err != nil {
		return fmt.Errorf("inspect %q: %w", rel, err)
	}
	if binary {
		return nil
	}

	data, err := os.ReadFile(abs)
	if err != nil {
		return fmt.Errorf("read %q: %w", rel, err)
	}
	if int64(len(data)) > maxFileBytes {
		return nil
	}
	if err := checkContext(ctx); err != nil {
		return err
	}

	needle := query
	if opts.IgnoreCase {
		needle = strings.ToLower(query)
	}

	lines := strings.Split(string(data), "\n")
	for i, line := range lines {
		line = strings.TrimSuffix(line, "\r")
		haystack := line
		if opts.IgnoreCase {
			haystack = strings.ToLower(line)
		}
		col := strings.Index(haystack, needle)
		if col < 0 {
			continue
		}
		if len(*matches) >= maxMatches {
			return errLimitReached
		}
		*matches = append(*matches, SearchMatch{
			Path:     rel,
			Line:     i + 1,
			Column:   col + 1,
			LineText: line,
			Before:   contextBefore(lines, i, contextLines),
			After:    contextAfter(lines, i, contextLines),
		})
	}
	return nil
}

func contextBefore(lines []string, idx int, count int) []string {
	if count <= 0 {
		return nil
	}
	start := idx - count
	if start < 0 {
		start = 0
	}
	return cleanLines(lines[start:idx])
}

func contextAfter(lines []string, idx int, count int) []string {
	if count <= 0 {
		return nil
	}
	end := idx + 1 + count
	if end > len(lines) {
		end = len(lines)
	}
	return cleanLines(lines[idx+1 : end])
}

func cleanLines(lines []string) []string {
	if len(lines) == 0 {
		return nil
	}
	result := make([]string, len(lines))
	for i, line := range lines {
		result[i] = strings.TrimSuffix(line, "\r")
	}
	return result
}
