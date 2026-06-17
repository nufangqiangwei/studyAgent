package workspace

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
)

const binarySampleBytes = 8000

var defaultIgnoredDirs = map[string]struct{}{
	".git":         {},
	".hg":          {},
	".svn":         {},
	"node_modules": {},
	"vendor":       {},
}

var defaultIgnoredFiles = map[string]struct{}{
	".DS_Store": {},
	"Thumbs.db": {},
}

type ignoreMatcher struct {
	rules []ignoreRule
}

type ignoreRule struct {
	pattern  string
	negated  bool
	dirOnly  bool
	anchored bool
	hasSlash bool
}

func newIgnoreMatcher(root string) (*ignoreMatcher, error) {
	matcher := &ignoreMatcher{}

	file, err := os.Open(filepath.Join(root, ".gitignore"))
	if err != nil {
		if os.IsNotExist(err) {
			return matcher, nil
		}
		return nil, fmt.Errorf("open .gitignore: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		rule, ok := parseIgnoreRule(scanner.Text())
		if ok {
			matcher.rules = append(matcher.rules, rule)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read .gitignore: %w", err)
	}
	return matcher, nil
}

func parseIgnoreRule(line string) (ignoreRule, bool) {
	line = strings.TrimSpace(line)
	if line == "" {
		return ignoreRule{}, false
	}
	if strings.HasPrefix(line, `\#`) {
		line = strings.TrimPrefix(line, `\`)
	} else if strings.HasPrefix(line, "#") {
		return ignoreRule{}, false
	}

	negated := false
	if strings.HasPrefix(line, "!") {
		negated = true
		line = strings.TrimPrefix(line, "!")
	}

	line = strings.ReplaceAll(line, "\\", "/")
	dirOnly := strings.HasSuffix(line, "/")
	line = strings.TrimSuffix(line, "/")
	anchored := strings.HasPrefix(line, "/")
	line = strings.TrimPrefix(line, "/")
	if line == "" {
		return ignoreRule{}, false
	}

	clean := path.Clean(line)
	if clean == "." {
		return ignoreRule{}, false
	}

	return ignoreRule{
		pattern:  clean,
		negated:  negated,
		dirOnly:  dirOnly,
		anchored: anchored,
		hasSlash: strings.Contains(clean, "/"),
	}, true
}

func (m *ignoreMatcher) ignored(rel string, isDir bool) bool {
	rel = path.Clean(filepath.ToSlash(rel))
	if rel == "." {
		return false
	}
	if defaultIgnored(rel) {
		return true
	}

	ignored := false
	if m == nil {
		return ignored
	}
	for _, rule := range m.rules {
		if rule.matches(rel, isDir) {
			ignored = !rule.negated
		}
	}
	return ignored
}

func defaultIgnored(rel string) bool {
	for _, segment := range strings.Split(rel, "/") {
		if _, ok := defaultIgnoredDirs[segment]; ok {
			return true
		}
	}
	if _, ok := defaultIgnoredFiles[path.Base(rel)]; ok {
		return true
	}
	return false
}

func (r ignoreRule) matches(rel string, isDir bool) bool {
	if r.dirOnly {
		for _, candidate := range dirCandidates(rel, isDir) {
			if r.matchPath(candidate) {
				return true
			}
		}
		return false
	}
	if r.matchPath(rel) {
		return true
	}
	for _, candidate := range dirCandidates(rel, isDir) {
		if r.matchPath(candidate) {
			return true
		}
	}
	return false
}

func (r ignoreRule) matchPath(rel string) bool {
	if r.anchored || r.hasSlash {
		return matchPattern(r.pattern, rel)
	}
	for _, segment := range strings.Split(rel, "/") {
		if matchPattern(r.pattern, segment) {
			return true
		}
	}
	return false
}

func dirCandidates(rel string, isDir bool) []string {
	segments := strings.Split(rel, "/")
	limit := len(segments)
	if !isDir {
		limit--
	}
	if limit <= 0 {
		return nil
	}

	candidates := make([]string, 0, limit)
	for i := 1; i <= limit; i++ {
		candidates = append(candidates, strings.Join(segments[:i], "/"))
	}
	return candidates
}

func matchPattern(pattern, name string) bool {
	matched, err := path.Match(pattern, name)
	if err != nil {
		return pattern == name
	}
	return matched
}

func isBinaryFile(filePath string) (bool, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return false, err
	}
	defer file.Close()

	buf := make([]byte, binarySampleBytes)
	n, err := file.Read(buf)
	if err != nil && err != io.EOF {
		return false, err
	}
	return bytes.Contains(buf[:n], []byte{0}), nil
}
