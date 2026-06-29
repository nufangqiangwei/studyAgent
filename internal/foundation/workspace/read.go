package workspace

import (
	"context"
	"fmt"
	"io"
	"os"
)

func (w *LocalWorkspace) Read(ctx context.Context, filePath string, opts ReadOptions) (FileContent, error) {
	if w == nil {
		return FileContent{}, fmt.Errorf("read workspace file: nil workspace")
	}
	ctx = contextOrBackground(ctx)
	if err := checkContext(ctx); err != nil {
		return FileContent{}, err
	}

	rel, err := normalizeRel(filePath)
	if err != nil {
		return FileContent{}, err
	}
	if !opts.IncludeIgnored && w.ignore.ignored(rel, false) {
		return FileContent{}, fmt.Errorf("read %q: path is ignored", rel)
	}

	abs, err := w.Resolve(filePath)
	if err != nil {
		return FileContent{}, err
	}
	info, err := os.Stat(abs)
	if err != nil {
		return FileContent{}, fmt.Errorf("stat %q: %w", rel, err)
	}
	if info.IsDir() {
		return FileContent{}, fmt.Errorf("read %q: is a directory", rel)
	}

	maxBytes := defaultInt64(opts.MaxBytes, DefaultMaxReadBytes)
	if info.Size() > maxBytes {
		return FileContent{}, fmt.Errorf("read %q: file size %d exceeds limit %d", rel, info.Size(), maxBytes)
	}

	if !opts.AllowBinary {
		binary, err := isBinaryFile(abs)
		if err != nil {
			return FileContent{}, fmt.Errorf("inspect %q: %w", rel, err)
		}
		if binary {
			return FileContent{}, fmt.Errorf("read %q: binary file is ignored", rel)
		}
	}

	file, err := os.Open(abs)
	if err != nil {
		return FileContent{}, fmt.Errorf("open %q: %w", rel, err)
	}
	defer file.Close()

	data, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		return FileContent{}, fmt.Errorf("read %q: %w", rel, err)
	}
	if int64(len(data)) > maxBytes {
		return FileContent{}, fmt.Errorf("read %q: file exceeds limit %d", rel, maxBytes)
	}
	if err := checkContext(ctx); err != nil {
		return FileContent{}, err
	}

	return FileContent{
		Path:    rel,
		Content: string(data),
		Size:    int64(len(data)),
	}, nil
}
