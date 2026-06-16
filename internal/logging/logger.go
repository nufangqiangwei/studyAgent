package logging

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
	LevelSilent
)

func ParseLevel(value string) (Level, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "info":
		return LevelInfo, nil
	case "debug":
		return LevelDebug, nil
	case "warn", "warning":
		return LevelWarn, nil
	case "error":
		return LevelError, nil
	case "silent", "off":
		return LevelSilent, nil
	default:
		return LevelInfo, fmt.Errorf("unknown log level %q", value)
	}
}

type Logger struct {
	out   io.Writer
	level Level
	mu    sync.Mutex
}

func New(out io.Writer, level Level) *Logger {
	return &Logger{out: out, level: level}
}

func (l *Logger) Debugf(format string, args ...any) {
	l.write(LevelDebug, "DEBUG", format, args...)
}

func (l *Logger) Infof(format string, args ...any) {
	l.write(LevelInfo, "INFO", format, args...)
}

func (l *Logger) Warnf(format string, args ...any) {
	l.write(LevelWarn, "WARN", format, args...)
}

func (l *Logger) Errorf(format string, args ...any) {
	l.write(LevelError, "ERROR", format, args...)
}

func (l *Logger) write(level Level, label string, format string, args ...any) {
	if l == nil || l.out == nil || l.level == LevelSilent || level < l.level {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	timestamp := time.Now().Format(time.RFC3339)
	message := fmt.Sprintf(format, args...)
	fmt.Fprintf(l.out, "%s [%s] %s\n", timestamp, label, message)
}
