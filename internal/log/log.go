// Package log is a thin slog-backed wrapper that preserves the
// outbrain/golib/log call surface (Debug/Info/Warning/Fatal etc.) while
// removing the external dependency.  Output format: "YYYY-MM-DD HH:MM:SS LEVEL message".
package log

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// Level aliases slog.Level so existing log.DEBUG / log.INFO / log.WARNING
// constants compile without changes at call sites.
type Level = slog.Level

const (
	DEBUG   Level = slog.LevelDebug
	INFO    Level = slog.LevelInfo
	WARNING Level = slog.LevelWarn
)

// plainHandler emits "YYYY-MM-DD HH:MM:SS LEVEL message\n" to stderr,
// matching the format produced by the previous outbrain/golib/log dependency.
type plainHandler struct{ minLevel slog.Level }

func (h *plainHandler) Enabled(_ context.Context, l slog.Level) bool { return l >= h.minLevel }

func (h *plainHandler) Handle(_ context.Context, r slog.Record) error {
	level := r.Level.String()
	if r.Level == slog.LevelWarn {
		level = "WARNING"
	}
	fmt.Fprintf(os.Stderr, "%s %s %s\n", r.Time.Format("2006-01-02 15:04:05"), level, r.Message)
	return nil
}

func (h *plainHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *plainHandler) WithGroup(_ string) slog.Handler      { return h }

var handler = &plainHandler{minLevel: INFO}

// SetLevel sets the minimum log level.
func SetLevel(l Level) { handler.minLevel = l }

var (
	exitHooksMu sync.Mutex
	exitHooks   []func()
)

// RegisterExitHook registers fn to be called before os.Exit in Fatal/Fatalf.
// Hooks run in registration order. Safe to call concurrently.
func RegisterExitHook(fn func()) {
	exitHooksMu.Lock()
	defer exitHooksMu.Unlock()
	exitHooks = append(exitHooks, fn)
}

func runExitHooks() {
	exitHooksMu.Lock()
	hooks := make([]func(), len(exitHooks))
	copy(hooks, exitHooks)
	exitHooksMu.Unlock()
	for _, fn := range hooks {
		fn()
	}
}

func emit(l slog.Level, msg string) {
	if handler.Enabled(context.Background(), l) {
		_ = handler.Handle(context.Background(), slog.NewRecord(time.Now(), l, msg, 0))
	}
}

func Debug(args ...any)                    { emit(slog.LevelDebug, fmt.Sprint(args...)) }
func Debugf(format string, args ...any)    { emit(slog.LevelDebug, fmt.Sprintf(format, args...)) }
func Info(args ...any)                     { emit(slog.LevelInfo, fmt.Sprint(args...)) }
func Infof(format string, args ...any)     { emit(slog.LevelInfo, fmt.Sprintf(format, args...)) }
func Warning(args ...any)                  { emit(slog.LevelWarn, fmt.Sprint(args...)) }
func Warningf(format string, args ...any)  { emit(slog.LevelWarn, fmt.Sprintf(format, args...)) }
func Error(args ...any)                    { emit(slog.LevelError, fmt.Sprint(args...)) }
func Errorf(format string, args ...any)    { emit(slog.LevelError, fmt.Sprintf(format, args...)) }
func Critical(args ...any)                 { emit(slog.LevelError, fmt.Sprint(args...)) }
func Criticalf(format string, args ...any) { emit(slog.LevelError, fmt.Sprintf(format, args...)) }
func Fatal(args ...any)                 { emit(slog.LevelError, fmt.Sprint(args...)); runExitHooks(); os.Exit(1) }
func Fatalf(format string, args ...any) { emit(slog.LevelError, fmt.Sprintf(format, args...)); runExitHooks(); os.Exit(1) }
