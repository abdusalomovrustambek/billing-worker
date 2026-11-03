// Package logging writes log lines in the format
//
//	2006-01-02T15:04:05.000Z INFO billing.jobs: charged order 812 (49.90 USD)
//
// The component comes from a "component" attribute on the logger; other
// attributes follow the message as key=value.
package logging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
)

const timeLayout = "2006-01-02T15:04:05.000Z07:00"

// Handler is a slog.Handler that writes one text line per record.
type Handler struct {
	mu        *sync.Mutex
	w         io.Writer
	level     slog.Leveler
	component string
	attrs     []slog.Attr
}

// New returns a logger that writes to w and drops records below level.
func New(w io.Writer, level slog.Leveler) *slog.Logger {
	return slog.New(&Handler{mu: &sync.Mutex{}, w: w, level: level})
}

// Component returns a logger whose lines carry the given component name.
func Component(l *slog.Logger, name string) *slog.Logger {
	return l.With("component", name)
}

// ParseLevel maps debug, info, warn and error (any case) to a slog level.
// An empty string means info.
func ParseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug, nil
	case "", "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	}
	return 0, fmt.Errorf("unknown log level %q", s)
}

// Enabled reports whether a record at level would be written.
func (h *Handler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level.Level()
}

// WithAttrs returns a handler with attrs added; a "component" attr sets the
// component instead of being printed.
func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h2 := *h
	h2.attrs = append([]slog.Attr(nil), h.attrs...)
	for _, a := range attrs {
		if a.Key == "component" {
			h2.component = a.Value.String()
			continue
		}
		h2.attrs = append(h2.attrs, a)
	}
	return &h2
}

// WithGroup is not supported: groups are ignored and keys stay flat.
func (h *Handler) WithGroup(string) slog.Handler { return h }

// Handle writes the record as one line.
func (h *Handler) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder
	b.WriteString(r.Time.UTC().Format(timeLayout))
	b.WriteByte(' ')
	b.WriteString(r.Level.String())
	b.WriteByte(' ')
	component := h.component
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "component" {
			component = a.Value.String()
		}
		return true
	})
	if component != "" {
		b.WriteString(component)
		b.WriteString(": ")
	}
	b.WriteString(r.Message)
	for _, a := range h.attrs {
		writeAttr(&b, a)
	}
	r.Attrs(func(a slog.Attr) bool {
		if a.Key != "component" {
			writeAttr(&b, a)
		}
		return true
	})
	b.WriteByte('\n')

	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := io.WriteString(h.w, b.String())
	return err
}

func writeAttr(b *strings.Builder, a slog.Attr) {
	a.Value = a.Value.Resolve()
	if a.Equal(slog.Attr{}) {
		return
	}
	b.WriteByte(' ')
	b.WriteString(a.Key)
	b.WriteByte('=')
	s := a.Value.String()
	if s == "" || strings.ContainsAny(s, " \t\r\n\"=") {
		s = strconv.Quote(s)
	}
	b.WriteString(s)
}
