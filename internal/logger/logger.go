// Package logger provides structured JSON logging with correlation ID propagation.
package logger

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

type ctxKey struct{}

// New returns a JSON slog.Logger at the given level.
func New(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

// WithCorrelationID stores the correlation ID on the context.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// CorrelationID returns the correlation ID stored on the context, if any.
func CorrelationID(ctx context.Context) string {
	id, _ := ctx.Value(ctxKey{}).(string)
	return id
}

// FromContext returns a logger enriched with the context's correlation ID.
// When a trace is active its trace_id is added too, linking logs to traces.
func FromContext(ctx context.Context, base *slog.Logger) *slog.Logger {
	l := base
	if id := CorrelationID(ctx); id != "" {
		l = l.With("correlation_id", id)
	}
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		l = l.With("trace_id", sc.TraceID().String())
	}
	return l
}
