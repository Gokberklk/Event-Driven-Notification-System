package tracing

import (
	"context"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestInjectExtractRoundTrip(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()

	ctx, span := tp.Tracer("test").Start(context.Background(), "api")
	defer span.End()

	tp1 := Inject(ctx)
	if tp1 == "" {
		t.Fatal("expected a traceparent for an active span")
	}

	// A worker continuing from the stored traceparent must share the trace ID.
	wctx := Extract(context.Background(), tp1)
	child, cspan := tp.Tracer("test").Start(wctx, "worker")
	defer cspan.End()
	if TraceID(child) != TraceID(ctx) {
		t.Fatalf("trace id mismatch: %s vs %s", TraceID(child), TraceID(ctx))
	}
}

func TestNoSpan(t *testing.T) {
	if Inject(context.Background()) != "" || TraceID(context.Background()) != "" {
		t.Fatal("no span should give empty values")
	}
	if Extract(context.Background(), "") != context.Background() {
		t.Fatal("empty traceparent must return ctx unchanged")
	}
}

func TestInitWithoutEndpointIsNoop(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	shutdown, err := Init(context.Background(), "test")
	if err != nil {
		t.Fatal(err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
