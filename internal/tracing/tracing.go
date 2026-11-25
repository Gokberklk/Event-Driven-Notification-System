// Package tracing configures OpenTelemetry distributed tracing.
//
// Tracing is enabled when OTEL_EXPORTER_OTLP_ENDPOINT is set (Docker Compose
// points it at Jaeger). Without it, spans are no-ops, so tests and local runs
// need no collector. W3C trace context is always propagated.
package tracing

import (
	"context"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const instrumentation = "github.com/Gokberklk/Event-Driven-Notification-System"

var propagator = propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})

// Init installs the global tracer provider and propagator. The returned
// function flushes pending spans and must be called on shutdown.
func Init(ctx context.Context, serviceName string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagator)
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" && os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") == "" {
		return func(context.Context) error { return nil }, nil
	}
	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(semconv.SchemaURL,
		semconv.ServiceName(serviceName)))
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// Tracer returns the application tracer.
func Tracer() trace.Tracer { return otel.Tracer(instrumentation) }

// Inject serialises the span context in ctx as a W3C traceparent value, or
// returns "" when there is no active span.
func Inject(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	propagator.Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// Extract returns ctx carrying the remote span context from traceparent.
// Used by workers to continue the trace started by the API request.
func Extract(ctx context.Context, traceparent string) context.Context {
	if traceparent == "" {
		return ctx
	}
	return propagator.Extract(ctx, propagation.MapCarrier{"traceparent": traceparent})
}

// TraceID returns the trace ID of the span in ctx, or "".
func TraceID(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ""
	}
	return sc.TraceID().String()
}
