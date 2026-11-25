package httpx

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Tracing starts a server span per request (continuing an incoming
// traceparent if present). Health, metrics and docs are not traced.
func Tracing(next http.Handler) http.Handler {
	return otelhttp.NewHandler(routeName(next), "http.server",
		otelhttp.WithFilter(func(r *http.Request) bool {
			switch r.URL.Path {
			case "/health", "/metrics", "/docs", "/openapi.yaml", "/live", "/ws":
				return false
			}
			return true
		}),
	)
}

// routeName renames the span to "METHOD /route/{pattern}" once chi has
// matched the route, keeping span names low-cardinality.
func routeName(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
		if rc := chi.RouteContext(r.Context()); rc != nil {
			if p := rc.RoutePattern(); p != "" {
				span := trace.SpanFromContext(r.Context())
				span.SetName(r.Method + " " + p)
				span.SetAttributes(attribute.String("http.route", p))
			}
		}
	})
}
