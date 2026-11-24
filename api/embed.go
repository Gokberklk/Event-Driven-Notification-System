// Package api embeds the OpenAPI specification and serves Swagger UI.
package api

import (
	_ "embed"
	"net/http"
)

//go:embed openapi.yaml
var spec []byte

const swaggerHTML = `<!doctype html>
<html><head><meta charset="utf-8"><title>Notification API</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.17.14/swagger-ui.css"></head>
<body><div id="ui"></div>
<script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.17.14/swagger-ui-bundle.js"></script>
<script>SwaggerUIBundle({url: "/openapi.yaml", dom_id: "#ui"});</script>
</body></html>`

// Spec serves the raw OpenAPI document.
func Spec(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/yaml")
	_, _ = w.Write(spec)
}

// Docs serves Swagger UI pointed at /openapi.yaml.
func Docs(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(swaggerHTML))
}
