// Package health exposes liveness and readiness checks.
package health

import (
	"context"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/notification-system/notification-system/internal/httpx"
)

type Handler struct {
	DB    *pgxpool.Pool
	Redis *redis.Client
}

type response struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks"`
}

// ServeHTTP reports "ok" only if PostgreSQL and Redis are reachable.
func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	checks := map[string]string{"postgres": "ok", "redis": "ok"}
	healthy := true
	if err := h.DB.Ping(ctx); err != nil {
		checks["postgres"] = err.Error()
		healthy = false
	}
	if err := h.Redis.Ping(ctx).Err(); err != nil {
		checks["redis"] = err.Error()
		healthy = false
	}
	if !healthy {
		httpx.JSON(w, http.StatusServiceUnavailable, response{Status: "unhealthy", Checks: checks})
		return
	}
	httpx.JSON(w, http.StatusOK, response{Status: "ok", Checks: checks})
}
