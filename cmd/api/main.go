// Command api runs the notification HTTP API.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/redis/go-redis/v9"

	"github.com/Gokberklk/Event-Driven-Notification-System/api"
	"github.com/Gokberklk/Event-Driven-Notification-System/internal/config"
	"github.com/Gokberklk/Event-Driven-Notification-System/internal/database"
	"github.com/Gokberklk/Event-Driven-Notification-System/internal/health"
	"github.com/Gokberklk/Event-Driven-Notification-System/internal/httpx"
	"github.com/Gokberklk/Event-Driven-Notification-System/internal/logger"
	"github.com/Gokberklk/Event-Driven-Notification-System/internal/metrics"
	"github.com/Gokberklk/Event-Driven-Notification-System/internal/notification"
	"github.com/Gokberklk/Event-Driven-Notification-System/internal/queue"
)

func main() {
	cfg := config.Load()
	log := logger.New(cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := database.Migrate(cfg.DatabaseURL); err != nil {
		log.Error("migrations failed", "error", err)
		os.Exit(1)
	}
	db, err := database.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("database connection failed", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()

	r := chi.NewRouter()
	r.Use(middleware.Recoverer, httpx.CorrelationID, httpx.RequestLogger(log))
	r.Method(http.MethodGet, "/health", health.Handler{DB: db, Redis: rdb})
	r.Get("/openapi.yaml", api.Spec)
	r.Get("/docs", api.Docs)

	q := queue.New(rdb)
	channels := make([]string, len(notification.Channels))
	for i, c := range notification.Channels {
		channels[i] = string(c)
	}
	r.Method(http.MethodGet, "/metrics", metrics.Handler{Recorder: metrics.New(rdb), Queue: q, Channels: channels})

	svc := notification.NewService(notification.NewRepository(db), q, log, cfg.MaxAttempts)
	r.Route("/api/v1", notification.NewHandler(svc, log).Routes)

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: r, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Info("api listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	log.Info("api stopped")
}
