// Command worker consumes the notification queue and delivers messages.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"

	"github.com/Gokberklk/Event-Driven-Notification-System/internal/config"
	"github.com/Gokberklk/Event-Driven-Notification-System/internal/database"
	"github.com/Gokberklk/Event-Driven-Notification-System/internal/events"
	"github.com/Gokberklk/Event-Driven-Notification-System/internal/logger"
	"github.com/Gokberklk/Event-Driven-Notification-System/internal/metrics"
	"github.com/Gokberklk/Event-Driven-Notification-System/internal/notification"
	"github.com/Gokberklk/Event-Driven-Notification-System/internal/provider"
	"github.com/Gokberklk/Event-Driven-Notification-System/internal/queue"
	"github.com/Gokberklk/Event-Driven-Notification-System/internal/ratelimit"
	"github.com/Gokberklk/Event-Driven-Notification-System/internal/tracing"
	"github.com/Gokberklk/Event-Driven-Notification-System/internal/worker"
)

func main() {
	cfg := config.Load()
	log := logger.New(cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := tracing.Init(ctx, "notification-worker")
	if err != nil {
		log.Error("tracing init failed", "error", err)
		os.Exit(1)
	}
	defer func() { _ = shutdownTracing(context.Background()) }()

	db, err := database.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("database connection failed", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()
	if err := redisotel.InstrumentTracing(rdb); err != nil {
		log.Warn("redis tracing disabled", "error", err)
	}

	w := worker.New(
		worker.DefaultConfig(cfg.WorkerCount),
		notification.NewRepository(db),
		queue.New(rdb),
		provider.NewClient(cfg.ProviderURL, cfg.ProviderTimeout),
		ratelimit.New(rdb, cfg.RateLimitPerSec),
		metrics.New(rdb),
		log,
	).WithEvents(events.NewRedisPublisher(rdb, log))
	log.Info("worker started", "workers_per_channel", cfg.WorkerCount, "rate_limit_per_sec", cfg.RateLimitPerSec)
	w.Run(ctx)
	log.Info("worker stopped")
}
