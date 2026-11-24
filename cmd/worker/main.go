// Command worker consumes the notification queue and delivers messages.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"

	"github.com/notification-system/notification-system/internal/config"
	"github.com/notification-system/notification-system/internal/database"
	"github.com/notification-system/notification-system/internal/logger"
	"github.com/notification-system/notification-system/internal/metrics"
	"github.com/notification-system/notification-system/internal/notification"
	"github.com/notification-system/notification-system/internal/provider"
	"github.com/notification-system/notification-system/internal/queue"
	"github.com/notification-system/notification-system/internal/ratelimit"
	"github.com/notification-system/notification-system/internal/worker"
)

func main() {
	cfg := config.Load()
	log := logger.New(cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := database.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("database connection failed", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()

	w := worker.New(
		worker.DefaultConfig(cfg.WorkerCount),
		notification.NewRepository(db),
		queue.New(rdb),
		provider.NewClient(cfg.ProviderURL, cfg.ProviderTimeout),
		ratelimit.New(rdb, cfg.RateLimitPerSec),
		metrics.New(rdb),
		log,
	)
	log.Info("worker started", "workers_per_channel", cfg.WorkerCount, "rate_limit_per_sec", cfg.RateLimitPerSec)
	w.Run(ctx)
	log.Info("worker stopped")
}
