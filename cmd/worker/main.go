// Command worker consumes the notification queue and delivers messages.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/notification-system/notification-system/internal/config"
	"github.com/notification-system/notification-system/internal/logger"
)

func main() {
	cfg := config.Load()
	log := logger.New(cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("worker started", "workers", cfg.WorkerCount)
	<-ctx.Done()
	log.Info("worker stopped")
}
