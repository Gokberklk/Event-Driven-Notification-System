// Package events publishes notification status changes over Redis Pub/Sub
// and streams them to WebSocket clients.
//
// Every API and worker process publishes to one Redis channel; every API
// process subscribes once and fans events out to its connected clients. This
// keeps WebSocket updates working when the API and workers are scaled out.
package events

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisChannel is the Pub/Sub channel carrying status events.
const RedisChannel = "notifications:events"

// Event describes a notification status change.
type Event struct {
	Type           string    `json:"type"` // always "notification.status"
	NotificationID string    `json:"notificationId"`
	BatchID        string    `json:"batchId,omitempty"`
	Channel        string    `json:"channel"`
	Status         string    `json:"status"`
	Attempts       int       `json:"attempts"`
	Error          string    `json:"error,omitempty"`
	CorrelationID  string    `json:"correlationId,omitempty"`
	Timestamp      time.Time `json:"timestamp"`
}

// Publisher sends events. Publishing is best effort: a lost event never
// affects delivery, because PostgreSQL remains the source of truth.
type Publisher interface {
	Publish(ctx context.Context, e Event)
}

// Nop discards events.
type Nop struct{}

func (Nop) Publish(context.Context, Event) {}

// RedisPublisher publishes events to Redis Pub/Sub.
type RedisPublisher struct {
	rdb *redis.Client
	log *slog.Logger
}

func NewRedisPublisher(rdb *redis.Client, log *slog.Logger) *RedisPublisher {
	return &RedisPublisher{rdb: rdb, log: log}
}

func (p *RedisPublisher) Publish(ctx context.Context, e Event) {
	e.Type = "notification.status"
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now().UTC()
	}
	b, _ := json.Marshal(e)
	if err := p.rdb.Publish(ctx, RedisChannel, b).Err(); err != nil {
		p.log.Warn("publish status event failed", "notification_id", e.NotificationID, "error", err)
	}
}
