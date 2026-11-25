// Package worker consumes the per-channel queues and delivers notifications
// through the provider with rate limiting, retries and exponential backoff.
package worker

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/Gokberklk/Event-Driven-Notification-System/internal/events"
	"github.com/Gokberklk/Event-Driven-Notification-System/internal/logger"
	"github.com/Gokberklk/Event-Driven-Notification-System/internal/notification"
	"github.com/Gokberklk/Event-Driven-Notification-System/internal/provider"
	"github.com/Gokberklk/Event-Driven-Notification-System/internal/queue"
	"github.com/Gokberklk/Event-Driven-Notification-System/internal/tracing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Store is the persistence contract the worker needs.
type Store interface {
	Claim(ctx context.Context, id uuid.UUID) (notification.Notification, bool, error)
	MarkSent(ctx context.Context, id uuid.UUID, providerID string) error
	MarkRetry(ctx context.Context, id uuid.UUID, reason string, next time.Time) error
	MarkFailed(ctx context.Context, id uuid.UUID, reason string) error
	Stale(ctx context.Context, olderThan time.Duration, limit int) ([]notification.Notification, error)
	ResetStuck(ctx context.Context, id uuid.UUID) error
}

type Queue interface {
	Enqueue(ctx context.Context, items []queue.Item, runAt time.Time) error
	Pop(ctx context.Context, channel string) (string, bool, error)
	PromoteDue(ctx context.Context, limit int) (int, error)
}

type Sender interface {
	Send(ctx context.Context, req provider.Request, idempotencyKey, correlationID string) (provider.Response, error)
}

type Limiter interface {
	Wait(ctx context.Context, channel string) error
}

type Metrics interface {
	Sent(ctx context.Context, channel string, latency time.Duration)
	Failed(ctx context.Context, channel string)
	Retried(ctx context.Context, channel string)
}

type Config struct {
	WorkersPerChannel int
	PollInterval      time.Duration
	PromoteInterval   time.Duration
	ReconcileInterval time.Duration
	StaleAfter        time.Duration
	BaseBackoff       time.Duration
	MaxBackoff        time.Duration
}

func DefaultConfig(workers int) Config {
	return Config{
		WorkersPerChannel: workers,
		PollInterval:      100 * time.Millisecond,
		PromoteInterval:   500 * time.Millisecond,
		ReconcileInterval: 30 * time.Second,
		StaleAfter:        60 * time.Second,
		BaseBackoff:       2 * time.Second,
		MaxBackoff:        5 * time.Minute,
	}
}

type Worker struct {
	cfg     Config
	store   Store
	queue   Queue
	sender  Sender
	limiter Limiter
	metrics Metrics
	events  events.Publisher
	log     *slog.Logger
}

func New(cfg Config, s Store, q Queue, snd Sender, l Limiter, m Metrics, log *slog.Logger) *Worker {
	return &Worker{cfg: cfg, store: s, queue: q, sender: snd, limiter: l, metrics: m, events: events.Nop{}, log: log}
}

// WithEvents enables publishing of status-change events.
func (w *Worker) WithEvents(p events.Publisher) *Worker {
	w.events = p
	return w
}

// Run starts consumers for every channel plus the promoter and reconciler
// loops, and blocks until ctx is cancelled and in-flight sends finish.
func (w *Worker) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, ch := range notification.Channels {
		for i := 0; i < w.cfg.WorkersPerChannel; i++ {
			wg.Add(1)
			go func(channel string) {
				defer wg.Done()
				w.consume(ctx, channel)
			}(string(ch))
		}
	}
	wg.Add(2)
	go func() { defer wg.Done(); w.every(ctx, w.cfg.PromoteInterval, w.promote) }()
	go func() { defer wg.Done(); w.every(ctx, w.cfg.ReconcileInterval, w.reconcile) }()
	wg.Wait()
}

func (w *Worker) every(ctx context.Context, d time.Duration, fn func(context.Context)) {
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		fn(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (w *Worker) consume(ctx context.Context, channel string) {
	for ctx.Err() == nil {
		id, ok, err := w.queue.Pop(ctx, channel)
		if err != nil || !ok {
			if err != nil && ctx.Err() == nil {
				w.log.Warn("queue pop failed", "channel", channel, "error", err)
			}
			sleep(ctx, w.cfg.PollInterval)
			continue
		}
		// In-flight work uses a detached context so a shutdown signal does
		// not abandon a notification halfway through.
		w.Process(context.WithoutCancel(ctx), id)
	}
}

// Process handles one queued notification ID end to end.
func (w *Worker) Process(ctx context.Context, rawID string) {
	id, err := uuid.Parse(rawID)
	if err != nil {
		w.log.Error("invalid queued id", "id", rawID)
		return
	}
	n, claimed, err := w.store.Claim(ctx, id)
	if err != nil {
		w.log.Error("claim failed", "id", id, "error", err)
		return
	}
	if !claimed {
		// Cancelled, already sent, or picked up by another worker.
		w.log.Debug("skip unclaimable notification", "id", id)
		return
	}

	// Continue the trace started by the API request that created this
	// notification, so one trace shows API -> queue -> worker -> provider.
	if n.TraceParent != nil {
		ctx = tracing.Extract(ctx, *n.TraceParent)
	}
	ctx, span := tracing.Tracer().Start(ctx, "notification.deliver",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("notification.id", n.ID.String()),
			attribute.String("notification.channel", string(n.Channel)),
			attribute.String("notification.priority", string(n.Priority)),
			attribute.Int("notification.attempt", n.Attempts),
		))
	defer span.End()

	cid := ""
	if n.CorrelationID != nil {
		cid = *n.CorrelationID
	}
	log := logger.FromContext(logger.WithCorrelationID(ctx, cid), w.log).
		With("notification_id", n.ID, "channel", n.Channel, "attempt", n.Attempts)
	w.events.Publish(ctx, notification.StatusEvent(n, notification.StatusProcessing, ""))

	waitCtx, waitSpan := tracing.Tracer().Start(ctx, "ratelimit.wait")
	err = w.limiter.Wait(waitCtx, string(n.Channel))
	waitSpan.End()
	if err != nil {
		span.SetAttributes(attribute.String("notification.outcome", "retry"))
		w.retry(ctx, log, n, "rate limiter unavailable: "+err.Error())
		return
	}

	start := time.Now()
	resp, err := w.sender.Send(ctx, provider.Request{
		To: n.Recipient, Channel: string(n.Channel), Content: n.Content,
	}, n.ID.String(), cid)
	latency := time.Since(start)

	if err == nil {
		span.SetAttributes(attribute.String("notification.outcome", "sent"),
			attribute.String("provider.message_id", resp.MessageID))
		if err := w.store.MarkSent(ctx, n.ID, resp.MessageID); err != nil {
			log.Error("mark sent failed", "error", err)
		}
		w.metrics.Sent(ctx, string(n.Channel), latency)
		w.events.Publish(ctx, notification.StatusEvent(n, notification.StatusSent, ""))
		log.Info("notification sent", "provider_message_id", resp.MessageID, "latency_ms", latency.Milliseconds())
		return
	}

	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	if !provider.IsRetryable(err) {
		span.SetAttributes(attribute.String("notification.outcome", "failed"))
		w.fail(ctx, log, n, err.Error())
		return
	}
	span.SetAttributes(attribute.String("notification.outcome", "retry"))
	w.retry(ctx, log, n, err.Error())
}

func (w *Worker) retry(ctx context.Context, log *slog.Logger, n notification.Notification, reason string) {
	if n.Attempts >= n.MaxAttempts {
		w.fail(ctx, log, n, "max attempts reached: "+reason)
		return
	}
	delay := Backoff(n.Attempts, w.cfg.BaseBackoff, w.cfg.MaxBackoff)
	next := time.Now().Add(delay)
	if err := w.store.MarkRetry(ctx, n.ID, reason, next); err != nil {
		log.Error("mark retry failed", "error", err)
		return
	}
	item := queue.Item{ID: n.ID.String(), Channel: string(n.Channel), Priority: string(n.Priority), Rank: n.Priority.Rank()}
	if err := w.queue.Enqueue(ctx, []queue.Item{item}, next); err != nil {
		log.Warn("re-enqueue failed, reconciler will pick it up", "error", err)
	}
	w.metrics.Retried(ctx, string(n.Channel))
	w.events.Publish(ctx, notification.StatusEvent(n, notification.StatusPending, reason))
	log.Warn("delivery failed, will retry", "reason", reason, "retry_in", delay.String())
}

func (w *Worker) fail(ctx context.Context, log *slog.Logger, n notification.Notification, reason string) {
	if err := w.store.MarkFailed(ctx, n.ID, reason); err != nil {
		log.Error("mark failed failed", "error", err)
	}
	w.metrics.Failed(ctx, string(n.Channel))
	w.events.Publish(ctx, notification.StatusEvent(n, notification.StatusFailed, reason))
	log.Error("delivery failed permanently", "reason", reason)
}

// Backoff returns base * 2^(attempt-1) with +/-20% jitter, capped at max.
func Backoff(attempt int, base, max time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := float64(base) * math.Pow(2, float64(attempt-1))
	if d > float64(max) {
		d = float64(max)
	}
	jitter := 0.8 + rand.Float64()*0.4
	return time.Duration(d * jitter)
}

func (w *Worker) promote(ctx context.Context) {
	if _, err := w.queue.PromoteDue(ctx, 500); err != nil && !errors.Is(err, context.Canceled) {
		w.log.Warn("promote delayed failed", "error", err)
	}
}

// reconcile re-enqueues notifications that PostgreSQL says are due but that
// may have been lost from Redis, and recovers ones stuck in processing after
// a worker crash. Claim() makes duplicate queue entries harmless.
func (w *Worker) reconcile(ctx context.Context) {
	stale, err := w.store.Stale(ctx, w.cfg.StaleAfter, 1000)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			w.log.Warn("reconcile query failed", "error", err)
		}
		return
	}
	if len(stale) == 0 {
		return
	}
	items := make([]queue.Item, 0, len(stale))
	for _, n := range stale {
		if n.Status == notification.StatusProcessing {
			if err := w.store.ResetStuck(ctx, n.ID); err != nil {
				continue
			}
		}
		items = append(items, queue.Item{ID: n.ID.String(), Channel: string(n.Channel), Priority: string(n.Priority), Rank: n.Priority.Rank()})
	}
	if err := w.queue.Enqueue(ctx, items, time.Now()); err != nil {
		w.log.Warn("reconcile enqueue failed", "error", err)
		return
	}
	w.log.Info("reconciled stale notifications", "count", len(items))
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
