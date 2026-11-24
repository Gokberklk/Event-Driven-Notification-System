package notification

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/Gokberklk/Event-Driven-Notification-System/internal/logger"
	"github.com/Gokberklk/Event-Driven-Notification-System/internal/queue"
)

// Enqueuer is the subset of the queue the service needs.
type Enqueuer interface {
	Enqueue(ctx context.Context, items []queue.Item, runAt time.Time) error
}

// Store is the persistence contract used by the service.
type Store interface {
	InsertMany(ctx context.Context, ns []Notification) ([]InsertResult, error)
	Get(ctx context.Context, id uuid.UUID) (Notification, error)
	List(ctx context.Context, f ListFilter) (Page, error)
	BatchSummary(ctx context.Context, batchID uuid.UUID) (map[Status]int, int, error)
	Cancel(ctx context.Context, id uuid.UUID) (Notification, error)
}

type Service struct {
	store       Store
	queue       Enqueuer
	log         *slog.Logger
	maxAttempts int
	now         func() time.Time
}

func NewService(store Store, q Enqueuer, log *slog.Logger, maxAttempts int) *Service {
	return &Service{store: store, queue: q, log: log, maxAttempts: maxAttempts, now: time.Now}
}

// BatchResult is returned from batch creation.
type BatchResult struct {
	BatchID uuid.UUID      `json:"batchId"`
	Count   int            `json:"count"`
	Items   []Notification `json:"items"`
}

// BatchStatus summarises a batch.
type BatchStatus struct {
	BatchID uuid.UUID      `json:"batchId"`
	Total   int            `json:"total"`
	Counts  map[Status]int `json:"counts"`
}

// Create validates and stores one notification, then enqueues it.
// created=false means the idempotency key matched an existing notification.
func (s *Service) Create(ctx context.Context, req CreateRequest) (Notification, bool, error) {
	req.Normalize()
	if err := req.Validate(s.now()); err != nil {
		return Notification{}, false, err
	}
	res, err := s.persistAndEnqueue(ctx, []CreateRequest{req}, nil)
	if err != nil {
		return Notification{}, false, err
	}
	return res[0].Notification, res[0].Created, nil
}

// CreateBatch validates all requests up front (all-or-nothing) and stores
// them under one batch ID.
func (s *Service) CreateBatch(ctx context.Context, reqs []CreateRequest) (BatchResult, error) {
	if len(reqs) == 0 || len(reqs) > MaxBatchSize {
		return BatchResult{}, &ValidationError{Fields: map[string]string{
			"notifications": "must contain between 1 and 1000 items",
		}}
	}
	errs := map[string]string{}
	seen := map[string]int{}
	for i := range reqs {
		reqs[i].Normalize()
		if err := reqs[i].Validate(s.now()); err != nil {
			for k, v := range err.(*ValidationError).Fields {
				errs[itemField(i, k)] = v
			}
		}
		if k := reqs[i].IdempotencyKey; k != "" {
			if j, dup := seen[k]; dup {
				errs[itemField(i, "idempotencyKey")] = "duplicates item " + strconv.Itoa(j)
			}
			seen[k] = i
		}
	}
	if len(errs) > 0 {
		return BatchResult{}, &ValidationError{Fields: errs}
	}

	batchID := uuid.New()
	res, err := s.persistAndEnqueue(ctx, reqs, &batchID)
	if err != nil {
		return BatchResult{}, err
	}
	out := BatchResult{BatchID: batchID, Count: len(res), Items: make([]Notification, len(res))}
	for i, r := range res {
		out.Items[i] = r.Notification
	}
	return out, nil
}

func (s *Service) persistAndEnqueue(ctx context.Context, reqs []CreateRequest, batchID *uuid.UUID) ([]InsertResult, error) {
	cid := logger.CorrelationID(ctx)
	ns := make([]Notification, len(reqs))
	for i, r := range reqs {
		n := Notification{
			ID:          uuid.New(),
			BatchID:     batchID,
			Recipient:   r.Recipient,
			Channel:     r.Channel,
			Content:     r.Content,
			Priority:    r.Priority,
			Status:      StatusPending,
			MaxAttempts: s.maxAttempts,
			ScheduledAt: r.ScheduledAt,
		}
		if r.IdempotencyKey != "" {
			k := r.IdempotencyKey
			n.IdempotencyKey = &k
		}
		if cid != "" {
			n.CorrelationID = &cid
		}
		if r.ScheduledAt != nil && r.ScheduledAt.After(s.now()) {
			n.Status = StatusScheduled
		}
		ns[i] = n
	}

	res, err := s.store.InsertMany(ctx, ns)
	if err != nil {
		return nil, err
	}

	// Enqueue only newly created rows. Immediate and scheduled items are
	// grouped by run time. If Redis is unavailable the rows stay pending in
	// PostgreSQL and the worker's reconciler re-enqueues them later.
	now := s.now()
	immediate := []queue.Item{}
	for _, r := range res {
		if !r.Created {
			continue
		}
		n := r.Notification
		it := queue.Item{ID: n.ID.String(), Channel: string(n.Channel), Priority: string(n.Priority), Rank: n.Priority.Rank()}
		if n.ScheduledAt != nil && n.ScheduledAt.After(now) {
			if err := s.queue.Enqueue(ctx, []queue.Item{it}, *n.ScheduledAt); err != nil {
				logger.FromContext(ctx, s.log).Warn("enqueue scheduled failed, reconciler will retry", "id", n.ID, "error", err)
			}
			continue
		}
		immediate = append(immediate, it)
	}
	if err := s.queue.Enqueue(ctx, immediate, now); err != nil {
		logger.FromContext(ctx, s.log).Warn("enqueue failed, reconciler will retry", "count", len(immediate), "error", err)
	}
	return res, nil
}

func (s *Service) Get(ctx context.Context, id uuid.UUID) (Notification, error) {
	return s.store.Get(ctx, id)
}

func (s *Service) GetBatch(ctx context.Context, id uuid.UUID) (BatchStatus, error) {
	counts, total, err := s.store.BatchSummary(ctx, id)
	if err != nil {
		return BatchStatus{}, err
	}
	if total == 0 {
		return BatchStatus{}, ErrNotFound
	}
	return BatchStatus{BatchID: id, Total: total, Counts: counts}, nil
}

func (s *Service) Cancel(ctx context.Context, id uuid.UUID) (Notification, error) {
	return s.store.Cancel(ctx, id)
}

func (s *Service) List(ctx context.Context, f ListFilter) (Page, error) {
	return s.store.List(ctx, f)
}

func itemField(i int, field string) string {
	return "notifications[" + strconv.Itoa(i) + "]." + field
}
