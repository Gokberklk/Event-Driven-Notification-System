package notification

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Gokberklk/Event-Driven-Notification-System/internal/queue"
)

type fakeStore struct {
	byKey    map[string]Notification
	inserted []Notification
}

func (f *fakeStore) InsertMany(_ context.Context, ns []Notification) ([]InsertResult, error) {
	out := make([]InsertResult, len(ns))
	for i, n := range ns {
		if n.IdempotencyKey != nil {
			if ex, ok := f.byKey[*n.IdempotencyKey]; ok {
				out[i] = InsertResult{Notification: ex}
				continue
			}
			f.byKey[*n.IdempotencyKey] = n
		}
		f.inserted = append(f.inserted, n)
		out[i] = InsertResult{Notification: n, Created: true}
	}
	return out, nil
}
func (f *fakeStore) Get(context.Context, uuid.UUID) (Notification, error) {
	return Notification{}, ErrNotFound
}
func (f *fakeStore) List(context.Context, ListFilter) (Page, error) { return Page{}, nil }
func (f *fakeStore) BatchSummary(context.Context, uuid.UUID) (map[Status]int, int, error) {
	return nil, 0, nil
}
func (f *fakeStore) Cancel(context.Context, uuid.UUID) (Notification, error) {
	return Notification{}, nil
}

type enqueueCall struct {
	items []queue.Item
	runAt time.Time
}

type fakeQueue struct{ calls []enqueueCall }

func (q *fakeQueue) Enqueue(_ context.Context, items []queue.Item, runAt time.Time) error {
	if len(items) > 0 {
		q.calls = append(q.calls, enqueueCall{items, runAt})
	}
	return nil
}

func newTestService() (*Service, *fakeStore, *fakeQueue) {
	st := &fakeStore{byKey: map[string]Notification{}}
	q := &fakeQueue{}
	return NewService(st, q, slog.New(slog.NewTextHandler(io.Discard, nil)), 5), st, q
}

func TestCreateIsIdempotent(t *testing.T) {
	svc, st, q := newTestService()
	req := CreateRequest{Recipient: "+905551234567", Channel: "sms", Content: "hi", IdempotencyKey: "abc"}

	first, created, err := svc.Create(context.Background(), req)
	if err != nil || !created {
		t.Fatalf("first create: created=%v err=%v", created, err)
	}
	second, created, err := svc.Create(context.Background(), req)
	if err != nil || created {
		t.Fatalf("second create: created=%v err=%v", created, err)
	}
	if first.ID != second.ID {
		t.Fatal("idempotent create must return the original notification")
	}
	if len(st.inserted) != 1 || len(q.calls) != 1 {
		t.Fatalf("expected 1 insert and 1 enqueue, got %d and %d", len(st.inserted), len(q.calls))
	}
}

func TestCreateScheduledGoesToDelayedQueue(t *testing.T) {
	svc, _, q := newTestService()
	at := time.Now().Add(time.Hour)
	n, _, err := svc.Create(context.Background(), CreateRequest{Recipient: "t", Channel: "push", Content: "x", ScheduledAt: &at})
	if err != nil {
		t.Fatal(err)
	}
	if n.Status != StatusScheduled {
		t.Fatalf("status = %s, want scheduled", n.Status)
	}
	if len(q.calls) != 1 || !q.calls[0].runAt.Equal(at) {
		t.Fatalf("expected one delayed enqueue at %v, got %+v", at, q.calls)
	}
}

func TestCreateBatchValidation(t *testing.T) {
	svc, st, _ := newTestService()

	_, err := svc.CreateBatch(context.Background(), make([]CreateRequest, MaxBatchSize+1))
	if _, ok := err.(*ValidationError); !ok {
		t.Fatalf("oversized batch: want ValidationError, got %v", err)
	}

	_, err = svc.CreateBatch(context.Background(), []CreateRequest{
		{Recipient: "a@b.com", Channel: "email", Content: "ok", IdempotencyKey: "k"},
		{Recipient: "bad", Channel: "sms", Content: "x"},
		{Recipient: "c@d.com", Channel: "email", Content: "ok", IdempotencyKey: "k"},
	})
	ve, ok := err.(*ValidationError)
	if !ok {
		t.Fatalf("want ValidationError, got %v", err)
	}
	if _, ok := ve.Fields["notifications[1].recipient"]; !ok {
		t.Errorf("missing item 1 recipient error: %v", ve.Fields)
	}
	if _, ok := ve.Fields["notifications[2].idempotencyKey"]; !ok {
		t.Errorf("missing duplicate key error: %v", ve.Fields)
	}
	if len(st.inserted) != 0 {
		t.Fatal("invalid batch must not insert anything")
	}
}

func TestCreateBatchSharesBatchID(t *testing.T) {
	svc, _, q := newTestService()
	reqs := []CreateRequest{
		{Recipient: "a@b.com", Channel: "email", Content: "1"},
		{Recipient: "+905551234567", Channel: "sms", Content: "2", Priority: "high"},
	}
	res, err := svc.CreateBatch(context.Background(), reqs)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range res.Items {
		if n.BatchID == nil || *n.BatchID != res.BatchID {
			t.Fatal("every item must carry the batch ID")
		}
	}
	if len(q.calls) != 1 || len(q.calls[0].items) != 2 {
		t.Fatalf("expected one enqueue call with 2 items, got %+v", q.calls)
	}
}
