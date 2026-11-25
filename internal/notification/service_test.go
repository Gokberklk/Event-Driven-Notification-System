package notification

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Gokberklk/Event-Driven-Notification-System/internal/queue"
	"github.com/Gokberklk/Event-Driven-Notification-System/internal/template"
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
	return NewService(st, q, fakeTemplates{}, slog.New(slog.NewTextHandler(io.Discard, nil)), 5), st, q
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

var welcomeTemplateID = uuid.MustParse("11111111-1111-1111-1111-111111111111")

type fakeTemplates struct{}

func (fakeTemplates) Get(_ context.Context, id uuid.UUID) (template.Template, error) {
	if id != welcomeTemplateID {
		return template.Template{}, template.ErrNotFound
	}
	return template.Template{ID: id, Name: "welcome", Channel: "sms", Body: "Hi {{name}}, order {{orderId}} shipped"}, nil
}

func TestCreateFromTemplate(t *testing.T) {
	svc, st, _ := newTestService()
	id := welcomeTemplateID
	n, _, err := svc.Create(context.Background(), CreateRequest{
		Recipient: "+905551234567", TemplateID: &id,
		Variables: map[string]string{"name": "Ayşe", "orderId": "A-42"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if n.Content != "Hi Ayşe, order A-42 shipped" || n.Channel != ChannelSMS {
		t.Fatalf("content=%q channel=%q", n.Content, n.Channel)
	}
	if st.inserted[0].TemplateID == nil || *st.inserted[0].TemplateID != id {
		t.Fatal("template id must be stored on the notification")
	}
}

func TestCreateFromTemplateErrors(t *testing.T) {
	id, unknown := welcomeTemplateID, uuid.New()
	tests := []struct {
		name  string
		req   CreateRequest
		field string
	}{
		{"missing variable", CreateRequest{Recipient: "+905551234567", TemplateID: &id, Variables: map[string]string{"name": "x"}}, "variables"},
		{"unknown template", CreateRequest{Recipient: "+905551234567", TemplateID: &unknown}, "templateId"},
		{"channel mismatch", CreateRequest{Recipient: "a@b.com", Channel: "email", TemplateID: &id, Variables: map[string]string{"name": "x", "orderId": "y"}}, "channel"},
		{"content and template", CreateRequest{Recipient: "+905551234567", Content: "hi", TemplateID: &id}, "content"},
		{"variables without template", CreateRequest{Recipient: "+905551234567", Channel: "sms", Content: "hi", Variables: map[string]string{"a": "b"}}, "variables"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, _, _ := newTestService()
			_, _, err := svc.Create(context.Background(), tt.req)
			ve, ok := err.(*ValidationError)
			if !ok {
				t.Fatalf("want ValidationError, got %v", err)
			}
			if _, ok := ve.Fields[tt.field]; !ok {
				t.Fatalf("want error on %q, got %v", tt.field, ve.Fields)
			}
		})
	}
}

func TestBatchFromTemplateRendersEachItem(t *testing.T) {
	svc, _, _ := newTestService()
	id := welcomeTemplateID
	res, err := svc.CreateBatch(context.Background(), []CreateRequest{
		{Recipient: "+905551234567", TemplateID: &id, Variables: map[string]string{"name": "A", "orderId": "1"}},
		{Recipient: "+905551234568", TemplateID: &id, Variables: map[string]string{"name": "B", "orderId": "2"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Items[0].Content != "Hi A, order 1 shipped" || res.Items[1].Content != "Hi B, order 2 shipped" {
		t.Fatalf("got %q / %q", res.Items[0].Content, res.Items[1].Content)
	}
}
