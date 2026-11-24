package worker

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/notification-system/notification-system/internal/notification"
	"github.com/notification-system/notification-system/internal/provider"
	"github.com/notification-system/notification-system/internal/queue"
)

type fakeStore struct {
	n         notification.Notification
	claimable bool
	final     string
	reason    string
	nextAt    time.Time
}

func (s *fakeStore) Claim(context.Context, uuid.UUID) (notification.Notification, bool, error) {
	if !s.claimable {
		return notification.Notification{}, false, nil
	}
	s.claimable = false
	s.n.Attempts++
	return s.n, true, nil
}
func (s *fakeStore) MarkSent(_ context.Context, _ uuid.UUID, _ string) error {
	s.final = "sent"
	return nil
}
func (s *fakeStore) MarkRetry(_ context.Context, _ uuid.UUID, reason string, next time.Time) error {
	s.final, s.reason, s.nextAt = "retry", reason, next
	return nil
}
func (s *fakeStore) MarkFailed(_ context.Context, _ uuid.UUID, reason string) error {
	s.final, s.reason = "failed", reason
	return nil
}
func (s *fakeStore) Stale(context.Context, time.Duration, int) ([]notification.Notification, error) {
	return nil, nil
}
func (s *fakeStore) ResetStuck(context.Context, uuid.UUID) error { return nil }

type fakeQueue struct{ delayed []queue.Item }

func (q *fakeQueue) Enqueue(_ context.Context, items []queue.Item, _ time.Time) error {
	q.delayed = append(q.delayed, items...)
	return nil
}
func (q *fakeQueue) Pop(context.Context, string) (string, bool, error) { return "", false, nil }
func (q *fakeQueue) PromoteDue(context.Context, int) (int, error)      { return 0, nil }

type fakeSender struct {
	err   error
	calls int
}

func (s *fakeSender) Send(context.Context, provider.Request, string, string) (provider.Response, error) {
	s.calls++
	return provider.Response{MessageID: "m-1"}, s.err
}

type noLimit struct{}

func (noLimit) Wait(context.Context, string) error { return nil }

type countMetrics struct{ sent, failed, retried int }

func (m *countMetrics) Sent(context.Context, string, time.Duration) { m.sent++ }
func (m *countMetrics) Failed(context.Context, string)              { m.failed++ }
func (m *countMetrics) Retried(context.Context, string)             { m.retried++ }

func setup(attempts, max int, sendErr error) (*Worker, *fakeStore, *fakeQueue, *fakeSender, *countMetrics) {
	st := &fakeStore{claimable: true, n: notification.Notification{
		ID: uuid.New(), Channel: "sms", Priority: "high", Recipient: "+905551234567",
		Content: "hi", Attempts: attempts, MaxAttempts: max,
	}}
	q, snd, m := &fakeQueue{}, &fakeSender{err: sendErr}, &countMetrics{}
	w := New(DefaultConfig(1), st, q, snd, noLimit{}, m, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return w, st, q, snd, m
}

func TestProcessSuccess(t *testing.T) {
	w, st, _, _, m := setup(0, 5, nil)
	w.Process(context.Background(), st.n.ID.String())
	if st.final != "sent" || m.sent != 1 {
		t.Fatalf("final=%s metrics=%+v", st.final, m)
	}
}

func TestProcessTransientFailureSchedulesRetry(t *testing.T) {
	w, st, q, _, m := setup(0, 5, &provider.Error{StatusCode: 503, Retryable: true})
	w.Process(context.Background(), st.n.ID.String())
	if st.final != "retry" || m.retried != 1 {
		t.Fatalf("final=%s metrics=%+v", st.final, m)
	}
	if len(q.delayed) != 1 || q.delayed[0].Rank != 0 {
		t.Fatalf("expected re-enqueue keeping high priority, got %+v", q.delayed)
	}
	if !st.nextAt.After(time.Now()) {
		t.Fatal("retry must be scheduled in the future")
	}
}

func TestProcessPermanentFailure(t *testing.T) {
	w, st, q, _, m := setup(0, 5, &provider.Error{StatusCode: 400, Retryable: false})
	w.Process(context.Background(), st.n.ID.String())
	if st.final != "failed" || m.failed != 1 || len(q.delayed) != 0 {
		t.Fatalf("final=%s metrics=%+v queued=%d", st.final, m, len(q.delayed))
	}
}

func TestProcessGivesUpAfterMaxAttempts(t *testing.T) {
	w, st, _, _, m := setup(4, 5, &provider.Error{StatusCode: 500, Retryable: true})
	w.Process(context.Background(), st.n.ID.String())
	if st.final != "failed" || m.failed != 1 {
		t.Fatalf("final=%s metrics=%+v", st.final, m)
	}
}

func TestProcessSkipsUnclaimable(t *testing.T) {
	w, st, _, snd, _ := setup(0, 5, nil)
	st.claimable = false // e.g. cancelled or already sent
	w.Process(context.Background(), st.n.ID.String())
	if snd.calls != 0 {
		t.Fatal("must not send a notification that could not be claimed")
	}
}

func TestBackoff(t *testing.T) {
	base, max := time.Second, 30*time.Second
	prev := time.Duration(0)
	for attempt := 1; attempt <= 4; attempt++ {
		d := Backoff(attempt, base, max)
		lo := time.Duration(float64(base) * float64(int(1)<<(attempt-1)) * 0.8)
		hi := time.Duration(float64(base) * float64(int(1)<<(attempt-1)) * 1.2)
		if d < lo || d > hi {
			t.Fatalf("attempt %d: %v not in [%v, %v]", attempt, d, lo, hi)
		}
		if d < prev/2 {
			t.Fatalf("backoff should grow: %v after %v", d, prev)
		}
		prev = d
	}
	if d := Backoff(20, base, max); d > time.Duration(float64(max)*1.2) {
		t.Fatalf("backoff not capped: %v", d)
	}
}
