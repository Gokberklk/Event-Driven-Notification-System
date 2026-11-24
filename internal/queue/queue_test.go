package queue

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestQueue(t *testing.T) (*Queue, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	return New(redis.NewClient(&redis.Options{Addr: mr.Addr()})), mr
}

func TestPriorityOrder(t *testing.T) {
	q, _ := newTestQueue(t)
	ctx := context.Background()
	now := time.Now()
	_ = q.Enqueue(ctx, []Item{{ID: "low", Channel: "sms", Rank: 2}}, now)
	_ = q.Enqueue(ctx, []Item{{ID: "normal", Channel: "sms", Rank: 1}}, now)
	_ = q.Enqueue(ctx, []Item{{ID: "high", Channel: "sms", Rank: 0}}, now)
	_ = q.Enqueue(ctx, []Item{{ID: "email", Channel: "email", Rank: 0}}, now)

	for _, want := range []string{"high", "normal", "low"} {
		got, ok, err := q.Pop(ctx, "sms")
		if err != nil || !ok || got != want {
			t.Fatalf("pop = %q ok=%v err=%v, want %q", got, ok, err, want)
		}
	}
	if _, ok, _ := q.Pop(ctx, "sms"); ok {
		t.Fatal("sms queue should be empty; channels must be isolated")
	}
}

func TestEnqueueIsDeduplicated(t *testing.T) {
	q, _ := newTestQueue(t)
	ctx := context.Background()
	it := []Item{{ID: "a", Channel: "push", Rank: 1}}
	_ = q.Enqueue(ctx, it, time.Now())
	_ = q.Enqueue(ctx, it, time.Now())
	depth, _, _ := q.Depth(ctx, []string{"push"})
	if depth["push"] != 1 {
		t.Fatalf("depth = %d, want 1", depth["push"])
	}
}

func TestDelayedPromotion(t *testing.T) {
	q, _ := newTestQueue(t)
	ctx := context.Background()
	_ = q.Enqueue(ctx, []Item{{ID: "later", Channel: "sms", Rank: 0}}, time.Now().Add(time.Hour))
	_ = q.Enqueue(ctx, []Item{{ID: "soon", Channel: "sms", Rank: 2}}, time.Now().Add(20*time.Millisecond))

	if _, ok, _ := q.Pop(ctx, "sms"); ok {
		t.Fatal("delayed items must not be ready yet")
	}
	time.Sleep(30 * time.Millisecond)
	n, err := q.PromoteDue(ctx, 100)
	if err != nil || n != 1 {
		t.Fatalf("promoted %d err=%v, want 1", n, err)
	}
	got, ok, _ := q.Pop(ctx, "sms")
	if !ok || got != "soon" {
		t.Fatalf("pop = %q, want soon", got)
	}
	_, delayed, _ := q.Depth(ctx, []string{"sms"})
	if delayed != 1 {
		t.Fatalf("delayed = %d, want 1", delayed)
	}
}

func TestDelayedEncoding(t *testing.T) {
	in := Item{ID: "123e4567-e89b-12d3-a456-426614174000", Channel: "email", Rank: 2}
	out, err := decodeDelayed(encodeDelayed(in))
	if err != nil || out.ID != in.ID || out.Channel != in.Channel || out.Rank != in.Rank {
		t.Fatalf("round trip failed: %+v %v", out, err)
	}
}
