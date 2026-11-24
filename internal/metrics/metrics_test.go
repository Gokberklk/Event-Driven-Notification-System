package metrics

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestSnapshot(t *testing.T) {
	mr := miniredis.RunT(t)
	r := New(redis.NewClient(&redis.Options{Addr: mr.Addr()}))
	ctx := context.Background()
	for i := 1; i <= 20; i++ {
		r.Sent(ctx, "sms", time.Duration(i)*time.Millisecond)
	}
	r.Failed(ctx, "sms")
	r.Retried(ctx, "sms")

	s, err := r.Snapshot(ctx, []string{"sms", "email"})
	if err != nil {
		t.Fatal(err)
	}
	sms := s["sms"]
	if sms.Sent != 20 || sms.Failed != 1 || sms.Retried != 1 {
		t.Fatalf("counters = %+v", sms)
	}
	if got := sms.SuccessRate; got < 0.95 || got > 0.96 {
		t.Fatalf("success rate = %v, want 20/21", got)
	}
	if sms.AvgLatencyMs != 10.5 || sms.P95LatencyMs != 19 {
		t.Fatalf("latency avg=%v p95=%v", sms.AvgLatencyMs, sms.P95LatencyMs)
	}
	if s["email"].Sent != 0 {
		t.Fatal("email should be empty")
	}
}
