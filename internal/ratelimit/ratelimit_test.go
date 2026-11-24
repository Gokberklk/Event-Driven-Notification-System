package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestAllowPerChannelPerSecond(t *testing.T) {
	mr := miniredis.RunT(t)
	l := New(redis.NewClient(&redis.Options{Addr: mr.Addr()}), 100)
	fixed := time.Unix(1_700_000_000, 0)
	l.now = func() time.Time { return fixed }
	ctx := context.Background()

	for i := 0; i < 100; i++ {
		if ok, err := l.Allow(ctx, "sms"); !ok || err != nil {
			t.Fatalf("request %d denied: %v", i, err)
		}
	}
	if ok, _ := l.Allow(ctx, "sms"); ok {
		t.Fatal("101st request in the same second must be denied")
	}
	if ok, _ := l.Allow(ctx, "email"); !ok {
		t.Fatal("channels must have independent budgets")
	}

	fixed = fixed.Add(time.Second)
	if ok, _ := l.Allow(ctx, "sms"); !ok {
		t.Fatal("budget must reset in the next second")
	}
}
