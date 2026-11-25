package events

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

func TestFilterMatch(t *testing.T) {
	e := Event{NotificationID: "n1", BatchID: "b1"}
	cases := []struct {
		f    Filter
		want bool
	}{
		{Filter{}, true},
		{Filter{NotificationID: "n1"}, true},
		{Filter{NotificationID: "n2"}, false},
		{Filter{BatchID: "b1"}, true},
		{Filter{BatchID: "b2"}, false},
	}
	for _, c := range cases {
		if got := c.f.Match(e); got != c.want {
			t.Errorf("%+v.Match = %v, want %v", c.f, got, c.want)
		}
	}
}

func TestHubStreamsPublishedEventsWithFilter(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hub := NewHub(rdb, log)
	go hub.Run(ctx)
	srv := httptest.NewServer(hub)
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	batch := uuid.NewString()
	all := dial(t, wsURL)
	filtered := dial(t, wsURL+"?batchId="+batch)
	waitFor(t, func() bool { return hub.Clients() == 2 })
	// Give the Redis subscription a moment to become active.
	waitFor(t, func() bool { return mr.PubSubNumSub(RedisChannel)[RedisChannel] == 1 })

	pub := NewRedisPublisher(rdb, log)
	pub.Publish(ctx, Event{NotificationID: "other", Status: "sent"})
	pub.Publish(ctx, Event{NotificationID: "mine", BatchID: batch, Status: "sent"})

	if got := read(t, all); got.NotificationID != "other" || got.Type != "notification.status" {
		t.Fatalf("unfiltered client got %+v", got)
	}
	if got := read(t, all); got.NotificationID != "mine" {
		t.Fatalf("unfiltered client second event %+v", got)
	}
	if got := read(t, filtered); got.NotificationID != "mine" || got.BatchID != batch {
		t.Fatalf("filtered client got %+v", got)
	}

	_ = all.Close()
	waitFor(t, func() bool { return hub.Clients() == 1 })
}

func TestHubRejectsInvalidFilter(t *testing.T) {
	hub := NewHub(nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	rec := httptest.NewRecorder()
	hub.ServeHTTP(rec, httptest.NewRequest("GET", "/ws?notificationId=nope", nil))
	if rec.Code != 400 {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func dial(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func read(t *testing.T, c *websocket.Conn) Event {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, msg, err := c.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	var e Event
	if err := json.Unmarshal(msg, &e); err != nil {
		t.Fatal(err)
	}
	return e
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
