package events

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"

	"github.com/Gokberklk/Event-Driven-Notification-System/internal/httpx"
)

const (
	sendBuffer   = 256
	writeTimeout = 10 * time.Second
	pingInterval = 30 * time.Second
	pongTimeout  = 60 * time.Second
)

// Filter selects which events a client receives. Empty fields match all.
type Filter struct {
	NotificationID string
	BatchID        string
}

func (f Filter) Match(e Event) bool {
	if f.NotificationID != "" && f.NotificationID != e.NotificationID {
		return false
	}
	if f.BatchID != "" && f.BatchID != e.BatchID {
		return false
	}
	return true
}

type client struct {
	filter Filter
	send   chan []byte
}

// Hub fans events from Redis out to WebSocket clients.
type Hub struct {
	rdb      *redis.Client
	log      *slog.Logger
	upgrader websocket.Upgrader

	mu      sync.RWMutex
	clients map[*client]struct{}
}

func NewHub(rdb *redis.Client, log *slog.Logger) *Hub {
	return &Hub{
		rdb: rdb,
		log: log,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			// The API is consumed by other services and tools, not by a
			// cookie-authenticated browser app, so any origin is allowed.
			CheckOrigin: func(*http.Request) bool { return true },
		},
		clients: map[*client]struct{}{},
	}
}

// Run subscribes to Redis and broadcasts until ctx is cancelled.
func (h *Hub) Run(ctx context.Context) {
	sub := h.rdb.Subscribe(ctx, RedisChannel)
	defer sub.Close()
	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			h.Broadcast([]byte(msg.Payload))
		}
	}
}

// Broadcast delivers a raw event to every matching client. Slow clients
// whose buffer is full miss the event instead of blocking everyone else.
func (h *Hub) Broadcast(payload []byte) {
	var e Event
	if err := json.Unmarshal(payload, &e); err != nil {
		h.log.Warn("invalid status event", "error", err)
		return
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		if !c.filter.Match(e) {
			continue
		}
		select {
		case c.send <- payload:
		default:
		}
	}
}

// Clients returns the number of connected clients.
func (h *Hub) Clients() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// ServeHTTP upgrades to WebSocket. Optional query params notificationId and
// batchId restrict which events are streamed.
func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f := Filter{NotificationID: r.URL.Query().Get("notificationId"), BatchID: r.URL.Query().Get("batchId")}
	for k, v := range map[string]string{"notificationId": f.NotificationID, "batchId": f.BatchID} {
		if v != "" {
			if _, err := uuid.Parse(v); err != nil {
				httpx.Error(w, http.StatusBadRequest, k+" must be a UUID", nil)
				return
			}
		}
	}
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return // Upgrade already wrote the error response.
	}
	c := &client{filter: f, send: make(chan []byte, sendBuffer)}
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()

	done := make(chan struct{})
	go h.readLoop(conn, done)
	h.writeLoop(conn, c, done)

	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
	_ = conn.Close()
}

// readLoop discards client messages and detects disconnects via pongs.
func (h *Hub) readLoop(conn *websocket.Conn, done chan struct{}) {
	defer close(done)
	conn.SetReadLimit(512)
	_ = conn.SetReadDeadline(time.Now().Add(pongTimeout))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(pongTimeout))
	})
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}

func (h *Hub) writeLoop(conn *websocket.Conn, c *client, done chan struct{}) {
	ping := time.NewTicker(pingInterval)
	defer ping.Stop()
	for {
		select {
		case <-done:
			return
		case msg := <-c.send:
			_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ping.C:
			_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
