package metrics

import (
	"context"
	"net/http"

	"github.com/Gokberklk/Event-Driven-Notification-System/internal/httpx"
)

// DepthReader reports queue depth.
type DepthReader interface {
	Depth(ctx context.Context, channels []string) (map[string]int64, int64, error)
}

// Handler serves GET /metrics.
type Handler struct {
	Recorder *Recorder
	Queue    DepthReader
	Channels []string
}

type snapshot struct {
	QueueDepth map[string]int64        `json:"queueDepth"`
	Delayed    int64                   `json:"delayed"`
	Channels   map[string]ChannelStats `json:"channels"`
}

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	depth, delayed, err := h.Queue.Depth(r.Context(), h.Channels)
	if err != nil {
		httpx.Error(w, http.StatusServiceUnavailable, "queue unavailable", nil)
		return
	}
	stats, err := h.Recorder.Snapshot(r.Context(), h.Channels)
	if err != nil {
		httpx.Error(w, http.StatusServiceUnavailable, "metrics unavailable", nil)
		return
	}
	httpx.JSON(w, http.StatusOK, snapshot{QueueDepth: depth, Delayed: delayed, Channels: stats})
}
