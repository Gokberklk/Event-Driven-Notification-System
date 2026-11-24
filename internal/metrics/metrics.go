// Package metrics records delivery metrics in Redis so that every worker
// contributes and the API process can serve a single real-time view.
package metrics

import (
	"context"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const latencySamples = 1000

type Recorder struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *Recorder { return &Recorder{rdb: rdb} }

func counterKey(channel string) string { return "metrics:" + channel }
func latencyKey(channel string) string { return "metrics:" + channel + ":latency" }

func (r *Recorder) incr(ctx context.Context, channel, field string) {
	r.rdb.HIncrBy(ctx, counterKey(channel), field, 1)
}

// Sent records a successful delivery and its provider latency.
func (r *Recorder) Sent(ctx context.Context, channel string, latency time.Duration) {
	pipe := r.rdb.Pipeline()
	pipe.HIncrBy(ctx, counterKey(channel), "sent", 1)
	pipe.LPush(ctx, latencyKey(channel), float64(latency.Microseconds())/1000)
	pipe.LTrim(ctx, latencyKey(channel), 0, latencySamples-1)
	_, _ = pipe.Exec(ctx)
}

func (r *Recorder) Failed(ctx context.Context, channel string)  { r.incr(ctx, channel, "failed") }
func (r *Recorder) Retried(ctx context.Context, channel string) { r.incr(ctx, channel, "retried") }

type ChannelStats struct {
	Sent         int64   `json:"sent"`
	Failed       int64   `json:"failed"`
	Retried      int64   `json:"retried"`
	SuccessRate  float64 `json:"successRate"`
	AvgLatencyMs float64 `json:"avgLatencyMs"`
	P95LatencyMs float64 `json:"p95LatencyMs"`
}

// Snapshot reads current counters and latency percentiles for each channel.
func (r *Recorder) Snapshot(ctx context.Context, channels []string) (map[string]ChannelStats, error) {
	out := make(map[string]ChannelStats, len(channels))
	for _, c := range channels {
		h, err := r.rdb.HGetAll(ctx, counterKey(c)).Result()
		if err != nil {
			return nil, err
		}
		lat, err := r.rdb.LRange(ctx, latencyKey(c), 0, -1).Result()
		if err != nil {
			return nil, err
		}
		s := ChannelStats{Sent: atoi(h["sent"]), Failed: atoi(h["failed"]), Retried: atoi(h["retried"])}
		if done := s.Sent + s.Failed; done > 0 {
			s.SuccessRate = float64(s.Sent) / float64(done)
		}
		s.AvgLatencyMs, s.P95LatencyMs = latencyStats(lat)
		out[c] = s
	}
	return out, nil
}

func latencyStats(raw []string) (avg, p95 float64) {
	if len(raw) == 0 {
		return 0, 0
	}
	vals := make([]float64, 0, len(raw))
	var sum float64
	for _, v := range raw {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			continue
		}
		vals = append(vals, f)
		sum += f
	}
	if len(vals) == 0 {
		return 0, 0
	}
	sort.Float64s(vals)
	idx := int(float64(len(vals))*0.95+0.5) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(vals) {
		idx = len(vals) - 1
	}
	return sum / float64(len(vals)), vals[idx]
}

func atoi(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}
