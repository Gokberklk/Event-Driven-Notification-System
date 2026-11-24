// Package queue implements a Redis-backed priority queue with per-channel
// ready queues and a shared delayed set for scheduled sends and retries.
//
// Layout:
//
//	notifications:ready:{channel}  ZSET  member=id, score=rank*1e13 + enqueueMillis
//	notifications:delayed          ZSET  member=channel|priority|id, score=dueMillis
//
// The score encoding gives strict priority ordering (high < normal < low)
// with FIFO order inside the same priority.
package queue

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	rankWeight = 1e13
	delayedKey = "notifications:delayed"
)

// Item is a queued reference to a notification.
type Item struct {
	ID       string
	Channel  string
	Priority string
	Rank     int
}

type Queue struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *Queue { return &Queue{rdb: rdb} }

func ReadyKey(channel string) string { return "notifications:ready:" + channel }

func score(rank int, t time.Time) float64 {
	return float64(rank)*rankWeight + float64(t.UnixMilli())
}

// Enqueue adds items to their channel's ready queue (or to the delayed set
// when runAt is in the future). Re-enqueueing an existing ID is a no-op for
// ready items because ZADD NX keeps the original position.
func (q *Queue) Enqueue(ctx context.Context, items []Item, runAt time.Time) error {
	if len(items) == 0 {
		return nil
	}
	now := time.Now()
	pipe := q.rdb.Pipeline()
	for _, it := range items {
		if runAt.After(now) {
			pipe.ZAdd(ctx, delayedKey, redis.Z{Score: float64(runAt.UnixMilli()), Member: encodeDelayed(it)})
			continue
		}
		pipe.ZAddNX(ctx, ReadyKey(it.Channel), redis.Z{Score: score(it.Rank, now), Member: it.ID})
	}
	_, err := pipe.Exec(ctx)
	return err
}

// Pop removes and returns the highest-priority item for a channel, or
// ok=false when the queue is empty.
func (q *Queue) Pop(ctx context.Context, channel string) (string, bool, error) {
	res, err := q.rdb.ZPopMin(ctx, ReadyKey(channel), 1).Result()
	if err != nil {
		return "", false, err
	}
	if len(res) == 0 {
		return "", false, nil
	}
	return res[0].Member.(string), true, nil
}

// promoteScript atomically moves due members from the delayed set to their
// ready queues so that two workers never promote the same item twice.
var promoteScript = redis.NewScript(`
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, m in ipairs(due) do
  local sep1 = string.find(m, '|', 1, true)
  local sep2 = string.find(m, '|', sep1 + 1, true)
  local channel = string.sub(m, 1, sep1 - 1)
  local rank = tonumber(string.sub(m, sep1 + 1, sep2 - 1))
  local id = string.sub(m, sep2 + 1)
  redis.call('ZADD', 'notifications:ready:' .. channel, 'NX', rank * 1e13 + tonumber(ARGV[1]), id)
  redis.call('ZREM', KEYS[1], m)
end
return #due
`)

// PromoteDue moves up to limit due delayed items into ready queues.
func (q *Queue) PromoteDue(ctx context.Context, limit int) (int, error) {
	n, err := promoteScript.Run(ctx, q.rdb, []string{delayedKey}, time.Now().UnixMilli(), limit).Int()
	return n, err
}

// Depth returns the number of ready items per channel plus the delayed count.
func (q *Queue) Depth(ctx context.Context, channels []string) (map[string]int64, int64, error) {
	pipe := q.rdb.Pipeline()
	cmds := make(map[string]*redis.IntCmd, len(channels))
	for _, c := range channels {
		cmds[c] = pipe.ZCard(ctx, ReadyKey(c))
	}
	delayed := pipe.ZCard(ctx, delayedKey)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, 0, err
	}
	out := make(map[string]int64, len(channels))
	for c, cmd := range cmds {
		out[c] = cmd.Val()
	}
	return out, delayed.Val(), nil
}

func encodeDelayed(it Item) string {
	return fmt.Sprintf("%s|%d|%s", it.Channel, it.Rank, it.ID)
}

// decodeDelayed is used in tests to verify the encoding round-trips.
func decodeDelayed(m string) (Item, error) {
	parts := strings.SplitN(m, "|", 3)
	if len(parts) != 3 {
		return Item{}, fmt.Errorf("bad delayed member %q", m)
	}
	var rank int
	if _, err := fmt.Sscanf(parts[1], "%d", &rank); err != nil {
		return Item{}, err
	}
	return Item{Channel: parts[0], Rank: rank, ID: parts[2]}, nil
}
