// Package ratelimit implements a distributed per-channel rate limiter on
// Redis so that all worker processes share the same budget.
package ratelimit

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Fixed one-second window: INCR the counter for the current second and
// expire it shortly after. Atomic in a script so the check and increment
// cannot race between workers.
var allowScript = redis.NewScript(`
local n = redis.call('INCR', KEYS[1])
if n == 1 then redis.call('PEXPIRE', KEYS[1], 2000) end
if n > tonumber(ARGV[1]) then return 0 end
return 1
`)

type Limiter struct {
	rdb   *redis.Client
	limit int
	now   func() time.Time
}

func New(rdb *redis.Client, perSecond int) *Limiter {
	return &Limiter{rdb: rdb, limit: perSecond, now: time.Now}
}

// Allow consumes one token for the channel if available in the current second.
func (l *Limiter) Allow(ctx context.Context, channel string) (bool, error) {
	sec := l.now().Unix()
	key := "ratelimit:" + channel + ":" + strconv.FormatInt(sec, 10)
	ok, err := allowScript.Run(ctx, l.rdb, []string{key}, l.limit).Int()
	return ok == 1, err
}

// Wait blocks until a token is available or ctx is done.
func (l *Limiter) Wait(ctx context.Context, channel string) error {
	for {
		ok, err := l.Allow(ctx, channel)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		now := l.now()
		next := now.Truncate(time.Second).Add(time.Second)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(next.Sub(now)):
		}
	}
}
