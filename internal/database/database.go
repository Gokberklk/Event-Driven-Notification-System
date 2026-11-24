// Package database manages the PostgreSQL connection pool and schema migrations.
package database

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5" // pgx5:// driver
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Gokberklk/Event-Driven-Notification-System/migrations"
)

// startupTimeout bounds how long services wait for PostgreSQL on boot. In
// container environments DNS and the database can take a few seconds to
// become reachable even after the health check passes.
const startupTimeout = 60 * time.Second

// retry runs fn until it succeeds, ctx is done or startupTimeout elapses.
func retry(ctx context.Context, fn func() error) error {
	deadline := time.Now().Add(startupTimeout)
	delay := 500 * time.Millisecond
	for {
		err := fn()
		if err == nil || time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(delay):
		}
		if delay < 5*time.Second {
			delay *= 2
		}
	}
}

// Connect opens a pgx connection pool, retrying until PostgreSQL is reachable.
func Connect(ctx context.Context, url string) (*pgxpool.Pool, error) {
	var pool *pgxpool.Pool
	err := retry(ctx, func() error {
		var err error
		pool, err = connectOnce(ctx, url)
		return err
	})
	return pool, err
}

func connectOnce(ctx context.Context, url string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}

// Migrate applies all pending up-migrations embedded in the binary,
// retrying while the database is not yet reachable.
func Migrate(ctx context.Context, url string) error {
	return retry(ctx, func() error { return migrateOnce(url) })
}

func migrateOnce(url string) error {
	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return fmt.Errorf("load migrations: %w", err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, toPgx5URL(url))
	if err != nil {
		return fmt.Errorf("init migrate: %w", err)
	}
	defer m.Close()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}

func toPgx5URL(url string) string {
	for _, p := range []string{"postgres://", "postgresql://"} {
		if len(url) >= len(p) && url[:len(p)] == p {
			return "pgx5://" + url[len(p):]
		}
	}
	return url
}
