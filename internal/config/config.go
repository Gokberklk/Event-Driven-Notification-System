// Package config loads application configuration from environment variables.
package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr        string
	DatabaseURL     string
	RedisAddr       string
	ProviderURL     string
	ProviderTimeout time.Duration
	RateLimitPerSec int
	WorkerCount     int
	MaxAttempts     int
	LogLevel        string
}

func Load() Config {
	return Config{
		HTTPAddr:        env("HTTP_ADDR", ":8080"),
		DatabaseURL:     env("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/notifications?sslmode=disable"),
		RedisAddr:       env("REDIS_ADDR", "localhost:6379"),
		ProviderURL:     env("PROVIDER_URL", "https://webhook.site/your-uuid"),
		ProviderTimeout: envDuration("PROVIDER_TIMEOUT", 5*time.Second),
		RateLimitPerSec: envInt("RATE_LIMIT_PER_SEC", 100),
		WorkerCount:     envInt("WORKER_COUNT", 10),
		MaxAttempts:     envInt("MAX_ATTEMPTS", 5),
		LogLevel:        env("LOG_LEVEL", "info"),
	}
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(key)); err == nil {
		return v
	}
	return def
}
