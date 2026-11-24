TEST_DB ?= postgres://postgres:postgres@localhost:5432/notifications?sslmode=disable

.PHONY: up down build test test-integration lint run-api run-worker

up: ## Start the whole stack
	docker compose up --build

down: ## Stop and remove containers and volumes
	docker compose down -v

build:
	go build ./...

test: ## Unit tests (no external dependencies)
	go test -race ./...

test-integration: ## Unit + PostgreSQL integration tests
	docker compose up -d --wait postgres
	TEST_DATABASE_URL="$(TEST_DB)" go test -race -count=1 ./...

lint:
	gofmt -l . && go vet ./...

run-api:
	go run ./cmd/api

run-worker:
	go run ./cmd/worker
