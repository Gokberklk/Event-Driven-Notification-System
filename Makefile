.PHONY: up down build test lint run-api run-worker

up:
	docker compose up --build

down:
	docker compose down -v

build:
	go build ./...

test:
	go test -race ./...

lint:
	go vet ./...

run-api:
	go run ./cmd/api

run-worker:
	go run ./cmd/worker
