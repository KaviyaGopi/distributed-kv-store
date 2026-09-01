.PHONY: build test lint compose-up compose-down

build:
	go build ./...

test:
	go test ./... -race -count=1

lint:
	go vet ./...

compose-up:
	docker compose -f deploy/docker-compose.yml up --build

compose-down:
	docker compose -f deploy/docker-compose.yml down -v
