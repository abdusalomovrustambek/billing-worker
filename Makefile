.PHONY: build test lint docker

build:
	go build -o bin/billing-worker ./cmd/billing-worker

test:
	go test -race ./...

lint:
	go vet ./...
	golangci-lint run

docker:
	docker build -t billing-worker .
