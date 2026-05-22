COMMIT_SHA := $(shell git rev-parse HEAD)
default: run

test:
	@go clean -testcache
	@go test -p 1 ./... -v

run:
	@go run .

build:
	@go build -ldflags="-X main.commit=$(COMMIT_SHA)"
