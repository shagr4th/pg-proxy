COMMIT_SHA := $(shell git rev-parse HEAD)
default: run

test:
	@go clean -testcache
	@go test ./... -v

run:
	@go run main.go proxy.go translator_base.go translator_ingres.go

build:
	@go build -ldflags="-X main.commit=$(COMMIT_SHA)"

