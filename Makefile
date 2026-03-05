COMMIT_SHA := $(shell git rev-parse HEAD)
default: run

test:
	@go clean -testcache
# -p 1
	@go test ./... -v

run:
	@go run main.go proxy.go translator_base.go translator_ingres.go web.go

build:
	@go build -ldflags="-X main.commit=$(COMMIT_SHA)"

