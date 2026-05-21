COMMIT_SHA := $(shell git rev-parse HEAD)
default: run

test:
	@go clean -testcache
	@go test -p 1 ./... -v

run:
	@go run main.go proxy.go translator_base.go translator_ingres.go web.go

build:
	@go build -ldflags="-X main.commit=$(COMMIT_SHA)"

load-test:
	@go test -tags load -v -count=1 -timeout 120s ./ingres/ -run TestLoad

