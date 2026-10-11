.PHONY: lint test test-race test-cover test-integration

export GOCACHE := $(CURDIR)/.cache/go-build

lint:
	cd store-sql && golangci-lint run ./...

test:
	cd store-sql && go test ./...

test-race:
	cd store-sql && go test -race -count=1 ./...

test-cover:
	cd store-sql && go test -count=1 -covermode=atomic -coverprofile=coverage.out ./...
	python3 scripts/check_coverage.py store-sql/coverage.out
	cd store-sql && go tool cover -func=coverage.out

test-integration:
	cd store-sql && go test -tags=integration -race -count=1 ./test/integration
