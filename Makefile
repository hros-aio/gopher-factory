.PHONY: lint test test-race test-cover test-integration event-manager-lint event-manager-test event-manager-test-race event-manager-test-cover event-manager-test-integration

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

event-manager-lint:
	cd event-manager && golangci-lint run ./...

event-manager-test:
	cd event-manager && go test -count=1 ./...

event-manager-test-race:
	cd event-manager && go test -race -count=1 ./...

event-manager-test-cover:
	cd event-manager && go test -count=1 -covermode=atomic -coverprofile=coverage.out ./...
	python3 scripts/check_coverage.py event-manager/coverage.out

event-manager-test-integration:
	cd event-manager && go test -tags=integration -race -count=1 ./test/integration/...
