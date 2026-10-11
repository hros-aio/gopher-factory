# Validation Guide: Event Manager Kafka Kit

This guide describes how to validate the completed module. The public signatures are in [contracts/public-api.md](contracts/public-api.md), and record/state rules are in [data-model.md](data-model.md).

## Prerequisites

- Go 1.23 and `golangci-lint` compatible with the repository's Go version.
- Docker or another Testcontainers-supported container runtime for integration tests.
- The `event-manager` module implemented and added to root `go.work`.

## Unit and quality checks

From `event-manager/`:

```bash
go test -count=1 ./...
go test -race -count=1 ./...
go test -coverprofile=coverage.out ./...
golangci-lint run ./...
```

Expected: unit tests need no broker; no race or leak failure. The coverage gate reports at least 85% for pure logic and 70% for Kafka wrappers. Each production Go file has a matching test file. The README wiring example is under 30 Go lines.

## Broker integration

From `event-manager/`:

```bash
go test -tags=integration -race -count=1 ./test/integration/...
```

The suite starts a pinned Kafka KRaft container, creates only test topics, and cleans up the container. Expected cases:

1. Publishing a Node-compatible envelope receives broker acknowledgment; a separate consumer sees the original key and mapped fields.
2. A successful handler commits its offset. A handler error without DLQ handoff makes `Run` fail and leaves the record available to a new consumer instance in the same group.
3. A retryable error gets bounded attempts; a permanent error gets one business attempt. After acknowledged DLQ publication, the source offset commits and the DLQ record contains the original record and failure metadata.
4. A malformed JSON envelope never reaches a business handler; its raw bytes and decode error reach DLQ and only then can its source offset commit.
5. Two group members rebalance with cancellation-aware handlers without overlapping work on a transferred partition.
6. Cancellation stops fetches, drains completed work, leaves unfinished work uncommitted, releases the rebalance block, and returns within the shutdown bound. Repeated close is safe.

## Worker adoption check

Copy the README's `New` → middleware composition → `Register` → `Run` → `Close` example into a small worker test. It must compile within 30 Go lines of wiring. Swap the root interfaces to `eventtest` fakes and drive one success and one failure without starting Kafka.
