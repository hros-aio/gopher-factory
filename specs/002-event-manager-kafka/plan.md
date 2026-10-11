# Implementation Plan: Event Manager Kafka Kit

**Branch**: `master` (feature directory `002-event-manager-kafka`; no branch hook configured) | **Date**: 2026-10-11 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/002-event-manager-kafka/spec.md` and the user's module, layout, dependency, consumer, and ADR constraints.

## Summary

Create an independent Go 1.23 `event-manager` module with a library-independent root API, a franz-go Kafka adapter, and in-memory fakes. The adapter publishes Node-compatible JSON using idempotent all-ISR-ack production. Its group subscriber uses manual commits and a blocked rebalance for bounded polls, processes each partition in order and partitions concurrently, and commits only after handler or DLQ success. Root middleware handles panic recovery, structured logging, classification, retries, DLQ routing, and optional caller-owned idempotency.

## Technical Context

**Language/Version**: Go 1.23.0 baseline. User-supplied `github.com/<org>/gopher-factory/event-manager` resolves to `github.com/hros-aio/gopher-factory/event-manager`, matching the existing `store-sql` module's organization; `<org>` is not a literal import path.

**Primary Dependencies**: `github.com/twmb/franz-go/pkg/kgo` through franz-go v1.18.1 in `kafka/` only. Standard library `context`, `encoding/json`, `crypto/rand`, `log/slog`, `crypto/tls`, and `sync`. No Redis, SQL, Fx, or sibling-kit dependency. [Version research](research.md) explains the Go floor.

**Storage**: Kafka broker records and consumer-group offsets only; idempotency persistence is supplied by the caller.

**Testing**: `testify` and `goleak` for unit tests; `testcontainers-go` core and Kafka KRaft module v0.38.0 for integration tests under `test/integration`. Integration tests use an explicit build tag and a pinned broker image. Every non-generated Go source file has a matching `_test.go`.

**Target Platform**: Go 1.23 worker services connecting to Kafka-compatible brokers.

**Project Type**: Independently versioned reusable library in the root Go workspace.

**Performance Goals**: No invented throughput or latency SLA. Bounded poll batches cap resident records and worker goroutines. One worker per partition in a polled batch allows different partitions to progress concurrently.

**Constraints**: At-least-once delivery; broker acknowledgment from all in-sync replicas; no auto commit; no overlapping partition handlers during cooperative rebalance; bounded shutdown; no global mutable state, environment reads, payload logging, or franz-go types in root signatures.

**Scale/Scope**: One `kafka.Client` instance supplies publishing and optional group consumption; one handler per registered topic; up to `MaxPollRecords` records and workers bounded by the partition count in a poll. Topic provisioning, transactions, schema registry, and outbox remain outside scope.

## Constitution Check

*GATE: Checked before research and rechecked after Phase 1 design.*

| Rule | Design evidence | Result |
|---|---|---|
| Library lifecycle | `New(ctx, cfg, ...Option)`, `Ping(ctx)`, `Close(ctx)`; no startup hooks or environment access | PASS |
| Independent modules | Dedicated `event-manager/go.mod`, root `go.work` adds `./event-manager`, no sibling imports | PASS after implementation |
| Dependency isolation | franz-go only in `kafka/`; root API and `eventtest/` use standard library | PASS |
| Consumer-owned contracts | Root `Publisher`, `Subscriber`, `Handler`, `Message`, `Envelope[T]`; no `kgo` type in public root API | PASS |
| Context and errors | Context first for I/O; wrapped typed errors remain discoverable with `errors.Is` | PASS |
| Safe logging | Injected `slog`, default `slog.Default()`; metadata only, no credentials or raw payloads | PASS |
| External schema management | No schema migration or topic administration API | PASS |
| Tests | Paired test files, table cases, `testify`, `goleak`, race, coverage, container integration | PASS after implementation |
| Stable API | Doc comments, README, module changelog, independent semantic tag | PASS after implementation |
| Reliable messaging | Manual contiguous commit frontier, DLQ acknowledgment, rebalance barrier, bounded drain | PASS |

**Post-design recheck:** The contracts, state model, and validation guide preserve all applicable gates. Go 1.23 compatibility requires the researched franz-go pin; raising it without checking the module's `go` directive would fail the language gate. No constitution exception is requested.

## Design

### API and envelope

The root files contain only standard-library types and consumer-facing contracts. `Envelope[T]` has idiomatic Go fields with the exact Node JSON names. `Message` holds topic, partition, offset, key, raw value, and a decoded `Envelope[json.RawMessage]` when valid. Malformed records retain raw bytes and bypass the business handler. `Publisher.Publish(ctx, topic, key, event Event)` accepts any `Envelope[T]` through a small root `Event` conversion interface; a non-generic method is necessary because Go interfaces cannot declare generic methods. Root normalization validates and fills metadata on a copy before the Kafka mapper serializes it. The caller's original event remains unchanged.

`WithCorrelationID(ctx, id)` and `CorrelationID(ctx)` provide a typed context key. Publishing uses an existing envelope value, then context value, then a generated UUID. Generated `eventId` and `occurredAt` use UUID v4 and RFC 3339 UTC. `tenantCode` is required as a JSON string, with empty string for global events. A global message key is `:<aggregate_id>`; topic helper output is `<domain>.<event>.v<n>`.

### Publisher and configuration

`kafka.New(ctx, Config, ...Option)` returns one `*Client` implementing both root `Publisher` and `Subscriber`. The client creates its publishing broker connection and verifies a bounded startup ping; it creates the group consumer when `Run` begins with frozen handler registrations. `Config.ConsumerEnabled` explicitly selects group consumption and requires `GroupID` during pre-network validation; publisher-only use leaves both unset. Settings cover brokers, client ID, optional TLS, optional SASL PLAIN/SCRAM-256/SCRAM-512, dial/publish/handler/shutdown/rebalance timeouts, a poll-processing budget, bounded poll size, and DLQ topic. Retry and backoff settings live in a root `RetryConfig`. Defaults are documented in [the public contract](contracts/public-api.md); validation rejects impossible timing combinations and secrets are never included in errors. TLS settings are cloned so a caller mutation cannot change a live client.

Publisher config explicitly selects `kgo.RequiredAcks(kgo.AllISRAcks())` and never disables idempotent writes. `Publish` uses `ProduceSync`, so return success follows the broker acknowledgment. A canceled or timed-out publish may have reached the broker; its error communicates an ambiguous outcome. Close transitions open → closing → closed, rejects new publishes, flushes with the caller's context or configured bound, closes the client, and shares one completion result across repeated calls.

### Subscriber concurrency and commits

Configure `kgo.DisableAutoCommit()`, `kgo.BlockRebalanceOnPoll()`, `kgo.ConsumerGroup(groupID)`, and the registered topics. Freeze registrations before `Run`; registration during a run returns a state error. Call `PollRecords(ctx, MaxPollRecords)` in one poll loop. For each nonempty poll, group records by topic-partition. Start at most one worker per partition present in that batch; each worker processes its partition's records sequentially, while different partitions proceed in parallel. No worker persists into the next poll. The poll loop waits for all workers, commits each partition through its last contiguous success or acknowledged DLQ record with a blocking commit, then calls `AllowRebalance()` exactly once before another poll or close.

Stop a partition worker at its first unresolved record, so no later record in that partition can be processed or committed. A handler error after retries, a failed DLQ publish, idempotency-store error, or missing DLQ for malformed input stops new fetching for the entire subscriber. Other active partition workers drain within the shutdown bound. Commit their completed frontiers, release the rebalance block, close group resources, and return a wrapped processing error. Caller restart triggers failed-record redelivery. A commit failure is reported and must not be treated as durable progress; duplicate handling remains possible.

Set a per-handler deadline and a per-poll processing budget below `RebalanceTimeout` with margin. Retries and remaining records are cut off at that budget; completed frontiers are committed and unfinished records redeliver after `Run` exits. On rebalance, the poll barrier prevents ownership transfer until the current batch settles and is committed. A cancellation-aware handler must stop when its context is canceled. A non-cooperative handler can continue after a timed-out release, as explicitly accepted in the spec; caller-owned idempotency or an atomic claim protects business effects in that case. The kit does not claim to terminate arbitrary caller code.

### Middleware and dead-letter records

`Middleware func(Handler) Handler` composes in the documented order `Logging(Idempotency(DeadLetter(Retry(Recover(handler)))))`. `Recover` converts panic to a wrapped processing error per attempt. `Retry` repeats only explicitly retryable errors, with capped exponential backoff and jitter from an injectable clock/random seam for deterministic tests. `DeadLetter` handles permanent, unclassified, and exhausted failures. Its payload includes the original coordinates, key, raw value, decoded envelope if valid, error class/message, attempt count, and failure time. Messages are fixed safe summaries (`handler failed` or `invalid event envelope`) so handler error strings containing credentials are not copied to logs or DLQ metadata; the `invalid_event` class identifies decode failure. It publishes through the root `Publisher`, so acknowledgment rules match ordinary publishing. Successful DLQ publication returns a terminal success to the subscriber; failed publication returns an error. Decode failures enter the same DLQ mechanism before middleware, with no business-handler call.

`IdempotencyStore` exposes completion lookup and completion mark by event ID. The middleware skips confirmed completions; it marks only after the inner chain succeeds, including a terminal DLQ handoff. A store error stops processing without commit. Check-then-mark is not atomic with business work or offset commits; callers needing stronger cross-worker exclusion supply their own atomic claim or transaction.

### Shutdown and health

Cancellation stops polling and starting work. A separate grace context derived with `context.WithoutCancel` and bounded by `ShutdownTimeout` lets in-flight cooperative handlers finish and permits final commits even though the run context is canceled. The drain reserves up to 250 ms to cancel and settle cooperative handlers before releasing the rebalance block. An expired poll budget gets the same 250 ms settling allowance; final commits may use up to 2 s of broker overhead inside the 10 s rebalance margin. After final commits, always release the rebalance barrier and use a close path that allows rebalance; never call a close while the poll barrier is held. Normal cancellation returns nil after clean drain; incomplete work yields an identifiable timeout error and remains uncommitted. Since franz-go close is not context-aware, one closer goroutine may finish asynchronously after a deadline; repeated `Close(ctx)` calls observe the same completion. Tests release deliberately blocked handlers before asserting zero kit-owned goroutine leaks.

`Ping(ctx)` checks current broker reachability through franz-go and reports a closed client distinctly. It does not claim topic existence or consumer-group health.

## Project Structure

### Documentation (this feature)

```text
specs/002-event-manager-kafka/
├── spec.md
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── adr-001-franz-go.md
├── contracts/public-api.md
└── checklists/requirements.md
```

### Source Code (repository root)

```text
go.work                         # add ./event-manager
event-manager/
├── go.mod                       # github.com/hros-aio/gopher-factory/event-manager
├── README.md
├── CHANGELOG.md
├── envelope.go                  ├── envelope_test.go
├── publisher.go                 ├── publisher_test.go
├── subscriber.go                ├── subscriber_test.go
├── message.go                   ├── message_test.go
├── errors.go                    ├── errors_test.go
├── middleware.go                ├── middleware_test.go
├── naming.go                    ├── naming_test.go
├── kafka/
│   ├── config.go                ├── config_test.go
│   ├── publisher.go             ├── publisher_test.go
│   ├── subscriber.go            ├── subscriber_test.go
│   └── mapper.go                └── mapper_test.go
├── eventtest/
│   ├── fake_publisher.go        ├── fake_publisher_test.go
│   └── fake_subscriber.go       └── fake_subscriber_test.go
└── test/integration/
    └── kafka_test.go            # real Kafka KRaft via Testcontainers
```

**Structure Decision:** One independent module; root contracts and middleware have no Kafka dependency, `kafka/` owns transport details, and `eventtest/` supports broker-free consumer tests. The matched `_test.go` file rule applies to each production `.go` file. Integration tests are test-only files.

## Complexity Tracking

No constitution violations require an exception. The poll barrier, partition workers, and explicit commit frontier are necessary to satisfy the user-requested manual commits, parallel partitions, and rebalance behavior.
