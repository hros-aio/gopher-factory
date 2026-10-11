# Tasks: Event Manager Kafka Kit

**Input**: Design documents from `specs/002-event-manager-kafka/`

**Prerequisites**: [plan.md](plan.md), [spec.md](spec.md), [research.md](research.md), [data-model.md](data-model.md), [public API contract](contracts/public-api.md), and [quickstart.md](quickstart.md)

**Tests**: Required by the feature spec and constitution. Write each story's tests before its implementation; unit tests must run without Kafka, and broker integration tests use Testcontainers.

**Organization**: Tasks are grouped by the four spec user stories. Paths are relative to the repository root. A `[P]` marker means the task can run alongside other marked tasks in the same stage because it edits separate files and has no unfinished dependency.

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Establish the independent Go 1.23 module and its quality commands.

- [X] T001 Create `event-manager/go.mod` with module `github.com/hros-aio/gopher-factory/event-manager`, `go 1.23.0`, franz-go v1.18.1, testify, goleak, and Testcontainers Go core/Kafka module v0.38.0; verify the resolved dependency graph keeps the Go 1.23 floor.
- [X] T002 Add `./event-manager` to `go.work` after T001 without removing `./store-sql`.
- [X] T003 [P] Add event-manager lint, unit, race, coverage, and integration targets to `Makefile` while preserving the existing store-sql targets.
- [X] T004 [P] Add `.github/workflows/event-manager.yml` with Go 1.23 build, lint, race, coverage, and container integration jobs scoped to `event-manager/`.
- [X] T005 [P] Extend `scripts/check_coverage.py` to distinguish module-relative paths so event-manager pure-logic files enforce 85% and Kafka wrappers enforce 70% without changing store-sql's current coverage rules.

**Checkpoint**: The workspace recognizes the new module and has scoped quality commands.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Define broker-independent event and lifecycle contracts plus validated connection construction before any story implementation.

- [X] T006 [P] Write `event-manager/envelope_test.go` for Node JSON field names, absent/null optional strings, UUID v4 generation, positive `eventVersion`, required `eventType`/`producer`/`payload`, required `tenantCode` JSON field with empty string allowed for global events, RFC 3339 `occurredAt`, `Event.ToRaw`, and metadata preservation.
- [X] T007 [P] Write `event-manager/errors_test.go` for stable error identities and `errors.Is` preservation through wrapped errors.
- [X] T008 [P] Write `event-manager/message_test.go` for raw key/value copying, decoded envelope access, and absent envelope on malformed input.
- [X] T009 [P] Write `event-manager/publisher_test.go` and `event-manager/subscriber_test.go` as compile-time contract tests for root `Publisher`, `Subscriber`, `Handler`, and `Middleware` without importing franz-go.
- [X] T010 [P] Write `event-manager/kafka/config_test.go` for field-specific pre-network validation, TLS clone, SASL PLAIN/SCRAM-256/SCRAM-512, broker/client ID requirements, publisher-only optional group ID, required group ID before `New` when `ConsumerEnabled`, DLQ/source-topic separation, and the contract defaults of 5 s dial, 10 s publish/handler, 45 s poll budget, 60 s rebalance, 30 s shutdown, and 32 records.
- [X] T011 Implement `event-manager/envelope.go` with generic `Envelope[T]`, the root `Event` conversion interface, typed correlation-context helpers, copy-on-prepare normalization, and Node-compatible JSON tags; make T006 pass.
- [X] T012 Implement `event-manager/errors.go` with wrapped sentinel errors and classification-ready error structure; make T007 pass.
- [X] T013 Implement `event-manager/message.go` with the broker-independent `Message` type and immutable-copy helpers; make T008 pass after T011.
- [X] T014 Implement root interfaces in `event-manager/publisher.go` and `event-manager/subscriber.go`; make T009 pass after T011–T013.
- [X] T015 Implement `event-manager/kafka/config.go` with `Config.Validate()`, explicit defaults, TLS/SASL options, `New(ctx, cfg, ...Option)`, startup broker ping, injected `slog`, and no environment reads; make T010 pass.
- [X] T016 Compile and run broker-free foundational tests under `event-manager/` with `go test -count=1 ./...`; resolve any Go 1.23 or root API dependency leak in `event-manager/go.mod` and the foundational Go files.

**Checkpoint**: Root contracts compile without franz-go imports, and invalid settings fail before network I/O.

---

## Phase 3: User Story 1 — Publish interoperable events (Priority: P1) 🎯 MVP

**Goal**: Publish Node-compatible envelopes with all-ISR acknowledgment, idempotent production, health checks, and bounded flush on close.

**Independent Test**: Publish to a real Kafka container, read with a separate consumer, compare Node fixtures, and exercise error/close cases without a broker in unit tests.

### Tests for User Story 1

- [X] T017 [P] [US1] Write `event-manager/naming_test.go` for `<domain>.<event>.v<n>` with nonempty components/positive version and `<tenant_id>:<aggregate_id>` with required aggregate ID, including global key `:<aggregate_id>`.
- [X] T018 [P] [US1] Write `event-manager/kafka/mapper_test.go` with Node JSON fixtures covering required `eventId`, `eventType`, positive integer `eventVersion`, `tenantCode`, `occurredAt`, `producer`, `payload`, and optional nullable `correlationId`, `causationId`, `traceId`; verify exact JSON names and invalid-envelope errors.
- [ ] T019 [P] [US1] Write `event-manager/kafka/publisher_test.go` using a broker seam to assert all-ISR acks, idempotent producer options, context cancellation, broker/encode error wrapping, startup/live/closed `Ping`, rejection after close, pending flush, repeated close, and bounded timeout.

### Implementation for User Story 1

- [X] T020 [US1] Implement topic and key builders/validators in `event-manager/naming.go`; make T017 pass.
- [X] T021 [US1] Implement Node envelope encode/decode mapping and validation in `event-manager/kafka/mapper.go`; make T018 pass after T011 and T020.
- [X] T022 [US1] Implement `Publish`, all-ISR/idempotent producer setup, `Ping`, and bounded repeat-safe flush/close in `event-manager/kafka/publisher.go`; make T019 pass after T015 and T021.
- [X] T023 [US1] Add the broker-acknowledged publish and Node fixture round-trip cases to `event-manager/test/integration/kafka_test.go` using a pinned Kafka KRaft Testcontainer, with test topic setup and container cleanup confined to the test.
- [ ] T024 [US1] Run the US1 unit and integration cases in `event-manager/`; verify that a successful `Publish` has a readable record and a failed/ambiguous publish never reports success.

**Checkpoint**: A publisher-only worker can construct, publish, ping, and close without a consumer group.

---

## Phase 4: User Story 2 — Process group messages without loss (Priority: P1)

**Goal**: Consume by group with sequential work per partition, parallel work across partitions, manual contiguous commits, rebalance safety, and bounded shutdown.

**Independent Test**: Seed records independently, run two group members, and inspect offsets after success, failure, rebalance, cancellation, and restart.

### Tests for User Story 2

- [ ] T025 [US2] Write `event-manager/kafka/subscriber_test.go` tests for pre-`Run` registration, duplicate/late registration errors, single-use `Run`, `kgo.DisableAutoCommit`, `kgo.BlockRebalanceOnPoll`, one worker per partition, sequential partition order, parallel partitions, and no commit before successful handling.
- [ ] T026 [US2] Extend `event-manager/kafka/subscriber_test.go` with first-failure partition stop, other-partition drain, highest contiguous commit frontier, failed commit reporting, wrapped `Run` error/redelivery on a new client, release of every blocked poll, rebalance non-overlap for cancellation-aware handlers, bounded cooperative/non-cooperative shutdown, and `goleak` after blocked handlers are released.

### Implementation for User Story 2

- [X] T027 [US2] Implement handler registration, lazy group-client startup, bounded `PollRecords`, partition grouping, and sequential-per-partition/parallel-across-partitions dispatch in `event-manager/kafka/subscriber.go`; make T025 pass.
- [X] T028 [US2] Implement synchronous `CommitRecords` per completed frontier, stop-on-unresolved behavior, `AllowRebalance` pairing, bounded drain, direct `Close(ctx)`, and closed-state `Ping(ctx)` in `event-manager/kafka/subscriber.go`; make T026 pass.
- [X] T029 [US2] Extend `event-manager/test/integration/kafka_test.go` with successful commit, failed-handler redelivery after a new same-group client starts, two-member cooperative rebalance without overlapping handlers, and shutdown with an unfinished record left for redelivery.
- [ ] T030 [US2] Run US2 unit and integration cases in `event-manager/`; inspect actual committed offsets and confirm that no later offset skips an unresolved record.

**Checkpoint**: A group worker can safely consume and acknowledge success; failure stops `Run` and redelivers after restart.

---

## Phase 5: User Story 3 — Apply a consistent failure policy (Priority: P1)

**Goal**: Compose recovery, logging, explicit retry classification, DLQ routing, and optional caller-backed idempotency.

**Independent Test**: Drive the root middleware with fake handlers/publishers, then verify retry and DLQ outcomes against the real broker.

### Tests for User Story 3

- [X] T031 [P] [US3] Extend `event-manager/errors_test.go` for `MarkRetryable`/`MarkPermanent` and `errors.Is`; write `event-manager/middleware_test.go` for panic recovery, metadata-only `slog` records, only-explicit-retryable attempts, maximum total attempts including the first, capped exponential full jitter, and cancellation during backoff without `time.Sleep`.
- [X] T032 [P] [US3] Add `event-manager/kafka/mapper_test.go` cases for DLQ record coordinates, key/raw value, decoded envelope when valid, failure class/message, attempt count/time, and credential-safe metadata; add `event-manager/kafka/subscriber_test.go` cases where malformed envelopes bypass handlers and either reach an acknowledged DLQ or stop without a commit.
- [X] T033 [US3] Extend `event-manager/middleware_test.go` for permanent/unclassified direct DLQ, exhausted retry DLQ, failed DLQ publication leaving the source unresolved, confirmed idempotent skip, mark only after business/DLQ success, store errors causing no commit, and composition order `Logging(Idempotency(DeadLetter(Retry(Recover(handler)))))`.

### Implementation for User Story 3

- [X] T034 [US3] Implement classification wrappers in `event-manager/errors.go` and recovery, logging, retry config validation, context-aware backoff, and injected deterministic timing/random seams in `event-manager/middleware.go`; make T031 pass.
- [X] T035 [US3] Implement DLQ and idempotency middleware in `event-manager/middleware.go`, with an `IdempotencyStore` completion-check/mark interface and no Redis/SQL import; make T033 pass.
- [X] T036 [US3] Implement the DLQ envelope mapper in `event-manager/kafka/mapper.go` and malformed-record DLQ path in `event-manager/kafka/subscriber.go`; make T032 pass and commit only after broker acknowledgment.
- [X] T037 [US3] Extend `event-manager/test/integration/kafka_test.go` with retryable success/exhaustion, permanent one-attempt DLQ, DLQ-publish failure leaving the source offset, and malformed raw-record DLQ delivery.
- [ ] T038 [US3] Run US3 unit and integration cases in `event-manager/`; verify panic does not terminate the worker and source offsets commit only after handler or acknowledged DLQ success.

**Checkpoint**: Failure policy is composable and leaves unresolved records eligible for redelivery.

---

## Phase 6: User Story 4 — Configure, monitor, and unit test workers (Priority: P2)

**Goal**: Make worker adoption concise and broker-free service tests deterministic.

**Independent Test**: Wire the documented worker in fewer than 30 Go lines, substitute eventtest fakes, and validate connection/security/health behavior.

### Tests for User Story 4

- [X] T039 [P] [US4] Write `event-manager/eventtest/fake_publisher_test.go` for ordered published records, injected acknowledgment/error outcomes, copy isolation, `Ping`, and repeat-safe `Close`.
- [X] T040 [P] [US4] Write `event-manager/eventtest/fake_subscriber_test.go` for deterministic topic delivery, handler error retention, commit eligibility, duplicate/late registration, cancellation, `Ping`, and `Close` without goroutines or Kafka.
- [X] T041 [US4] Extend `event-manager/kafka/config_test.go` and `event-manager/kafka/publisher_test.go` for TLS/SASL validation and redaction, deadline-aware live/closed health checks, and no environment reads.

### Implementation for User Story 4

- [X] T042 [US4] Implement `event-manager/eventtest/fake_publisher.go` as a root-`Publisher` fake with ordered inspection and injected outcomes; make T039 pass.
- [X] T043 [US4] Implement `event-manager/eventtest/fake_subscriber.go` as a root-`Subscriber` fake with controlled delivery and recorded outcomes; make T040 pass.
- [X] T044 [US4] Complete configuration and health-check behavior in `event-manager/kafka/config.go` and `event-manager/kafka/publisher.go`; make T041 pass without logging credentials.
- [X] T045 [US4] Write `event-manager/README.md` with a compiling under-30-line `New` → publish → middleware → register → run → ping → close example, Node wire contract, cooperative-handler requirement, idempotency limits, and scoped test commands.
- [X] T046 [US4] Add a broker-free example-worker test using both fakes in `event-manager/eventtest/fake_subscriber_test.go`; run it and verify the README wiring compiles.

**Checkpoint**: A worker author can configure and test a service without Kafka.

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Enforce repository gates and finish independent module documentation.

- [X] T047 [P] Add `event-manager/CHANGELOG.md` with the initial independent module release entry and document the future `event-manager/v0.1.0` tag convention.
- [X] T048 [P] Review exported API comments and package dependency boundaries in `event-manager/envelope.go`, `event-manager/publisher.go`, `event-manager/subscriber.go`, `event-manager/message.go`, `event-manager/errors.go`, `event-manager/middleware.go`, `event-manager/naming.go`, `event-manager/kafka/*.go`, and `event-manager/eventtest/*.go`; remove any franz-go type from root signatures.
- [X] T049 Audit every production `.go` file under `event-manager/` for a matching `_test.go`, table-driven varying cases, `testify/require` setup, `testify/assert` outcomes, no `time.Sleep` synchronization, and `goleak` coverage; add missing cases in the corresponding test files.
- [ ] T050 Run the complete [quickstart.md](quickstart.md) commands from `event-manager/`: unit, `go test -race -count=1`, coverage-floor script, `golangci-lint`, and tagged Testcontainers integration; fix failures in the affected `event-manager/` files and `scripts/check_coverage.py`.
- [X] T051 Run `go mod tidy` in `event-manager/`, verify `event-manager/go.mod` and `event-manager/go.sum` retain Go 1.23 compatibility and no sibling-kit, Redis, SQL, or Fx dependency, then run `git diff --check` on the finished module.

**Checkpoint**: All quality gates pass and the module is ready for review.

---

## Dependencies & Execution Order

### Phase dependencies

```text
Setup (T001–T005) → Foundation (T006–T016)
Foundation → US1 (T017–T024)
Foundation → US2 (T025–T030)
Foundation → US3 root middleware tests/logic (T031, T033–T035)
US1 + US2 → US3 Kafka DLQ and integration (T032, T036–T038)
Foundation → US4 fakes (T039–T040, T042–T043)
US1 + US2 + US3 → US4 wiring/health completion (T041, T044–T046)
US1 + US2 + US3 + US4 → Polish (T047–T051)
```

US1 is the suggested MVP: it can publish and verify Node-compatible events independently. US2 can seed broker records directly for its own tests. US3 middleware can be unit-tested with local stubs before the adapter exists, while its end-to-end DLQ path needs US1 and US2. US4 fakes can be built after foundation, while its README example needs the complete API.

### Within-story order

- Write the story's tests first and confirm the targeted behavior fails before implementing it.
- Complete root data types before Kafka mapping, and mapping/configuration before publishing.
- Complete subscriber polling before commit/rebalance/shutdown behavior; never treat fetched offset as committed.
- Complete classification/recovery/retry before DLQ/idempotency composition; complete unit behavior before broker integration.
- Append to `test/integration/kafka_test.go` in story order to avoid concurrent edits to that one file.

### Parallel opportunities

- T003, T004, and T005 edit separate setup files after T001's module-path decision.
- T006–T010 write distinct foundational test files.
- T017–T019 write naming, mapper, and publisher tests in separate files.
- T031 and T032 cover root middleware/errors and Kafka mapper/subscriber tests in separate files.
- T039 and T040 write the two fake test files independently.
- T047 and T048 edit documentation and API comments in separate files after all stories finish.

### Parallel example: User Story 1

```text
T017 → event-manager/naming_test.go
T018 → event-manager/kafka/mapper_test.go
T019 → event-manager/kafka/publisher_test.go
then T020–T022 implementations; then T023 broker integration; then T024 validation
```

### Parallel example: User Story 4

```text
T039 → event-manager/eventtest/fake_publisher_test.go
T040 → event-manager/eventtest/fake_subscriber_test.go
then T042–T043 fake implementations; then T046 worker adoption test
```

## Implementation Strategy

1. Finish setup and foundational contracts, then complete US1 as the publish-only MVP and validate it independently.
2. Complete US2's manual-commit subscriber and real-broker offset/rebalance checks.
3. Add US3 middleware and malformed-record DLQ handling; verify both the fake and broker paths.
4. Finish US4 fakes, configuration/health behavior, and the short worker example.
5. Run polish gates, inspect dependency isolation, and prepare the independent release record.

## Notes

- `[P]` tasks touch separate files and can be scheduled together only after their listed prerequisites are complete.
- All integration cases are test-only and may create topics inside their isolated container; production code never administers topics.
- A non-cooperative handler can outlive the shutdown bound; release it in tests before checking for kit-owned goroutine leaks.
