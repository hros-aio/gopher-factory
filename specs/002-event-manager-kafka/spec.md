# Feature Specification: Event Manager Kafka Kit

**Feature Branch**: `master` (no branch hook configured)
**Created**: 2026-10-11
**Status**: Draft
**Input**: Build the `event-manager` module for Go workers to publish and consume events exchanged with NestJS services over Kafka, with reliable delivery, middleware, lifecycle controls, and test fakes.

## Clarifications

### Session 2026-10-11

- The user chose the checked-in Node kit's camelCase JSON envelope and `payload` over the initially requested snake_case field names.
- Q: What should the subscriber do with a partition after a message fails and no dead-letter handoff succeeds? → A: Stop `Run`, leave the failed offset uncommitted, and redeliver it when the caller restarts consumption.
- Q: What should happen when a consumed record cannot be decoded as a valid Node envelope? → A: Send its raw record and decode error to the configured DLQ; commit only after DLQ acknowledgment. Without a successful DLQ handoff, stop `Run` and leave the source offset uncommitted.
- Q: Should cancellation-aware handlers be required for the guarantee that two consumers never process the same partition at once during a rebalance? → A: Yes; guarantee non-overlap for cancellation-aware handlers and document that non-cooperative handlers need caller-owned idempotency or an atomic claim.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Publish interoperable events (Priority: P1)

A worker author publishes a business event and receives a clear delivery result. Consumers in either language can read its identity, origin, version, tenant, correlation, time, and data.

**Why this priority**: Workers need a dependable way to return results to services.

**Independent Test**: Publish to a broker, consume from another client, and compare the JSON record with cross-language fixtures.

**Acceptance Scenarios**:

1. **Given** missing ID, occurrence time, and correlation ID, **When** a worker publishes, **Then** the kit fills them, using the context correlation ID if present and generating one otherwise.
2. **Given** supplied metadata, **When** a worker publishes, **Then** the values are preserved.
3. **Given** an accepted record, **When** publication returns success, **Then** all in-sync replicas have acknowledged it.
4. **Given** cancellation or broker failure, **When** publication fails, **Then** the caller receives an identifiable error.
5. **Given** pending records, **When** the publisher closes, **Then** it stops new sends, flushes within the deadline, and reports incomplete delivery.

---

### User Story 2 - Process group messages without loss (Priority: P1)

A worker author registers topic handlers and runs a consumer group. Completed messages advance offsets. Failed messages remain eligible for redelivery unless a durable dead-letter handoff resolves them.

**Why this priority**: A handler failure must not silently lose work.

**Independent Test**: Run success, failure, and rebalance cases with two group members; inspect committed offsets and handler overlap.

**Acceptance Scenarios**:

1. **Given** a fetched message, **When** its handler succeeds, **Then** its offset is committed afterward.
2. **Given** a handler error or failed dead-letter handoff, **When** processing ends, **Then** `Run` returns a processing error, its offset is not committed, and the message remains eligible for redelivery when consumption restarts.
3. **Given** an unresolved earlier record in a partition, **When** later records complete, **Then** the committed offset cannot skip the unresolved record.
4. **Given** cancellation-aware handlers and a partition rebalance, **When** a new member begins handling that partition, **Then** the old member has stopped handling it; one record is not processed by two group members simultaneously because of the rebalance.
5. **Given** cancellation, **When** the subscriber shuts down, **Then** fetching stops, completed work is committed, unresolved work is left for redelivery, and `Run` returns within the configured bound.

---

### User Story 3 - Apply a consistent failure policy (Priority: P1)

A worker author composes recovery, logging, retry, dead-letter, and optional idempotency behavior around handlers. Permanent failures go directly to the dead-letter topic; retryable failures have bounded attempts.

**Why this priority**: Workers need predictable recovery without crashing or losing records.

**Independent Test**: Drive each middleware and their documented composition with fakes; inspect attempts, dead-letter content, and commit eligibility.

**Acceptance Scenarios**:

1. **Given** a retryable error, **When** attempts remain, **Then** the handler is retried after exponential backoff with jitter, unless the context ends.
2. **Given** a permanent error, **When** it is returned, **Then** no further business attempt runs and the original record goes to the dead-letter topic.
3. **Given** exhausted retries, **When** dead-letter publication is acknowledged, **Then** the source record becomes eligible to commit.
4. **Given** failed dead-letter publication, **When** processing ends, **Then** the source offset stays uncommitted.
5. **Given** a panic, **When** recovery middleware is installed, **Then** it becomes a processing error and the worker remains running.
6. **Given** an event recorded as completed by the caller's idempotency store, **When** it is delivered again, **Then** business handling is skipped. Failed work is never recorded as completed.

---

### User Story 4 - Configure, monitor, and unit test workers (Priority: P2)

A worker author supplies explicit settings, checks broker readiness, and substitutes in-memory implementations for service tests.

**Why this priority**: Adoption should require little wiring and no broker for unit tests.

**Independent Test**: Validate settings, run the documented example, and drive a worker with the fakes.

**Acceptance Scenarios**:

1. **Given** invalid settings, **When** configuration is validated, **Then** a field-specific error appears before a network attempt.
2. **Given** a live publisher or subscriber, **When** `Ping(ctx)` runs, **Then** it reports current reachability within the caller's deadline.
3. **Given** fake clients, **When** a service publishes and consumes, **Then** tests can inspect records and deterministically invoke handlers without Kafka.
4. **Given** the example, **When** an engineer wires both clients and middleware, **Then** the wiring takes fewer than 30 Go lines.

### Edge Cases

- Missing required metadata, malformed JSON, invalid topic or key, and encoding failure produce identifiable errors. On malformed consumed input, the business handler does not run; the raw record and decode error go to the configured DLQ. Only an acknowledged DLQ handoff permits a commit. Without one, `Run` stops and leaves the source offset uncommitted.
- Existing metadata is preserved. With no context correlation ID, a missing correlation ID is generated.
- Cancellation interrupts a retry delay promptly.
- A dead-letter record preserves original topic, partition, offset, key, raw value, decoded envelope when available, failure class, description, attempt count, and failure time. Full payloads and secrets are excluded from logs.
- Lost dead-letter acknowledgments may lead to duplicate dead-letter records on source redelivery; consumers must tolerate duplicates.
- A health check after closure reports unavailability; repeated closure is safe.
- A handler that ignores cancellation may outlive the shutdown bound. Its unresolved record is not committed; the caller owns that handler's eventual termination.
- A failed idempotency lookup or completion write must not cause a message to be skipped or prematurely committed.
- When processing fails without acknowledged dead-letter handoff, the subscriber stops fetching, drains other in-flight work within the shutdown bound, commits completed contiguous offsets, and returns the processing error. Restarting consumption retries the failed record.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The root `event-manager` package MUST expose library-independent `Envelope[T]`, `Message`, `Handler`, `Publisher`, and `Subscriber` contracts. No franz-go type may appear in these public contracts. The Kafka implementation MUST live in `event-manager/kafka`; in-memory fakes MUST live in `event-manager/eventtest`. Concrete clients MUST be constructed through `New(ctx, cfg, ...Option)` without global mutable state or startup side effects.
- **FR-002**: `Envelope[T]` MUST use the checked-in Node kit's JSON contract: required `eventId`, `eventType`, positive integer `eventVersion`, `tenantCode`, `occurredAt`, `producer`, and `payload`, plus optional nullable `correlationId`, `causationId`, and `traceId`. Go field names may be idiomatic, but their JSON names and value types MUST match the Node contract. Shared fields MUST round-trip across Go and Node.js services without changing meaning.
- **FR-003**: `Publish(ctx, topic, key, event)` MUST use an idempotent producer, wait for all in-sync replicas to acknowledge, and return wrapped, identifiable errors for validation, encoding, cancellation, timeout, and broker failure. Missing ID, occurrence time, and correlation ID MUST be filled; context correlation ID takes precedence over generating one. Supplied values MUST remain unchanged.
- **FR-004**: Publisher `Close(ctx)` MUST reject new publications, flush pending records within the deadline, report incomplete delivery, and be repeat-safe.
- **FR-005**: The subscriber MUST register handlers by topic and offer blocking `Run(ctx)` in a named consumer group. A handler MUST have signature `func(ctx, Message) error`. Automatic offset commit MUST be disabled.
- **FR-006**: The subscriber MUST commit only through the highest contiguous record per partition that succeeded or had an acknowledged dead-letter handoff. Handler error, failed dead-letter handoff, or unfinished work MUST leave its record eligible for redelivery. When processing fails without acknowledged dead-letter handoff, `Run` MUST stop fetching, drain other in-flight work within the shutdown bound, commit completed contiguous offsets, and return a wrapped processing error; caller restart MUST permit redelivery of the failed record. Delivery MUST be at least once.
- **FR-007**: On partition revocation, the subscriber MUST stop new work for that partition, cancel in-flight handler contexts, wait for cooperative handlers to finish within a bounded rebalance period, commit only completed contiguous offsets, and release ownership. With handlers that honor cancellation, old and new group members MUST have no overlapping active processing for the transferred partition. The kit MUST document that a handler ignoring cancellation can continue after ownership transfer and requires a caller-owned idempotency or claim mechanism to prevent overlapping business effects.
- **FR-008**: The kit MUST provide composable `Recover`, `Logging`, `Retry`, `DeadLetter`, and optional `Idempotency` middleware and document an order that routes recovered panics through the failure policy.
- **FR-009**: `Recover` MUST convert panic to error without crashing the worker. `Logging` MUST include topic, partition, offset, and correlation ID when present, use injected `log/slog` with `slog.Default()` as default, and exclude full payloads and secrets.
- **FR-010**: Error helpers MUST classify errors as `Retryable` or `Permanent` while preserving their original identity for `errors.Is`. Permanent and unclassified errors MUST bypass retry and proceed to the configured dead-letter policy.
- **FR-011**: `Retry` MUST enforce a configurable maximum total attempt count including the initial call, use exponential backoff with jitter, honor context cancellation, and retry only explicitly retryable errors.
- **FR-012**: `DeadLetter` MUST publish the original message plus failure metadata to a configurable topic after exhaustion or a permanent error. Invalid envelopes MUST bypass the business handler and send their raw record plus decode error to the configured DLQ. An acknowledged dead-letter publication MUST make the source record commit-eligible. If publication fails or no DLQ is configured for an invalid envelope, `Run` MUST stop with an error and leave the source offset uncommitted.
- **FR-013**: Optional `Idempotency` MUST use a small caller-implemented completion-check interface with no Redis or SQL dependency. It MUST identify events by stable ID, skip only confirmed completions, record completion only after successful business handling or terminal dead-letter handoff, and surface store failures without committing unconfirmed work. Its documented contract MUST state the limits of a completion check without a caller-owned atomic claim.
- **FR-014**: On cancellation, `Run(ctx)` MUST stop fetching, allow in-flight handlers to finish within a configurable shutdown timeout, commit completed contiguous offsets, then close. It MUST return no later than that timeout plus documented bounded broker-close overhead and must not wait indefinitely for a non-cooperative handler. Subscriber `Close(ctx)` MUST provide the same bounded, repeat-safe closure when invoked directly.
- **FR-015**: Publisher and subscriber MUST offer `Ping(ctx)` to test current broker reachability, honor cancellation, and report closed-client state.
- **FR-016**: Plain configuration structs with `Validate()` MUST cover brokers, client ID, group ID where needed, TLS, SASL, connection/publish/shutdown timeouts, retry limits, backoff bounds, and DLQ topic where enabled. Validation MUST document optional defaults and reject invalid values before connecting. No component may read environment variables.
- **FR-017**: Helper functions MUST construct and validate topic names as `<domain>.<event>.v<n>` and message keys as `<tenant_id>:<aggregate_id>`. The documentation MUST define a key policy for global events without a tenant.
- **FR-018**: The module MUST be independent of sibling kits and contain no service startup, outbox, schema registry, Avro/Protobuf, exactly-once transactions, Fx wiring, or topic administration.
- **FR-019**: A README MUST show construction, publish, registration, run, health check, and close; the full wiring example MUST be under 30 Go lines. Exported identifiers MUST have doc comments and the module MUST maintain its own changelog.

### Verification Requirements

- **VR-001**: Unit tests MUST prove that handler and dead-letter errors leave offsets uncommitted; acknowledged dead-letter handoff commits; malformed envelopes bypass handlers and reach the DLQ or stop without committing; retryable and permanent errors differ; panic recovery keeps the worker running; cancellation interrupts backoff; and commits stay contiguous.
- **VR-002**: Tests MUST cover rebalance handoff with two members, bounded shutdown with cooperative and non-cooperative handlers, and no leaks among kit-owned goroutines.
- **VR-003**: Integration tests under `test/integration` MUST use a real containerized Kafka-compatible broker and verify acknowledgment, group delivery, failure redelivery, DLQ delivery, rebalance, and graceful shutdown.
- **VR-004**: The module MUST pass the constitution's race, lint, per-file test, and coverage gates. Unit tests MUST not require an external broker.

### Key Entities *(include if feature involves data)*

- **Envelope**: Business data and event identity, origin, version, tenant, correlation, and occurrence time.
- **Message**: Delivered record with topic, partition, offset, key, original bytes, and decoded envelope when available.
- **Handler**: Caller processing function whose outcome controls commit eligibility.
- **Publisher and Subscriber**: Lifecycle-managed send and consumer-group contracts.
- **Dead-letter record**: Original record and failure metadata retained for investigation and replay.
- **Config**: Explicit connection, security, timing, and failure-policy settings.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: An engineer can wire publishing, group consumption, middleware, and shutdown in fewer than 30 lines of documented Go code.
- **SC-002**: Across automated failure cases, zero source messages are acknowledged before successful handling or acknowledged dead-letter handoff.
- **SC-003**: Every retryable failure in tests succeeds within the configured attempt limit or reaches that limit; every permanent failure makes exactly one business-handler attempt before DLQ routing.
- **SC-004**: A two-member rebalance test using cancellation-aware handlers records zero overlapping handler executions for the transferred partition.
- **SC-005**: Every shutdown test completes within the configured timeout plus documented bounded close overhead; completed work is acknowledged and unresolved work remains eligible for redelivery.
- **SC-006**: Repeated normal and timed-out shutdown tests report zero kit-owned goroutine leaks.
- **SC-007**: Cross-language fixtures round-trip every required and optional Node envelope field, including an empty tenant code and present correlation ID, without value changes.
- **SC-008**: The module passes race and lint gates and the constitution's 85% pure-logic and 70% infrastructure-wrapper coverage floors.

## Assumptions

- Occurrence time uses RFC 3339; generated event and correlation IDs are UUID strings.
- `producer` identifies the publishing service. The caller supplies `eventType`; it is not silently inferred from the topic.
- Dead-letter publication is a separate action from a business-handler attempt. A failed DLQ handoff leads to source redelivery.
- Callers must make handlers honor context cancellation to obtain the rebalance non-overlap guarantee. The kit can bound its wait but cannot terminate arbitrary Go code; non-cooperative handlers need caller-owned idempotency or an atomic claim to prevent overlapping business effects.
- Broker and topics are provisioned externally. The kit never creates topics.
- `tenantCode` is required by the Node contract; an empty string represents a global event without a tenant. The publisher fills `correlationId` even though the Node contract permits it to be null.
- `docs/tech-stack.md` is absent from this checkout. The checked-in Node envelope and `docs/coding-convetions.md` provide current compatibility evidence; the constitution governs the franz-go choice.
