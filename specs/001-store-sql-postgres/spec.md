# Feature Specification: Store SQL PostgreSQL Kit

**Feature Branch**: Not created (this checkout is not a Git repository)

**Created**: 2026-10-11

**Status**: Draft

**Input**: User description: "Build the store-sql module as a reusable PostgreSQL kit for Go workers, with validated configuration, startup connectivity, health checks, graceful closure, context-bound GORM access, nested transactions, safe slog logging, duplicate-key translation, options, and unit/integration tests."

## Clarifications

### Session 2026-10-11

- Q: When `Close(ctx)` runs while database operations or transactions are in flight, what should happen? → A: Stop accepting new work, wait for active work until the context deadline, then close the pool.
- Q: If a nested `WithinTx` call returns an error but the outer callback handles it and returns nil, should the shared transaction commit or roll back? → A: Commit; the outermost callback decides the outcome.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Connect and manage a worker database (Priority: P1)

A backend engineer supplies database settings, starts a worker, checks readiness, and releases
the connection when the worker stops. Invalid settings fail before a network attempt; an
unreachable database fails startup with a useful error.

**Why this priority**: A worker cannot safely run without a known-good database connection.

**Independent Test**: Construct a client with valid and invalid settings, check readiness, and
close it; verify startup behavior and the returned errors.

**Acceptance Scenarios**:

1. **Given** a valid configuration and reachable database, **When** the engineer creates a
   client, **Then** creation verifies connectivity and returns a usable client.
2. **Given** missing or invalid settings, **When** configuration is validated or a client is
   created, **Then** a field-specific error is returned before any connection attempt.
3. **Given** a valid configuration and unreachable database, **When** a client is created,
   **Then** creation fails promptly with a wrapped connectivity error and no usable client.
4. **Given** a live client, **When** a health check runs with a context, **Then** it reports
   current connectivity and respects cancellation or deadline.
5. **Given** a live client with active work, **When** shutdown is requested, **Then** new work
   is rejected and active work can finish until the shutdown context deadline. A successful
   return means the pool is closed; a deadline error means closure continues in the background.
   Repeated closure is safe.

---

### User Story 2 - Use repositories inside transactions (Priority: P1)

A backend engineer obtains database access for each operation. Repository calls made within a
transaction automatically use that transaction, including calls nested under another
transaction. The outermost transaction decides the final outcome.

**Why this priority**: Worker use cases must keep multi-step writes atomic without requiring
repositories to manage transaction boundaries.

**Independent Test**: Run repository operations inside and outside a transaction, and verify
commit, rollback, panic, and nested-call behavior.

**Acceptance Scenarios**:

1. **Given** a normal operation context, **When** a repository obtains database access,
   **Then** it receives a handle bound to that context.
2. **Given** a context supplied inside a transaction callback, **When** a repository obtains
   database access, **Then** its work joins that transaction.
3. **Given** a callback that returns nil, **When** the outermost transaction finishes,
   **Then** its writes are committed.
4. **Given** a callback that returns an error, **When** the outermost transaction finishes,
   **Then** its writes are rolled back and the error remains identifiable.
5. **Given** a callback that panics, **When** the transaction unwinds, **Then** its writes are
   rolled back and the original panic continues to the caller.
6. **Given** an active transaction, **When** the callback starts a nested transaction,
   **Then** the nested callback uses the same transaction and creates no independent commit.
7. **Given** a nested callback returns an error without leaving the database transaction
   aborted, **When** the outer callback handles it and returns nil, **Then** the shared
   transaction commits.

---

### User Story 3 - Observe and extend database behavior safely (Priority: P2)

A backend engineer supplies a logger and optional extensions. Database errors and slow queries
are visible at appropriate levels without exposing credentials or query parameter values.
Duplicate-key failures have a stable error identity.

**Why this priority**: Operators need useful diagnostics while protecting sensitive data and
keeping repository error handling predictable.

**Independent Test**: Capture log records and injected extensions while exercising errors,
slow queries, and duplicate-key failures.

**Acceptance Scenarios**:

1. **Given** a supplied logger, **When** a query fails, **Then** an Error-level record is sent
   to that logger without DSN credentials or bound values.
2. **Given** a query above the configured slow threshold, **When** it completes, **Then** a
   Warn-level record is emitted without bound values.
3. **Given** a unique-constraint violation, **When** the error reaches a repository,
   **Then** it is identifiable as `gorm.ErrDuplicatedKey`.
4. **Given** an extra GORM plugin, **When** the client is created, **Then** the plugin is
   available to the client, or creation fails clearly if registration fails.

### Edge Cases

- Zero-value optional pool and timing settings receive documented defaults; negative values,
  invalid durations, empty DSNs, and incompatible connection limits fail validation.
- A canceled or expired startup context causes startup to fail promptly with an identifiable
  context error.
- A transaction callback that returns an error after a nested call causes the outer transaction
  to roll back; a nested call cannot commit separately.
- A nested callback error does not mark the shared transaction for rollback if the outer
  callback handles that error and returns nil. A failed PostgreSQL statement can still abort
  the transaction, in which case commit must report the database failure.
- A panic preserves the original panic value after rollback. Rollback failure must not hide it.
- Health checks after closure report that the client is unusable.
- If active work outlasts the shutdown deadline, closure stops waiting, initiates pool closure,
  and reports the deadline outcome; a later Close call can wait for final closure.
- Failed plugin registration must not leak an opened connection.
- A DSN with a password and queries with private bound values do not reveal those values in
  kit log records or kit-generated connection and configuration errors.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The kit MUST accept a plain `Config` containing DSN, maximum open connections,
  maximum idle connections, connection maximum lifetime, connection maximum idle time, and slow
  query threshold.
- **FR-002**: `Config.Validate()` MUST apply documented defaults to omitted optional fields
  and reject invalid settings with field-specific errors before any connection attempt.
- **FR-003**: `New(ctx, cfg, ...Option)` MUST establish the connection, verify it with a
  startup ping, and return a descriptive wrapped error when connectivity fails.
- **FR-004**: The client MUST expose `Ping(ctx)` for current readiness and `Close(ctx)` for
  bounded, repeat-safe shutdown. `Close(ctx)` MUST reject new work, wait for active work until
  the context deadline, then initiate pool closure. If the deadline wins, it MUST return a
  deadline error while physical closure continues. After closing begins, new operations MUST
  fail clearly.
- **FR-005**: The client MUST let repositories obtain a `*gorm.DB` bound to the current
  context; when that context carries an active transaction, the handle MUST use it.
- **FR-006**: `WithinTx(ctx, fn func(context.Context) error) error` MUST satisfy the
  consumer-owned `TxManager` contract. It MUST commit on nil, roll back on error, and roll
  back and re-panic on panic.
- **FR-007**: A nested `WithinTx` MUST use the outer transaction and MUST NOT independently
  begin, commit, or roll back a database transaction. The outermost callback determines the
  final commit or rollback, including when it handles a nested error and returns nil, provided
  the database transaction remains valid. A database-aborted transaction MUST NOT be reported
  as committed.
- **FR-008**: Errors returned by kit methods MUST be wrapped when adding context without
  losing error identity. Repository code owns wrapping errors returned directly by GORM.
  Unique-constraint violations MUST be identifiable with `errors.Is(err, gorm.ErrDuplicatedKey)`.
- **FR-009**: GORM logs MUST route through an injected `log/slog` logger, defaulting to
  `slog.Default()`. Errors MUST use Error level; queries exceeding the validated threshold
  MUST use Warn level.
- **FR-010**: Kit log records and kit-generated connection and configuration errors MUST NOT
  disclose DSN passwords, secrets, bound parameter values, or full payloads containing PII.
  Repository code using the returned GORM handle owns handling of its query errors.
- **FR-011**: Options MUST include `WithLogger(*slog.Logger)` and an extension point for
  additional GORM plugins; failed plugin setup MUST fail creation clearly.
- **FR-012**: The module MUST remain a library with no automatic schema migration,
  repository base class, Fx wiring, read-replica support, or multiple-database support.
- **FR-013**: The module MUST have an independent module identity and dependency set, with
  no imports from sibling kits.
- **FR-014**: The module MUST provide an example of configuration, creation, one operation,
  health check, and closure that a worker can adapt.

### Verification Requirements

- **VR-001**: Unit tests MUST use `sqlmock` and no real external service. They MUST prove
  invalid configuration fails before a connection attempt, error identity, log redaction,
  transaction commit, rollback on error, rollback on panic, nested joins, commit after a
  handled nested error, and closure with active work both before and after a shutdown deadline.
- **VR-002**: Integration tests MUST live under `test/integration` and use a real PostgreSQL
  instance through `testcontainers-go`. They MUST verify startup ping, repository access,
  duplicate-key translation, and transaction outcomes.
- **VR-003**: Tests MUST detect goroutine leaks after `Close` and obey the constitution's
  race, lint, coverage, and per-file test requirements.

### Key Entities *(include if feature involves data)*

- **Config**: Connection settings, pool limits, and slow-query threshold validated before use.
- **Client**: Owner of the database connection lifecycle and source of context-bound access.
- **Transaction context**: The operation context carrying the active transaction so repository
  calls join it.
- **Option**: Optional logger or plugin customization supplied during client creation.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A worker author can show configuration, client creation, one database operation,
  a readiness check, and closure in fewer than 15 lines of example code.
- **SC-002**: Every invalid-configuration case in the documented validation table fails
  before any connection attempt and identifies the offending setting.
- **SC-003**: Automated checks demonstrate 100% of the specified transaction outcomes:
  commit on success, rollback on error, rollback on panic, nested participation, and commit
  after a handled nested error.
- **SC-004**: Repeated create, health-check, and completed-close test runs report zero
  goroutine leaks, including after a timed-out close is later completed.
- **SC-005**: In automated sensitive-data cases, zero kit log records or kit-generated
  connection and configuration errors contain fixture credentials or bound values.
- **SC-006**: A disconnected database is reported during creation, and a connection lost
  afterward is reported by the next readiness check.
- **SC-007**: The module meets the constitution's 70% infrastructure-wrapper coverage floor
  and passes its race and lint gates.
- **SC-008**: Shutdown tests verify that no new work starts after closure begins, active work
  can finish before the deadline, and an expired deadline produces a visible timeout outcome.

## Assumptions

- Zero values for optional pool sizes and durations mean "use the documented default";
  zero is not an explicit request for an unlimited setting.
- Omitted MaxIdleConns defaults to the smaller of 5 and the validated MaxOpenConns value.
- The transaction interface is the `TxManager` signature shown in
  `docs/coding-convetions.md`; `docs/implementation-rules.md` is not present in this checkout.
  Its exact wording remains to be checked when supplied.
- Nested transactions share one transaction and do not introduce savepoints. The outermost
  callback controls the commit or rollback.
- The caller manages schema and service-specific repositories.
- The only supported database for this feature is PostgreSQL.
