---
description: "Dependency-ordered implementation tasks for the store-sql PostgreSQL kit"
---

# Tasks: Store SQL PostgreSQL Kit

**Input**: Design documents from `specs/001-store-sql-postgres/`

**Prerequisites**: [plan.md](plan.md), [spec.md](spec.md), [research.md](research.md),
[data-model.md](data-model.md), [public API contract](contracts/public-api.md)

**Tests**: Required by the feature specification and constitution. Create the named tests
before each corresponding implementation task and confirm the tests fail for the expected
missing behavior. Unit tests use sqlmock; integration tests use Testcontainers.

**Organization**: Tasks follow the three user stories in priority order. Every non-generated
Go source file has its matching test file.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel with other tasks in the same phase because it touches different
  files and has no incomplete prerequisite.
- **[USn]**: User story from [spec.md](spec.md).
- Paths are relative to the repository root.

## Path Conventions

- Root workspace and build files: `go.work`, `Makefile`, `.github/workflows/store-sql.yml`.
- Library files: `store-sql/`; tests are next to their matching source file.
- Container tests: `store-sql/test/integration/`.

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Establish the independently versioned Go 1.23 module and workspace.

- [ ] T001 Create `store-sql/go.mod` with module `github.com/hros-aio/gopher-factory/store-sql` and Go 1.23; start with GORM v1.30.0, PostgreSQL driver v1.6.0, Testcontainers core and `modules/postgres` v0.38.0, testify, `github.com/DATA-DOG/go-sqlmock`, and goleak, then resolve a Go 1.23-compatible graph into `store-sql/go.sum`.
- [X] T002 Replace the existing root `go.mod` with root `go.work` using `./store-sql`, and verify `go list -m` from `store-sql/` resolves only its module identity.
- [X] T003 [P] Create root `Makefile` targets `lint`, `test`, `test-race`, `test-cover`, and `test-integration` that execute from `store-sql/`; keep the integration target behind its explicit build tag and make `test-cover` report coverage.

**Checkpoint**: The workspace points to one kit module; no root umbrella module remains.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Establish shared error and option contracts before client stories.

**CRITICAL**: Complete this phase before starting a user story.

- [ ] T004 [P] Write `store-sql/errors_test.go` tests for `ErrClosed`, `ErrInvalidConfig`, and `errors.Is` identity through wrapping.
- [ ] T005 [P] Write `store-sql/option_test.go` tests for default `slog.Default()`, `WithLogger`, `WithPlugin`, and nil-option rejection; test validation before connection attempts in US1 client tests.
- [ ] T006 Implement documented error sentinels and safe error wrapping helpers in `store-sql/errors.go`; ensure kit-generated configuration and connection errors never include a DSN password.
- [ ] T007 Implement `Option`, `WithLogger(*slog.Logger)`, and `WithPlugin(gorm.Plugin)` in `store-sql/option.go`; reject nil logger or plugin without global state or environment access.

**Checkpoint**: Public error and option contracts have matching tests.

---

## Phase 3: User Story 1 - Connect and manage a worker database (Priority: P1) MVP

**Goal**: Validate settings before I/O, start only with a reachable database, expose readiness
and a bounded, repeat-safe close, and supply a context-bound database handle.

**Independent Test**: Use sqlmock to prove defaulting, validation-before-ping, one startup
ping, current readiness, context cancellation, and graceful/timeout shutdown. The module
must work without the transaction or extra-plugin stories.

### Tests for User Story 1

- [ ] T008 [P] [US1] Write table-driven `store-sql/config_test.go` cases for DSN "Required; parse as PostgreSQL DSN without network I/O; never echo value in errors", MaxOpenConns default 10 and positive value, MaxIdleConns default 5 and no greater than MaxOpenConns, ConnMaxLifetime default 30m and positive value, ConnMaxIdleTime default 5m and positive value, and SlowQueryThreshold default 200ms and positive value; assert invalid `New` does no ping.
- [ ] T009 [P] [US1] Write `store-sql/client_test.go` sqlmock tests for `New` startup `PingContext`, invalid options before a connection attempt, unreachable/canceled startup, `Ping(ctx)` after connection loss, `DB(ctx)` context binding, close idempotence, rejection after closing begins, active work completing before deadline, timeout returning `ctx.Err()`, and goleak after the closer finishes.

### Implementation for User Story 1

- [ ] T010 [US1] Implement `Config` and `(*Config).Validate() error` in `store-sql/config.go` with the exact T008 defaults and field constraints; validate offline and keep DSN values out of errors.
- [ ] T011 [US1] Implement `New(ctx, cfg, ...Option)`, `Ping(ctx)`, `DB(ctx)`, and `Close(ctx)` in `store-sql/client.go`: validate a Config copy and options first, open GORM with automatic ping disabled, run one `sql.DB.PingContext(ctx)`, set pool limits, close on startup failure, and maintain open/closing/closed state.
- [ ] T012 [US1] Complete the shutdown path in `store-sql/client.go` with a gate for new GORM statements, active-work tracking, one shared pool closer, deadline return while asynchronous closure continues, and repeatable `Close(ctx)`; make the T009 shutdown and goleak cases pass without `time.Sleep`.
- [ ] T013 [US1] Add tagged Testcontainers startup, readiness, and closure scenarios in `store-sql/test/integration/postgres_test.go` using a real PostgreSQL container and a finite context.

**Checkpoint**: `go test ./...` from `store-sql/` proves the connection lifecycle without an external service; tagged integration proves real PostgreSQL startup.

---

## Phase 4: User Story 2 - Use repositories inside transactions (Priority: P1)

**Goal**: Let repositories transparently join the current transaction and preserve outermost
commit, rollback, panic, and nested-call behavior.

**Independent Test**: sqlmock expects exactly one Begin and one final Commit/Rollback per
outermost call; PostgreSQL integration proves writes persist or disappear as specified.

### Tests for User Story 2

- [ ] T014 [P] [US2] Write `store-sql/tx_test.go` sqlmock cases for commit on nil, rollback with preserved `errors.Is` identity on callback error, rollback and original re-panic on panic even if rollback fails, nested joining without savepoints, a handled non-SQL nested error allowing commit, commit failure from an aborted transaction, foreign-client context rejection, and context-first method behavior.
- [ ] T015 [P] [US2] Extend `store-sql/client_test.go` to verify `DB(ctx)` returns the active transaction for the same Client and a context-bound ordinary handle otherwise.

### Implementation for User Story 2

- [ ] T016 [US2] Implement `WithinTx(ctx, fn)` and a private owner-checked transaction context value in `store-sql/tx.go`; begin once, commit on nil, rollback on error or panic, re-panic with the original value, and invoke nested callbacks directly without GORM savepoints.
- [ ] T017 [US2] Connect `DB(ctx)` in `store-sql/client.go` to the private transaction context from T016 while preserving the shutdown gate and context binding.
- [ ] T018 [US2] Extend `store-sql/test/integration/postgres_test.go` with real commit, error rollback, panic rollback, nested join, handled non-SQL error commit, and PostgreSQL aborted-statement failure cases; create test tables with explicit SQL only inside tests.

**Checkpoint**: Transaction unit and integration tests prove the whole `TxManager` contract, including PostgreSQL's aborted-transaction limit.

---

## Phase 5: User Story 3 - Observe and extend database behavior safely (Priority: P2)

**Goal**: Route diagnostics to slog without sensitive data, allow extra GORM plugins, and
surface duplicate-key errors with stable identity.

**Independent Test**: Capture slog records and plugin registration with sqlmock, then verify
unique-violation translation against real PostgreSQL.

### Tests for User Story 3

- [ ] T019 [P] [US3] Write `store-sql/gorm_logger_test.go` tests for Error-level failures, Warn-level queries over the configured threshold, no warning at or below it, and absence of DSN password, SQL text, bound values, raw driver errors, and PII fixtures.
- [ ] T020 [P] [US3] Extend `store-sql/option_test.go` to prove injected logger selection, extra plugin registration, duplicate plugin/nil plugin handling, and cleanup when registration fails.
- [ ] T021 [P] [US3] Extend `store-sql/errors_test.go` to prove duplicate-key identity through `errors.Is(err, gorm.ErrDuplicatedKey)` and safe classification for logs.
- [ ] T022 [US3] Extend `store-sql/client_test.go` with sqlmock duplicate-key translation and logger/plugin setup tests that check connection cleanup on failure.

### Implementation for User Story 3

- [ ] T023 [US3] Implement GORM `logger.Interface` and parameter filtering in `store-sql/gorm_logger.go`; emit safe timing/row/error-class attributes only and never invoke SQL interpolation for slog output.
- [ ] T024 [US3] Add safe database-error classification in `store-sql/errors.go` without string comparisons or leaking raw error text into logs.
- [ ] T025 [US3] Wire injected slog logger, `WithPlugin` registration, and GORM `TranslateError: true` into `store-sql/client.go`; make setup failures close the opened pool and preserve wrapped error identity.
- [ ] T026 [US3] Extend `store-sql/test/integration/postgres_test.go` with unique-constraint violation and plugin behavior scenarios; assert `errors.Is(err, gorm.ErrDuplicatedKey)` and inspect safe log records.

**Checkpoint**: Logger, extension, and duplicate-key behavior are independently verified.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Meet module documentation, CI, dependency, and quality gates.

- [X] T027 [P] Write `store-sql/README.md` with a minimal, compilable under-15-line `Config` → `New` → use → `Ping` → `Close` example, default table, transaction example, shutdown contract, and links to module documentation.
- [X] T028 [P] Create `store-sql/CHANGELOG.md` with the initial unreleased public API and `store-sql/v0.1.0` release convention.
- [X] T029 [P] Create `.github/workflows/store-sql.yml` for Go 1.23 `golangci-lint`, `go test -race -count=1`, a coverage report enforcing 85% for pure logic and 70% for infrastructure wrappers, and tagged Testcontainers integration on a runner with a container runtime.
- [ ] T030 Run `make lint`, `make test`, `make test-race`, `make test-cover`, and `make test-integration` using root `Makefile`; fix any failures in `store-sql/` and verify the [quickstart](quickstart.md) outcomes.
- [ ] T031 Audit `store-sql/`, root `go.work`, root `Makefile`, and `.github/workflows/store-sql.yml` for exported doc comments, one matching `_test.go` per non-generated Go file, table-driven tests where cases vary, `require` for setup, `assert` for results, no `time.Sleep` synchronization, no `AutoMigrate`, no `os.Getenv` or `init()` side effects, no sibling-kit imports, no Fx, and no Mongo/Kafka dependency in the module graph.

**Checkpoint**: All constitution gates and the full quickstart pass.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)** establishes `store-sql/go.mod` before `go.work`; T003 can be drafted
  in parallel with T001.
- **Foundational (Phase 2)** follows Setup and blocks all user stories. T004 and T005 can be
  written in parallel; T006 and T007 follow their tests.
- **US1 (Phase 3)** follows Foundational and produces the minimum working kit.
- **US2 (Phase 4)** uses the Client and `DB(ctx)` from US1; its transaction behavior can be
  tested independently once US1 exists.
- **US3 (Phase 5)** uses Client construction from US1 and may run alongside US2 after US1,
  except edits to `client.go`, `client_test.go`, and `postgres_test.go` must be sequenced.
- **Polish (Phase 6)** follows all desired stories; T027–T029 are independent drafts, while
  T030–T031 require completed implementation.

### User Story Dependencies

| Story | Prerequisite | Independent completion signal |
|---|---|---|
| US1 | Setup and Foundation | Config, startup ping, readiness, context-bound DB, bounded close pass unit and integration checks |
| US2 | US1 Client API | Commit, rollback, panic, nested join, and aborted-transaction cases pass |
| US3 | US1 Client construction | Safe slog records, plugin setup, and duplicate-key identity pass |

### Within Each User Story

1. Write story tests and confirm a relevant failure before implementing behavior.
2. Implement the files named in the tasks, keeping each source/test pair aligned.
3. Run that story's unit tests; run its tagged integration scenarios when available.
4. Do not use `time.Sleep` for synchronization; use channels, contexts, or sqlmock
   expectations.

### Parallel Opportunities

- **Foundation:** T004 and T005 (different test files).
- **US1:** T008 and T009 (config versus client tests).
- **US2:** T014 can be prepared while the independent T015 client test extension is planned,
  but writes to `client_test.go` follow US1 completion.
- **US3:** T019, T020, and T021 (different test files).
- **Polish:** T027, T028, and T029 (different documentation/CI files).

## Parallel Example: User Story 1

```text
T008: Write configuration validation cases in store-sql/config_test.go
T009: Write startup, health, and shutdown cases in store-sql/client_test.go
```

## Parallel Example: User Story 2

```text
T014: Write transaction cases in store-sql/tx_test.go
T015: Extend context-bound handle cases in store-sql/client_test.go
```

## Parallel Example: User Story 3

```text
T019: Write safe logging cases in store-sql/gorm_logger_test.go
T020: Extend option/plugin cases in store-sql/option_test.go
T021: Extend duplicate-key cases in store-sql/errors_test.go
```

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Setup and Foundation.
2. Complete US1 in test-first order.
3. Validate startup, readiness, context-bound access, and bounded close independently.
4. Continue to transactions and observability before declaring the full feature done.

### Incremental Delivery

1. US1 establishes a usable PostgreSQL client and lifecycle.
2. US2 adds context-propagated atomic work.
3. US3 adds safe diagnostics, plugin extension, and stable duplicate-key handling.
4. Polish completes docs, CI, coverage, dependency, and integration gates.

## Notes

- A task is complete only when its named test or validation outcome passes.
- `test/integration/postgres_test.go` is extended sequentially across stories.
- The root `go.mod` removal is intentional because the constitution permits one module per
  kit, not an umbrella module.
- `docs/implementation-rules.md` remains absent; the public `TxManager` method signature
  follows the user request and the available `docs/coding-convetions.md`.
