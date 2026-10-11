# Implementation Plan: Store SQL PostgreSQL Kit

**Branch**: Not created (checkout has no Git metadata) | **Date**: 2026-10-11 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/001-store-sql-postgres/spec.md`

## Summary

Create an independently versioned `store-sql` Go library for PostgreSQL workers. It validates
configuration before I/O, opens GORM with an explicit context-aware startup ping, supplies
context-bound repository handles and transaction management, routes redacted diagnostics to
slog, and closes its pool with a deadline-aware drain. A root Go workspace and Makefile drive
module checks; the existing root `go.mod` is removed during implementation because the
constitution requires one module per kit.

## Technical Context

**Language/Version**: Go 1.23 minimum; local toolchain is Go 1.26.

**Primary Dependencies**: `gorm.io/gorm`, `gorm.io/driver/postgres`; standard `context`,
`database/sql`, `log/slog`. The Go module path is
`github.com/hros-aio/gopher-factory/store-sql`, using the organization in the current root
`go.mod` to resolve the user's `<org>` placeholder. Initial Go 1.23-compatible pins are
GORM v1.30.0 and PostgreSQL driver v1.6.0; resolve the full module graph against Go 1.23.

**Storage**: PostgreSQL only. The kit never creates or migrates a schema.

**Testing**: `testify`, `go-sqlmock`, `goleak` for unit tests; `testcontainers-go` with
PostgreSQL for `test/integration`. Pin both the core and `modules/postgres` Testcontainers
modules to v0.38.0 for Go 1.23 compatibility. Run race and coverage checks per module.

**Target Platform**: Go worker processes on platforms supported by Go 1.23 and PostgreSQL.

**Project Type**: Library in a multi-module Go monorepo.

**Performance Goals**: No invented latency or throughput guarantee. A slow-query threshold
controls diagnostics; pool sizing is configurable.

**Constraints**: No global mutable state, `init()` effects, environment reads, Fx dependency,
other-kit imports, or AutoMigrate. Context first on I/O. No sensitive log values. Close must
reject new work, drain active work until its deadline, and report a timeout accurately.

**Scale/Scope**: One PostgreSQL connection pool per client; no read replicas or multi-database
routing.

## Constitution Check

*GATE: Evaluated before research and rechecked after design.*

| Constitution rule | Design evidence | Result |
|---|---|---|
| Library lifecycle, explicit construction | `New(ctx, Config, ...Option)`, `Ping(ctx)`, `Close(ctx)`; no startup side effects | PASS |
| Independent modules and dependencies | Dedicated `store-sql/go.mod`; workspace uses only `./store-sql`; root module removed; no sibling imports | PASS after migration |
| Context and errors | Context-bound GORM handle, context-aware ping, wrapped errors, `errors.Is` | PASS |
| Safe logging | Injected slog logger; trace records omit SQL, bound values, DSN, and raw database errors | PASS |
| No schema management | No migration API or calls | PASS |
| Testing and coverage | Matched unit test files, sqlmock, integration containers, race, goleak, 70% wrapper floor | PASS |
| Public API and docs | Exported comments, module README and CHANGELOG, semantic release tag | PASS |
| Messaging rules | No messaging code in this module | N/A |

The current root `go.mod` is a pre-existing structural conflict with the multi-module rule;
implementation must replace it with `store-sql/go.mod` and a root `go.work` before declaring
the feature complete. No design exception is requested.

**Post-design recheck:** The proposed API, lifecycle, test layout, and dependency graph pass
the applicable gates. The structural migration above is an implementation prerequisite.

### Design Decisions

1. **Transaction propagation through context**: `WithinTx` stores a transaction handle and
   owning client under a private typed context key. `DB(ctx)` returns a GORM session bound to
   the supplied context and uses the stored transaction only when its owner is this client.
   A cross-client transaction context is rejected. Context is never stored on the client.
2. **Nested transactions**: Nested `WithinTx` invokes the callback with the existing
   transaction context. It does not call GORM's nested `Transaction`, which would create a
   savepoint. A handled callback error may be committed if the outer callback returns nil and
   PostgreSQL still considers the transaction valid. A failed SQL statement can abort the
   transaction, so a later commit failure must be returned and never reported as success.
3. **Outermost outcome**: Begin once; commit on nil; rollback on returned error; on panic,
   attempt rollback and re-panic with the original value. Preserve original error identity
   with `%w`, and retain rollback failure as secondary context without losing the first error.
4. **Startup and duplicate keys**: Validate a copy of Config and options before network I/O.
   Open GORM with automatic ping disabled, configure error translation, then call
   `sql.DB.PingContext(ctx)` exactly once. Close the pool on plugin or ping failure.
5. **Logging and plugins**: Supply a GORM logger adapter backed by injected slog. Its trace
   path never calls the SQL interpolation function and emits only operation timing
   and a safe error class. Register additional GORM plugins during construction. Reject nil
   logger/plugin and return sanitized setup errors.
6. **Graceful shutdown**: State is open → closing → closed. Closing atomically rejects new
   client entry points and GORM statement starts. Track active `WithinTx` callbacks and GORM
   statement execution; wait for them until the Close context deadline. Start one pool closer
   after the drain or deadline. Because `sql.DB.Close()` is context-free and may wait for
   operations, a deadline makes `Close(ctx)` return `ctx.Err()` while that one closer finishes
   asynchronously. Repeated calls share the same completion signal. Normal closure waits for
   final pool closure. Tests release blocked work and verify the closer exits without a leak.
   Borrowed `*gorm.DB` sessions are valid only while the client is open; operations started
   after closing begins are rejected.
7. **Config defaults**: `Validate` fills zero-valued optional settings: 10 maximum open,
   minimum of 5 and maximum open for maximum idle, 30-minute maximum lifetime, 5-minute
   maximum idle time, 200-millisecond
   slow-query threshold. An empty or malformed DSN, negative values, or idle > open fail
   with a field-specific, credential-safe error. `New` validates a copy to avoid mutating
   caller-owned configuration.
8. **Module identity and build targets**: Use `github.com/hros-aio/gopher-factory/store-sql`
   and tag `store-sql/v0.1.0` for the first release. Root Makefile targets `lint`, `test`,
   `test-race`, `test-cover`, and `test-integration` run commands from the module.
   `lint` runs `golangci-lint`, `test` runs unit tests, `test-race` uses
   `go test -race -count=1`, `test-cover` writes a coverage report and checks 85% for pure
   logic and 70% for infrastructure wrappers, and `test-integration` runs the container
   suite. Integration tests use an explicit tag so normal unit test runs need no container
   runtime.

## Project Structure

### Documentation (this feature)

```text
specs/001-store-sql-postgres/
├── spec.md
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   └── public-api.md
├── checklists/
│   └── requirements.md
└── tasks.md                 # Created by speckit-tasks, not this phase
```

### Source Code (repository root)

```text
go.work                       # go 1.23; use ./store-sql
Makefile                      # lint, test, test-race, test-cover, test-integration
.github/workflows/store-sql.yml # Go 1.23 lint, race, coverage; integration with containers
store-sql/
├── go.mod                     # github.com/hros-aio/gopher-factory/store-sql; go 1.23
├── go.sum
├── config.go
├── config_test.go
├── option.go
├── option_test.go
├── client.go
├── client_test.go
├── gorm_logger.go
├── gorm_logger_test.go
├── tx.go
├── tx_test.go
├── errors.go
├── errors_test.go
├── README.md
├── CHANGELOG.md
└── test/
    └── integration/
        └── postgres_test.go
```

**Structure Decision**: Each kit will have its own module. This feature creates only
`store-sql`; future kits join `go.work` without becoming imports of this module. The root
`go.mod` is removed during implementation. Unit tests live next to each source file; the
container-backed test is isolated under `test/integration`. CI runs the constitution's
required lint, race, and coverage gates on Go 1.23 and runs container integration tests
when a container runtime is available.

## Complexity Tracking

No constitution violations are planned. The shutdown tracker exists to satisfy the explicit
bounded-drain behavior for a library returning a borrowed GORM handle.
