# Research: Store SQL PostgreSQL Kit

## R0. Go 1.23-compatible dependency versions

**Decision:** Start with `gorm.io/gorm` v1.30.0, `gorm.io/driver/postgres` v1.6.0,
and both `github.com/testcontainers/testcontainers-go` and its separate
`modules/postgres` module at v0.38.0. Confirm the resolved transitive graph and CI
build with Go 1.23 before release.

**Rationale:** These published module files declare support for Go 1.23 or older.
Newer PostgreSQL driver and Testcontainers releases raise the Go minimum, so selecting
their latest versions would silently violate the constitution.

**Alternatives considered:** Unpinned latest releases (can require Go 1.24–1.26);
raising this module's Go version (contradicts the requested Go 1.23 baseline).

**Sources:** [GORM v1.30.0](https://raw.githubusercontent.com/go-gorm/gorm/v1.30.0/go.mod),
[PostgreSQL driver v1.6.0](https://raw.githubusercontent.com/go-gorm/postgres/v1.6.0/go.mod),
[Testcontainers core v0.38.0](https://raw.githubusercontent.com/testcontainers/testcontainers-go/v0.38.0/go.mod),
[Testcontainers PostgreSQL v0.38.0](https://raw.githubusercontent.com/testcontainers/testcontainers-go/v0.38.0/modules/postgres/go.mod).

## R1. Module path and workspace

**Decision:** Use `github.com/hros-aio/gopher-factory/store-sql`, `go 1.23` in its
`go.mod`, and root `go.work` with `use ./store-sql`. Remove the current root
`go.mod` when implementing.

**Rationale:** The existing root module names `github.com/hros-aio/gopher-factory`; this
resolves the user's `<org>` placeholder. A root module would conflict with the
constitution's one-module-per-kit boundary.

**Alternatives considered:** Keep the root module as an umbrella module (rejected by the
constitution); use literal `<org>` (not a valid release identity).

## R2. Context-aware startup

**Decision:** Disable GORM's automatic ping, obtain the underlying `*sql.DB`, and perform
one explicit `PingContext(ctx)` after setup. Close the pool on any failure after open.

**Rationale:** GORM can ping during `gorm.Open` without the constructor's context.
An explicit ping makes cancellation and fail-fast behavior testable.

**Alternatives considered:** Rely on GORM automatic ping (does not carry constructor
context); skip ping (violates startup readiness).

**Sources:** [GORM configuration](https://gorm.io/docs/gorm_config.html),
[database/sql PingContext](https://pkg.go.dev/database/sql#DB.PingContext).

## R3. Duplicate-key translation

**Decision:** Enable GORM `TranslateError`; PostgreSQL SQLSTATE 23505 maps to
`gorm.ErrDuplicatedKey`. Preserve identity through wrapped errors.

**Rationale:** This gives repositories a stable `errors.Is` check without leaking
driver-specific error types.

**Alternatives considered:** Manual SQLSTATE parsing in repositories (duplicates logic and
exposes implementation); error-string matching (forbidden).

**Sources:** [GORM error handling](https://gorm.io/docs/error_handling.html),
[PostgreSQL dialector translator](https://github.com/go-gorm/postgres/blob/master/error_translator.go).

## R4. Safe GORM logging

**Decision:** Implement GORM `logger.Interface` with a parameter filter and omit raw
SQL entirely from slog records. Emit timing, rows, and safe error classification. Errors
use Error; queries over the threshold use Warn.

**Rationale:** GORM can interpolate bound values into SQL before its trace logger sees it.
Filtering parameters and never logging SQL protect both bound values and SQL literals.
The raw error string can also reveal data, so it is not a log attribute.

**Alternatives considered:** Set only `ParameterizedQueries` (less robust against plugin
behavior or literal values); scrub SQL/error strings after interpolation (fragile).

**Sources:** [GORM logger](https://gorm.io/docs/logger.html),
[GORM callback SQL interpolation](https://github.com/go-gorm/gorm/blob/master/callbacks.go).

## R5. Context-bound handles and nested transactions

**Decision:** Return `db.WithContext(ctx)` for ordinary repository work. Inside
`WithinTx`, place the active transaction and owner in a private context value and return
`tx.WithContext(ctx)`. Nested calls invoke their callback directly on that context.

**Rationale:** GORM `WithContext` creates a context-bound session. Calling GORM's
`Transaction` for a nested call creates a savepoint, contradicting the chosen join
semantics. The owner check prevents accidentally joining a different client's pool.

**Alternatives considered:** Store a transaction on the client (unsafe shared mutable
state); invoke nested `Transaction` (savepoint semantics); expose a transaction-specific
repository base class (out of scope).

**Caveat:** Handling a callback error leaves commit possible only if no SQL statement has
aborted the PostgreSQL transaction. PostgreSQL requires a savepoint to recover from a
statement error; this feature deliberately has no nested savepoints. Commit failure is
returned even when the outer callback returns nil.

**Sources:** [GORM context](https://gorm.io/docs/context.html),
[GORM transactions](https://gorm.io/docs/transactions.html),
[PostgreSQL transactions](https://www.postgresql.org/docs/current/tutorial-transactions.html).

## R6. Deadline-aware close

**Decision:** Gate new work, drain tracked active work, then close the underlying pool.
Run the final pool close in one controlled closer goroutine and let `Close(ctx)` wait for
its completion or return `ctx.Err()` at deadline. Later calls observe the same closer.

**Rationale:** `database/sql.DB.Close()` has no context and waits for already-started
queries. Calling it synchronously could hang past the caller's deadline. A timeout
therefore cannot truthfully mean that physical closure has completed. The client remains
unusable after shutdown begins, and the closer completes when in-flight work ends.
Normal closure and the test deadline path verify no lingering closer.

**Alternatives considered:** Direct synchronous `DB.Close` (unbounded caller wait);
return success at deadline (misstates closure); force-fail active work immediately
(contradicts graceful shutdown).

**Source:** [database/sql DB.Close](https://pkg.go.dev/database/sql#DB.Close).

## R7. Test strategy and plugin extension

**Decision:** Inject `sqlmock` through the PostgreSQL dialector's `Conn: sqlDB` in unit
tests; use GORM's `Use(plugin)` during construction; run container-backed PostgreSQL
tests under an integration build tag.

**Rationale:** The production API stays small while tests can assert Begin, Commit,
Rollback, Ping, redaction, and closure without external services. An integration suite
proves behavior against PostgreSQL, especially duplicate-key and aborted-transaction
cases.

**Alternatives considered:** A test-only public constructor (unnecessary API surface);
real PostgreSQL for every unit test (violates unit isolation).

**Sources:** [GORM PostgreSQL connection setup](https://gorm.io/docs/connecting_to_the_database.html),
[GORM plugins](https://gorm.io/docs/write_plugins.html),
[go-sqlmock](https://github.com/DATA-DOG/go-sqlmock),
[Testcontainers PostgreSQL module](https://golang.testcontainers.org/modules/postgres/),
[goleak](https://github.com/uber-go/goleak).
