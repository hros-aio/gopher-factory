# Quickstart Validation: Store SQL PostgreSQL Kit

## Prerequisites

- Go 1.23 or newer.
- `golangci-lint` installed for the lint target.
- Docker or another Testcontainers-compatible container runtime for integration tests.
- A reachable PostgreSQL instance only for the manual example; unit tests need none.

The module API is specified in [contracts/public-api.md](contracts/public-api.md). Defaults
and lifecycle states are in [data-model.md](data-model.md).

## Worker example

The module README should contain a runnable equivalent of this short lifecycle example:

```go
ctx := context.Background()
client, err := storesql.New(ctx, storesql.Config{DSN: dsn})
if err != nil { return err }
defer client.Close(ctx)
db, err := client.DB(ctx)
if err != nil { return err }
if err := db.Exec("SELECT 1").Error; err != nil { return err }
return client.Ping(ctx)
```

The example contains fewer than 15 Go statements from Config through use and Close. A real
worker uses its own shutdown context for `Close` and its own repository models.

## Validation commands

From the repository root after implementation:

```sh
make lint
make test
make test-race
make test-cover
make test-integration
```

Expected: lint and unit tests pass without an external service; race tests pass with
`-race -count=1`; coverage report shows at least 70% for infrastructure wrappers; the
integration target starts PostgreSQL through Testcontainers and passes.

## Scenario checks

1. Run the unit configuration table. Invalid DSN or pool settings fail before a ping; zero
   optional settings become the documented defaults.
2. Run the startup and health checks. An unreachable database makes `New` fail with a
   wrapped error; a later loss is reported by `Ping(ctx)`.
3. Run transaction tests. Nil callback commits; returned error and panic roll back; nested
   calls reuse the outer transaction. A handled non-SQL nested error permits commit. A
   PostgreSQL statement error that aborts the transaction cannot be falsely reported as
   committed.
4. Run log-capture tests. Secret fixture values do not appear in any slog record or
   kit-generated connection/configuration error. Error and slow-query levels match the
   contract.
5. Run shutdown tests. New work is rejected after Close begins; active work can complete
   before deadline; a timeout is reported if work remains. Release blocked test work and
   confirm the closer finishes with no goroutine leak.
6. Run integration tests to prove duplicate-key identity and transaction behavior against
   a real PostgreSQL instance.
