# store-sql

`store-sql` is a PostgreSQL connection kit for Go worker services. It owns one GORM-backed
connection pool and leaves repositories, schema management, and service wiring to callers.

Module: `github.com/hros-aio/gopher-factory/store-sql` (Go 1.23+).

## Quick start

```go
func run(ctx context.Context, dsn string) error {
    client, err := storesql.New(ctx, storesql.Config{DSN: dsn})
    if err != nil { return err }
    defer func() { _ = client.Close(context.Background()) }()
    db, err := client.DB(ctx)
    if err != nil { return err }
    if err := db.Exec("SELECT 1").Error; err != nil { return err }
    return client.Ping(ctx)
}
```

This example assumes imports for `context` and this module as `storesql`.

Pass the DSN from your worker's own configuration layer. The kit never reads environment
variables. Use a finite shutdown context for `Close` in production code.

## Configuration

| Field | Default when zero | Rule |
|---|---:|---|
| `DSN` | none | Required, valid PostgreSQL DSN |
| `MaxOpenConns` | 10 | Positive after defaulting |
| `MaxIdleConns` | min(5, MaxOpenConns) | No greater than MaxOpenConns |
| `ConnMaxLifetime` | 30m | Positive after defaulting |
| `ConnMaxIdleTime` | 5m | Positive after defaulting |
| `SlowQueryThreshold` | 200ms | Positive after defaulting |

`(*Config).Validate()` fills defaults on its receiver and checks settings without opening a
connection. `New` validates a copy, so it does not mutate your Config. A zero optional value
means “use the default”; negative values are invalid.

## Transactions

The application can own this small interface:

```go
type TxManager interface {
    WithinTx(ctx context.Context, fn func(context.Context) error) error
}
```

Repositories call `client.DB(ctx)` for each operation. Inside `WithinTx`, the returned
`*gorm.DB` joins the transaction carried in `ctx`. A nested `WithinTx` joins the outer
transaction without creating a savepoint. The outermost callback commits on nil and rolls
back on error or panic. Handling a nested callback error permits commit only if PostgreSQL
still considers the transaction valid; a failed SQL statement can abort it.

## Logging and plugins

`WithLogger(*slog.Logger)` changes the logger; the default is `slog.Default()`.
`WithPlugin(gorm.Plugin)` registers an additional GORM plugin during construction.
Database failures use Error level and queries above `SlowQueryThreshold` use Warn level.
Log records omit SQL, bound values, raw driver errors, and DSNs.

## Shutdown

`Close(ctx)` stops new client work and lets active work finish until the shutdown context
ends. A deadline returns a context error while physical pool closure continues. A later
`Close` call can wait for completion. Borrowed GORM handles are for operations that finish
before shutdown; use finite operation contexts and do not retain them beyond the client.

## Development

From the repository root:

```sh
make lint
make test
make test-race
make test-cover
make test-integration
```

The integration target requires a dedicated PostgreSQL database. Set
`STORE_SQL_TEST_DSN` to its connection string before running `make test-integration`.
CI starts PostgreSQL as a GitHub Actions service.
Schema migrations are outside this module.

See the [public API contract](../specs/001-store-sql-postgres/contracts/public-api.md)
and [design plan](../specs/001-store-sql-postgres/plan.md) for the complete behavior.
