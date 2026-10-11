# Data Model: Store SQL PostgreSQL Kit

This module owns connection configuration and lifecycle state. It does not own application
tables or schema.

## Config

| Field | Type | Default when zero | Validation |
|---|---|---:|---|
| DSN | string | none | Required; parse as PostgreSQL DSN without network I/O; never echo value in errors |
| MaxOpenConns | int | 10 | Must be positive after defaults |
| MaxIdleConns | int | min(5, MaxOpenConns) | Must be nonnegative and no greater than MaxOpenConns |
| ConnMaxLifetime | time.Duration | 30m | Must be positive after defaults |
| ConnMaxIdleTime | time.Duration | 5m | Must be positive after defaults |
| SlowQueryThreshold | time.Duration | 200ms | Must be positive after defaults |

`(*Config).Validate() error` mutates the Config value it is called on by filling defaults.
`New` receives Config by value and validates its local copy. Explicit negative values are
invalid; zero means omitted for the optional fields.

## Client

Fields are private: GORM database handle, underlying pool, logger configuration, lifecycle
state, active-work count, and close-completion signal. A Client owns one PostgreSQL pool.
It never owns repository models or migrations.

| State | Allowed behavior | Transition |
|---|---|---|
| open | New client work accepted; Ping and DB usable | Close begins → closing |
| closing | New client work rejected; already-active work drains; Close callers wait | Pool close completes → closed |
| closed | New work rejected; repeated Close returns recorded close outcome | Terminal |

The close context bounds the wait for a caller. If it expires, `Close` returns a deadline
error while the single pool closer continues; the state remains closing until the pool is
actually closed.

## Transaction context

A private context value contains the owning Client identity and active `*gorm.DB` transaction.
Only `WithinTx` creates it. `DB(ctx)` reads it only for the matching Client; otherwise it
returns a normal context-bound session or rejects a foreign transaction context. Nested
`WithinTx` reuses the same value and does not start a second transaction.

The outermost transaction begins once and ends once. Nil callback result requests commit;
an error requests rollback; panic requests rollback and re-panics. A PostgreSQL statement
error can leave the transaction aborted even if the callback error is handled; a failed
commit is surfaced.

## Option

An Option configures the client before connection setup. `WithLogger` selects a non-nil
slog logger; otherwise `slog.Default()` is used. `WithPlugin` adds a non-nil GORM plugin.
Options do not read environment variables or mutate process-wide state.
