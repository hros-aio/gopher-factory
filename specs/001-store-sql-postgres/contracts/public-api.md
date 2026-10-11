# Public API Contract: store-sql

Module: `github.com/hros-aio/gopher-factory/store-sql` (package name `storesql`).
This is a library contract. No HTTP, CLI, or service startup surface is added.

```go
type Config struct {
    DSN                string
    MaxOpenConns       int
    MaxIdleConns       int
    ConnMaxLifetime    time.Duration
    ConnMaxIdleTime    time.Duration
    SlowQueryThreshold time.Duration
}

func (c *Config) Validate() error

type Option func(*options) error

func WithLogger(logger *slog.Logger) Option
func WithPlugin(plugin gorm.Plugin) Option

func New(ctx context.Context, cfg Config, opts ...Option) (*Client, error)

func (c *Client) Ping(ctx context.Context) error
func (c *Client) Close(ctx context.Context) error
func (c *Client) DB(ctx context.Context) (*gorm.DB, error)
func (c *Client) WithinTx(ctx context.Context, fn func(context.Context) error) error

var ErrClosed error
var ErrInvalidConfig error
```

The concrete option representation is private; the signatures above document caller-visible
types. Exported declarations receive Go doc comments in source.

## Behavioral guarantees

- `Validate` fills defaults on the receiver and rejects invalid fields before network I/O.
  `New` validates its Config copy, so the caller's value is unchanged.
- `New` performs a context-aware startup ping. Invalid configuration, plugin failure, and
  connectivity failure return no usable Client; opened resources are closed.
- `Ping`, `DB`, and `WithinTx` reject nil or canceled contexts and reject calls after
  closing begins. `DB(ctx)` returns a context-bound session. Callers must use finite
  operation contexts and finish borrowed sessions before shutdown.
- `WithinTx` implements the consumer-owned `TxManager` method signature. Only the
  outermost call starts and ends the database transaction. A nested call runs in the same
  context and uses no savepoint.
- A handled nested callback error permits commit only while PostgreSQL still considers the
  transaction valid. A failed statement can abort it; a resulting commit failure is returned.
- `Close` is safe to call repeatedly. It stops new work and drains active work until the
  caller's deadline. If the deadline wins, it returns an error matching `ctx.Err()` while
  final closure proceeds in the background; a later `Close` can observe completion.
- Kit-returned errors preserve useful identities through `%w`. Duplicate-key errors satisfy
  `errors.Is(err, gorm.ErrDuplicatedKey)`. Kit-generated connection and configuration errors
  never include a DSN password or bound parameter value. Repository code owns handling of
  errors returned by operations on the GORM handle.
- Log records never contain raw SQL, bound parameters, DSNs, or raw database error strings.
  Error records use slog Error; traces over the slow threshold use Warn.

## Non-contracts

No schema migration, repository base type, Fx module, read replica, multi-database router,
or sibling-kit import is provided.
