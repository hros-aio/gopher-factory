# Public API Contract: event-manager

Module: `github.com/hros-aio/gopher-factory/event-manager` (`<org>` resolved from the sibling module). Root package name: `eventmanager`. The signatures below are design contracts for implementation; no `kgo` type may enter a root public signature.

## Root event and message contracts

```go
type Envelope[T any] struct {
    EventID       string  `json:"eventId"`
    EventType     string  `json:"eventType"`
    EventVersion  int     `json:"eventVersion"`
    TenantCode    string  `json:"tenantCode"`
    OccurredAt    string  `json:"occurredAt"`
    Producer      string  `json:"producer"`
    CorrelationID *string `json:"correlationId,omitempty"`
    CausationID   *string `json:"causationId,omitempty"`
    TraceID       *string `json:"traceId,omitempty"`
    Payload       T       `json:"payload"`
}

type Message struct {
    Topic     string
    Partition int32
    Offset    int64
    Key       []byte
    Value     []byte
    Envelope  *Envelope[json.RawMessage]
}

type Handler func(context.Context, Message) error
type Middleware func(Handler) Handler

// Envelope[T] implements Event for every payload type T.
type Event interface {
    ToRaw() (Envelope[json.RawMessage], error)
}
```

Optional JSON null and absent fields both decode to nil. Publishing fills the optional correlation ID when absent. For a malformed record, `Envelope` is nil and the business handler is not called.

## Root lifecycle contracts

```go
type Publisher interface {
    Publish(ctx context.Context, topic, key string, event Event) error
    Ping(ctx context.Context) error
    Close(ctx context.Context) error
}

type Subscriber interface {
    Register(topic string, handler Handler) error
    Run(ctx context.Context) error
    Ping(ctx context.Context) error
    Close(ctx context.Context) error
}
```

`Envelope[T].ToRaw` JSON-encodes its payload and returns a library-independent raw envelope; a value or pointer can satisfy `Event`. The concrete publisher prepares a copy, validates required fields, fills missing `eventId`, `occurredAt`, and `correlationId`, waits for all-ISR acknowledgment, and preserves `errors.Is` identity in returned errors. `producer` defaults to configured client ID if empty. A timeout may mean broker acceptance was ambiguous.

`Register` accepts one handler per exact topic before `Run`. `Run` is single-use per client and blocks until cancellation or failure. Clean cancellation after drain returns nil; failed processing, commit, or timeout returns an identifiable error. `Close` is repeat-safe and bounded by the context or configured shutdown timeout.

The root offers `WithCorrelationID(ctx, id)` and `CorrelationID(ctx)`, plus `Topic(domain, event string, version int) (string, error)` and `MessageKey(tenantID, aggregateID string) (string, error)`. A global key is `:<aggregate_id>`.

## Root middleware and errors

```go
func Chain(h Handler, middleware ...Middleware) Handler
func Recover() Middleware
func Logging(logger *slog.Logger) Middleware
func Retry(cfg RetryConfig) Middleware
func DeadLetter(pub Publisher, topic string) Middleware
func Idempotency(store IdempotencyStore) Middleware

type IdempotencyStore interface {
    Completed(ctx context.Context, eventID string) (bool, error)
    MarkCompleted(ctx context.Context, eventID string) error
}
```

`Chain` treats the first middleware as the outermost. The recommended order is `Logging, Idempotency, DeadLetter, Retry, Recover`. `RetryConfig` has `MaxAttempts`, `InitialBackoff`, and `MaxBackoff`; defaults are 3 total attempts, 200 ms, and 2 s, using capped exponential backoff with full jitter. Only errors wrapped with `MarkRetryable(err)` are retried; `MarkPermanent(err)` and unclassified errors bypass retry. Classification wrappers retain the original error for `errors.Is`. A successful DLQ acknowledgment returns terminal success to the subscriber; an unsuccessful one returns an error. Errors such as `ErrClosed`, `ErrInvalidConfig`, `ErrPublish`, `ErrProcess`, `ErrCommit`, and `ErrShutdownTimeout` are stable identities, with the original cause wrapped.
`RetryConfig.Validate()` applies zero-value defaults and rejects negative attempt/duration values or a maximum backoff below the initial backoff.

An idempotency store performs completion check and mark; it does not provide an atomic claim. The caller must supply a stronger claim or transaction if concurrent business effects must be excluded even when a handler ignores cancellation.

## Kafka adapter

```go
package kafka

type Config struct {
    Brokers              []string
    ClientID             string
    GroupID              string
    ConsumerEnabled      bool
    TLS                  *tls.Config
    SASL                 *SASLConfig
    DialTimeout          time.Duration
    PublishTimeout       time.Duration
    HandlerTimeout       time.Duration
    PollProcessingTimeout time.Duration
    RebalanceTimeout     time.Duration
    ShutdownTimeout      time.Duration
    MaxPollRecords       int
    DLQTopic             string
}

func (Config) Validate() error
func New(ctx context.Context, cfg Config, opts ...Option) (*Client, error)
```

`*Client` implements both root interfaces. Set `ConsumerEnabled` for group consumption; then `GroupID` is required by `Validate()` before `New` dials. Publisher-only use leaves both unset. At least one handler must be registered before `Run`. `SASLConfig` selects PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512 with explicit username/password; validation redacts them. `WithLogger(*slog.Logger)` injects logging and defaults to `slog.Default()`. Configuration is supplied directly; no environment variable is read.

| Optional setting | Default | Validation |
|---|---:|---|
| DialTimeout | 5 s | Positive |
| PublishTimeout | 10 s | Positive |
| HandlerTimeout | 10 s | Positive |
| PollProcessingTimeout | 45 s | Positive and below RebalanceTimeout with at least 10 s margin |
| RebalanceTimeout | 60 s | Greater than PollProcessingTimeout plus margin |
| ShutdownTimeout | 30 s | Positive |
| MaxPollRecords | 32 | 1–1,000 |

Zero values mean default; negative values fail. Brokers and client ID are required. A configured DLQ topic must pass the topic-name validator and differ from subscribed source topics. The adapter clones TLS configuration and does not log credentials. `Config.Validate` performs no network I/O.

## Test fakes

`eventtest.FakePublisher` implements root `Publisher`, records publishes in order, permits an injected error/ack outcome, and provides deterministic `Ping`/`Close`. `eventtest.FakeSubscriber` implements root `Subscriber`, supports topic registration and controlled message delivery, and records handler results and commit eligibility. Fake helpers are additional methods on concrete types; production callers depend only on root interfaces. Neither fake imports Kafka or starts goroutines.
