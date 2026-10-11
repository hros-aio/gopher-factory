# event-manager

Kafka event publishing and group consumption for Go workers. The module path is `github.com/hros-aio/gopher-factory/event-manager`; the Kafka adapter is in `/kafka` and test fakes are in `/eventtest`.

The wire format is the checked-in Node kit's JSON contract: `eventId`, `eventType`, `eventVersion`, `tenantCode`, `occurredAt`, `producer`, `payload`, and optional `correlationId`, `causationId`, `traceId`. An empty `tenantCode` represents a global event. Publishing fills missing event ID, timestamp, producer, and correlation ID on a copy. Publish waits for all in-sync replica acknowledgment with franz-go's idempotent producer default. A timed-out publish can still have reached Kafka; callers should deduplicate by event ID.

```go
cfg := kafka.Config{Brokers: []string{"localhost:9092"}, ClientID: "billing-worker", GroupID: "billing-workers", ConsumerEnabled: true, DLQTopic: "billing.dead.v1"}
client, err := kafka.New(ctx, cfg)
if err != nil { return err }
defer client.Close(context.Background())
handler := eventmanager.Chain(handle,
    eventmanager.Logging(logger),
    eventmanager.Idempotency(store),
    eventmanager.DeadLetter(client, cfg.DLQTopic),
    eventmanager.Retry(eventmanager.RetryConfig{}),
    eventmanager.Recover(),
)
if err := client.Register("billing.requested.v1", handler); err != nil { return err }
if err := client.Ping(ctx); err != nil { return err }
if err := client.Publish(ctx, "billing.started.v1", "tenant:aggregate", event); err != nil { return err }
return client.Run(ctx)
```

`handle` is `func(context.Context, eventmanager.Message) error`; `store` implements `IdempotencyStore`. For publisher-only use, omit `ConsumerEnabled` and `GroupID`. A `Run` uses one worker per partition in each poll, retaining order within each partition and parallelism across partitions. It commits only successful contiguous offsets or records acknowledged by the DLQ. An unresolved error stops the run, leaving the failed offset for redelivery on restart. Handlers must honor context cancellation for the rebalance non-overlap guarantee. A handler that ignores cancellation can outlive ownership; callers can use an atomic claim for business effects. The idempotency hook's check and mark are not an atomic transaction with handler work or Kafka commits.

Run `make event-manager-test`, `make event-manager-test-race`, `make event-manager-test-cover`, and `make event-manager-test-integration` from the repository root. The integration target requires Docker and starts a Kafka KRaft Testcontainer.
