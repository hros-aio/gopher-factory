# Research: Event Manager Kafka Kit

## ADR-001 — Use franz-go instead of Sarama

**Decision:** Implement Kafka transport with `github.com/twmb/franz-go/pkg/kgo` behind the root package contracts. This records and applies [constitution ADR-001](../../.specify/memory/constitution.md); it supersedes the older Sarama choice attributed there to `docs/tech-stack.md`.

**Rationale:** The constitution requires franz-go, and its producer, consumer-group, manual-commit, and rebalance APIs cover this kit without exposing transport types in the root API. [Franz-go's official documentation](https://github.com/twmb/franz-go/blob/master/docs/producing-and-consuming.md) describes explicit offset management and blocked rebalance processing.

**Alternatives considered:** Sarama conflicts with the ratified constitution; Kafka transactions exceed the specified at-least-once scope.

**Consequence:** The `kafka/` package owns all `kgo` imports, translation, and lifecycle behavior. Root contracts and `eventtest/` remain broker-library independent.

## Version compatibility

**Decision:** Plan `go 1.23.0`, franz-go `v1.18.1`, and Testcontainers Go core plus Kafka module `v0.38.0`; verify the resolved module graph with Go 1.23 during implementation. `testify` and `goleak` are test dependencies only.

**Rationale:** [Franz-go v1.18.1](https://raw.githubusercontent.com/twmb/franz-go/v1.18.1/go.mod) declares Go 1.21, while [v1.19.0](https://raw.githubusercontent.com/twmb/franz-go/v1.19.0/go.mod) requires Go 1.23.8 and [v1.20.5](https://raw.githubusercontent.com/twmb/franz-go/v1.20.5/go.mod) requires Go 1.24. [Testcontainers Go v0.38.0](https://raw.githubusercontent.com/testcontainers/testcontainers-go/v0.38.0/go.mod) declares Go 1.23.0. The existing sibling module already uses Testcontainers v0.38.0. The exact franz-go APIs cited below must be checked against the pinned tag during implementation, not assumed from newer master documentation.

**Alternatives considered:** Current library versions would raise the minimum Go toolchain beyond the requested 1.23 baseline.

## Consumer offsets and concurrency

**Decision:** Configure `kgo.ConsumerGroup`, `kgo.DisableAutoCommit()`, and `kgo.BlockRebalanceOnPoll()`. Poll a bounded batch with `PollRecords`. Group the batch by topic-partition, process each partition sequentially in one worker and different partitions concurrently, wait for all workers, synchronously `CommitRecords` through the last successful or DLQ-acknowledged record per partition, and always call `AllowRebalance` before the next poll or close. A failure without DLQ handoff ends `Run` after the bounded drain.

**Rationale:** [Franz-go's consumption guide](https://github.com/twmb/franz-go/blob/master/docs/producing-and-consuming.md) warns that manual commits need explicit rebalance handling and recommends a bounded poll when using blocked rebalance. [The kgo API](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#BlockRebalanceOnPoll) describes the rebalance block and release; [`CommitRecords`](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Client.CommitRecords) commits synchronously. Sequential work per partition makes a contiguous commit frontier simple; a failed record stops later records in that partition.

**Tradeoff:** A long handler or retry cycle may exceed the broker's rebalance timeout and cause group eviction despite the local block. Configuration must bound poll size and processing time below rebalance timeout with margin. Cancellation-aware handlers receive the no-overlap guarantee; non-cooperative handlers remain a documented caller risk. [`CloseAllowingRebalance`](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Client.CloseAllowingRebalance) is available for close after workers settle.

**Alternatives considered:** Free-running partition goroutines across polls complicate ownership and commits. Automatic commits can acknowledge unprocessed work. Serializing all partitions unnecessarily limits throughput.

## Producer acknowledgment and close

**Decision:** Configure `kgo.RequiredAcks(kgo.AllISRAcks())`, leave idempotent writes enabled, serialize a validated Node-compatible envelope, and use `ProduceSync(ctx, record)` for caller-visible acknowledgment. On close, stop new sends, call `Flush(ctx)`, then close the client. `Ping(ctx)` probes broker reachability.

**Rationale:** Franz-go documents [all in-sync replica acknowledgments](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#AllISRAcks), [idempotent writing](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#DisableIdempotentWrite), [synchronous production](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Client.ProduceSync), and [flush](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Client.Flush). A timed-out publish can still have reached the broker, so callers must treat ambiguous outcomes as possible duplicates.

**Alternatives considered:** Fire-and-forget production cannot meet the acknowledgment requirement. Kafka transactions are out of scope.

## Wire contract and failure policy

**Decision:** Match the checked-in Node `EventEnvelope<T>` JSON fields exactly: `eventId`, `eventType`, `eventVersion`, `tenantCode`, `occurredAt`, `producer`, `payload`, and optional nullable `correlationId`, `causationId`, `traceId`. Use fixture tests against [the sibling interface](../../../apis-factory/libs/libs-events/src/interfaces/event-payload.interface.ts). The root `Envelope[T]` carries these values; the Kafka mapper performs JSON encoding and decoding.

**Decision:** Build middleware as `Logging(Idempotency(DeadLetter(Retry(Recover(handler)))))`. Only explicit retryable errors get inline attempts. Permanent or unclassified errors go to DLQ when configured. A malformed envelope bypasses the handler and sends raw bytes plus decode failure to DLQ; commit requires its acknowledgment. Without successful handoff, stop `Run` and leave the offset uncommitted.

**Tradeoff:** A DLQ publish acknowledged by the broker but reported as failed may duplicate on source redelivery. The dead-letter record therefore includes original topic, partition, offset, key, raw value, failure class, attempt count, and time; its stable source coordinates support deduplication by downstream operators.

## Integration validation

**Decision:** Use the [Testcontainers Go Kafka KRaft module](https://golang.testcontainers.org/modules/kafka/) under `event-manager/test/integration`, pinned with the Testcontainers core module. Use a versioned compatible image and create topics in test setup only. Unit tests use `eventtest` and injected internal transport seams; no unit test needs a broker.

**Rationale:** A real broker verifies group assignment, committed offsets, redelivery, DLQ records, and shutdown. A single-broker container validates the configured all-ISR path with one replica; a unit assertion on producer options covers the required acks setting. Multi-broker ISR failure simulation is outside the initial integration suite.

**Alternatives considered:** A fake broker cannot prove group or offset behavior; a persistent shared broker makes tests harder to reproduce.
