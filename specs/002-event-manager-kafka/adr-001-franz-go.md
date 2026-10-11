# ADR-001: Use franz-go for the Kafka adapter

**Status**: Accepted by the [project constitution](../../.specify/memory/constitution.md)
**Date**: 2026-10-11

## Context

The event-manager root API must remain independent of a Kafka client library while its implementation needs idempotent all-ISR production, consumer groups, manual commits, and rebalance control. The older Sarama choice attributed to `docs/tech-stack.md` conflicts with the ratified constitution.

## Decision

Use `github.com/twmb/franz-go/pkg/kgo` only inside `event-manager/kafka`. Pin a Go 1.23-compatible release for this feature. Root types and `eventtest` must not import franz-go.

## Consequences

- The adapter uses `DisableAutoCommit`, `BlockRebalanceOnPoll`, bounded `PollRecords`, synchronous commits, and explicit `AllowRebalance` to meet the specified at-least-once model.
- Producer configuration explicitly requires all in-sync replica acknowledgments and retains idempotent writes.
- Rebalance and shutdown code must pair every blocked poll with a release before close; a processing budget must remain below the rebalance timeout.
- The `kafka` package owns version compatibility checks and transport error mapping. Public contracts remain stable if the adapter changes later.

## Alternatives considered

- **Sarama:** Rejected because the constitution explicitly supersedes it.
- **Kafka transactions:** Rejected because exactly-once transactions are out of scope.

See [research.md](research.md) for primary-source API and version evidence.
