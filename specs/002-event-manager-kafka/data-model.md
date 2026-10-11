# Data Model: Event Manager Kafka Kit

## Envelope[T]

The root generic event value maps to the checked-in Node JSON contract. It is a value object prepared on a copy before publish.

| Go concept | JSON field | Type | Rule |
|---|---|---|---|
| EventID | `eventId` | string | UUID v4; generated if empty |
| EventType | `eventType` | string | Required nonempty type supplied by caller |
| EventVersion | `eventVersion` | integer | Required positive schema version |
| TenantCode | `tenantCode` | string | Required field; empty string means global event |
| OccurredAt | `occurredAt` | RFC 3339 string | Generated in UTC if empty; caller value preserved |
| Producer | `producer` | string | Required producing service; defaults to configured client ID if omitted |
| CorrelationID | `correlationId` | optional nullable string | Existing value, context value, then generated UUID |
| CausationID | `causationId` | optional nullable string | Caller-supplied, absent/null accepted |
| TraceID | `traceId` | optional nullable string | Caller-supplied, absent/null accepted |
| Payload | `payload` | T | Required JSON-encodable business data |

Absent and JSON null optional string fields normalize to a nil Go value; a present string retains its value. Cross-language fixtures compare the shared values after normalization. The producer validates fields before sending; malformed consumed envelopes retain their raw bytes for DLQ routing.

## Message

`Message` represents one broker record. It has topic, partition, offset, key bytes, raw value bytes, and a decoded `Envelope[json.RawMessage]` when valid. Topic-partition-offset identifies the source position; `eventId` identifies the business event across redelivery and supports idempotency. A malformed record has no decoded envelope, so the handler is not invoked.

## Dead-letter record

The DLQ publication is itself a Node-compatible envelope whose `payload` includes original topic, partition, offset, key, raw value, decoded original envelope when available, failure class, safe failure message, attempt count, and failure time. Original raw bytes are encoded in JSON using the standard byte representation. The outer envelope has its own event ID; the payload's source coordinates and original event ID support deduplication and replay. The failure message must not contain credentials.

## Offset frontier

For each topic-partition in a poll batch, the frontier starts before the first record. It advances only after successful business handling, confirmed idempotent skip, or acknowledged DLQ publication. A failed or unfinished record freezes that partition's frontier; later records in that partition are not handled in the same poll. A synchronous commit stores the next offset after the last frontier record. Other partitions have independent frontiers and may commit while one fails.

## Processing and lifecycle states

| Entity | States | Transitions |
|---|---|---|
| Message | fetched → decoding → handling → completed / failed → DLQ pending → DLQ acknowledged / unresolved | Only completed or DLQ acknowledged enters the commit frontier |
| Publisher/client | open → closing → closed | Closing rejects new publication and registration; repeated close waits on the same completion |
| Subscriber run | idle → running → draining → closed | Failure or cancellation stops fetching; bounded drain commits completed work and releases the rebalance barrier |
| Idempotency event | unknown → completed | Completion is recorded only after handler or terminal DLQ success; lookup/write failures leave work unresolved |

## Validation and identity

- Topic names follow `<domain>.<event>.v<n>` with nonempty domain/event and positive version. The topic builder rejects separators that would make the three components ambiguous.
- Keys follow `<tenant_id>:<aggregate_id>`; a global event uses `:<aggregate_id>`. An aggregate ID is required.
- Registration allows one handler per exact topic before `Run`; duplicate or late registration fails clearly.
- DLQ topic must differ from source topics to avoid accidental processing loops and must satisfy the same topic-name validator.
