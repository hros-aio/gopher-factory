<!--
Sync Impact Report
- Version change: unratified template → 1.0.0 (initial constitution).
- Modified principles: template placeholders → I–X below.
- Added sections: Technology and Recorded Decisions; Development Workflow and Quality Gates.
- Removed sections: none; template placeholder sections were filled.
- Follow-up TODO: The four requested ./docs files were absent from this checkout.
  Compare them with this constitution when available and add any further differences.
-->
# gopher-factory Constitution

## Core Principles

### I. Library-First Lifecycle

gopher-factory is a library of reusable infrastructure kits for Go worker services, a sibling
of the Node.js `apis-factory` kit. It MUST NOT contain service startup behavior. Kits MUST NOT
use global mutable state, `init()` side effects, or `os.Getenv`. Each kit MUST construct its
client through `New(ctx, cfg, ...Option)` and expose `Ping(ctx)` and `Close(ctx)`. Configuration
and dependencies MUST be supplied explicitly so callers control lifecycle and environment.

### II. Independent Modules

The repository MUST use `go.work` and one `go.mod` per kit: `store-sql`, `store-mongo`, and
`event-manager`. A kit MUST NOT import another kit. Each module MUST be released and tagged
independently, for example `store-sql/v0.1.0`. This boundary keeps kits reusable and releases
independent.

### III. Dependency Isolation

Each module MUST depend only on packages needed for its own function. A service importing
PostgreSQL support alone MUST NOT acquire MongoDB or Kafka dependencies through module imports.
Optional integrations MUST remain outside core module dependency paths.

### IV. Consumer-Owned Contracts

Interfaces MUST be defined on the consumer side and kept to the operations the consumer uses.
Kafka implementation types from `franz-go` MUST remain inside `event-manager/kafka`. The public
messaging API MUST expose only gopher-factory types such as `Envelope`, `Message`, `Handler`,
`Publisher`, and `Subscriber`. This prevents implementation dependencies from becoming public
contracts.

### V. Context and Error Semantics

`context.Context` MUST be the first parameter of every I/O operation. Errors MUST be wrapped
with `%w` when adding context and compared with `errors.Is`; code MUST NOT compare error strings.
These rules preserve cancellation and machine-readable failure behavior.

### VI. Safe, Injected Logging

Logging MUST use `log/slog`, injected through an `Option`, with `slog.Default()` as the default.
Kits MUST NOT log secrets, DSNs containing passwords, tokens, or complete payloads containing
PII. Logging MUST preserve useful operational context without disclosing sensitive data.

### VII. External Schema Management

`AutoMigrate` MUST NOT be used anywhere in this repository. Schema management is outside this
library's scope; callers own migration planning and execution.

### VIII. Testable Behavior

Every non-generated `.go` file MUST have a matching `_test.go` file. Tests MUST be table-driven
where cases vary, use `testify/require` for setup and `testify/assert` for results, avoid
`time.Sleep` for synchronization, and use `goleak` to detect goroutine leaks. Unit tests MUST
not use real external services. Integration tests MUST live under `test/integration` and use
`testcontainers-go`. Pure logic MUST reach at least 85% coverage; infrastructure wrappers
MUST reach at least 70% coverage.

### IX. Stable Public APIs

Each module MUST follow semantic versioning and maintain its own `CHANGELOG.md`. Every exported
identifier MUST have a doc comment. Breaking public API changes MUST receive a major version
bump for the affected module. Independent release records let consumers upgrade deliberately.

### X. Reliable Messaging

Messaging MUST default to at-least-once delivery. Consumer offsets MUST be committed only after
the handler succeeds. Graceful shutdown MUST finish or safely leave in-flight messages for
redelivery without hanging. These guarantees govern the default Kafka implementation.

## Technology and Recorded Decisions

- The minimum supported Go version is 1.23.
- PostgreSQL support MUST use GORM with `gorm.io/driver/postgres`.
- MongoDB support MUST use `go.mongodb.org/mongo-driver/v2`.
- Kafka support MUST use `github.com/twmb/franz-go`.
- Uber Fx MUST NOT be a dependency of any core module. Any future Fx support MUST live in a
  separate optional package and preserve dependency isolation.

### ADR-001: Use franz-go for Kafka

**Decision:** `event-manager/kafka` uses `franz-go`. This supersedes the Sarama choice stated
for this repo in `docs/tech-stack.md`. The driver stays behind gopher-factory messaging types
so consumers do not depend on Kafka library types.

### Differences from Referenced Docs

- `docs/tech-stack.md`: its Sarama selection is replaced by `franz-go` for this repository
  (ADR-001).
- `docs/coding-conventions.md`, `docs/testing-strategy.md`, `docs/implementation-rules.md`,
  and `docs/tech-stack.md` were absent from the checkout when this constitution was ratified.
  Further differences cannot be verified until those documents are available.

## Development Workflow and Quality Gates

For every feature, the affected modules MUST compile, pass `golangci-lint`, and pass
`go test -race -count=1`. CI MUST run these checks and produce a coverage report for each
module, enforcing the coverage floors in Principle VIII. The affected module README MUST include
a minimal `New` → use → `Close` example, its `CHANGELOG.md` MUST be updated, and exported API
MUST have doc comments. Integration tests MUST verify behavior against containerized services
when the feature depends on a database or broker contract.

## Governance

This constitution takes precedence over other repository guidance, including the referenced
`docs` files where they conflict. Proposed amendments MUST identify the affected principles,
the reason for change, compatibility impact, and any migration work. Reviewers MUST verify
constitution compliance for each feature and record justified exceptions in its review or
design artifacts; an exception does not amend the constitution.

Constitution changes MUST follow semantic versioning: MAJOR for incompatible removals or
redefinitions of governance or principles, MINOR for new principles or materially expanded
requirements, and PATCH for clarifications without a behavioral change. The amendment MUST
update the version, amendment date, and Sync Impact Report. Module versions remain independent
of the constitution version.

**Version**: 1.0.0 | **Ratified**: 2026-10-11 | **Last Amended**: 2026-10-11
