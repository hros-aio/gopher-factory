# Changelog

Changes to `store-sql` are tracked independently from other gopher-factory modules.
Release tags use `store-sql/vMAJOR.MINOR.PATCH`.

## Unreleased

- Add validated PostgreSQL pool configuration and context-aware startup ping.
- Add readiness, context-bound GORM access, and graceful client closure.
- Add outermost transaction management with nested transaction joining.
- Add safe slog diagnostics, GORM plugin options, and duplicate-key translation.

The first release will use tag `store-sql/v0.1.0` after all quality gates pass.
