# ADR 0004: Immutable point-in-time snapshot schema

## Status

Accepted for Phase 1 schema version 1.

## Context

Kalshi and options data must be replayable without future metadata, float drift, or mutable parser output. Raw source evidence and normalized interpretations have different retention and identity requirements.

## Decision

Each snapshot envelope records:

- `schema_version` fixed to `1`;
- a source identifier;
- a SHA-256 canonical request fingerprint over HTTP method, path, and parameters;
- a source observation time and local ingestion time, both timezone-aware UTC with microsecond precision;
- a parser version;
- the SHA-256 of the exact raw response bytes;
- a recursively immutable normalized value tree;
- a content-derived snapshot ID; and
- an idempotency key.

Canonical JSON uses UTF-8, sorted object keys, no insignificant whitespace, exact integer values, and decimal values encoded as the reserved typed object `{"$decimal":"<exact text>"}`. This keeps decimal values distinct from ordinary strings and round-trips them as `Decimal`. Source-normalized mappings may not use the reserved `$decimal` key. Binary floating-point values and non-finite decimals are rejected.

The request fingerprint requires a non-empty unpadded HTTP method token, a query-free absolute path, and a mapping of request parameters. Query values are represented only in that canonical parameter mapping so the same request cannot acquire multiple identities through embedded query strings. The snapshot ID binds the complete envelope, including ingestion time and normalized output. The idempotency key binds source, request fingerprint, source observation time, parser version, and raw payload hash. A later retry with the same key and identical normalized output returns the first stored snapshot. Divergent normalized output for the same key is a parser nondeterminism conflict and must not overwrite evidence.

Raw response bytes are stored separately by SHA-256 and verified atomically when an envelope is inserted. The persistence boundary reconstructs a new validated, recursively frozen envelope from every supplied field and persists only that copy; it does not trust that caller initialization hooks ran. This also rejects objects fabricated through runtime initialization bypasses such as `object.__new__`. An envelope cannot be replayed without supplying bytes that match its raw hash.

Every envelope construction path is validation: direct construction and factory construction both freeze normalized input recursively and recompute the expected snapshot and idempotency identities. Deserialization additionally rejects duplicate JSON keys, non-canonical serialization, unknown or missing fields, raw-hash or identity mismatch, invalid clock order, non-UTC timestamps, and unsupported schema versions. Schema migration requires an explicit source-to-target path and validates the complete source envelope before returning a result; version 1 currently supports only identity migration to version 1.

## Consequences

- Adapters must capture exact raw bytes before parsing.
- Adapters must distinguish source observation time from local receipt/ingestion time.
- Normalizers use `Decimal` or integers, never floats.
- A parser-version change creates a distinct idempotency domain and never rewrites prior snapshots.
- The in-memory store is reference behavior for tests, parser development, and bounded replay tooling, not live collection durability.
- Durable restart-safe storage is now a Phase 1 prerequisite for recurring collection. Its implementation remains a separate reviewed work item and must preserve these identities, raw-byte checks, idempotency, append-only lineage, and replay semantics.
