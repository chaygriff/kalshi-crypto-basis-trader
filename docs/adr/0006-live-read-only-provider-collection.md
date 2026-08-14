# ADR 0006: Live read-only provider collection

## Status

Accepted as the current Phase 1 implementation plan; not yet implemented in this repository.

## Context

Fixture-backed ingestion established strict identity, parsing, pagination, snapshot, replay, and gap semantics, but it cannot validate real provider behavior. Production Kalshi signed REST and authenticated WebSocket connectivity were successfully exercised through the separate `kalshi-exa-research` reference implementation using external owner-only credentials and no mutation request. That result proves connection prerequisites, not this repository's transport, collection, persistence, or replay path.

The project will establish live read-only collection during Phase 1 so contract equivalence and options modeling are built from retained provider evidence rather than fixtures alone.

## Decision

### Separate runtime layers

Implement and review these capabilities in order:

1. provider-specific concrete HTTP transports;
2. bounded one-shot collectors;
3. durable restart-safe evidence storage;
4. supervised live smoke checks through the complete collection path;
5. recurring scheduler;
6. long-running service lifecycle and health reporting; and
7. WebSocket synchronization where latency requirements justify it.

The first four stages are current Phase 1 work. Scheduling, service operation, and streaming remain separate work items and stay disabled until the preceding acceptance gates pass.

### Kalshi transport

The Kalshi transport implements only the existing GET capability required by the ingestion domain. It uses an exact reviewed production origin, HTTPS with certificate verification, disabled redirects, strict path construction, bounded connect/read timeouts, bounded response and decompressed size, exact raw response bytes, and local receipt clocks. Signing is supplied only to a read process and does not add order, cancel, amend, account-mutation, generic request, or arbitrary-URL methods.

The one-shot collector uses reviewed BTC/ETH Series configuration, traverses bounded discovery and linked evidence routes, persists each raw response and validated snapshot, produces a deterministic completeness/gap report, and exits. A successful network response is not automatically a complete collection.

### Deribit transport

Initial Deribit instrument, index, ticker, summary, and order-book collection uses documented public read-only interfaces and no credentials unless a later reviewed requirement proves them necessary. Native instrument identity, expiry, strike, option type, lifecycle, quote sizes, index/underlying references, open interest, provider clocks, and exact raw evidence are retained.

The one-shot options collector declares its expected instrument set and synchronization window before completion. Missing, stale, conflicting, or temporally incoherent observations produce an incomplete batch and explicit gaps; they are never silently dropped from a claimed complete surface.

### Durability and recurring operation

The accepted in-memory snapshot store remains the reference implementation, not recurring live storage. Durable storage must preserve content identities, exact raw bytes, parser lineage, idempotency, atomic run completion, and append-only recovery. Recurring collection cannot start until crash/restart, corruption, idempotent rerun, retention, and backup behavior pass review.

A scheduler may invoke only the same reviewed one-shot commands. It must prevent overlapping runs, bound retries and backlog, preserve every terminal run state, and expose missed-run health. A service entry point adds no provider or financial authority; it owns configuration validation, process lifecycle, graceful shutdown, and health/readiness only.

WebSocket data supplements but does not replace the REST evidence baseline. After disconnect, sequence uncertainty, or overflow, local stream state is discarded and remains unusable until a fresh authoritative snapshot is retained.

## Credential and authority boundary

- Credentials remain outside Git and are loaded only by the narrowly scoped read process that requires them.
- CI, deterministic fixtures, and independent reviewers remain credential-free.
- Logs and reports never include credential values, signatures, authorization headers, balances, private portfolio values, or private account identifiers.
- Connection success, authenticated reads, collection completion, GitHub approval, and documentation approval confer no forecasting, recommendation, authorization, submission, cancellation, or trading authority.
- No production mutation is used to validate credentials or read architecture.

## Verification order

1. Unit tests for signing, strict parsing, origin/path policy, and capability absence.
2. Local HTTP-server tests for redirects, timeouts, body limits, content types, malformed evidence, rate limits, pagination, and partial runs.
3. Durable-store tests for raw identity, atomic completion, restart, rerun, corruption, and incomplete recovery.
4. Credential-present tests proving zero mutation methods and zero mutation requests.
5. Independent exact-tree review.
6. One owner-authorized, bounded live read-only smoke check that reports only redacted status, schema, clocks, counts, and gaps.
7. Replay the retained live observation through the same parser and compare normalized identity before enabling repeated collection.

## Consequences

Phase 1 gains earlier evidence about real provider schemas, clocks, rate limits, data gaps, collection duration, synchronization quality, and storage volume. The additional transport and operational scope increases tests and review work, but reduces the risk of building contract equivalence and models on unrealistic fixtures. Scheduler, service, streaming, modeling, recommendations, and mutation remain independently gated capabilities.
