# Kalshi Crypto Basis Trader

Public, audit-first implementation of an option-implied digital-basis research and eventual owner-authorized execution system for Kalshi crypto event contracts through an owner-restricted WhatsApp interface.

## Goal

Launch a production-ready Kalshi crypto trading account workflow that is reachable through WhatsApp while keeping research, signal generation, portfolio construction, owner authorization, order transport, and reconciliation as separate security boundaries.

## Current status

**Phase 0 governance and the first two Phase 1 foundations are complete. Live trading is disabled.** The repository now contains the immutable snapshot schema and the abstract GET-only Kalshi BTC/ETH ingestion boundary. It does not yet contain a concrete HTTP transport, one-shot collector, scheduler, service entry point, or mutation transport.

Production Kalshi REST and authenticated WebSocket connectivity have been verified through the separate `kalshi-exa-research` reference implementation without exposing credentials or making a mutation request. That verifies the external connection prerequisites, not this repository's ingestion architecture. The current Phase 1 plan is therefore to wire provider-specific live read-only HTTP transports and bounded one-shot collectors through this repository's immutable evidence boundary, beginning with Kalshi and Deribit. Durable restart-safe storage precedes recurring collection; scheduling, long-running service lifecycle, and streaming synchronization follow only after reviewed one-shot collection succeeds.

No issue, pull request, project-card movement, documentation change, agent message, model output, data connection, or successful read authorizes an order.

## Proposed strategy

For exactly matched BTC and ETH terminal-price contracts:

1. Build an arbitrage-constrained terminal distribution from a liquid options surface.
2. Convert the risk-neutral distribution into a walk-forward calibrated real-world forecast.
3. Compare the forecast interval with depth-weighted Kalshi executable prices.
4. Generate a recommendation only when conservative edge survives fees, spread, slippage, model uncertainty, and settlement-basis reserves.
5. Allocate deterministically under correlated risk, liquidity, concentration, and drawdown limits.
6. Require an exact, fresh, single-use owner authorization through WhatsApp before any live submission.

See [`PROJECT_PLAN.md`](PROJECT_PLAN.md) for the phased delivery plan and promotion gates.

Governance and data-architecture decisions live under [`docs/adr`](docs/adr),
[`docs/governance`](docs/governance), and [`docs/runbooks`](docs/runbooks).

## Non-negotiable boundaries

- Credentials and WhatsApp session material remain outside this repository.
- LLMs may assist research and explanation but never own ticker identity, side mapping, quantity, fees, authorization, expiry, replay behavior, or submission.
- Shadow and paper records are structurally unable to call live transport.
- All live buys use bounded limit IOC behavior after fresh quote and market-state revalidation.
- `submission_unknown` is terminal pending exchange reconciliation and is never retried automatically.
- Production promotion requires objective evidence, independent fail-closed review, and a separately authorized canary.

## Coordination

GitHub Issues are the work units. The GitHub Project board is the coordination surface for humans and agents. Kanban content is planning metadata, never financial authority.

## Intended stack

- Python 3.12+
- `uv` for dependency and environment management
- SQLite append-only evidence and audit ledgers initially; migration path documented before multi-host operation
- Provider-specific Kalshi HTTPS transport and bounded one-shot collector behind the existing GET-only interface
- Public Deribit BTC/ETH instrument and options-chain transport plus bounded one-shot collector
- Scheduler and long-running service lifecycle only after durable one-shot collection and restart recovery pass review
- Kalshi WebSocket synchronization after REST baseline and gap semantics are proven
- A separate narrow authenticated order transport only in the later execution phase
- Deterministic WhatsApp command router
- `pytest`, Ruff, mypy/pyright, secret scanning, dependency audit, and CI

## License

Source-visible public repository. No open-source license is granted unless the owner explicitly adds one.
