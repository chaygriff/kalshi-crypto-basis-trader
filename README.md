# Kalshi Crypto Basis Trader

Public, audit-first implementation of an option-implied digital-basis research and eventual owner-authorized execution system for Kalshi crypto event contracts through an owner-restricted WhatsApp interface.

## Goal

Launch a production-ready Kalshi crypto trading account workflow that is reachable through WhatsApp while keeping research, signal generation, portfolio construction, owner authorization, order transport, and reconciliation as separate security boundaries.

## Current status

**Phase 0 governance and the initial Phase 1 evidence foundations are complete. Live trading is disabled.** The repository now contains the immutable snapshot schema, PostgreSQL evidence and collection-run ledgers, the abstract GET-only Kalshi BTC/ETH ingestion boundary, and a concrete public read-only Deribit options transport and bounded one-shot collector. It does not contain a concrete Kalshi HTTP transport, scheduler, service entry point, WebSocket synchronization, model, or mutation transport.

Production Kalshi REST and authenticated WebSocket connectivity have been verified through the separate `kalshi-exa-research` reference implementation without exposing credentials or making a mutation request. That verifies external connection prerequisites, not this repository's Kalshi ingestion architecture. PostgreSQL and the public Deribit one-shot evidence path are implemented here; recurring collection remains disabled. Scheduling, long-running service lifecycle, and streaming synchronization remain separate later stages.

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
- PostgreSQL append-only evidence snapshots and collection-run event ledgers with protected external connection configuration
- Provider-specific Kalshi HTTPS transport and bounded one-shot collector behind the existing GET-only interface
- Public Deribit BTC/ETH instrument and options-chain transport plus bounded one-shot collector
- Scheduler and long-running service lifecycle only after durable one-shot collection and restart recovery pass review
- Kalshi WebSocket synchronization after REST baseline and gap semantics are proven
- A separate narrow authenticated order transport only in the later execution phase
- Deterministic WhatsApp command router
- `pytest`, Ruff, mypy/pyright, secret scanning, dependency audit, and CI

## PostgreSQL integration tests

PostgreSQL integration tests require external libpq services named by
`KCB_POSTGRES_ADMIN_SERVICE`, `KCB_POSTGRES_MIGRATOR_SERVICE`, and
`KCB_POSTGRES_RUNTIME_SERVICE`. Credentials and connection details remain outside
the repository. The admin service is test setup authority only; production runtime
code continues to use the migrator and runtime services according to their separate
roles.

The tests refuse to run if the fixed `kalshi_crypto_basis_test` database already
exists. They create it with a unique disposable marker, apply migrations and exercise
the real runtime login there, verify the marker before terminating test connections,
and drop the database in unconditional fixture cleanup. This keeps deterministic test
rows out of the append-only development evidence ledger.

## License

Source-visible public repository. No open-source license is granted unless the owner explicitly adds one.
