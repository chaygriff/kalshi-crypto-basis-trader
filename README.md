# Kalshi Crypto Basis Trader

Private, audit-first system for researching and eventually operating an option-implied digital-basis strategy on Kalshi crypto event contracts through an owner-restricted WhatsApp interface.

## Goal

Launch a production-ready Kalshi crypto trading account workflow that is reachable through WhatsApp while keeping research, signal generation, portfolio construction, owner authorization, order transport, and reconciliation as separate security boundaries.

## Current status

**Planning only. Live trading is disabled.** No issue, pull request, project-card movement, agent message, or model output authorizes an order.

## Proposed strategy

For exactly matched BTC and ETH terminal-price contracts:

1. Build an arbitrage-constrained terminal distribution from a liquid options surface.
2. Convert the risk-neutral distribution into a walk-forward calibrated real-world forecast.
3. Compare the forecast interval with depth-weighted Kalshi executable prices.
4. Generate a recommendation only when conservative edge survives fees, spread, slippage, model uncertainty, and settlement-basis reserves.
5. Allocate deterministically under correlated risk, liquidity, concentration, and drawdown limits.
6. Require an exact, fresh, single-use owner authorization through WhatsApp before any live submission.

See [`PROJECT_PLAN.md`](PROJECT_PLAN.md) for the phased delivery plan and promotion gates.

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
- SQLite append-only audit ledgers initially; migration path documented before multi-host operation
- Kalshi read APIs/WebSockets plus narrow authenticated order transport
- Options-chain provider adapter beginning with public Deribit BTC/ETH data
- Deterministic WhatsApp command router
- `pytest`, Ruff, mypy/pyright, secret scanning, dependency audit, and CI

## License

Private/proprietary unless the owner explicitly chooses otherwise.
