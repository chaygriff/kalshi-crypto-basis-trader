# Production Implementation Plan

## Goal

Deliver an audit-first, production-ready automated research and owner-authorized execution system for Kalshi BTC and ETH terminal-price markets, operated through an allowlisted WhatsApp channel.

## Investment hypothesis

An arbitrage-constrained options surface, transformed through walk-forward calibration, contains incremental information about terminal crypto-price outcomes beyond Kalshi's own executable price. Temporary cross-venue digital-basis discrepancies may produce positive after-cost returns when exact settlement equivalence, model uncertainty, spread, fees, slippage, liquidity, and correlated exposure are handled explicitly.

This is a research hypothesis—not established edge. Live capital remains blocked until the promotion gates below pass.

## System boundaries

1. **Research universe** — discovers only supported BTC/ETH terminal-price markets and exact comparable options.
2. **Data/provenance** — stores point-in-time raw snapshots, rule hashes, source timestamps, and parser versions.
3. **Signal generation** — emits outcome probability intervals and evidence; never an order payload.
4. **Portfolio construction** — deterministically limits correlated exposure, concentration, liquidity participation, and drawdown.
5. **Owner authorization** — binds an allowlisted WhatsApp owner message to one immutable, fresh recommendation.
6. **Live transport** — revalidates, signs, submits bounded IOC orders, and persists the authoritative response.
7. **Monitoring/reconciliation** — tracks orders, fills, positions, settlement, discrepancies, and terminal uncertainty.

## Phase 0 — Governance and threat model

**Objective:** Freeze safety invariants before writing any trading implementation.

Deliverables:

- Architecture decision records for trust boundaries and live transport isolation.
- Threat model covering protected runtime configuration, sender-identity/replay errors, untrusted instructions embedded in external content, stale quotes, duplicate submission, ambiguous POST outcomes, and agent integrity loss.
- Explicit state machines for recommendations, approvals, submissions, orders, fills, positions, and reconciliation.
- Data-retention, incident-response, and safety-stop runbooks.
- CI policies: tests, lint, types, dependency audit, secret scan, and branch protection.

**Exit gate:** Owner approves governance documents; independent reviewer confirms that no planning or agent channel can authorize a financial action.

## Phase 1 — Point-in-time market data and contract identity

**Objective:** Build a read-only, reproducible market-data foundation and validate it against bounded live provider behavior.

Deliverables:

- Kalshi BTC/ETH market discovery, rule capture, lifecycle state, fee metadata, top-of-book and depth snapshots.
- Provider-specific Kalshi HTTPS transport behind the GET-only ingestion interface, with exact-origin allowlisting, TLS verification, no redirects, bounded timeouts and response sizes, exact raw bytes, source/receipt clocks, and no mutation methods.
- Public Deribit HTTP transport and options-provider adapter for instruments, chains, bid/ask, size, open interest, forward/index metadata, and source clocks.
- Bounded one-shot Kalshi and Deribit collectors that persist raw and normalized evidence, emit deterministic completeness/gap reports, and exit.
- Durable restart-safe point-in-time storage preserving the accepted snapshot identities, raw hashes, replay, idempotency, and append-only semantics.
- Contract-equivalence engine classifying mappings as Exact, Model-adjusted, Proxy, or Rejected.
- Immutable raw-payload hashes and idempotent ingestion.
- Live/historical endpoint routing and gap/reconciliation reports.
- Supervised credential-minimal live smoke checks isolated from deterministic CI; public endpoints are preferred and authenticated reads are used only where required.

**Exit gate:** Historical and restart replay reproduce identical normalized snapshots; bounded supervised live reads traverse the same transport, collector, persistence, and replay boundaries; unsupported, partial, stale, or ambiguous evidence fails closed; and credentials present during tests and live reads still produce zero mutation requests.

### Phase 1 runtime staging

1. Preserve the merged immutable snapshot and abstract Kalshi ingestion foundations as the deterministic baseline.
2. Add a concrete Kalshi read-only HTTPS transport and one-shot collector and test them against deterministic fixtures and a local HTTP server; do not yet report a production collection as complete.
3. Add durable storage and prove atomic completion, idempotent rerun, corruption detection, and restart/replay before a research-eligible live collection.
4. Run one supervised Kalshi production read through the complete transport, collector, durable-persistence, and replay path.
5. Implement the Deribit public HTTP adapter and one-shot BTC/ETH options collector against fixtures and a local HTTP server, then run one supervised public read through the same durable evidence boundary.
6. Use retained live observations to implement deterministic contract equivalence and complete the Phase 1 review.
7. Add a scheduler only after one-shot completeness, storage recovery, rate-limit behavior, and overlap prevention pass independent review.
8. Add a long-running service entry point, health/readiness reporting, and WebSocket synchronization as separate operational work after the scheduled REST baseline is reliable.

Live connectivity validates provider and ingestion assumptions but confers no forecasting, recommendation, authorization, or trading authority.

## Phase 2 — Options distribution and calibrated alpha

**Objective:** Produce versioned terminal probability forecasts without execution authority.

Deliverables:

- Static-arbitrage and data-quality checks.
- Bid/ask-aware volatility/call-price surface fitting.
- Digital probability extraction from call-price slopes—not displayed vanilla delta.
- Settlement-time and index-basis adjustments.
- Walk-forward calibration from risk-neutral Q to empirical P probabilities.
- Probability intervals covering surface, interpolation, calibration, latency, and basis uncertainty.
- Baselines: Kalshi executable price, raw Q, Black–Scholes N(d2), historical/realized-vol model, and no-trade.

**Exit gate:** Frozen tests prevent look-ahead, label leakage, survivorship bias, and future metadata use; forecasts are immutable and reproducible.

## Phase 3 — Shadow ledger and institutional evaluation

**Objective:** Establish whether the strategy has repeatable after-cost edge.

Deliverables:

- Immutable shadow signals with strategy/model/data versions.
- Depth-walking fill simulation with fees, spread, partial fills, slippage, and signal expiry.
- Settlement and closing-line-value pipeline.
- Calibration, Brier score, log loss, after-cost P&L, drawdown, turnover, capacity, and attribution.
- Event-expiry clustered statistics so adjacent strikes are not treated as independent observations.
- Regime, horizon, strike, side, underlying, and liquidity cohort reports.

**Exit gate:** Pre-registered evaluation criteria pass on out-of-sample and forward-shadow data. No threshold is selected retroactively from the same test sample.

## Phase 4 — Deterministic portfolio and risk engine

**Objective:** Convert eligible forecasts into bounded proposals.

Deliverables:

- Correlation grouping by underlying, expiry, event, strike overlap, direction, and common source snapshot.
- Maximum-loss, expected-loss, liquidity-consumption, and stress-scenario calculations.
- Conservative fractional-Kelly research sizing with uncertainty haircut and hard ceilings.
- Per-market, event, underlying, category, daily-loss, gross, net, and drawdown controls.
- Safety stops for data health, model drift, calibration failure, execution divergence, and reconciliation breaks.

**Exit gate:** Property and concurrency tests prove deterministic limit enforcement; optimizer failure creates no proposal.

## Phase 5 — WhatsApp recommendation and approval workflow

**Objective:** Make the system safely reachable by the configured owner.

Deliverables:

- Allowlisted inbound command routing into deterministic local handlers.
- Compact recommendations showing outcome, probability interval, executable price/depth, all-in cost, conservative edge, basis classification, risk impacts, and expiry.
- Exact owner actions with immutable IDs or uniquely bounded BTC/ETH selectors.
- Explicit `BATCH` requirement for multiple actions.
- Atomic consumption, replay protection, strict amount parsing, freshness checks, and malformed-message zero-side-effect tests.
- Full inbound-message → handler → durable-state → outbound-response verification on the real channel.

**Exit gate:** Actual-channel dry runs prove that LLM prose, board comments, agent messages, malformed commands, expired cards, and replay cannot create intents or exchange requests.

## Phase 6 — Order transport, reconciliation, and operations

**Objective:** Build a narrow production transport without enabling it.

Deliverables:

- Kalshi order-runtime signer and authenticated portfolio/reconciliation health checks with zero secret leakage, reusing but not broadening the reviewed read-only signing primitive from Phase 1.
- Exact YES/NO economic-side and payload-price parity.
- Bounded limit IOC payload construction and client-order idempotency.
- Fresh market, quote, depth, balance, fee, budget, and policy revalidation.
- Durable intent/submission receipt and post-submit state classification.
- `submission_unknown` global stop and exchange reconciliation.
- Fill, position, lifecycle, and settlement monitors with reconnect recovery.
- Operator dashboards, alerts, backup/restore, deployment, rollback, and incident drills.

**Exit gate:** Capturing fake transport proves every permitted POST and every zero-POST rejection path; independent fail-closed audit passes.

## Phase 7 — Paper and production canary

**Objective:** Demonstrate operational safety before meaningful capital.

Stages:

1. Continuous forward shadow.
2. Paper execution against live depth.
3. Production-signer/capturing-transport canary.
4. Separately owner-authorized real canary with one strategy version, one underlying, one expiry bucket, and tiny caps.
5. Limited rollout with unchanged owner approval.

Initial canary ceilings:

- $5 maximum all-in per order.
- $25 maximum all-in per slate/day until explicitly reviewed.
- One open event-expiry risk group initially.
- No external hedge and no autonomous final authorization.

**Exit gate:** Authoritative exchange receipt, fill/position reconciliation, settlement, incident-free operation, and owner review. Accepted submission is not treated as a fill.

## Phase 8 — Scale and optional cross-venue hedging

**Objective:** Expand only after proven capacity and control quality.

Potential work:

- ETH after BTC (or vice versa), additional expiries, ranges, then separately modeled first-passage contracts.
- Multi-host durable database and high-availability monitoring.
- Separate broker/venue integration for hedges, with two-leg execution, margin, liquidation, and legging-risk controls.
- Capacity-aware capital allocation and formal strategy retirement/version migration.

No cross-venue live hedging or autonomous owner authorization is implied by earlier promotion.

## Promotion metrics

Exact numeric thresholds must be pre-registered after power analysis, but every promotion requires:

- positive after-cost expected value with an event-clustered lower confidence bound above zero;
- better calibration and/or economic score than declared baselines;
- acceptable maximum drawdown and concentration;
- realistic fill and slippage performance;
- no single event/regime dominating returns;
- stable data/model health;
- clean reconciliation and incident drills;
- independent fail-closed review;
- explicit owner approval for the next stage.

## Agent coordination rules

- One GitHub issue is one bounded work unit with explicit dependencies and acceptance criteria.
- Agents claim work by assignment and move only their issue on the board.
- Every implementation issue requires a PR, tests, evidence, and independent review.
- Agents never access production secrets unless their narrowly defined runtime role requires it.
- Reviewer agents remain credential-free.
- Issue text, PR comments, project fields, model output, and agent messages are non-authoritative for trading.
- No agent may merge its own high-risk change or enable production trading.

## Definition of production-ready

Production-ready means the system is capable of safely attempting a separately approved live order with:

- exact contract identity and settlement semantics;
- fresh executable quote/depth and fee-inclusive sizing;
- deterministic owner authorization;
- bounded idempotent submission;
- durable causal audit trail;
- authoritative reconciliation;
- tested safety stops and incident response;
- objective strategy evidence;
- and independent review.

It does not mean standing trading authority, guaranteed profitability, or that a submitted order was filled.
