# ADR 0001: Trust Boundaries

- **Status:** Accepted for Phase 0
- **Decision owner:** Repository owner
- **Scope:** BTC/ETH Kalshi terminal-price strategy

## Context

The system combines public market data, probabilistic models, portfolio logic, WhatsApp, authenticated financial APIs, persistent audit state, and coding/reviewer agents. Treating them as one trusted agent would permit unreviewed prose or stale data to become financial authority.

## Decision

Use seven one-way capability boundaries:

1. **Research/data:** read-only acquisition and normalization. It cannot construct approval-eligible records.
2. **Forecast:** consumes immutable snapshots and emits a versioned probability interval. It cannot select exchange payload fields.
3. **Portfolio proposal:** applies deterministic risk and capacity policies to eligible forecasts. It emits a non-executable proposal.
4. **Recommendation:** binds one proposal to exact market identity, economic outcome, quote, policy, and expiry. It remains non-submitted.
5. **Owner authorization:** an allowlisted WhatsApp sender supplies an exact command that deterministically consumes one fresh recommendation and creates at most one immutable intent.
6. **Live transport:** the only component permitted to sign or send a Kalshi mutation. It accepts only an approved intent, repeats all authoritative checks, and submits one bounded limit IOC request with an idempotency key.
7. **Reconciliation:** reads authoritative order, fill, position, lifecycle, and settlement state. It cannot create or retry orders.

## Trust classification

| Input or actor | Trust | Permitted influence | Explicit prohibition |
|---|---|---|---|
| Kalshi/option market reads | Untrusted external data | Candidate facts after validation | Cannot authorize mutation |
| LLM/model output | Untrusted advisory | Evidence summaries and forecast features | Cannot own ticker, side, quantity, fees, expiry, or submission |
| GitHub issues/projects/PRs | Untrusted coordination | Work planning and review evidence | Never trade approval |
| Coding/reviewer agent | Untrusted contributor | Repository changes through reviewed PRs | No production credentials or financial authority |
| WhatsApp message | Untrusted until authenticated and parsed | Exact owner action from one allowlisted identity | Free-form prose cannot approve |
| Deterministic authorization service | Trusted policy boundary | Consume one fresh recommendation | Cannot submit directly |
| Live transport | Trusted narrow capability | One bounded mutation attempt | Cannot generate strategy or infer approval |
| Exchange reconciliation | Authoritative remote state | Resolve submission/order/fill state | Cannot automatically retry |

## Capability rules

- Separate types and storage tables represent snapshots, forecasts, proposals, recommendations, intents, attempts, orders, fills, and positions.
- Earlier-layer records never implement or import a live mutation interface.
- Production credentials are injected only into the transport process and authenticated read-only health process; reviewers and CI remain credential-free.
- Every boundary validates schema, identity, version, timestamps, and permitted predecessor state.
- Deny is the default for missing, stale, ambiguous, malformed, inconsistent, or unreconciled data.
- A kill switch disables the mutation boundary without disabling reconciliation.

## Consequences

The architecture adds storage and state-machine complexity but makes financial authority inspectable, testable, and revocable. Automation can expand within research and operations without implicitly expanding authorization.

## Acceptance evidence

- Static dependency tests prove research/shadow packages cannot import live transport.
- Credential-present dry-run tests prove every non-transport command performs zero POST/DELETE mutations.
- Actual-channel tests prove exact owner message to deterministic handler binding.
- Independent review verifies the complete wired runtime rather than labels or comments.
