# ADR 0002: Financial State Machines

- **Status:** Accepted for Phase 0
- **Rule:** Every transition is append-only, atomic, policy-versioned, timestamped, and causally linked.

## Recommendation lifecycle

`draft -> published -> consumed | expired | superseded | rejected`

- Only a validated portfolio proposal can create `draft`.
- `published` requires exact contract/rules identity, economic outcome, executable quote, fee policy, risk policy, creation time, and expiry.
- An exact owner action atomically moves one `published` row to `consumed` while creating one intent.
- `expired`, `superseded`, `rejected`, and `consumed` are terminal.

## Authorization and intent lifecycle

`reserved -> approved -> revalidating -> rejected_pre_submit | ready_to_submit -> submission_started`

- Parsing and validation complete before any reservation.
- Authorization binds owner identity, source message identity, recommendation ID, ticker, economic outcome, exchange side, quantity, limit, all-in cap, policy version, and expiry.
- Any attempted fresh revalidation consumes the one-time authority.
- `ready_to_submit` is reachable only from `revalidating` after market status, rules identity, executable quote, depth, fees, caps, reservations, mode, and kill-switch checks all pass.
- `rejected_pre_submit` is terminal and guarantees no mutation request was sent.

## Submission attempt lifecycle

`submission_started -> submitted | submission_unknown`

- The durable `submission_started` record exists before transport invocation.
- A valid authoritative exchange response produces `submitted` plus the remote order identity atomically.
- Timeout, connection reset, malformed response, or local persistence uncertainty after invocation produces terminal `submission_unknown`.
- `submission_unknown` is never automatically retried. It triggers the global mutation stop and authoritative reconciliation.

## Order lifecycle

`submitted -> resting | partially_filled | filled | cancelled | expired_exchange | rejected_exchange | order_unknown`

- IOC normally resolves to `filled`, `partially_filled`, `cancelled`, or `rejected_exchange`.
- Local status never substitutes for exchange evidence.
- A submitted order is not a fill; a fill is not a settled position.
- Conflicting or missing authoritative reads produce `order_unknown` and block new risk in the same event group.

## Position lifecycle

`opening -> open | flat -> determined -> finalized | disputed | amended`

- Position quantity derives only from reconciled fills.
- Market lifecycle and position settlement are separate records.
- Dispute or amendment invalidates provisional performance and blocks strategy promotion.

## Reconciliation lifecycle

`required -> querying -> reconciled | unresolved | escalated`

- Reconciliation may read exchange state and append corrections.
- It cannot create, resize, replace, cancel, or retry an order.
- `unresolved` exceeding the operational deadline becomes `escalated` and keeps live mutation disabled.

## Forbidden transitions

- `expired|superseded|rejected|consumed -> published`
- `approved -> submission_started`
- `revalidating -> submission_started`
- `rejected_pre_submit -> ready_to_submit|submission_started`
- The original attempt remains terminal as `submission_unknown`; reconciliation appends separate order and reconciliation events and never changes or revives the attempt.
- `submitted -> filled` without authoritative fill evidence
- `shadow|paper -> approved|submission_started`
- Any state -> earlier state by update or deletion
- Any GitHub, model, agent, alert, or monitoring event -> owner authorization

## Atomicity and idempotency

- Recommendation consumption and intent creation share one serializable transaction.
- Client order IDs are unique, deterministic, strategy-owned, immutable, and bound before invocation.
- Duplicate source messages and concurrent approvals return the stored terminal result.
- State is appended; derived current status is reconstructed from validated transitions.
- Unknown states fail closed rather than being coerced into a known success or retry state.
