# ADR 0003: Live Transport Isolation

- **Status:** Accepted for Phase 0
- **Current implementation status:** No mutation transport exists and live trading remains disabled. Phase 1 now plans separate concrete read-only provider transports and collectors under ADR 0006; they do not implement this mutation interface.

## Decision

The eventual Kalshi mutation client will live behind a single process boundary and narrow interface. No research, model, portfolio, rendering, messaging, board-worker, monitor, or reconciliation module may import or instantiate it.

## Required interface

The transport accepts only a fully materialized `ApprovedIntent` and returns one of:

- authoritative accepted/rejected response;
- typed definite pre-invocation rejection;
- terminal `submission_unknown` when invocation outcome is ambiguous.

It may not accept free-form text, ticker aliases, model probabilities, budgets requiring sizing, or mutable recommendation objects.

## Deployment controls

- No `LIVE=true` or repository-file enablement flag.
- Production mutation requires deployment-level capability injection, an owner-approved release record, passing required checks, independent review evidence, and an unexpired canary policy.
- Credentials are available only to the narrowly scoped runtime that requires them, never CI, GitHub, test fixtures, reviewer agents, model workers, or shadow workers. A Phase 1 authenticated read process receives signing capability without receiving any mutation method or route.
- Network policy should allow the transport only to required Kalshi hosts; shadow and reviewer processes have no mutation credential.
- Safety-stop state is independently readable and defaults to stopped on absence, corruption, or timeout.

Read-only connectivity, authenticated health, collection success, and persisted market evidence are data capabilities only. They cannot enable, configure, or invoke the eventual mutation transport.

## Order constraints

- Limit IOC only for initial live rollout.
- Exact fixed-point quantity and payload price; no binary floating-point arithmetic.
- Fresh market, rule digest, quote, depth, fees, balance, aggregate risk, and authorization expiry are revalidated immediately before invocation.
- One intent allows at most one invocation.
- Strategy-owned client order IDs support safe lookup but never justify blind retry.

## Verification

- Import-graph test enforces one-way dependencies.
- Capturing fake transport receives exactly one expected request on the approved path.
- Every rejection path proves zero transport calls.
- Credential-present shadow and paper tests still prove zero network mutations.
- Failure injection covers before-send, during-send, response-loss, persistence failure, and reconciliation conflict.
