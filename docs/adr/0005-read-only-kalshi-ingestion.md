# ADR 0005: Read-only Kalshi BTC/ETH ingestion

## Status

Accepted and merged for Phase 1.

## Context

Kalshi market identity, rules, lifecycle, settlement metadata, and executable depth are authoritative exchange evidence. The adapter must preserve exact point-in-time responses without allowing ticker text, HTTP fallbacks, credentials, or model interpretation to create market identity or mutation authority.

## Decision

The adapter accepts an explicit reviewed mapping from native Series ticker to `BTC` or `ETH`. It never derives Series, Event, Market, underlying, strike, date, or partition from ticker syntax or display text. An unreviewed Series fails before transport access.

Every native identifier interpolated into an API route must first match the strict ASCII grammar `[A-Za-z0-9][A-Za-z0-9._-]*`. Encoded separators, slashes, backslashes, whitespace, control characters, Unicode separators, query, fragment, and dot-segment forms fail before transport access. Reviewed underlying configuration is runtime-validated as exactly `BTC` or `ETH`; static type annotations are not treated as trust-boundary validation. Source JSON with a duplicate object member at any nesting depth or a non-standard `NaN`/infinity constant is ambiguous evidence and is rejected.

The transport capability exposes only `GET`. No POST, order, cancellation, amendment, or mutation interface exists. Tests with a credential-present transport exercise discovery, detail, order-book, cutoff, and routed lookup while proving zero mutation requests.

### Discovery and pagination

Live discovery uses `GET /trade-api/v2/markets` with exact reviewed `series_ticker`, `status=open`, and `limit=1000`. Filters remain fixed across pages and opaque cursors are returned without decoding or construction. Empty cursor completes a traversal. Repeated cursors, conflicting duplicate tickers, malformed pages, and a configured page-budget exhaustion fail closed. Identical duplicate tickers are deduplicated in first-seen order; divergent records are fatal.

Each returned discovery record must itself name the exact reviewed Series, report `status=open`, and report `market_type=binary`. The adapter preserves those source fields; it never overwrites or relabels a returned Series identity from the request filter.

Every page becomes an immutable `SnapshotEnvelope` containing exact raw bytes, request fingerprint, source and receive clocks, parser version, and fixed-point normalized content.

### Exact detail evidence

A live evidence bundle independently fetches:

- `GET /series/{series_ticker}`;
- `GET /events/{event_ticker}?with_nested_markets=false`;
- `GET /markets/{market_ticker}`.

Returned native identifiers must link exactly: Series → Event → Market. Series and Event settlement sources must be present. Fee metadata is retained but no fee formula is calculated in this issue. Event fee override type and multiplier must be jointly populated or jointly null.

Market detail requires a binary market, a supported lifecycle state, coherent supported strike fields, non-empty primary and secondary rules, canonical UTC close and expected-expiration times, and an explicit non-empty price grid. Price ranges use plain decimal strings without exponent notation. They must be finite, begin at zero, end at one, be contiguous, align both endpoints to a positive step, and cover the complete binary-price domain without overlaps or gaps.

### Order books

Current order books use `GET /markets/{ticker}/orderbook?depth=0`. REST bid tuples must be exactly two strings containing a finite price and strictly positive count. Each side remains strictly ascending; duplicate or descending prices are rejected rather than sorted or repaired. Empty sides are valid evidence of no displayed liquidity.

REST books preserve YES and NO side-leg bids. Complementary asks are derived only for a validated binary market through exact Decimal arithmetic: `ask = 1.00 - opposite_bid`, retaining the opposite bid's count and sorting asks ascending through reverse traversal. A best-YES plus best-NO bid above one is crossed and rejected. No midpoint or missing ask is synthesized.

A book becomes usable only by supplying its persisted order-book snapshot ID together with the persisted Market snapshot ID. Ingestion writes each complete validated envelope and exact raw bytes through the existing snapshot persistence boundary; a later ingestor may validate those IDs from the same store. Consumer wrappers never cross this trust boundary and cannot assert what observation they “originally” represented. Validation loads both records solely by content-derived identity, verifies their GET route fingerprints and parser versions, reparses persisted bytes, derives the underlying from the persisted Market's exact reviewed Series, reconstructs fresh typed records, binds their tickers, and checks every level against the reconstructed grid. Selecting another valid persisted ID explicitly selects another retained observation; replacing wrapper fields cannot impersonate one because wrappers are not validator inputs. There is no owner-free semantic validator. Snapshot-store write authority is explicitly part of the trusted computing base; Python attribute privacy is not treated as protection against arbitrary code that already controls that authority. Prices are never rounded onto a grid.

### Historical routing

Each routed lookup first snapshots `GET /historical/cutoff`. A caller must supply an authoritative UTC settlement time from prior evidence:

- settlement strictly before `market_settled_ts` routes to `/historical/markets/{ticker}`;
- settlement at or after the cutoff routes to `/markets/{ticker}`;
- missing settlement time returns `PARTITION_UNKNOWN` before any transport access.

A 404 on one tier is never treated as proof that the other tier owns the ticker. Ticker dates and assumed retention periods are not routing evidence.

The routed Market response must return a native `series_ticker` that exists in the reviewed Series mapping. The Market's underlying is derived from that exact mapping. Any caller-supplied underlying is only a consistency assertion and must equal the returned Series mapping; it cannot label the Market.

### Gaps

Gap reports use deterministic one-reason precedence per discovered ticker:

1. `DETAIL_MISSING`;
2. `ORDERBOOK_MISSING`;
3. `NO_EXECUTABLE_LIQUIDITY`.

An empty order book is present evidence and is distinguished from a missing snapshot. Gaps are sorted by ticker and code. Any gap makes the report incomplete.

## Consequences

This adapter can produce replayable discovery, detail, routing, and depth evidence for later contract equivalence and modeling. It cannot place or authorize orders. It does not claim historical order-book coverage, fee calculation, atomic REST/stream equality, or complete consistency across a multi-page traversal. Those limitations remain explicit evidence gaps rather than inferred facts.

The merged implementation is an ingestion domain layer behind an abstract GET-only transport. Its malformed-input and boundary tests do not by themselves establish live collection. ADR 0006 governs the separate concrete HTTP transport, one-shot collector, durable-storage, supervised-live-check, scheduler, service, and streaming stages. Concrete credentials must not change this adapter's route eligibility or add a mutation-capable interface.
