# Security Policy

## Reporting

Report security concerns privately to the repository owner. Do not open a public issue containing exploit details, credentials, account identifiers, WhatsApp session data, or production logs.

This repository is public. Treat every tracked file, commit, branch, pull request, Actions log, artifact, issue, discussion, and release as globally visible and permanently copyable.

## Sensitive material

This repository must never contain:

- Kalshi API key IDs or private signing keys;
- WhatsApp pairing/session material;
- GitHub, model-provider, database, or deployment tokens;
- unredacted account balances, order payload signatures, or private owner identifiers;
- production `.env` files, database snapshots, logs, or backups.

Use local secret stores or deployment secret injection. Documentation uses `[REDACTED]` placeholders only.

## High-impact vulnerability classes

Treat these as release blockers:

- unauthorized or implicit order approval;
- replay or duplicate submission;
- stale quote/rule/market-state use;
- YES/NO economic-side or payload-price mismatch;
- cap or fee bypass;
- shadow/paper paths reaching production transport;
- ambiguous submission automatically retried;
- ledger writes that are not atomic with state transitions;
- WhatsApp sender-identity or message-transformation bypass;
- secret exposure through logs, traces, CI artifacts, prompts, or exceptions.

## Production response

On suspected compromise or inconsistent state:

1. Disable live submission at the narrow transport boundary.
2. Cancel only strategy-owned resting orders through an owner-authorized operational path.
3. Preserve logs and ledgers without exposing secrets.
4. Reconcile orders, fills, and positions from authoritative exchange APIs.
5. Rotate affected credentials and WhatsApp sessions.
6. Complete a documented incident review before re-enabling any mutation.
