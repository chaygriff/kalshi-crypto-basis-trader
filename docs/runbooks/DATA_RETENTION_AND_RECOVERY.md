# Data Retention and Recovery

## Classification

| Data | Minimum retention | Protection |
|---|---:|---|
| Raw market/rule/options snapshots and hashes | Strategy life + 7 years | Immutable/object versioning; no credentials |
| Forecast/proposal/recommendation lifecycle | 7 years | Append-only, versioned, integrity manifests |
| Approval source metadata and normalized action | 7 years | Restricted access; minimize message content and owner identifiers |
| Intents, attempts, exchange responses, orders, fills, positions, settlement | 7 years | Encrypted, append-only, authoritative IDs |
| Security/audit/deployment events | 2 years minimum | Encrypted and access logged |
| Debug logs | 30 days by default | Redacted; no payload signatures, keys, sessions, or balances unless essential |
| Backups | Daily with defined rotation | Encrypted, separate account/location, integrity manifest |

Longer legal or regulatory requirements override these engineering defaults after counsel review.

## Backup rules

- Never back up `.env`, signing keys, WhatsApp sessions, or plaintext secrets with application data.
- Encrypt in transit and at rest using keys outside the backup set.
- Use consistent snapshots and include schema version, event count, first/last event identity, and cryptographic manifest.
- Restrict restore capability and log every access.

## Phase 1 live-collection durability

- A supervised connectivity probe may retain only a redacted success record when its purpose is authentication or protocol validation.
- A provider observation used for research, equivalence, modeling, or later replay must persist its exact raw bytes and validated snapshot envelope outside Git before the collection run is reported complete.
- The in-memory reference store is not an acceptable destination for recurring collection or evidence needed after process exit.
- Every one-shot collection run has a durable run identity and terminal state: `complete`, `incomplete`, or `failed`. A partial run never becomes complete after restart without a new explicitly linked attempt.
- Scheduler enablement requires tested atomic completion, idempotent rerun, non-overlap, bounded backlog, corruption detection, storage-capacity alarms, and clean restart behavior.
- Long-running service and WebSocket state must rebuild from authoritative retained REST evidence after a gap; buffered deltas alone cannot establish a complete recovered state.

## Recovery objectives

Before live canary, define measured RPO/RTO from deployment architecture. Initial targets are RPO <= 5 minutes for financial events and RTO <= 60 minutes for read-only reconciliation; mutation remains disabled throughout recovery.

## Restore drill

Quarterly and before first canary: restore into an isolated credential-free environment, validate manifests and schema, reconstruct current states, reconcile counts, prove no network mutation capability, and document results. Failed restoration blocks production promotion.
