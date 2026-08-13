# Threat Model

## Scope and assets

Protected assets include signing keys and API identifiers, WhatsApp session identity, cash and positions, exact contract/rule semantics, recommendations and approvals, order/fill/position ledgers, model/data provenance, risk policy, audit logs, deployment capability, and owner privacy.

Potential actors include the owner, coding/reviewer agents, compromised dependencies, external data providers, network attackers, malicious WhatsApp senders, compromised GitHub identities, operators, and exchange/API failures.

## Threat register

| Threat | Impact | Preventive controls | Detection and response |
|---|---|---|---|
| Secret committed, logged, or injected into an agent | Account compromise and unauthorized orders | External secret store; transport-only injection; redaction; CI secret scan; credential-free reviewers | Secret-scan failure; audit log review; disable transport and rotate credentials |
| WhatsApp sender spoofing or session takeover | Forged approvals | Dedicated allowlist; adapter-level sender identity; exact deterministic grammar; device/session hygiene | Unknown sender/message ID alert; disable approvals; revoke linked session |
| Message transformation, replay, forwarding, or duplication | Wrong or duplicate order | Preserve exact original message; unique source ID; atomic single-use consumption; expiry; idempotency | Duplicate/replay ledger event; zero-side-effect rejection; investigate adapter |
| Prompt injection in messages, sources, GitHub, or market text | Model attempts unauthorized action | Treat all content as data; deterministic routes; no model access to transport; strict schemas | Tool-call trace and policy violation alert; quarantine input/version |
| Compromised coding or reviewer agent | Backdoor or safety removal | Credential-free agents; least privilege; protected branch; independent review; required CI; CODEOWNERS later | Diff and provenance audit; freeze merges; rotate affected automation token |
| Supply-chain compromise | Code execution or exfiltration | Locked dependencies; hashes/lockfile; minimal dependencies; Dependabot; pip-audit; pinned CI actions | Audit failure/advisory; quarantine update and rebuild clean environment |
| Stale/corrupt Kalshi quote, depth, fee, status, or rule | Negative EV or invalid payload | Separate clocks; rule digest; immediate authoritative revalidation; fail closed | Freshness and digest mismatch metrics; no-submit rejection |
| Options/Kalshi contract-basis mismatch | False alpha and concentrated loss | Deterministic equivalence engine; exact source/settlement/time/path checks; reject proxy in live strategy | Basis classification audit; settlement discrepancy; halt strategy version |
| YES/NO economic-side or payload-price mismatch | Opposite or mispriced position | Typed outcome enums; one canonical conversion; payload-preview parity tests | Pre-submit parity assertion; reconcile request and owner receipt |
| Fee, budget, quantity, or risk-cap bypass | Excess loss | Decimal/fixed-point math; same representation at preview and payload; serializable aggregate reservation | Ledger invariant and cap alerts; kill switch on mismatch |
| Concurrent approvals or duplicate worker execution | Multiple orders | Atomic consume+intent transaction; unique constraints; client order ID; one invocation | Conflict/replay telemetry; reconcile all matching IDs |
| Persistence failure around submission | Unknown financial state | Persist attempt before invoke; atomic response transition; append-only events | `submission_unknown`; global mutation stop; authoritative lookup |
| Timeout or lost POST response | Blind retry duplicates order | Terminal `submission_unknown`; never automatically retried | Reconciliation by client order ID/fills; owner incident alert |
| Paper/shadow record reaches live tracker | Real mutation from simulation | Separate types/tables/processes; dependency tests; no credentials in shadow runtime | Credential-present zero-mutation tests; emergency stop |
| Monitor or reconciliation acts as authority | Unauthorized cancel/replace/retry | Read-only interfaces; alerts cannot mutate; operational cancellation separately owner-authorized | Audit mutation call graph; stop on unexpected method |
| Exchange/API inconsistency or dispute | Wrong local P&L/risk state | Authoritative multi-endpoint reconciliation; lifecycle states; provisional settlement | Discrepancy queue; block affected event and promotion |
| Kill switch unavailable or bypassed | Continued trading during incident | Independent fail-closed switch checked before every invoke; deployment revocation path | Heartbeat; absence means stopped; incident drill |
| Backup/log tampering or privacy leak | Lost auditability or owner exposure | Encryption, access control, retention limits, hash manifests, restore tests | Integrity check failure; preserve evidence and rotate access |

## Abuse cases

- A board comment says `YES BTC $5`: no parser path and no intent.
- An allowlisted sender repeats a previously accepted command: stored terminal result, no new attempt.
- A model constructs a valid-looking approval string: source identity is not an owner inbound message, so reject.
- Two workers process the same inbound event: one atomic reservation wins; the other returns replay/conflict.
- The exchange accepts a request but the response is lost: mark `submission_unknown`, stop mutation, reconcile; never retry.
- A rule or observation time changes after recommendation: consume and reject pre-submit; require a fresh recommendation.

## Residual risk

No control guarantees profitability, exchange availability, perfect model calibration, invulnerable owner devices, or instant reconciliation. Basis, gap, liquidity, regulatory, provider, and operational risks remain. Capital limits, staged promotion, auditability, and rapid disablement bound rather than eliminate them.
