# Threat Model

## Scope and assets

Protected assets include signing keys and API identifiers, WhatsApp session identity, cash and positions, exact contract/rule semantics, recommendations and approvals, order/fill/position ledgers, model/data provenance, risk policy, audit logs, deployment capability, and owner privacy.

Potential sources of risk include operator mistakes, agent or reviewer integrity loss, dependency integrity failures, external data providers, sender-identity mismatches, account-session loss, and exchange/API failures.

## Threat register

| Threat | Impact | Preventive controls | Detection and response |
|---|---|---|---|
| Protected runtime configuration is committed, logged, or placed in an agent context | Account access loss or unapproved orders | External secret store; transport-only injection; redaction; CI secret scan; credential-free reviewers | Secret-scan failure; audit log review; disable transport and rotate affected access material |
| Credential-bearing read follows an unexpected redirect or origin | Credential disclosure or unreviewed provider evidence | Exact HTTPS origin allowlist; redirects disabled; sign only the reviewed path; no generic URL input | Reject response; record transport gap; rotate credentials if disclosure is plausible |
| Malformed, oversized, compressed, duplicated-key, or non-finite provider response | Resource exhaustion or corrupted evidence | Body/decompression limits; strict content type and JSON; exact raw-byte retention; deterministic parser rejection | Mark run incomplete; preserve bounded diagnostics; no downstream use |
| Partial or temporally incoherent collection is labeled complete | Invalid equivalence, surface, or forecast | Bounded traversal and synchronization windows; expected-instrument accounting; durable run terminal state | Deterministic gap report; quarantine batch; no forecast |
| Scheduler overlap, restart, or retry duplicates or mixes observations | Misstated point-in-time state and storage growth | Durable run IDs; atomic completion; idempotency; single-run lease; bounded retry/backlog | Stop recurring collection; reconcile run ledger and snapshot identities |
| WebSocket disconnect or sequence gap is silently bridged | Incorrect live book or instrument state | Snapshot-before-delta; sequence/gap detection; discard and reacquire REST baseline | Mark stream stale; suppress downstream use until complete resynchronization |
| WhatsApp sender identity mismatch or session loss | Invalid approvals | Dedicated allowlist; adapter-level sender identity; exact deterministic grammar; device/session hygiene | Unknown sender/message ID alert; disable approvals; revoke linked session |
| Message transformation, replay, forwarding, or duplication | Wrong or duplicate order | Preserve exact original message; unique source ID; atomic single-use consumption; expiry; idempotency | Duplicate/replay ledger event; zero-side-effect rejection; investigate adapter |
| Untrusted instructions embedded in messages, sources, GitHub, or market text | Model treats external content as authority | Treat all content as data; deterministic routes; no model access to transport; strict schemas | Tool-call trace and policy alert; isolate the input/version for review |
| Agent or reviewer integrity loss | Unreviewed code or safety-control removal | Credential-free agents; least privilege; protected branch; independent review; required CI; CODEOWNERS later | Diff and provenance audit; freeze merges; rotate affected automation token |
| Dependency integrity failure | Unexpected code behavior or data disclosure | Locked dependencies; hashes/lockfile; minimal dependencies; Dependabot; pip-audit; pinned CI actions | Audit failure/advisory; isolate the update and rebuild a clean environment |
| Stale/corrupt Kalshi quote, depth, fee, status, or rule | Negative EV or invalid payload | Separate clocks; rule digest; immediate authoritative revalidation; fail closed | Freshness and digest mismatch metrics; no-submit rejection |
| Options/Kalshi contract-basis mismatch | False alpha and concentrated loss | Deterministic equivalence engine; exact source/settlement/time/path checks; reject proxy in live strategy | Basis classification audit; settlement discrepancy; halt strategy version |
| YES/NO economic-side or payload-price mismatch | Opposite or mispriced position | Typed outcome enums; one canonical conversion; payload-preview parity tests | Pre-submit parity assertion; reconcile request and owner receipt |
| Fee, budget, quantity, or risk-cap enforcement failure | Excess loss | Decimal/fixed-point math; same representation at preview and payload; serializable aggregate reservation | Ledger invariant and cap alerts; safety stop on mismatch |
| Concurrent approvals or duplicate worker execution | Multiple orders | Atomic consume+intent transaction; unique constraints; client order ID; one invocation | Conflict/replay telemetry; reconcile all matching IDs |
| Persistence failure around submission | Unknown financial state | Persist attempt before invoke; atomic response transition; append-only events | `submission_unknown`; global mutation stop; authoritative lookup |
| Timeout or lost POST response | Blind retry duplicates order | Terminal `submission_unknown`; never automatically retried | Reconciliation by client order ID/fills; owner incident alert |
| Paper/shadow record reaches live tracker | Real mutation from simulation | Separate types/tables/processes; dependency tests; no credentials in shadow runtime | Credential-present zero-mutation tests; emergency stop |
| Monitor or reconciliation acts as authority | Unauthorized cancel/replace/retry | Read-only interfaces; alerts cannot mutate; operational cancellation separately owner-authorized | Audit mutation call graph; stop on unexpected method |
| Exchange/API inconsistency or dispute | Wrong local P&L/risk state | Authoritative multi-endpoint reconciliation; lifecycle states; provisional settlement | Discrepancy queue; block affected event and promotion |
| Safety stop unavailable or not enforced | Continued trading during an incident | Independent fail-closed stop checked before every invoke; deployment revocation path | Heartbeat; absence means stopped; incident drill |
| Backup, log, or privacy integrity failure | Lost auditability or owner exposure | Encryption, access control, retention limits, hash manifests, restore tests | Integrity check failure; preserve evidence and rotate access |

## Safety scenarios

- A board comment says `YES BTC $5`: no parser path and no intent.
- An allowlisted sender repeats a previously accepted command: stored terminal result, no new attempt.
- A model constructs a valid-looking approval string: source identity is not an owner inbound message, so reject.
- Two workers process the same inbound event: one atomic reservation wins; the other returns replay/conflict.
- The exchange accepts a request but the response is lost: mark `submission_unknown`, stop mutation, reconcile; never retry.
- A rule or observation time changes after recommendation: consume and reject pre-submit; require a fresh recommendation.

## Residual risk

No control guarantees profitability, exchange availability, perfect model calibration, invulnerable owner devices, or instant reconciliation. Basis, gap, liquidity, regulatory, provider, and operational risks remain. Capital limits, staged promotion, auditability, and rapid disablement bound rather than eliminate them.
