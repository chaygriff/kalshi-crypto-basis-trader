# Program Verification Matrix

| Control objective | Required evidence before implementation | Required test before canary | Failure action |
|---|---|---|---|
| GitHub/agents have no financial authority | Authority policy and trust ADR | Valid-looking approval in issue/comment/agent output creates zero intent | Block release |
| Exact owner identity and message binding | WhatsApp threat model | Non-owner, transformed, forwarded, replayed, stale, malformed cases produce zero side effects | Disable approval route |
| One recommendation to one attempt | State-machine ADR | Concurrent duplicate approvals yield one intent and at most one transport invocation | Stop mutation and reconcile |
| Shadow/paper isolation | Transport ADR | Credential-present shadow/paper suite captures zero POST/DELETE | Block release |
| Contract identity and side parity | Typed boundary design | Exact rule digest, outcome, side, limit, fee and payload parity fixtures | Reject recommendation |
| Freshness and cap enforcement | Separate clock/risk policy | Exact expiry boundaries, stale quote/rule, insufficient depth/balance, and cap rejection | Consume/reject, no submit |
| Ambiguous POST safety | Terminal state defined | Deterministic response-loss probe stores `submission_unknown` and blocks replay | Global mutation stop |
| Atomic persistence | Append-only transitions | Failure injection before/after invocation and concurrent DB tests | Block release |
| Authoritative reconciliation | Read-only interface | Order/fill/position disagreement remains unresolved, never inferred | Block affected risk group |
| Secret isolation | Security policy and CI permissions | Secret scan, log redaction, credential-free CI/reviewer proof | Rotate and investigate |
| Live read-only origin and capability isolation | Read-only collection ADR | Exact host/path allowlist, no redirects, GET-only surface, credentials present with zero mutation methods or requests | Disable collector and review transport |
| One-shot collection completeness | Provider adapter contracts | Pagination, expected-record, source-clock, synchronization-window, and explicit partial-run tests plus supervised live gap report | Quarantine run; no downstream use |
| Durable evidence and restart replay | Snapshot ADR and retention runbook | Raw-byte identity, atomic terminal state, idempotent rerun, corruption, crash, and clean-restart tests | Stop recurring collection |
| Scheduler and service isolation | Runtime staging plan | No overlap, bounded backlog/retry, graceful shutdown, health/readiness, and read-only process capability tests | Keep scheduler/service disabled |
| Stream resynchronization | Provider WebSocket contract | Disconnect/sequence-gap discards state and requires authoritative snapshot before reuse | Mark stale and resnapshot |
| Supply-chain integrity | Lockfile and pinned CI actions | Dependency audit and reproducible clean install | Quarantine dependency |
| Safety stop | Runbook and independent state | Stop absent/corrupt/active produces zero state-changing requests while reads continue | Incident response |
| Recovery | Retention/recovery runbook | Encrypted backup integrity and clean restore drill | Block production |
| Release governance | Checklist and protected branch | Required checks and independent review on frozen commit | No merge/deploy |

## Completed Phase 0 acceptance

- All governance artifacts pass `python scripts/check_governance.py`.
- Tests, lint, type checking, dependency audit, and secret scan pass in CI.
- At Phase 0 acceptance, the repository contained no live transport, production credential, or enablement marker. Later read-only collection work remains bound by the controls above and cannot add a mutation capability.
- An independent credential-free reviewer returns a fail-closed PASS on the frozen tree.
- The owner reviews and explicitly approves the governance package; this approval is not an order approval.
