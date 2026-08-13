# Incident Response Runbook

## Trigger conditions

Invoke this runbook for suspected credential/session compromise, unexpected mutation, `submission_unknown`, duplicate intent/order, ledger or reconciliation conflict, rule/side/payload mismatch, cap breach, unauthorized sender event, secret exposure, or inability to verify kill-switch state.

## Immediate actions

1. Activate the kill switch and revoke the transport deployment capability. Absence of confirmation means assume stopped but verify from an independent read path.
2. Preserve WhatsApp source events, append-only ledgers, deployment metadata, and redacted logs. Do not delete or rewrite evidence.
3. Do not retry, replace, resize, or cancel based solely on local state.
4. Query authoritative Kalshi orders, fills, positions, balances, and market lifecycle through read-only reconciliation.
5. Notify the owner with known facts, unknowns, maximum exposure, and explicit actions that were not taken.
6. Rotate affected Kalshi, WhatsApp, GitHub, deployment, or provider credentials outside the repository.

## Containment

- Block the affected strategy version and event-expiry risk group.
- Cancel only strategy-owned resting orders through a separately owner-authorized operational action after identity reconciliation.
- Keep market-data collection and reconciliation available if safe; keep all mutation disabled.

## Recovery criteria

- Root cause documented and regression reproduced.
- Fix passes full tests, security gates, and independent frozen-tree review.
- Orders, fills, positions, balances, and settlements reconcile authoritatively.
- Credentials/sessions rotated where exposure was possible.
- Owner explicitly approves runtime re-enablement; a new exact order approval is still required per order.

## Post-incident report

Record timeline, source events, affected versions, exposure, authoritative remote state, control successes/failures, corrective actions, residual risk, and evidence links without secrets or private key material.
