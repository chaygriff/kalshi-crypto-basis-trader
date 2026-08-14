# Safety Stop Runbook

## Policy

The safety stop is independent of strategy and messaging state. It is checked immediately before every transport invocation. Missing, unreadable, stale, contradictory, or corrupt stop state means **STOPPED**. It never disables read-only reconciliation. This is the control commonly called a kill switch in exchange operations; the repository uses “safety stop” in agent-facing prose.

## Activation triggers

Activate on owner request, `submission_unknown`, unauthorized sender activity, secret exposure, duplicate/concurrent submission anomaly, cap or side mismatch, ledger inconsistency, stale rule/fee state, monitor/reconciliation failure, drawdown breach, or deployment uncertainty.

## Activation procedure

1. Revoke or disable the transport runtime capability at the deployment boundary.
2. Set durable switch state to stopped through the authenticated operational control.
3. Verify from a separate read-only process that new mutations are denied.
4. Reconcile current orders, fills, positions, and balances.
5. Notify the owner with timestamp, reason, known exposure, and reconciliation state.

Never rely on a GitHub commit, environment-file edit, model instruction, or WhatsApp prose alone as the switch mechanism.

## Re-enablement

Requires resolved incident, clean reconciliation, passing regression and full gates, independent review, owner approval of the release/canary stage, and a short-lived deployment capability. Re-enablement never approves an order.
