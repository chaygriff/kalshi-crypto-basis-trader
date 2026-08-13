# Repository and CI Controls

## Required checks

Every PR and push to `main` must run:

- `quality`: governance validator, pytest, Ruff lint/format, and strict mypy;
- `dependency-audit`: locked-environment `pip-audit`;
- `secret-scan`: full-history verified-secret scan.

Workflow permissions default to read-only, checkout does not persist credentials, jobs have bounded timeouts, dependencies are lockfile-based, and third-party actions are pinned to immutable commit SHAs.

## Intended branch policy

`main` should require:

- pull requests rather than direct pushes;
- passing `quality`, `dependency-audit`, and `secret-scan` checks;
- branches current with `main`;
- one approving independent review, with stale approvals dismissed;
- resolution of review conversations;
- no force pushes or branch deletion;
- administrator enforcement;
- signed commits when operationally available.

## Public repository policy

On 2026-08-13, the owner authorized public visibility so GitHub branch protection could be enforced without a paid plan. Before the visibility change, the complete Git history and tracked tree were scanned for verified and unverified secrets, sensitive filenames, private contact/session identifiers, and hidden blobs. Commit metadata was rewritten to use the owner's GitHub noreply address.

Public visibility does not permit sensitive operational data. Never commit or publish credentials, API key IDs, private keys, account identifiers, WhatsApp sender/chat/session identifiers, owner phone or private email addresses, balances, positions, local databases, logs, approval messages, production configuration, backup archives, or private incident evidence.

If sensitive information is discovered:

1. activate incident response and disable affected capabilities;
2. rotate or revoke the exposed material immediately;
3. assess clones, forks, caches, Actions logs, artifacts, and Git history;
4. remove data only after containment—history rewriting is not a substitute for rotation;
5. document sanitized remediation evidence without reproducing the secret.

Moving back to private visibility requires confirming that the account or organization plan continues to support the same branch-protection controls.

## Release governance

- CI success proves only repository checks, not profitability, runtime readiness, or trade approval.
- Failed or unavailable required checks block merge and release.
- Dependabot may propose lockfile or action updates; each update still requires tests and review.
- No CI workflow receives Kalshi, WhatsApp, deployment, or production database credentials.
- Deployments, runtime capability enablement, strategy promotion, and exact order approval are separate decisions.
