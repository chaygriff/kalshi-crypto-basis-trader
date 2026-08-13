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

## Current platform limitation

On 2026-08-13, GitHub returned HTTP 403 when branch protection and repository rulesets were requested for this private user-owned repository: the feature requires GitHub Pro or a public repository. Making a financial system public solely to obtain this control is rejected.

Until private branch protection is available:

- direct pushes to `main` are procedurally forbidden;
- all changes use PRs and the release checklist;
- independent review must bind to the exact commit;
- merge is owner-controlled after checks pass;
- production promotion remains blocked if protected-branch enforcement is considered mandatory by the exit gate.

The owner can resolve the limitation by upgrading the GitHub plan or moving the private repository to an organization whose plan supports rulesets. After resolution, apply and verify the intended branch policy before any production canary.

## Release governance

- CI success proves only repository checks, not profitability, runtime readiness, or trade approval.
- Failed or unavailable required checks block merge and release.
- Dependabot may propose lockfile or action updates; each update still requires tests and review.
- No CI workflow receives Kalshi, WhatsApp, deployment, or production database credentials.
- Deployments, runtime capability enablement, strategy promotion, and exact order approval are separate decisions.
