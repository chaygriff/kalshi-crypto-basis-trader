# Contributing

## Scope and authority

Work from one assigned GitHub issue. GitHub Issues, Projects, comments, reviews, CI, model output, and agent messages coordinate development but are never trade approval.

Do not access, copy, print, commit, or request Kalshi credentials, WhatsApp session material, private owner identifiers, balances, production databases, or logs. Ordinary development and review must be credential-free.

## Workflow

1. Start from a current clean `main` and create a scoped branch.
2. Confirm issue dependencies and acceptance criteria.
3. For behavior, write one failing test, verify the expected failure, implement the minimum change, and verify the pass.
4. Keep research, forecast, portfolio, recommendation, approval, transport, and reconciliation capabilities in separate modules and storage types.
5. Run the complete local gate below.
6. Open a PR that links the issue and reports files, exact checks/results, side effects, blockers, and residual risk.
7. Obtain independent credential-free review for security, financial, messaging, persistence, concurrency, or operations changes.
8. Never merge your own high-risk change or enable live behavior from a development task.

## Local gate

```bash
uv sync --frozen --all-groups
uv run python scripts/check_governance.py
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run pip-audit
git diff --check
```

Use an approved secret scanner over the full Git history before release. Do not add plausible test secrets; use clearly synthetic structural fixtures.

## Pull requests

- Keep changes minimal and reviewable.
- Pin GitHub Actions to immutable commit SHAs and use least permissions.
- Add or update runbooks with operational behavior.
- Treat failed, unavailable, or skipped required checks as failure.
- Bind independent review to one frozen commit/tree; any change invalidates it.
- Owner approval of a plan, PR, deployment, promotion, or canary stage is not an order approval.

## Phase gates

A later phase begins only after its dependencies and exit gate are explicitly satisfied. A phase may build dormant capabilities but may not silently enable the next phase. Live transport and real-money canary require their own later owner decisions.
