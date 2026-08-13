# Release Checklist

## Required for every release

- [ ] Scope maps to approved issues and contains no unrelated changes.
- [ ] Working tree and commit SHA are frozen for review.
- [ ] `uv sync --frozen --all-groups` succeeds in a clean environment.
- [ ] `uv run pytest` passes.
- [ ] `uv run ruff check .` passes.
- [ ] `uv run ruff format --check .` passes.
- [ ] `uv run mypy` passes.
- [ ] `uv run pip-audit` passes or a time-bounded owner-approved exception is documented.
- [ ] Secret scan passes across the full Git history and working tree.
- [ ] Governance validator passes.
- [ ] Independent credential-free fail-closed review passes on the exact commit.
- [ ] Required branch checks and review protections pass.
- [ ] Migration, rollback, monitoring, backup, and incident impacts are documented.
- [ ] No credential, session, database, log, backup, or production enablement marker is present.

## Additional gate for financial authority or transport changes

- [ ] Every owner-message rejection path proves zero intent/transport side effects.
- [ ] Exact economic outcome, exchange side, payload price, quantity, fee, limit, and budget parity pass.
- [ ] Concurrent replay and persistence failure injection pass.
- [ ] Capturing fake transport observes only the expected bounded IOC request.
- [ ] Ambiguous invocation stores terminal `submission_unknown` and blocks replay.
- [ ] Actual-channel deterministic routing and owner-visible output are verified.
- [ ] Kill-switch and reconciliation drills pass.
- [ ] Owner separately approves promotion/deployment stage.

## Prohibited shortcuts

A green PR, merged branch, deployed build, board status, owner plan approval, or enabled runtime is not an order approval. Do not merge or deploy around failed or unavailable checks.
