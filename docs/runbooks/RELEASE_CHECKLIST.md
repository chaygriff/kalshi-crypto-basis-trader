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

## Additional gate for live read-only data changes

- [ ] Provider origin, route, HTTP method, redirect, TLS, timeout, response-size, decompression, and content-type policies are explicit and tested.
- [ ] The exposed transport capability contains no generic request or mutation method; credential-present tests capture zero mutation requests.
- [ ] Local HTTP-server tests cover malformed responses, duplicate keys, non-finite values, rate limits, pagination termination, partial collection, and redacted errors.
- [ ] Every research-eligible provider response is retained as exact raw bytes plus a validated immutable envelope outside Git.
- [ ] One-shot run identity, terminal completeness state, deterministic gap report, idempotent rerun, crash recovery, and restart replay pass.
- [ ] A supervised live smoke check is bounded, read-only, isolated from CI, and reports no credentials, headers, signatures, private account values, or private identifiers.
- [ ] Retained live evidence replays to the same normalized identity before recurring collection is considered.
- [ ] Scheduler, service, and streaming capabilities remain disabled unless their separate verification rows and release scope pass.

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
