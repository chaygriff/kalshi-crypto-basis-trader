"""Fail-closed validation of Phase 0 governance artifacts."""

import re
from pathlib import Path


class GovernanceError(RuntimeError):
    """Raised when the repository governance contract is incomplete."""


REQUIRED_ARTIFACTS = (
    "docs/adr/0001-trust-boundaries.md",
    "docs/adr/0002-financial-state-machines.md",
    "docs/adr/0003-live-transport-isolation.md",
    "docs/governance/AUTHORITY.md",
    "docs/governance/REPOSITORY_CONTROLS.md",
    "docs/governance/THREAT_MODEL.md",
    "docs/governance/VERIFICATION_MATRIX.md",
    "docs/runbooks/INCIDENT_RESPONSE.md",
    "docs/runbooks/KILL_SWITCH.md",
    "docs/runbooks/DATA_RETENTION_AND_RECOVERY.md",
    "docs/runbooks/RELEASE_CHECKLIST.md",
)

AUTHORITY_ASSERTIONS = (
    "Only an exact command from the allowlisted owner identity, received through the configured "
    "WhatsApp adapter and accepted by deterministic code, may consume one fresh immutable "
    "recommendation and create at most one order intent.",
    "The approval is single-use, expires independently of quote freshness, and binds every "
    "economic and payload field.",
    "The following are never trade approval and cannot create an execution intent:",
    "GitHub issues, pull requests, comments, reviews, Projects fields, labels, or automation;",
    "Approving a plan, PR, deployment, strategy version, shadow promotion, paper run, or canary "
    "eligibility does not approve an order.",
    "Phase 0 grants neither.",
)

STATE_MACHINE_ASSERTIONS = (
    "`reserved -> approved -> revalidating -> rejected_pre_submit | ready_to_submit -> "
    "submission_started`",
    "`ready_to_submit` is reachable only from `revalidating` after market status, rules identity, "
    "executable quote, depth, fees, caps, reservations, mode, and kill-switch checks all pass.",
    "`submission_unknown` is never automatically retried.",
    "The original attempt remains terminal as `submission_unknown`; reconciliation appends "
    "separate order and reconciliation events and never changes or revives the attempt.",
    "Client order IDs are unique, deterministic, strategy-owned, immutable, and bound before "
    "invocation.",
    "`shadow|paper -> approved|submission_started`",
)

FORBIDDEN_ENABLEMENT_MARKERS = {
    "LIVE_TRADING_ENABLED",
    "PRODUCTION_ENABLED",
    ".live-trading-enabled",
}
IGNORED_PARTS = {
    ".git",
    ".venv",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "__pycache__",
}
ENABLEMENT_ASSIGNMENT = re.compile(
    r"(?im)^\s*(?:LIVE|LIVE_TRADING_ENABLED|PRODUCTION_ENABLED)\s*=\s*(?:1|true|yes|on)\s*$"
)
FORBIDDEN_SOURCE_CAPABILITIES = re.compile(
    r"\b(?:place_order|submit_order|cancel_all|authenticated_post|sign_order)\b"
)


def validate_governance(root: Path) -> None:
    """Validate mandatory artifacts, policy assertions, and Phase 0 isolation."""
    for relative in REQUIRED_ARTIFACTS:
        path = root / relative
        if not path.is_file() or not path.read_text(encoding="utf-8").strip():
            raise GovernanceError(f"missing required governance artifact: {relative}")

    authority = (root / "docs/governance/AUTHORITY.md").read_text(encoding="utf-8")
    _require_assertions("AUTHORITY.md", authority, AUTHORITY_ASSERTIONS)

    states = (root / "docs/adr/0002-financial-state-machines.md").read_text(encoding="utf-8")
    _require_assertions("state-machine ADR", states, STATE_MACHINE_ASSERTIONS)

    _reject_phase_zero_capabilities(root)


def _require_assertions(document: str, content: str, assertions: tuple[str, ...]) -> None:
    for assertion in assertions:
        if assertion not in content:
            raise GovernanceError(f"{document} missing required policy assertion: {assertion}")


def _reject_phase_zero_capabilities(root: Path) -> None:
    for path in root.rglob("*"):
        if not path.is_file() or any(part in IGNORED_PARTS for part in path.parts):
            continue
        relative = path.relative_to(root)
        if path.name in FORBIDDEN_ENABLEMENT_MARKERS:
            raise GovernanceError(f"Phase 0 forbidden capability: enablement marker {relative}")
        if _is_policy_or_validator_fixture(relative):
            continue
        try:
            content = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        if ENABLEMENT_ASSIGNMENT.search(content):
            raise GovernanceError(f"Phase 0 forbidden capability: live assignment in {relative}")
        if relative.parts[0] == "src" and FORBIDDEN_SOURCE_CAPABILITIES.search(content):
            raise GovernanceError(f"Phase 0 forbidden capability: mutation code in {relative}")


def _is_policy_or_validator_fixture(relative: Path) -> bool:
    return (
        relative.parts[0] in {"docs", "tests"}
        or relative == Path("src/kalshi_crypto_basis/governance.py")
        or relative == Path(".github/pull_request_template.md")
    )
