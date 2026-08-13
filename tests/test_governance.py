from pathlib import Path

import pytest

from kalshi_crypto_basis import __version__
from kalshi_crypto_basis.governance import GovernanceError, validate_governance

ROOT = Path(__file__).parents[1]


def test_package_version_matches_phase_zero_manifest() -> None:
    assert __version__ == "0.0.0"


def test_repository_governance_contract_is_complete() -> None:
    validate_governance(ROOT)


def test_missing_required_artifact_fails_closed(tmp_path: Path) -> None:
    with pytest.raises(GovernanceError, match="missing required governance artifact"):
        validate_governance(tmp_path)


def test_unreviewed_live_enablement_marker_fails_closed(tmp_path: Path) -> None:
    _copy_governance(tmp_path)
    (tmp_path / "LIVE_TRADING_ENABLED").write_text("true\n", encoding="utf-8")

    with pytest.raises(GovernanceError, match="Phase 0 forbidden capability"):
        validate_governance(tmp_path)


def test_authority_document_must_deny_github_and_agents(tmp_path: Path) -> None:
    _copy_governance(tmp_path)
    authority = tmp_path / "docs/governance/AUTHORITY.md"
    authority.write_text(
        authority.read_text(encoding="utf-8").replace("GitHub", "planning system"),
        encoding="utf-8",
    )

    with pytest.raises(GovernanceError, match=r"AUTHORITY\.md missing required policy assertion"):
        validate_governance(tmp_path)


def test_keyword_only_authority_policy_fails_closed(tmp_path: Path) -> None:
    _copy_governance(tmp_path)
    authority = tmp_path / "docs/governance/AUTHORITY.md"
    authority.write_text(
        "GitHub agent WhatsApp single-use deterministic never trade approval\n",
        encoding="utf-8",
    )

    with pytest.raises(GovernanceError, match="missing required policy assertion"):
        validate_governance(tmp_path)


def test_keyword_only_state_machine_fails_closed(tmp_path: Path) -> None:
    _copy_governance(tmp_path)
    states = tmp_path / "docs/adr/0002-financial-state-machines.md"
    states.write_text(
        "submission_unknown never automatically retried forbidden transitions terminal "
        "deterministic, strategy-owned ready_to_submit\n",
        encoding="utf-8",
    )

    with pytest.raises(GovernanceError, match="missing required policy assertion"):
        validate_governance(tmp_path)


@pytest.mark.parametrize(
    ("relative", "content"),
    [
        ("config/LIVE_TRADING_ENABLED", "true\n"),
        (".env", "LIVE=true\n"),
        ("src/live_transport.py", "def place_order(): ...\n"),
    ],
)
def test_phase_zero_live_capability_fails_closed(
    tmp_path: Path, relative: str, content: str
) -> None:
    _copy_governance(tmp_path)
    path = tmp_path / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")

    with pytest.raises(GovernanceError, match="Phase 0 forbidden capability"):
        validate_governance(tmp_path)


def _copy_governance(target: Path) -> None:
    for relative in (
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
    ):
        source = ROOT / relative
        destination = target / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
