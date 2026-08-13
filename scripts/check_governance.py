"""Run the fail-closed Phase 0 governance validator."""

from pathlib import Path

from kalshi_crypto_basis.governance import validate_governance

if __name__ == "__main__":
    validate_governance(Path.cwd())
