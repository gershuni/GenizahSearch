"""Shared loaders for Phase 81B skill tests. Imported explicitly by test files."""
import json
from pathlib import Path

FIXTURE_DIR = Path(__file__).parent.parent / "skills" / "cairo-genizah-research" / "scripts" / "fixtures"


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))
