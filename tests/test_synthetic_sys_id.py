"""Phase 85 SYNTH-01 helper unit tests.

Covers is_synthetic_sys_id, encode_inventory_sys_id, decode_inventory_id.
Test fixtures imported from tests/fixtures/synthetic_fixtures.py and reused
by Plans 02-05 to ensure consistent boundary cases across the phase.

REVIEWS-MODE NOTE: the positive ``is_synthetic_sys_id(990001234560000000) -> True``
test (with int input) was INTENTIONALLY REMOVED per Codex MEDIUM-1. The helper
internally tolerates int via str(s) coercion (migration safety) but D-01b
discipline says string-only. The TestNoIntCoercion class is the affirmative
D-01b enforcement — it walks first-party Python files and fails CI if any
``int(sys_id)`` pattern is found outside the helper module itself.
"""
from __future__ import annotations

import pathlib
import re

import pytest

from shared.synthetic_sys_id import (
    decode_inventory_id,
    encode_inventory_sys_id,
    is_synthetic_sys_id,
)
from tests.fixtures.synthetic_fixtures import (
    D13_NORMALIZATION_NEGATIVES,
    REAL_ALMA_NEGATIVE_CASES,
    SYNTHETIC_GOLDEN_CASES,
)


class TestIsSyntheticSysId:
    @pytest.mark.parametrize("inv_id, sys_id, tier, notes", SYNTHETIC_GOLDEN_CASES)
    def test_synthetic_positives(self, inv_id, sys_id, tier, notes):
        assert is_synthetic_sys_id(sys_id) is True, (
            f"{sys_id} ({tier}, {notes}) should classify synthetic"
        )

    @pytest.mark.parametrize("sys_id, library_code, notes", REAL_ALMA_NEGATIVE_CASES)
    def test_real_alma_not_synthetic(self, sys_id, library_code, notes):
        # D-01a invariant: real Alma rows MUST NOT classify synthetic.
        assert is_synthetic_sys_id(sys_id) is False, (
            f"{sys_id} ({library_code}, {notes}) classified synthetic — D-01a collision violation"
        )

    @pytest.mark.parametrize("bad_input, reason", D13_NORMALIZATION_NEGATIVES)
    def test_d13_normalization_contract(self, bad_input, reason):
        # D-13: helper accepts canonical all-digit form only.
        assert is_synthetic_sys_id(bad_input) is False, (
            f"{bad_input!r} ({reason}) should reject"
        )

    # NOTE: positive int-input test (`is_synthetic_sys_id(990001234560000000) is True`)
    # INTENTIONALLY ABSENT per Codex MEDIUM-1 review. See module docstring.
    # The helper still tolerates int internally via `str(s)` coercion for migration
    # safety; that property is exercised indirectly via D13_NORMALIZATION_NEGATIVES
    # (which include None) and via the synthetic positives (which pass strings).


class TestEncodeInventorySysId:
    def test_encode_min_boundary(self):
        assert encode_inventory_sys_id(1) == "990000000001000000"
        assert len(encode_inventory_sys_id(1)) == 18

    def test_encode_typical(self):
        # Per zfill(10) semantics: 123456 -> "0000123456" -> "99" + "0000123456" + "000000".
        assert encode_inventory_sys_id(123456) == "990000123456000000"
        assert len(encode_inventory_sys_id(123456)) == 18

    def test_encode_max_boundary(self):
        assert encode_inventory_sys_id(9999999999) == "999999999999000000"
        assert len(encode_inventory_sys_id(9999999999)) == 18

    @pytest.mark.parametrize("bad", [0, -1, -100])
    def test_encode_non_positive(self, bad):
        with pytest.raises(ValueError, match="positive"):
            encode_inventory_sys_id(bad)

    def test_encode_overflow(self):
        with pytest.raises(ValueError, match="exceeds"):
            encode_inventory_sys_id(10 ** 10)

    @pytest.mark.parametrize("bad", ["123", 1.5, None, [1]])
    def test_encode_non_int(self, bad):
        with pytest.raises(ValueError):
            encode_inventory_sys_id(bad)

    def test_encode_bool_rejected(self):
        # bool is a subclass of int in Python — explicit rejection.
        with pytest.raises(ValueError):
            encode_inventory_sys_id(True)
        with pytest.raises(ValueError):
            encode_inventory_sys_id(False)

    def test_encode_returns_string_never_int(self):
        # D-01b: encode result is a string, always.
        result = encode_inventory_sys_id(123456)
        assert isinstance(result, str)
        assert not isinstance(result, int)


class TestDecodeInventoryId:
    @pytest.mark.parametrize("inv_id, sys_id, tier, notes", SYNTHETIC_GOLDEN_CASES)
    def test_decode_synthetic(self, inv_id, sys_id, tier, notes):
        assert decode_inventory_id(sys_id) == inv_id

    @pytest.mark.parametrize("sys_id, library_code, notes", REAL_ALMA_NEGATIVE_CASES)
    def test_decode_real_alma_returns_none(self, sys_id, library_code, notes):
        assert decode_inventory_id(sys_id) is None

    def test_decode_empty(self):
        assert decode_inventory_id("") is None
        assert decode_inventory_id(None) is None

    @pytest.mark.parametrize("bad_input, reason", D13_NORMALIZATION_NEGATIVES)
    def test_decode_d13_negatives_return_none(self, bad_input, reason):
        # decode never raises on non-synthetic input; returns None.
        assert decode_inventory_id(bad_input) is None, (
            f"{bad_input!r} ({reason}) should decode as None"
        )


class TestRoundTrip:
    @pytest.mark.parametrize(
        "inv_id", [1, 100, 12345, 123456, 999999, 1234567890, 9999999999]
    )
    def test_encode_decode_identity(self, inv_id):
        sys_id = encode_inventory_sys_id(inv_id)
        assert is_synthetic_sys_id(sys_id) is True
        assert decode_inventory_id(sys_id) == inv_id


class TestRealAlmaCollisionNegative:
    """D-01a invariant — collision check is the safety net.

    These assertions MUST hold for the entire libraries.csv corpus. Plan 02
    adds an export-time scan that re-uses is_synthetic_sys_id; failures here
    indicate the discriminator is broken.
    """

    @pytest.mark.parametrize("sys_id, library_code, notes", REAL_ALMA_NEGATIVE_CASES)
    def test_no_real_alma_classifies_synthetic(self, sys_id, library_code, notes):
        assert not is_synthetic_sys_id(sys_id), (
            f"D-01a VIOLATION: real Alma {sys_id} ({library_code}) classified synthetic"
        )


class TestNoIntCoercion:
    """REVIEWS-MODE — D-01b drift guard (Codex MEDIUM-1 affirmative replacement).

    Repo-grep that fails CI if any first-party Python file contains
    ``int(sys_id)``, ``int(raw_sys_id)``, or analogous coercion patterns
    OUTSIDE the helper module itself.

    Allowlist:
      - shared/synthetic_sys_id.py (the documented ``int(str(sys_id)[2:12])`` slice)
      - tests/test_synthetic_sys_id.py (this file — references the pattern in regex)
      - third-party packages, .git/, .planning/, _tmp/, build/

    Detection patterns (see PATTERN regex below):
      - ``int(sys_id)``
      - ``int(raw_sys_id)``
      - ``int(self.sys_id)``
      - ``int(state.sys_id)``
      - ``int(page.sys_id)``
      - Any ``int(<token containing sys_id or sysid or sid>)``
    """

    # Files allowed to contain int(sys_id) coercion (the helper itself + this lint test).
    ALLOWLIST = {
        "shared/synthetic_sys_id.py",
        "tests/test_synthetic_sys_id.py",  # this file
    }

    # Skip these directories (third-party, version control, build artifacts).
    SKIP_DIRS = {
        ".git",
        ".venv",
        "venv",
        "__pycache__",
        "node_modules",
        "_tmp",
        "build",
        "dist",
        ".planning",
        "FIST_DB_BACKUP",
        "fist_data",
        "nli_data",
        "pgp_data",
        "joins_data",
        "Genizah_Index",
        "_internal",
        "extension",
        ".claude",
    }

    # Detect: int(<token containing sys_id/sysid/sid>).
    # Note: helper's documented slice ``int(str(sys_id)[2:12])`` does NOT match this pattern
    # because the inner token is ``str(sys_id)[2:12]`` — starts with ``str(``, not a bare
    # identifier. So the regex naturally tolerates the helper's allowed slice.
    PATTERN = re.compile(
        r"\bint\(\s*([a-zA-Z_][\w.]*sys_?id[\w.]*|[a-zA-Z_][\w.]*sid[\w.]*)\s*\)"
    )

    def _walk_first_party(self, root: pathlib.Path):
        for path in root.rglob("*.py"):
            parts = set(path.relative_to(root).parts)
            if parts & self.SKIP_DIRS:
                continue
            yield path

    def test_no_int_sys_id_coercion(self):
        root = pathlib.Path(__file__).resolve().parent.parent
        violations = []
        for path in self._walk_first_party(root):
            rel = str(path.relative_to(root)).replace("\\", "/")
            if rel in self.ALLOWLIST:
                continue
            try:
                content = path.read_text(encoding="utf-8")
            except (UnicodeDecodeError, OSError):
                continue
            for ln, line in enumerate(content.splitlines(), 1):
                # Skip comment-only lines.
                stripped = line.lstrip()
                if stripped.startswith("#"):
                    continue
                m = self.PATTERN.search(line)
                if m:
                    violations.append(f"{rel}:{ln}: {line.strip()[:160]}")
        assert not violations, (
            "D-01b VIOLATION: bare int(sys_id) coercion found outside the helper module.\n"
            "Use decode_inventory_id() from shared.synthetic_sys_id instead.\n"
            "Violations:\n  " + "\n  ".join(violations[:20])
        )
