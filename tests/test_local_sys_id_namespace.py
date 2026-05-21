# -*- coding: utf-8 -*-
"""Phase 95 REQ-2: LOCAL sys_id namespace guarantees.

Tests:
  - is_local_sys_id golden / negative cases (REQ-2)
  - Namespace disjoint from synthetic-99 (MEDIUM-3 review fix: full libraries.csv scan)
  - _machine_id / _content_hash always exactly 8 decimal digits (D-19)
  - generate_local_sys_id format (D-18 / D-19)
  - TestNoIntCoercion AST lint (mirrors test_synthetic_sys_id.py::TestNoIntCoercion)
"""
from __future__ import annotations

import pathlib
import re

import pytest

from shared.local_sys_id import (
    _content_hash,
    _machine_id,
    generate_local_sys_id,
    is_local_sys_id,
)
from shared.synthetic_sys_id import is_synthetic_sys_id
from tests.fixtures.local_sys_id_fixtures import (
    LOCAL_GOLDEN_CASES,
    LOCAL_NEGATIVE_CASES,
    LOCAL_REAL_ALMA_NEGATIVE_CASES,
    LOCAL_SYNTHETIC_99_NEGATIVE_CASES,
)


class TestIsLocalSysId:
    @pytest.mark.parametrize("sys_id", LOCAL_GOLDEN_CASES)
    def test_golden_cases(self, sys_id):
        assert is_local_sys_id(sys_id) is True, (
            f"{sys_id!r} should classify as LOCAL"
        )

    @pytest.mark.parametrize("sys_id", LOCAL_REAL_ALMA_NEGATIVE_CASES)
    def test_real_alma_negative(self, sys_id):
        assert is_local_sys_id(sys_id) is False, (
            f"{sys_id!r} (real Alma) must NOT classify as LOCAL"
        )

    @pytest.mark.parametrize("sys_id", LOCAL_SYNTHETIC_99_NEGATIVE_CASES)
    def test_synthetic_99_negative(self, sys_id):
        assert is_local_sys_id(sys_id) is False, (
            f"{sys_id!r} (99-prefix synthetic) must NOT classify as LOCAL"
        )

    @pytest.mark.parametrize("bad_input", LOCAL_NEGATIVE_CASES)
    def test_negative_cases(self, bad_input):
        assert is_local_sys_id(bad_input) is False, (
            f"{bad_input!r} should NOT classify as LOCAL"
        )


class TestNamespaceDisjoint:
    """Verify LOCAL and synthetic-99 namespaces are fully disjoint."""

    @pytest.mark.parametrize("sys_id", LOCAL_GOLDEN_CASES)
    def test_synthetic_helper_rejects_local(self, sys_id):
        assert is_synthetic_sys_id(sys_id) is False, (
            f"is_synthetic_sys_id({sys_id!r}) must return False (LOCAL is not synthetic)"
        )

    @pytest.mark.parametrize("sys_id", LOCAL_SYNTHETIC_99_NEGATIVE_CASES)
    def test_local_helper_rejects_synthetic(self, sys_id):
        assert is_local_sys_id(sys_id) is False, (
            f"is_local_sys_id({sys_id!r}) must return False (synthetic is not LOCAL)"
        )

    def test_full_libraries_csv_no_local(self):
        """SPEC REQ-2 acceptance: is_local_sys_id is False for EVERY sys_id in libraries.csv.

        MEDIUM-3 review fix (2026-05-21): the previous version capped iteration at
        1,000 rows for runtime savings. SPEC REQ-2 explicitly requires the scan of
        every row in libraries.csv (255K rows). The cap is removed; the full file
        is scanned. Runtime cost: ~0.5-1.5s on a modern machine -- acceptable for a
        locked acceptance criterion. This is a one-time check at test time, NOT a
        runtime cost on every search.
        """
        csv_path = pathlib.Path(__file__).parent.parent / "libraries.csv"
        if not csv_path.exists():
            pytest.skip("libraries.csv not present in test environment")
        import csv as csvmod

        offenders = []
        row_count = 0
        with open(csv_path, encoding="utf-8-sig", errors="replace") as f:
            reader = csvmod.reader(f)
            for i, row in enumerate(reader):
                row_count += 1
                if not row:
                    continue
                sid = row[0].strip()
                if is_local_sys_id(sid):
                    offenders.append((i, sid))
                    if len(offenders) > 50:
                        # Bail early if the helper is fundamentally broken.
                        break
        # MEDIUM-3: the scan covered the FULL file, not a 1,000-row prefix.
        assert row_count > 200_000, (
            f"libraries.csv only had {row_count} rows scanned; SPEC REQ-2 requires "
            f"~255K rows. Test may be reading the wrong file or hitting an early termination."
        )
        assert not offenders, (
            f"libraries.csv contains {len(offenders)} LOCAL-classified rows (97-prefix); "
            f"first 10: {offenders[:10]}. Namespace boundary is violated."
        )


class TestMachineId:
    def test_always_8_digits(self):
        mid = _machine_id()
        assert len(mid) == 8, f"_machine_id() returned {len(mid)} chars: {mid!r}"
        assert mid.isdigit(), f"_machine_id() returned non-digit chars: {mid!r}"

    def test_deterministic(self):
        assert _machine_id() == _machine_id(), "_machine_id() must be deterministic"


class TestContentHash:
    def test_always_8_digits(self):
        h = _content_hash("/any/path")
        assert len(h) == 8, f"_content_hash returned {len(h)} chars: {h!r}"
        assert h.isdigit(), f"_content_hash returned non-digit chars: {h!r}"

    def test_collision_slot_returns_different(self):
        path = "/tmp/test_collision.pdf"
        h0 = _content_hash(path, slot=0)
        h1 = _content_hash(path, slot=1)
        assert h0 != h1, (
            f"_content_hash slot=0 and slot=1 must differ for same path; got {h0!r} == {h1!r}"
        )

    def test_slot_out_of_range_raises(self):
        with pytest.raises(ValueError, match="slot out of range"):
            _content_hash("/tmp/foo", slot=8)
        with pytest.raises(ValueError, match="slot out of range"):
            _content_hash("/tmp/foo", slot=-1)


class TestGenerateLocalSysId:
    def test_format(self):
        sid = generate_local_sys_id("/tmp/test.pdf")
        assert len(sid) == 18, f"generate_local_sys_id returned {len(sid)}-char string: {sid!r}"
        assert sid.startswith("97"), f"Expected '97' prefix, got: {sid[:2]!r}"
        assert is_local_sys_id(sid), f"generate_local_sys_id result fails is_local_sys_id: {sid!r}"

    def test_slot_changes_result(self):
        sid0 = generate_local_sys_id("/tmp/test.pdf", slot=0)
        sid1 = generate_local_sys_id("/tmp/test.pdf", slot=1)
        # machine_id part is the same; content_hash part differs
        assert sid0[:10] == sid1[:10], "machine_id part should match"
        assert sid0 != sid1, "slot=0 and slot=1 must produce different sys_ids"


class TestNoIntCoercion:
    """D-19 drift guard: repo-grep that fails CI if any first-party Python file
    contains ``int(sys_id)``, ``int(local_id)`` or analogous coercion patterns
    OUTSIDE the helper module itself.

    Mirrors tests/test_synthetic_sys_id.py::TestNoIntCoercion line-for-line;
    swaps the ALLOWLIST and TARGET.
    """

    ALLOWLIST = {
        "shared/local_sys_id.py",
        "tests/test_local_sys_id_namespace.py",  # this file
        # test_synthetic_sys_id.py documents the int(sys_id) pattern in its
        # module docstring and class docstring as comment text — not executable coercion.
        "tests/test_synthetic_sys_id.py",
    }

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

    # Detect: int(<token containing sys_id/sysid/local_id/sid>).
    PATTERN = re.compile(
        r"\bint\(\s*([a-zA-Z_][\w.]*local_?id[\w.]*|[a-zA-Z_][\w.]*sys_?id[\w.]*|[a-zA-Z_][\w.]*sid[\w.]*)\s*\)"
    )

    def _walk_first_party(self, root: pathlib.Path):
        for path in root.rglob("*.py"):
            parts = set(path.relative_to(root).parts)
            if parts & self.SKIP_DIRS:
                continue
            yield path

    def test_no_int_coercion_outside_allowlist(self):
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
                stripped = line.lstrip()
                if stripped.startswith("#"):
                    continue
                m = self.PATTERN.search(line)
                if m:
                    violations.append(f"{rel}:{ln}: {line.strip()[:160]}")
        assert not violations, (
            "D-19 VIOLATION: bare int(local_id) / int(sys_id) coercion found outside the helper module.\n"
            "Use is_local_sys_id() from shared.local_sys_id instead.\n"
            "Violations:\n  " + "\n  ".join(violations[:20])
        )
