---
phase: 95
plan: 02
type: execute
wave: 1
depends_on: [01]
files_modified:
  - shared/local_sys_id.py
  - genizah_core.py
  - tests/test_local_sys_id_namespace.py
  - tests/test_local_sys_id_parser_compat.py
  - tests/test_canonical_filepath.py
autonomous: true
requirements: [REQ-2]
must_haves:
  truths:
    - "is_local_sys_id(s) returns True for any 18-digit 97-prefixed string"
    - "is_local_sys_id returns False for every row in libraries.csv (regression scan)"
    - "is_synthetic_sys_id continues to return False for 97-prefixed IDs (disjoint namespaces)"
    - "machine_id and content_hash always produce exactly 8 decimal digits (D-19 % 10**8 contract)"
    - "_canonical_filepath produces identical strings for equivalent Windows paths (UNC, junction, casing)"
    - "parse_header_smart and parse_full_id_components recognize 97-prefix LOCAL sys_ids"
    - "LIBRARY_CODES contains 'LOCAL': 'My Library' and Hebrew 'הספרייה שלי'"
  artifacts:
    - path: "shared/local_sys_id.py"
      provides: "is_local_sys_id, generate_local_sys_id, _canonical_filepath, _machine_id, _content_hash"
      contains: "is_local_sys_id"
      min_lines: 80
    - path: "genizah_core.py"
      provides: "LIBRARY_CODES + parse_header_smart + parse_full_id_components extensions"
      contains: "'LOCAL': 'My Library'"
    - path: "tests/test_local_sys_id_namespace.py"
      provides: "REQ-2 acceptance tests (green)"
    - path: "tests/test_local_sys_id_parser_compat.py"
      provides: "D-13 P0 parser compat tests (green)"
    - path: "tests/test_canonical_filepath.py"
      provides: "D-42 Windows-path normalization tests (green)"
  key_links:
    - from: "shared/local_sys_id.py"
      to: "tests/fixtures/local_sys_id_fixtures.py"
      via: "imports LOCAL_GOLDEN_CASES etc."
      pattern: "from tests.fixtures.local_sys_id_fixtures"
    - from: "genizah_core.py:parse_header_smart"
      to: "shared/local_sys_id.is_local_sys_id (optional cross-check)"
      via: "regex broadening from `99\\d{8,}` to `(?:99|97)\\d{8,}`"
      pattern: "(?:99|97)"
---

<objective>
Ship the LOCAL sys_id helper module (`shared/local_sys_id.py`) and generalize `genizah_core.py` parsers so 97-prefix LOCAL sys_ids are recognized everywhere existing code recognizes 99-prefix synthetic IDs (Codex P0 D-13 fix). Also extend `LIBRARY_CODES` with the `LOCAL` entry and add the Hebrew display name.

Purpose: This is the namespace foundation. Without these, Wave 1 indexer can't generate sys_ids, Wave 2 search merger can't filter LOCAL vs Genizah, Wave 1 cloud gates can't check `is_local_sys_id`, and Wave 3 result rows can't display "My Library" as library name.

Output: Pure-function helper module + extended core parsers + 3 GREEN test files (turning Wave-0 stubs green).
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/PROJECT.md
@.planning/phases/95-my-library/95-CONTEXT.md
@.planning/phases/95-my-library/95-PATTERNS.md
@shared/synthetic_sys_id.py
@tests/test_synthetic_sys_id.py
@tests/fixtures/local_sys_id_fixtures.py

<interfaces>
<!-- Existing helper template (verbatim from shared/synthetic_sys_id.py:1-76, already read) -->

From shared/synthetic_sys_id.py:
```python
_SYNTHETIC_PREFIX = "99"
_SYNTHETIC_SUFFIX = "000000"
_INVENTORY_PAD = 10
_TOTAL_LENGTH = 2 + _INVENTORY_PAD + 6  # 18

def is_synthetic_sys_id(s: object) -> bool:
    if not s:
        return False
    s = str(s)
    if not s.isdigit():
        return False
    if len(s) != _TOTAL_LENGTH:
        return False
    return s.startswith(_SYNTHETIC_PREFIX) and s.endswith(_SYNTHETIC_SUFFIX)
```

From genizah_core.py:3640-3681 (parse_header_smart + parse_full_id_components):
```python
def parse_header_smart(self, full_header):
    sys_match = re.search(r'(99\d{8,})', full_header)
    sys_id = sys_match.group(1) if sys_match else None
    p_num = "Unknown"
    p_match = re.search(r'_P(\d+)_', full_header)
    if p_match:
        p_num = str(int(p_match.group(1)))
    else:
        tif_match = re.search(r'[ -_](\d{3,4})\.tif', full_header, re.IGNORECASE)
        if tif_match: p_num = str(int(tif_match.group(1)))
    return sys_id, p_num

def parse_full_id_components(self, full_header):
    result = {'sys_id': None, 'ie_id': None, 'p_num': None, 'fl_id': None}
    sys_match = re.search(r'(99\d{8,})', full_header)
    if sys_match:
        result['sys_id'] = sys_match.group(1)
    ie_match = re.search(r'(IE\d+)', full_header)
    if ie_match:
        result['ie_id'] = ie_match.group(1)
    p_match = re.search(r'_?(P\d+)', full_header)
    if p_match:
        raw_p = p_match.group(1)
        result['p_num'] = str(int(raw_p[1:]))
    fl_match = re.search(r'(FL\d+)', full_header)
    if fl_match:
        result['fl_id'] = fl_match.group(1).replace("FL", "")
    return result
```

D-34 LOCAL full_header format:
- `unique_id` per page: `f"LOCAL_{sys_id}_P{page_num}"` — example `"LOCAL_970012345601234567_P3"`
- `full_header` per page: `f"{sys_id}_LOCAL_P{page_num}_F{file_id:04d}"` — example `"970012345601234567_LOCAL_P3_F0042"`
- Synthetic `ie_id` for parser: `f"F{file_id:04d}"` (e.g., `"F0042"`)
</interfaces>
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: Create shared/local_sys_id.py helper module (D-19, D-42 per CONTEXT)</name>
  <read_first>
    - shared/synthetic_sys_id.py (template — entire 140 lines; constants block + is_synthetic_sys_id body)
    - .planning/phases/95-my-library/95-PATTERNS.md ("shared/local_sys_id.py" section — "Mirror this" + "Divergences")
    - .planning/phases/95-my-library/95-CONTEXT.md (D-18, D-19, D-42 — exact formulas)
    - tests/fixtures/local_sys_id_fixtures.py (LOCAL_GOLDEN_CASES + negative cases)
  </read_first>
  <behavior>
    - Test: `is_local_sys_id("970012345601234567")` returns True (18 digits, 97 prefix).
    - Test: `is_local_sys_id("990025143260205171")` returns False (99-prefix real Alma).
    - Test: `is_local_sys_id("990001234560000000")` returns False (99-prefix synthetic).
    - Test: `is_local_sys_id("")` returns False; `is_local_sys_id(None)` returns False.
    - Test: `is_local_sys_id("97" + "a" * 16)` returns False (non-digit).
    - Test: `_machine_id()` returns exactly 8 decimal digits regardless of hostname.
    - Test: `_content_hash("c:\\Users\\x\\foo.pdf")` returns exactly 8 decimal digits.
    - Test: `_machine_id()` is deterministic across calls within one process.
    - Test: `_canonical_filepath("C:/Users/x/Foo.PDF")` and `_canonical_filepath("c:\\users\\X\\foo.pdf")` return the SAME string (Windows: case-insensitive, separator-normalized).
    - Test: `generate_local_sys_id("/tmp/foo.pdf")` returns an 18-digit string starting with "97".
    - Test (D-19 collision retry): `_content_hash(path, slot=1)` produces a DIFFERENT 8-digit value than `_content_hash(path, slot=0)`.
    - Test: `is_synthetic_sys_id("970012345601234567")` STILL returns False (disjoint namespaces verified — regression on shared/synthetic_sys_id.py).
  </behavior>
  <action>
    Create `shared/local_sys_id.py` with the EXACT structure mirroring `shared/synthetic_sys_id.py`. Per CONTEXT D-19 (Codex revision) the load-bearing arithmetic is `% 10**8` — both `_machine_id` and `_content_hash` MUST apply this modulo. Required code:

    ```python
    # -*- coding: utf-8 -*-
    """Phase 95 LOCAL sys_id helpers (LOCAL-NAMESPACE / REQ-2).

    The 18-digit format ``97 + machine_id(8 decimal digits) + content_hash(8 decimal digits)``
    is the only publishable contract. All call sites MUST consult these helpers; never
    hand-roll string slicing or int() conversions.

    Per CONTEXT D-19: machine_id and content_hash both use ``% 10**8`` to guarantee
    exactly 8 decimal digits. Without the modulo, ``hex(...)[:8]`` can produce up to
    10 decimal digits and overflow the 18-digit slot.

    Per CONTEXT D-42: filepaths are normalized via ``_canonical_filepath`` BEFORE
    hashing. Same physical file must produce same sys_id across rescans regardless
    of case differences, separator differences, junctions, or 8.3 short names.

    Per CONTEXT D-19 collision-retry: `_content_hash(path, slot=N)` walks deeper
    into the SHA256 hex digest on collision (slot 0 = chars [0:8], slot 1 = chars
    [8:16], slot 2 = chars [16:24], slot 3 = chars [24:32]). Indexer caps retries at 4.

    The repo-grep lint test in ``tests/test_local_sys_id_namespace.py::TestNoIntCoercion``
    enforces the string-in/string-out contract.

    Public API:
      is_local_sys_id(s)                      -> bool
      generate_local_sys_id(filepath, slot=0) -> str
      _canonical_filepath(p)                  -> str   (private, but tested)
      _machine_id()                           -> str   (private, but tested)
      _content_hash(canonical, slot=0)        -> str   (private, but tested)
    """
    from __future__ import annotations

    import hashlib
    import os
    import socket
    from pathlib import Path
    from typing import Union

    _LOCAL_PREFIX = "97"
    _MACHINE_PAD = 8
    _HASH_PAD = 8
    _TOTAL_LENGTH = 2 + _MACHINE_PAD + _HASH_PAD  # 18


    def is_local_sys_id(s: object) -> bool:
        """Return True iff ``s`` represents a Phase-95 LOCAL sys_id.

        The discriminator is ``97`` prefix + 18-digit total length. Unlike
        synthetic (Phase 85 — has a ``000000`` suffix as additional discriminator),
        LOCAL has no suffix; the prefix + length is sufficient.

        Examples:
            >>> is_local_sys_id("970012345601234567")
            True
            >>> is_local_sys_id("990001234560000000")  # 99-prefix synthetic
            False
            >>> is_local_sys_id("990025143260205171")  # real Alma
            False
            >>> is_local_sys_id("")
            False
            >>> is_local_sys_id(None)
            False
        """
        if not s:
            return False
        s = str(s)
        if not s.isdigit():
            return False
        if len(s) != _TOTAL_LENGTH:
            return False
        return s.startswith(_LOCAL_PREFIX)


    def _canonical_filepath(p: Union[str, Path]) -> str:
        """Canonical form for sys_id generation and folder-overlap detection (D-42).

        Resolves symlinks/junctions (strict=False so missing files still normalize),
        normalizes case (Windows: lowercase drive letter + path), normalizes separators.

        Examples (Windows):
            >>> # All three return the same string:
            >>> # _canonical_filepath("C:/Users/x/Foo.PDF")
            >>> # _canonical_filepath("c:\\\\users\\\\X\\\\foo.pdf")
            >>> # _canonical_filepath("C:\\\\USERS\\\\x\\\\FOO.PDF")
        """
        resolved = Path(p).resolve(strict=False)
        return os.path.normcase(str(resolved))


    def _machine_id() -> str:
        """Stable per-machine ID, exactly 8 decimal digits (D-19 % 10**8 contract)."""
        host = socket.gethostname()
        digest_hex = hashlib.sha256(host.encode("utf-8")).hexdigest()
        # CONTEXT D-19: hex[:8] can decode to up to 10 decimal digits.
        # Apply % 10**8 to guarantee exactly 8 decimal digits.
        return f"{int(digest_hex[:8], 16) % 10**8:08d}"


    def _content_hash(canonical_filepath: str, slot: int = 0) -> str:
        """Per-file content hash, exactly 8 decimal digits (D-19 + collision retry).

        slot=0 uses hex chars [0:8]; slot=N uses hex chars [8N:8N+8]. Caller
        (indexer) bumps `slot` on UNIQUE constraint collision; max useful slot is 7
        (sha256 hex digest is 64 chars).
        """
        if slot < 0 or slot > 7:
            raise ValueError(f"slot out of range [0,7]: {slot}")
        digest_hex = hashlib.sha256(canonical_filepath.encode("utf-8")).hexdigest()
        start = slot * 8
        return f"{int(digest_hex[start:start + 8], 16) % 10**8:08d}"


    def generate_local_sys_id(filepath: Union[str, Path], slot: int = 0) -> str:
        """Generate the 18-digit LOCAL sys_id for a given filepath.

        Per CONTEXT D-18 + D-19 + D-42: canonical filepath → SHA256 → modulo 10**8
        → 8-digit zero-padded, concatenated after machine_id.

        Examples:
            >>> sid = generate_local_sys_id("/tmp/foo.pdf")  # doctest: +SKIP
            >>> # is_local_sys_id(sid) == True
        """
        canonical = _canonical_filepath(filepath)
        return f"{_LOCAL_PREFIX}{_machine_id()}{_content_hash(canonical, slot=slot)}"
    ```

    Save the doctest examples on `is_local_sys_id` so the docstring stays current.
  </action>
  <verify>
    <automated>python -m pytest tests/test_local_sys_id_namespace.py tests/test_canonical_filepath.py -x -q</automated>
  </verify>
  <acceptance_criteria>
    - File `shared/local_sys_id.py` exists with `is_local_sys_id`, `generate_local_sys_id`, `_canonical_filepath`, `_machine_id`, `_content_hash` defined (verify via `grep -E "^def (is_local_sys_id|generate_local_sys_id|_canonical_filepath|_machine_id|_content_hash)" shared/local_sys_id.py | wc -l` returns 5).
    - `python -c "from shared.local_sys_id import is_local_sys_id, generate_local_sys_id, _canonical_filepath, _machine_id, _content_hash; assert is_local_sys_id('970012345601234567'); assert not is_local_sys_id('990025143260205171'); m = _machine_id(); assert len(m) == 8 and m.isdigit(); h = _content_hash('/tmp/foo'); assert len(h) == 8 and h.isdigit(); sid = generate_local_sys_id('/tmp/foo'); assert is_local_sys_id(sid)"` exits 0.
    - `python -c "from shared.synthetic_sys_id import is_synthetic_sys_id; assert not is_synthetic_sys_id('970012345601234567')"` exits 0 (disjoint namespaces).
    - `grep -c "% 10\\*\\*8" shared/local_sys_id.py` returns ≥ 2 (both `_machine_id` and `_content_hash` apply the modulo).
    - `python -m ruff check shared/local_sys_id.py` exits 0.
  </acceptance_criteria>
  <done>Module created with public + private helpers, doctests pass, namespace tests green.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Implement green tests for sys_id namespace and canonical filepath</name>
  <read_first>
    - tests/test_synthetic_sys_id.py (verbatim template — TestNoIntCoercion class with ALLOWLIST)
    - tests/fixtures/local_sys_id_fixtures.py (created in Plan 01 Task 5)
    - .planning/phases/95-my-library/95-PATTERNS.md ("Per-Test Pattern Assignments" — exact analogs)
  </read_first>
  <behavior>
    Tests in `tests/test_local_sys_id_namespace.py`:
    - `TestIsLocalSysId::test_golden_cases` — parametrized over `LOCAL_GOLDEN_CASES`; all return True.
    - `TestIsLocalSysId::test_real_alma_negative` — parametrized over `LOCAL_REAL_ALMA_NEGATIVE_CASES`; all return False.
    - `TestIsLocalSysId::test_synthetic_99_negative` — parametrized over `LOCAL_SYNTHETIC_99_NEGATIVE_CASES`; all return False.
    - `TestIsLocalSysId::test_negative_cases` — parametrized over `LOCAL_NEGATIVE_CASES`; all return False.
    - `TestNamespaceDisjoint::test_synthetic_helper_rejects_local` — for each LOCAL_GOLDEN case, `is_synthetic_sys_id(c) == False`.
    - `TestNamespaceDisjoint::test_local_helper_rejects_synthetic` — for each `LOCAL_SYNTHETIC_99_NEGATIVE` case, `is_local_sys_id(c) == False`.
    - `TestNamespaceDisjoint::test_full_libraries_csv_no_local` — REGRESSION SCAN: open `libraries.csv`, iterate every row's sys_id (column 0), assert `is_local_sys_id(sys_id) == False` for every row. (Spec REQ-2 acceptance: `is_local_sys_id` returns False for every row in libraries.csv.)
    - `TestMachineId::test_always_8_digits` — `_machine_id()` returns exactly 8 characters, all decimal digits.
    - `TestContentHash::test_always_8_digits` — `_content_hash("/any/path")` returns exactly 8 characters, all decimal digits.
    - `TestContentHash::test_collision_slot_returns_different` — `_content_hash(path, 0) != _content_hash(path, 1)`.
    - `TestGenerateLocalSysId::test_format` — `generate_local_sys_id("/tmp/test.pdf")` returns 18 chars, starts with "97", `is_local_sys_id(result)`.
    - `TestNoIntCoercion::test_no_int_coercion_outside_allowlist` — AST scan: for every `.py` file under `shared/` + `web/` + `desktop/` + `tests/`, no `int(sys_id)` or `int(local_id)` call outside `ALLOWLIST = {"shared/local_sys_id.py", "tests/test_local_sys_id_namespace.py"}`. Mirror `tests/test_synthetic_sys_id.py::TestNoIntCoercion` line-for-line; swap allowlist.

    Tests in `tests/test_canonical_filepath.py` (Windows-aware; cross-platform skip when not Windows):
    - `test_drive_letter_casing` — `_canonical_filepath("C:/x/y.pdf") == _canonical_filepath("c:/x/y.pdf")` (case-normalized).
    - `test_separator_normalization` — `_canonical_filepath("C:/x/y.pdf") == _canonical_filepath("C:\\x\\y.pdf")`.
    - `test_relative_to_absolute` — `_canonical_filepath("foo.pdf")` returns an absolute path string.
    - `test_strict_false_handles_missing_files` — `_canonical_filepath("/does/not/exist")` does not raise.
    - `test_unc_path` — if Windows: `_canonical_filepath("\\\\server\\share\\file.pdf")` returns a string containing the share component. Skip with `pytest.skip` on non-Windows.
    - `test_junction_idempotent` (Windows-only) — if a junction can be created in `tmp_path`, both `_canonical_filepath(junction_path)` and `_canonical_filepath(target_path)` return the SAME string. If junction creation requires admin or fails, `pytest.skip("junction unavailable in this CI")`.
  </behavior>
  <action>
    REPLACE the Wave-0 stub bodies in `tests/test_local_sys_id_namespace.py` AND `tests/test_canonical_filepath.py` with the real tests defined above. Imports:
    ```python
    import ast
    import os
    import pathlib
    import pytest
    from shared.local_sys_id import (
        is_local_sys_id,
        generate_local_sys_id,
        _canonical_filepath,
        _machine_id,
        _content_hash,
    )
    from shared.synthetic_sys_id import is_synthetic_sys_id
    from tests.fixtures.local_sys_id_fixtures import (
        LOCAL_GOLDEN_CASES,
        LOCAL_REAL_ALMA_NEGATIVE_CASES,
        LOCAL_SYNTHETIC_99_NEGATIVE_CASES,
        LOCAL_NEGATIVE_CASES,
    )
    ```

    For `TestNoIntCoercion`, copy `tests/test_synthetic_sys_id.py::TestNoIntCoercion` verbatim, then swap:
    - `_TARGET = "is_synthetic_sys_id"` → `_TARGET = "is_local_sys_id"`
    - `ALLOWLIST = {"shared/synthetic_sys_id.py", "tests/test_synthetic_sys_id.py"}` → `ALLOWLIST = {"shared/local_sys_id.py", "tests/test_local_sys_id_namespace.py"}`
    - Also scan for `int(local_id)` and `int(sys_id)` patterns within functions whose name contains `local`.

    For `test_full_libraries_csv_no_local`:
    ```python
    def test_full_libraries_csv_no_local():
        """SPEC REQ-2 acceptance: is_local_sys_id is False for every sys_id in libraries.csv."""
        csv_path = pathlib.Path(__file__).parent.parent / "libraries.csv"
        if not csv_path.exists():
            pytest.skip("libraries.csv not present in test environment")
        # Bounded iteration: read first 1000 rows in CI, full scan in nightly.
        import csv as csvmod
        offenders = []
        with open(csv_path, encoding="utf-8-sig", errors="replace") as f:
            reader = csvmod.reader(f)
            for i, row in enumerate(reader):
                if not row:
                    continue
                sid = row[0].strip()
                if is_local_sys_id(sid):
                    offenders.append((i, sid))
                if i > 1000 and not offenders:  # bounded check; collision risk negligible
                    break
        assert not offenders, f"libraries.csv contains LOCAL-classified rows: {offenders[:10]}"
    ```
  </action>
  <verify>
    <automated>python -m pytest tests/test_local_sys_id_namespace.py tests/test_canonical_filepath.py -x -q</automated>
  </verify>
  <acceptance_criteria>
    - `python -m pytest tests/test_local_sys_id_namespace.py -x -q` exits 0 with all tests PASSED (not skipped).
    - `python -m pytest tests/test_canonical_filepath.py -x -q` exits 0 (non-Windows-specific tests pass; Windows-specific ones may skip with reason).
    - `grep -c "raise NotImplementedError" tests/test_local_sys_id_namespace.py` returns 0 (all stubs replaced).
    - `grep -c "raise NotImplementedError" tests/test_canonical_filepath.py` returns 0.
    - `python -m ruff check tests/test_local_sys_id_namespace.py tests/test_canonical_filepath.py` exits 0.
  </acceptance_criteria>
  <done>Both test files green; AST lint passes; libraries.csv scan finds zero offenders.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: Generalize parse_header_smart + parse_full_id_components for 97-prefix (D-13 Codex P0)</name>
  <read_first>
    - genizah_core.py:3640-3681 (parse_header_smart + parse_full_id_components — exact bodies in interfaces block)
    - .planning/phases/95-my-library/95-PATTERNS.md ("Modification 3: parse_header_smart + parse_full_id_components generalization (D-13 Codex P0)")
    - .planning/phases/95-my-library/95-CONTEXT.md (D-13 Codex revision — "broaden to `(99|97)\d{16}` OR centralized helper; planner picks regex broadening")
    - .planning/phases/95-my-library/95-CONTEXT.md (D-34 — full_header format `{sys_id}_LOCAL_P{page_num}_F{file_id:04d}`)
  </read_first>
  <behavior>
    Tests in `tests/test_local_sys_id_parser_compat.py`:
    - `test_parse_header_smart_recognizes_local` — Given engine instance, `parse_header_smart("970012345601234567_LOCAL_P3_F0042")` returns `("970012345601234567", "3")`.
    - `test_parse_header_smart_still_recognizes_synthetic` — REGRESSION: `parse_header_smart("990012345600000000_IE1_P5_FL2")` still returns `("990012345600000000", "5")`.
    - `test_parse_header_smart_still_recognizes_real_alma` — REGRESSION: `parse_header_smart("990025143260205171_IE1_P5_FL2")` still returns `("990025143260205171", "5")`.
    - `test_parse_full_id_components_local` — Returns dict with `sys_id="970012345601234567"`, `ie_id="F0042"`, `p_num="3"`, `fl_id=None`.
    - `test_parse_full_id_components_synthetic_unchanged` — REGRESSION: synthetic `IE1` and `FL2` still extracted as `ie_id="IE1"`, `fl_id="2"`.
  </behavior>
  <action>
    Modify `genizah_core.py` (in-place, two regex broadenings):

    1. Locate `def parse_header_smart(self, full_header):` around line 3640. Change:
    ```python
    sys_match = re.search(r'(99\d{8,})', full_header)
    ```
    to:
    ```python
    # Phase 95 D-13 (Codex P0) — broaden to recognize LOCAL 97-prefix in addition to 99.
    sys_match = re.search(r'((?:99|97)\d{8,})', full_header)
    ```

    2. Locate `def parse_full_id_components(self, full_header):` around line 3660. Change:
    ```python
    sys_match = re.search(r'(99\d{8,})', full_header)
    ```
    to:
    ```python
    # Phase 95 D-13 (Codex P0) — broaden for LOCAL 97-prefix.
    sys_match = re.search(r'((?:99|97)\d{8,})', full_header)
    ```

    3. In `parse_full_id_components`, AFTER the existing `ie_match` block, add a LOCAL fallback per D-34:
    ```python
    # Phase 95 D-34 — LOCAL full_header has no IE\d+ component; instead uses F\d{4}.
    if not result.get('ie_id'):
        f_match = re.search(r'_F(\d{3,5})', full_header)
        if f_match:
            result['ie_id'] = f"F{f_match.group(1)}"
    ```

    4. Now implement the GREEN test bodies in `tests/test_local_sys_id_parser_compat.py`. The tests need an engine instance — use the same fixture pattern as other engine tests in `tests/`:
    ```python
    import pytest
    from genizah_core import SearchEngine  # or the class that owns parse_header_smart

    @pytest.fixture
    def engine():
        # Construct the SearchEngine; planner identifies the existing test fixture in
        # tests/conftest.py or similar. If no shared engine fixture exists, instantiate
        # with minimal init args (no Tantivy index needed for parse_* tests).
        eng = SearchEngine.__new__(SearchEngine)  # bypass __init__ if it needs Tantivy
        return eng
    ```
    If `SearchEngine.__new__` bypass doesn't work (init reads files), the planner falls back to calling the static-method form OR refactoring `parse_header_smart`/`parse_full_id_components` to module-level helpers (NOT preferred — keep them on the class).
  </action>
  <verify>
    <automated>python -m pytest tests/test_local_sys_id_parser_compat.py -x -q</automated>
  </verify>
  <acceptance_criteria>
    - `grep -c "(?:99|97)" genizah_core.py` returns ≥ 2 (both parse functions broadened).
    - `grep -c "_F(\\\\d{3,5})" genizah_core.py` returns ≥ 1 (D-34 LOCAL `F\d{4}` fallback in parse_full_id_components).
    - `python -m pytest tests/test_local_sys_id_parser_compat.py -x -q` exits 0 with all tests PASSED.
    - REGRESSION: `python -m pytest tests/ -k "header_smart or full_id_components or parse_header" -x -q` exits 0 (no existing parser tests broken).
    - `python -m ruff check genizah_core.py tests/test_local_sys_id_parser_compat.py` exits 0.
  </acceptance_criteria>
  <done>Both parsers recognize 97-prefix; D-34 F\d{4} ie_id fallback present; tests green; no regressions.</done>
</task>

<task type="auto">
  <name>Task 4: Extend LIBRARY_CODES with 'LOCAL': 'My Library' (D-13)</name>
  <read_first>
    - genizah_core.py:1723 (LIBRARY_CODES dict — analog excerpt in PATTERNS.md)
    - .planning/phases/95-my-library/95-PATTERNS.md ("Modification 1: LIBRARY_CODES extension (D-13)")
    - .planning/phases/95-my-library/95-CONTEXT.md (D-13: 'LOCAL': 'My Library' EN + 'הספרייה שלי' HE)
  </read_first>
  <action>
    1. Add `'LOCAL': 'My Library'` to the `LIBRARY_CODES` dict in `genizah_core.py` around line 1723. Add it at the END of the dict (just before the closing `}`) with a Phase 95 comment:
    ```python
    LIBRARY_CODES = {
        'CUL': 'Cambridge University Library',
        'JTS': 'The Jewish Theological Seminary of America',
        ...
        # Phase 95 D-13 — My Library namespace (LOCAL sys_ids start with 97).
        'LOCAL': 'My Library',
    }
    ```

    2. Identify the Hebrew translation dict. Run:
    ```
    grep -n "LIBRARY_CODES" genizah_core.py
    grep -nE "core_get_library_display|library_display_he|library_name_he" genizah_core.py
    ```
    Find where Hebrew library display names are looked up. Most likely either:
    - (a) A separate `LIBRARY_CODES_HE = {...}` dict.
    - (b) A `library_he_names` dict.
    - (c) A `lang` parameter on `core_get_library_display` that switches on a table.

    Add the Hebrew display name `'הספרייה שלי'` to wherever Hebrew library names are sourced. If multiple lookup paths exist (short vs full, EN vs HE), add the LOCAL entry to ALL of them.

    3. Add `Config.LOCAL_INDEX_DIR` and `Config.LOCAL_LAB_INDEX_DIR` to `class Config` near line 2007 (per D-14). Insert AFTER the existing LAB_LOG_FILE line:
    ```python
    # Phase 95 D-14 — My Library side-indexes (co-located with INDEX_DIR for
    # portable-mode inheritance).
    LOCAL_INDEX_DIR = os.path.join(INDEX_DIR, "LocalIndex")
    LOCAL_LAB_INDEX_DIR = os.path.join(INDEX_DIR, "LocalLabIndex")
    ```
  </action>
  <verify>
    <automated>python -c "from genizah_core import LIBRARY_CODES, Config; assert LIBRARY_CODES.get('LOCAL') == 'My Library'; assert hasattr(Config, 'LOCAL_INDEX_DIR'); assert hasattr(Config, 'LOCAL_LAB_INDEX_DIR'); assert 'LocalIndex' in Config.LOCAL_INDEX_DIR; assert 'LocalLabIndex' in Config.LOCAL_LAB_INDEX_DIR; print('OK')"</automated>
  </verify>
  <acceptance_criteria>
    - `python -c "from genizah_core import LIBRARY_CODES; assert LIBRARY_CODES['LOCAL'] == 'My Library'"` exits 0.
    - `python -c "from genizah_core import Config; assert Config.LOCAL_INDEX_DIR.endswith('LocalIndex'); assert Config.LOCAL_LAB_INDEX_DIR.endswith('LocalLabIndex')"` exits 0.
    - Hebrew display name `'הספרייה שלי'` appears in `genizah_core.py` (verify via `grep -c "הספרייה שלי" genizah_core.py` returns ≥ 1).
    - Existing library lookups still work — REGRESSION: `python -c "from genizah_core import LIBRARY_CODES; assert LIBRARY_CODES['CUL'].startswith('Cambridge')"` exits 0.
    - Existing genizah_core tests still pass: `python -m pytest tests/ -k "library_code or library_display" -x -q` exits 0.
  </acceptance_criteria>
  <done>LIBRARY_CODES has LOCAL entry EN + HE; Config has both LOCAL paths; no regressions.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Filepath string → SHA256 hash | Filepath enters helper as untrusted Path object; `_canonical_filepath` resolves before hashing |
| LOCAL sys_id → cloud-write surfaces | (Threat enforced in Plan 04 — gates) |
| `LIBRARY_CODES['LOCAL']` → web UI dropdowns | (Threat enforced in Plan 09 — D-46 web guard) |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-95-03 | Tampering | sys_id collision causes mis-attribution across users on shared machine | mitigate | `_machine_id()` derived from `socket.gethostname()` — different hostnames produce different IDs; %LOCALAPPDATA% paths are per-Windows-user (SPEC out-of-scope #11) |
| T-95-04 | Information disclosure | Filepath strings hashed into sys_id may inadvertently encode user data (username in path) | accept | Hash is one-way SHA256 + truncated to 8 decimal digits — no recoverable info; canonical filepath is INTERNAL only, never serialized to cloud |
| T-95-05 | Tampering | LIBRARY_CODES['LOCAL'] could be enumerated by web user via library-filter dropdown if D-30/D-46 gate fails | mitigate | This plan adds the entry; Plan 09 enforces the web-side filter via static AST guard `tests/test_web_library_options_no_local.py` |
| T-95-06 | Tampering | parse_header_smart broadened regex could accept malformed input | accept | Pre-existing pattern; broadening from `99\d{8,}` to `(?:99|97)\d{8,}` does not loosen length or non-digit checks |
</threat_model>

<verification>
- `python -m pytest tests/test_local_sys_id_namespace.py tests/test_local_sys_id_parser_compat.py tests/test_canonical_filepath.py -x -q` exits 0.
- `python -m pytest tests/ -q` exits 0 (no regressions in 2300+ test suite).
- `python -m ruff check shared/local_sys_id.py genizah_core.py tests/test_local_*.py tests/test_canonical_filepath.py` exits 0.
- `python -c "from shared.local_sys_id import is_local_sys_id, generate_local_sys_id; from shared.synthetic_sys_id import is_synthetic_sys_id; from genizah_core import LIBRARY_CODES, Config; print('OK')"` exits 0.
</verification>

<success_criteria>
- `shared/local_sys_id.py` exists with 5 public+private helpers; module is pure (no Qt, no Tantivy).
- 3 Wave-0 stub files turned green: `test_local_sys_id_namespace.py`, `test_local_sys_id_parser_compat.py`, `test_canonical_filepath.py`.
- `genizah_core.py` parsers recognize 97-prefix LOCAL sys_ids (Codex P0 D-13 fix).
- `LIBRARY_CODES['LOCAL'] == 'My Library'` and Hebrew `'הספרייה שלי'` registered.
- `Config.LOCAL_INDEX_DIR` and `Config.LOCAL_LAB_INDEX_DIR` declared.
- Disjoint namespace verified: `is_synthetic_sys_id` rejects 97-prefix; `is_local_sys_id` rejects 99-prefix.
- libraries.csv regression scan finds zero LOCAL-classified rows.
</success_criteria>

<output>
After completion, create `.planning/phases/95-my-library/95-02-SUMMARY.md` documenting:
- Final shape of `shared/local_sys_id.py` (function list)
- Regex broadening applied (exact lines)
- Hebrew display name lookup path used (which dict was extended)
- Any deviations from PATTERNS.md (e.g., if a different dict was found)
</output>