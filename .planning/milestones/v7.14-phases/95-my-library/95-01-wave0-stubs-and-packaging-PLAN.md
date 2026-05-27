---
phase: 95
plan: 01
type: execute
wave: 0
depends_on: []
files_modified:
  - requirements.txt
  - GenizahSearchPro.spec
  - tests/conftest.py
  - tests/fixtures/local_indexer/hebrew_sample.pdf
  - tests/fixtures/local_indexer/hebrew_sample.expected.txt
  - tests/fixtures/local_indexer/mirror_reversed.pdf
  - tests/fixtures/local_indexer/single_word_per_line.pdf
  - tests/fixtures/local_indexer/sample.docx
  - tests/fixtures/local_indexer/sample.txt
  - tests/fixtures/local_indexer/unsupported.html
  - tests/fixtures/local_sys_id_fixtures.py
  - tests/test_local_sys_id_namespace.py
  - tests/test_local_sys_id_parser_compat.py
  - tests/test_local_indexer.py
  - tests/test_local_indexer_incremental.py
  - tests/test_local_indexer_scale.py
  - tests/test_local_indexer_mutex.py
  - tests/test_local_two_phase_commit.py
  - tests/test_local_delete_by_uid.py
  - tests/test_local_index_open_fallback.py
  - tests/test_local_unavailable_folder.py
  - tests/test_local_lab_invalidation.py
  - tests/test_local_filter_cascade.py
  - tests/test_local_filter_persistence.py
  - tests/test_local_namespace_no_api_leak.py
  - tests/test_local_namespace_no_lists_leak.py
  - tests/test_local_namespace_no_corrections_leak.py
  - tests/test_web_library_options_no_local.py
  - tests/test_export_dossier_local_handling.py
  - tests/test_local_schema_evolution.py
  - tests/test_local_pyinstaller_smoke.py
  - tests/test_local_post_dedup_merge.py
  - tests/test_side_index_merge.py
  - tests/test_canonical_filepath.py
  - tests/test_folder_overlap_detection.py
  - tests/test_local_ceiling_enforcement.py
  - tests/test_my_library_tab.py
autonomous: false
requirements: [REQ-1, REQ-2, REQ-3, REQ-4, REQ-5, REQ-6, REQ-7, REQ-8, REQ-9, REQ-10]
must_haves:
  truths:
    - "26 skipped-placeholder stub test files exist and are collectable by pytest (LOW-1 review fix — terminology: 'skipped-placeholder' replaces 'red-stub'; these are NOT true TDD-red failing assertions, they are downstream-pickup placeholders)"
    - "pymupdf>=1.24,<2.0 is in requirements.txt"
    - "GenizahSearchPro.spec collects pymupdf binaries via collect_all('pymupdf')"
    - "Hebrew PDF fixture + expected.txt committed for D-44 quality test"
    - "Every Wave-0 stub either raises NotImplementedError OR is an explicit pytest.skip/xfail carrying a tracking reference to the implementing plan (LOW-1 review fix)"
    - "wave_0_complete is set to true ONLY after downstream waves (02-09) demonstrate they have picked up these stubs (LOW-1 review fix — the flag is no longer flipped immediately at Wave-0 commit time; see Task 7)"
  artifacts:
    - path: "tests/test_local_sys_id_namespace.py"
      provides: "Wave-0 red stub for REQ-2 namespace"
      contains: "is_local_sys_id"
    - path: "tests/test_local_namespace_no_lists_leak.py"
      provides: "Wave-0 red stub for D-30 P0 gate placement"
      contains: "_get_client"
    - path: "tests/fixtures/local_indexer/hebrew_sample.pdf"
      provides: "D-44 Hebrew PDF fixture"
    - path: "requirements.txt"
      contains: "pymupdf"
    - path: "GenizahSearchPro.spec"
      contains: "collect_all('pymupdf')"
  key_links:
    - from: "tests/test_local_*.py (26 files)"
      to: "shared/local_sys_id.py + shared/local_indexer.py + desktop/my_library_tab.py (not yet built)"
      via: "ImportError or NotImplementedError"
      pattern: "pytest --collect-only"
---

<objective>
Establish Wave-0 foundations: (a) 26 red-stub test files matching `95-VALIDATION.md` so the Nyquist contract is enforced before any green task lands, (b) `pymupdf` dependency pin in `requirements.txt`, (c) PyMuPDF binary collection in `GenizahSearchPro.spec` per D-43, (d) Hebrew PDF fixture + expected-text reference for the D-44 quality test, (e) shared conftest fixtures (temp INDEX_DIR, mock Supabase client) used by Wave 1-3 plans.

Purpose: All later waves depend on this. Without red stubs, the verifier cannot confirm work was actually done (cardinal Nyquist rule). Without the PyMuPDF packaging fix, the shipped EXE will raise `ModuleNotFoundError: fitz._fitz` at runtime (Pitfall #5).

Output: Tests collectable, dep pinned, packaging spec updated, fixtures present. No production code yet — that's Wave 1+.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/PROJECT.md
@.planning/phases/95-my-library/95-CONTEXT.md
@.planning/phases/95-my-library/95-SPEC.md
@.planning/phases/95-my-library/95-PATTERNS.md
@.planning/phases/95-my-library/95-VALIDATION.md
@tests/test_synthetic_sys_id.py
@tests/test_pgp_filter_cascade.py
@requirements.txt
@GenizahSearchPro.spec

<interfaces>
<!-- Templates Wave-0 stubs MUST mirror -->

From tests/test_synthetic_sys_id.py (template for test_local_sys_id_namespace.py):
- Class layout: TestIsSyntheticSysId, TestEncodeDecodeRoundtrip, TestRealAlmaCollisionNegative, TestNoIntCoercion
- TestNoIntCoercion is an AST lint with `ALLOWLIST = {"shared/synthetic_sys_id.py", "tests/test_synthetic_sys_id.py"}`
- Mirror for LOCAL: `ALLOWLIST = {"shared/local_sys_id.py", "tests/test_local_sys_id_namespace.py"}`

From tests/test_pgp_filter_cascade.py (template for test_local_filter_cascade.py + test_web_library_options_no_local.py):
```python
def _function_contains_call(func_node, name: str) -> bool:
    for node in ast.walk(func_node):
        if isinstance(node, ast.Call):
            callee = node.func
            if isinstance(callee, ast.Name) and callee.id == name:
                return True
            if isinstance(callee, ast.Attribute) and callee.attr == name:
                return True
    return False
```

From GenizahSearchPro.spec (current state):
```python
hiddenimports = ['tantivy', 'numpy', 'PIL']
tmp_ret = collect_all('tantivy')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
```

From requirements.txt (current state — no pymupdf line):
```
python-docx==1.2.0
tantivy==0.25.1
```
</interfaces>
</context>

<tasks>

<task type="checkpoint:human-action" gate="blocking">
  <name>Task 1: User provides Hebrew PDF fixture for D-44</name>
  <read_first>
    - .planning/phases/95-my-library/95-CONTEXT.md (D-44 follow-up: planner picks Hebrew PDF fixture)
    - .planning/phases/95-my-library/95-PATTERNS.md (Fixtures section — "Small Word-authored Hebrew PDF; Avoid scanned-Hebrew-from-Acrobat")
  </read_first>
  <what-needed>
    A small (~1-3 page) Word-authored Hebrew PDF for `tests/fixtures/local_indexer/hebrew_sample.pdf`. This is the D-44 acceptance fixture that proves PyMuPDF's v1 happy path (D-02 dead-code helpers don't exercise the v1 extraction path).

    Constraints (from PATTERNS.md):
    - PREFER: small Word-authored Hebrew PDF (multi-column or single-column).
    - AVOID: scanned-Hebrew-from-Acrobat (highest RTL-corruption risk — Pitfall #1).
    - Include some mixed Hebrew/Latin text if possible (proves reading-order handling).

    Also needed: `hebrew_sample.expected.txt` — the hand-corrected reading-order reference string (what `get_text("blocks")` should return per page, concatenated with `\n\n` between pages).
  </what-needed>
  <how-to-provide>
    Option A: User commits the PDF + expected.txt to `tests/fixtures/local_indexer/` before the executor proceeds.
    Option B: User points the executor at an existing in-repo Hebrew sample to use as-is.
    Option C: Executor synthesizes a small Hebrew PDF using `python-docx` -> `docx2pdf` or a comparable in-repo tool; user reviews; user provides the expected.txt content.
  </how-to-provide>
  <resume-signal>Reply "fixture ready" with the path, OR "synthesize it" to authorize Option C, OR "skip D-44 fixture and mark test xfail" to defer.</resume-signal>
  <acceptance_criteria>
    - File `tests/fixtures/local_indexer/hebrew_sample.pdf` exists OR user has explicitly authorized the test to be marked `xfail` with reason `D-44 fixture pending`.
    - File `tests/fixtures/local_indexer/hebrew_sample.expected.txt` exists with at least one paragraph of expected Hebrew text (per-page output joined with `\n\n`).
  </acceptance_criteria>
  <done>User has resolved the fixture question one of the three ways above.</done>
</task>

<task type="auto">
  <name>Task 2: Pin pymupdf in requirements.txt</name>
  <read_first>
    - requirements.txt (current 15-line file — `python-docx==1.2.0` already present)
    - .planning/phases/95-my-library/95-CONTEXT.md (D-43 packaging spec)
    - .planning/phases/95-my-library/95-PATTERNS.md ("requirements.txt modifications (D-43)" — range vs pin discussion)
  </read_first>
  <action>
    Add ONE line to `requirements.txt` AFTER the existing `python-docx==1.2.0` (line 8). Per D-43 contract `pymupdf>=1.24,<2.0`:
    ```
    pymupdf>=1.24,<2.0
    ```
    Do NOT change any existing pins (per PATTERNS.md "Divergences": keep `python-docx==1.2.0` exact pin for reproducible builds).

    Insertion line:
    ```
    python-docx==1.2.0
    pymupdf>=1.24,<2.0  # Phase 95 D-43 — PDF extraction (Hebrew RTL)
    nicegui==3.8.0
    ```

    Then run `pip install -r requirements.txt` to confirm the constraint resolves on the current Python.
  </action>
  <verify>
    <automated>python -c "import fitz; assert fitz.VersionBind, 'fitz not installed'" &amp;&amp; grep -E "^pymupdf&gt;=1\.24,&lt;2\.0" requirements.txt</automated>
  </verify>
  <acceptance_criteria>
    - `grep -E "^pymupdf&gt;=1\.24,&lt;2\.0" requirements.txt` returns one match.
    - `python -c "import fitz; print(fitz.VersionBind)"` prints a version (≥ 1.24).
    - `python-docx==1.2.0` line is unchanged (verify via grep).
    - No other lines modified (verify via `git diff requirements.txt | head -20`).
  </acceptance_criteria>
  <done>requirements.txt has `pymupdf>=1.24,<2.0` line; `import fitz` works.</done>
</task>

<task type="auto">
  <name>Task 3: Update GenizahSearchPro.spec for PyMuPDF binary collection (D-43)</name>
  <read_first>
    - GenizahSearchPro.spec (current 54-line file — existing `collect_all('tantivy')` block)
    - .planning/phases/95-my-library/95-PATTERNS.md ("GenizahSearchPro.spec modifications (D-43)")
    - .planning/phases/95-my-library/95-RESEARCH.md (lines 86-105: critical packaging delta + Pitfall #5)
    - .planning/phases/95-my-library/95-CONTEXT.md (D-43)
  </read_first>
  <action>
    Modify `GenizahSearchPro.spec` two places (exact insertion per PATTERNS.md):

    1. Update `hiddenimports = ['tantivy', 'numpy', 'PIL']` → add `'fitz', 'pymupdf'`:
    ```python
    hiddenimports = ['tantivy', 'numpy', 'PIL', 'fitz', 'pymupdf']
    ```

    2. AFTER the existing `tmp_ret = collect_all('tantivy')` block (3 lines), ADD:
    ```python
    # Phase 95 D-43 — PyMuPDF C-extension binaries must be explicitly collected.
    # Without this, dist/GenizahSearch.exe raises ModuleNotFoundError: fitz._fitz
    # at runtime (95-RESEARCH.md Pitfall #5).
    tmp_ret = collect_all('pymupdf')
    datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
    ```

    Do NOT change any other parts of the .spec file (entry point, datas list, etc.). Do NOT add an stdout None-shim to genizah_app.py in this task — that's covered by the existing app entry point; if the smoke test reveals breakage, address in a future task.

    Also check `CompileScriptGenizah.iss` per PATTERNS.md "verify" row — likely no-op since `collect_all` puts binaries in `dist/`; do NOT change unless verification surfaces missing files.
  </action>
  <verify>
    <automated>python -c "import re,sys; s=open('GenizahSearchPro.spec',encoding='utf-8').read(); assert \"collect_all('pymupdf')\" in s, 'pymupdf collect_all missing'; assert \"'fitz'\" in s and \"'pymupdf'\" in s, 'fitz/pymupdf hidden import missing'; print('OK')"</automated>
  </verify>
  <acceptance_criteria>
    - `grep -c "collect_all('pymupdf')" GenizahSearchPro.spec` returns 1.
    - `grep -E "hiddenimports.*'fitz'.*'pymupdf'" GenizahSearchPro.spec` returns one match (both strings present).
    - The original `collect_all('tantivy')` block is still present (`grep -c "collect_all('tantivy')" GenizahSearchPro.spec` returns 1).
    - `python -c "import ast; ast.parse(open('GenizahSearchPro.spec', encoding='utf-8').read())"` exits 0 (file still parses as Python).
  </acceptance_criteria>
  <done>.spec file has both `collect_all('pymupdf')` and `'fitz', 'pymupdf'` in hiddenimports; no other regressions.</done>
</task>

<task type="auto">
  <name>Task 4: Create shared conftest fixtures for Wave 1-3</name>
  <read_first>
    - tests/conftest.py (existing — verify if file exists; create if absent)
    - .planning/phases/95-my-library/95-VALIDATION.md (Wave 0 Requirements: temp INDEX_DIR, mock Tantivy, mock Supabase)
    - .planning/phases/95-my-library/95-CONTEXT.md (D-14 portable-mode INDEX_DIR rules)
  </read_first>
  <action>
    Add (or extend) `tests/conftest.py` with the following pytest fixtures used by Wave 1-3 plans:

    ```python
    import os
    import sqlite3
    import tempfile
    from unittest.mock import MagicMock, patch

    import pytest


    @pytest.fixture
    def temp_local_index_dir(tmp_path, monkeypatch):
        """Isolated LOCAL_INDEX_DIR + LOCAL_LAB_INDEX_DIR for indexer tests (D-14)."""
        local = tmp_path / "LocalIndex"
        lab = tmp_path / "LocalLabIndex"
        local.mkdir()
        lab.mkdir()
        # Monkey-patch genizah_core.Config when the indexer reads it.
        from genizah_core import Config
        monkeypatch.setattr(Config, "LOCAL_INDEX_DIR", str(local), raising=False)
        monkeypatch.setattr(Config, "LOCAL_LAB_INDEX_DIR", str(lab), raising=False)
        return {"local": str(local), "lab": str(lab)}


    @pytest.fixture
    def mock_supabase_client():
        """Mock Supabase client for cloud-write gate tests (REQ-9).

        Used by test_local_namespace_no_lists_leak.py and
        test_local_namespace_no_corrections_leak.py. Any call to
        .table(...) or .from_(...) on this mock is RECORDED — tests assert
        call_count == 0 when LOCAL sys_id is passed."""
        mock = MagicMock(name="supabase_client")
        mock.table = MagicMock(name="supabase_client.table")
        mock.from_ = MagicMock(name="supabase_client.from_")
        return mock


    @pytest.fixture
    def local_indexer_fixtures_dir():
        """Path to tests/fixtures/local_indexer/."""
        return os.path.join(os.path.dirname(__file__), "fixtures", "local_indexer")
    ```

    If `tests/conftest.py` already exists, APPEND these fixtures (do NOT overwrite); guard imports against duplicate registration.
  </action>
  <verify>
    <automated>python -m pytest tests/conftest.py --collect-only -q 2>&amp;1 | grep -E "no tests collected|test session starts"</automated>
  </verify>
  <acceptance_criteria>
    - File `tests/conftest.py` exists and contains the three fixture names `temp_local_index_dir`, `mock_supabase_client`, `local_indexer_fixtures_dir`.
    - `python -m pytest --fixtures tests/ 2>&amp;1 | grep -E "temp_local_index_dir|mock_supabase_client|local_indexer_fixtures_dir" | wc -l` returns ≥ 3.
    - `python -m ruff check tests/conftest.py` exits 0.
  </acceptance_criteria>
  <done>Three fixtures registered, discoverable by pytest, ruff clean.</done>
</task>

<task type="auto">
  <name>Task 5: Create LOCAL sys_id test fixtures module</name>
  <read_first>
    - tests/test_synthetic_sys_id.py (verbatim template — TestRealAlmaCollisionNegative class)
    - .planning/phases/95-my-library/95-PATTERNS.md ("Helper-module template (`shared/` + `tests/`)" — LOCAL_GOLDEN_CASES etc.)
    - .planning/phases/95-my-library/95-CONTEXT.md (D-19 sys_id format: `97 + machine_id(8) + content_hash(8)`)
  </read_first>
  <action>
    Create `tests/fixtures/local_sys_id_fixtures.py` mirroring `tests/fixtures/synthetic_fixtures.py` shape (if that file doesn't exist, mirror the inline fixtures in `tests/test_synthetic_sys_id.py`). Required constants:

    ```python
    # -*- coding: utf-8 -*-
    """Phase 95 LOCAL sys_id test fixtures.

    Mirrors tests/fixtures/synthetic_fixtures.py shape. Per CONTEXT D-19:
    LOCAL sys_id = 97 + machine_id(8 decimal digits) + content_hash(8 decimal digits) = 18 digits.
    """

    # Valid LOCAL sys_ids (18 digits, 97-prefix). Real machine_id + content_hash
    # values from any deterministic SHA256 % 10**8 derivations.
    LOCAL_GOLDEN_CASES = [
        "970012345601234567",  # machine_id=00123456, content_hash=01234567
        "979999999999999999",  # max machine_id + max content_hash
        "970000000000000000",  # all zeros after prefix
        "971234567812345678",  # mixed
    ]

    # Real Alma sys_ids — MUST NOT classify as LOCAL.
    LOCAL_REAL_ALMA_NEGATIVE_CASES = [
        "990025143260205171",  # real Alma NLI 205171 suffix
        "991234560205171000",  # real Alma generic
        "990012345601234567",  # 99-prefix synthetic (Phase 85 SYNTH-06)
    ]

    # 99-prefix synthetic sys_ids (Phase 85) — MUST NOT classify as LOCAL.
    LOCAL_SYNTHETIC_99_NEGATIVE_CASES = [
        "990001234560000000",
        "990025143260000000",
    ]

    # Negative cases: wrong length, wrong prefix, non-numeric.
    LOCAL_NEGATIVE_CASES = [
        "",                          # empty
        None,                        # None
        "97001234560123456",         # 17 digits (too short)
        "9700123456012345678",       # 19 digits (too long)
        "98" + "0" * 16,             # 98-prefix (wrong prefix)
        "96" + "0" * 16,             # 96-prefix (wrong prefix)
        "97" + "a" * 16,             # non-numeric body
        "97 0012345601234567",       # contains space
    ]

    # D-19 normalization negatives: integer overflow attempts (modulo missing).
    # If sys_id derivation forgets `% 10**8`, the machine_id slot can be 9-10
    # digits and total length blows past 18.
    D_19_NORMALIZATION_NEGATIVES = [
        "97" + "4294967295" + "01234567",  # 18 -> 20 (full uint32 in machine slot)
    ]
    ```

    Use this as the central fixture import for `test_local_sys_id_namespace.py` and other LOCAL tests.
  </action>
  <verify>
    <automated>python -c "from tests.fixtures.local_sys_id_fixtures import LOCAL_GOLDEN_CASES, LOCAL_REAL_ALMA_NEGATIVE_CASES, LOCAL_SYNTHETIC_99_NEGATIVE_CASES, LOCAL_NEGATIVE_CASES, D_19_NORMALIZATION_NEGATIVES; assert len(LOCAL_GOLDEN_CASES) &gt;= 4; assert len(LOCAL_REAL_ALMA_NEGATIVE_CASES) &gt;= 3; assert all(len(s)==18 for s in LOCAL_GOLDEN_CASES); print('OK')"</automated>
  </verify>
  <acceptance_criteria>
    - File `tests/fixtures/local_sys_id_fixtures.py` exists.
    - All 5 constants importable: `LOCAL_GOLDEN_CASES`, `LOCAL_REAL_ALMA_NEGATIVE_CASES`, `LOCAL_SYNTHETIC_99_NEGATIVE_CASES`, `LOCAL_NEGATIVE_CASES`, `D_19_NORMALIZATION_NEGATIVES`.
    - Every entry in `LOCAL_GOLDEN_CASES` is 18 digits and starts with `97`.
    - `python -m ruff check tests/fixtures/local_sys_id_fixtures.py` exits 0.
  </acceptance_criteria>
  <done>Fixtures module importable and contains the 5 named constants.</done>
</task>

<task type="auto">
  <name>Task 6: Create 26 skipped-placeholder stub test files (Wave 0 contract — LOW-1 review fix: terminology clarified)</name>
  <read_first>
    - .planning/phases/95-my-library/95-VALIDATION.md (Wave 0 Requirements — list of 26 stub files)
    - tests/test_synthetic_sys_id.py (template for sys_id tests)
    - tests/test_pgp_filter_cascade.py (template for AST-cascade tests)
    - .planning/phases/95-my-library/95-PATTERNS.md ("Per-Test Pattern Assignments" section)
  </read_first>
  <action>
    Create 26 test files, each containing AT LEAST ONE explicit placeholder stub (LOW-1 review fix: these are placeholders, NOT TDD-red failing assertions) that:
    - Imports the target module IF it exists (else uses `pytest.importorskip(...)` so collection succeeds);
    - Has a `pytest.skip("Wave 0 placeholder — implemented in Wave N plan NN")` or `raise NotImplementedError("Wave 0 placeholder — implemented in Wave N plan NN")` body;
    - Is collectable by `pytest --collect-only` (no syntax errors);
    - Carries a TRACKING REFERENCE to the implementing plan in its skip/NotImplementedError message (the verifier uses this to confirm Wave-1+ replaced the stub with a real assertion).

    Files to create (exact paths from VALIDATION.md):

    1. `tests/test_local_sys_id_namespace.py` — stubs: `test_is_local_sys_id_golden`, `test_is_local_sys_id_negative`, `test_machine_id_always_8_digits`, `test_content_hash_always_8_digits`, `test_no_int_coercion` (AST lint stub).
    2. `tests/test_local_sys_id_parser_compat.py` — stub: `test_parse_header_smart_local`, `test_parse_full_id_components_local`.
    3. `tests/test_local_indexer.py` — stubs: `test_pymupdf_hebrew_extraction_quality` (D-44), `test_rtl_helpers_ported`, `test_supported_file_types_docx_pdf_txt`, `test_unsupported_extension_status`.
    4. `tests/test_local_indexer_incremental.py` — stubs: `test_second_scan_fast`, `test_modified_file_reextract_only`, `test_deleted_file_removed`.
    5. `tests/test_local_indexer_scale.py` — marked `@pytest.mark.slow`; stub: `test_5000_files_under_10_min`.
    6. `tests/test_local_indexer_mutex.py` — stub: `test_concurrent_refresh_no_interleave`.
    7. `tests/test_side_index_merge.py` — stub: `test_rrf_merge_genizah_plus_local`.
    8. `tests/test_local_post_dedup_merge.py` — stub: `test_local_hit_before_dedup_dropped`, `test_local_hit_after_dedup_survives`.
    9. `tests/test_local_lab_invalidation.py` — stub: `test_weights_hash_mismatch_triggers_banner`.
    10. `tests/test_local_two_phase_commit.py` — stub: `test_crash_between_tantivy_and_sqlite_recovers`.
    11. `tests/test_local_delete_by_uid.py` — stub: `test_delete_by_uid_with_raw_tokenizer`.
    12. `tests/test_local_index_open_fallback.py` — stub: `test_corrupt_local_index_falls_back_to_genizah_only`.
    13. `tests/test_local_unavailable_folder.py` — stub: `test_unavailable_folder_marked_status_unavailable`.
    14. `tests/test_canonical_filepath.py` — stub: `test_unc_path`, `test_junction`, `test_drive_letter_casing`.
    15. `tests/test_folder_overlap_detection.py` — stub: `test_overlap_via_commonpath`.
    16. `tests/test_local_filter_cascade.py` — stub: `test_local_filter_applied_after_pgp_filter`, `test_no_op_when_no_local_hits`.
    17. `tests/test_local_filter_persistence.py` — stub: `test_3_qsettings_keys_persist`.
    18. `tests/test_local_namespace_no_api_leak.py` — stub: `test_serialize_search_payload_drops_local`.
    19. `tests/test_local_namespace_no_lists_leak.py` — stub: `test_sync_item_to_cloud_zero_get_client_calls_for_local`.
    20. `tests/test_local_namespace_no_corrections_leak.py` — stub: `test_corrections_submit_returns_local_corrections_disabled`.
    21. `tests/test_web_library_options_no_local.py` — stub: `test_no_web_page_iterates_library_codes_without_local_guard`.
    22. `tests/test_export_dossier_local_handling.py` — stub: `test_skip_local_true_excludes_local_rows`, `test_skip_local_false_includes_local_rows`.
    23. `tests/test_local_schema_evolution.py` — stub: `test_folders_table_schema`, `test_local_files_table_schema`, `test_local_pages_table_schema`, `test_processed_files_table_schema`.
    24. `tests/test_local_pyinstaller_smoke.py` — marked `@pytest.mark.packaging`; stub: `test_packaged_exe_extracts_hebrew_pdf`.
    25. `tests/test_local_ceiling_enforcement.py` — stub: `test_prescan_warning_above_5000_files`, `test_prescan_warning_above_2gb`.
    26. `tests/test_my_library_tab.py` — stub: `test_my_library_tab_registered`, `test_my_library_tab_has_folder_list_widget`.

    Each stub MUST use one of these patterns:
    ```python
    # Pattern A: import target module (will fail in Wave 0 — that's the point)
    pytest = __import__("pytest")
    try:
        from shared.local_sys_id import is_local_sys_id
    except ImportError:
        pytest.skip("Wave 0 stub — shared.local_sys_id not yet implemented", allow_module_level=True)

    def test_is_local_sys_id_golden():
        raise NotImplementedError("Wave 0 stub for REQ-2 — implemented in Wave 1 plan 02")
    ```

    Pattern B (for tests that scan source files):
    ```python
    import pytest
    def test_local_filter_applied_after_pgp_filter():
        pytest.skip("Wave 0 stub — implemented in Wave 3 plan 06 (filter cascade)")
    ```

    Each stub MUST cite the implementing plan number in its skip/NotImplementedError message (e.g., "implemented in 95-02") so the verifier can trace it.

    No real assertions in stubs. The stubs are RED until Wave 1+ implements them.
  </action>
  <verify>
    <automated>python -m pytest tests/test_local_*.py tests/test_my_library_tab.py tests/test_canonical_filepath.py tests/test_folder_overlap_detection.py tests/test_web_library_options_no_local.py tests/test_export_dossier_local_handling.py tests/test_side_index_merge.py --collect-only -q 2>&amp;1 | tail -5</automated>
  </verify>
  <acceptance_criteria>
    - `python -m pytest tests/test_local_*.py tests/test_my_library_tab.py tests/test_canonical_filepath.py tests/test_folder_overlap_detection.py tests/test_web_library_options_no_local.py tests/test_export_dossier_local_handling.py tests/test_side_index_merge.py --collect-only 2>&amp;1 | grep -c "::test_"` returns at least 26 (one per file).
    - `python -m pytest tests/test_local_sys_id_namespace.py tests/test_local_indexer.py --collect-only` exits 0 (no syntax / import errors at collection time — uses `pytest.skip(..., allow_module_level=True)` if module missing).
    - `ls tests/test_local_*.py tests/test_my_library_tab.py tests/test_canonical_filepath.py tests/test_folder_overlap_detection.py tests/test_web_library_options_no_local.py tests/test_export_dossier_local_handling.py tests/test_side_index_merge.py | wc -l` returns 26.
    - `python -m ruff check tests/test_local_*.py tests/test_my_library_tab.py tests/test_canonical_filepath.py tests/test_folder_overlap_detection.py tests/test_web_library_options_no_local.py tests/test_export_dossier_local_handling.py tests/test_side_index_merge.py` exits 0.
    - Running them yields skips or NotImplementedError, NOT passes. Verify: `python -m pytest tests/test_local_sys_id_namespace.py -q 2>&amp;1 | grep -E "skip|error|fail"` returns matches; `grep "passed"` should NOT.
  </acceptance_criteria>
  <done>26 stub test files exist, all collectable, all RED (skip or NotImplementedError), ruff-clean.</done>
</task>

<task type="auto">
  <name>Task 7: Mark VALIDATION.md nyquist_compliant: true (LOW-1 review fix: wave_0_complete is NOT flipped here)</name>
  <read_first>
    - .planning/phases/95-my-library/95-VALIDATION.md (current frontmatter has `nyquist_compliant: false` AND `wave_0_complete: false`)
  </read_first>
  <action>
    **LOW-1 review fix — flag flip split into two steps:**

    Step A (THIS task): Flip ONLY `nyquist_compliant: false` → `nyquist_compliant: true`. This is correct at Wave-0 commit time — Nyquist compliance is about the verification *infrastructure* (26 stubs collectable, fixtures in place, pinned packaging spec), not about whether the stubs have been picked up.

    Step B (DEFERRED to Plan 09 closeout): Flip `wave_0_complete: false` → `wave_0_complete: true` ONLY after Plan 09 (the final closeout plan) confirms ALL 26 stubs have been picked up by Waves 1-3 and turned GREEN. Plan 09 Task 5 (project bookkeeping) is the natural place to flip this flag.

    Edit `.planning/phases/95-my-library/95-VALIDATION.md` frontmatter line `nyquist_compliant: false` → `nyquist_compliant: true`. Leave `wave_0_complete: false` UNCHANGED (Plan 09 flips it).

    Also update the per-task verification map: the planner has now mapped TBD-01..TBD-26 to concrete plan numbers (02-08). Update the `Plan` column of the per-task map with the correct plan IDs:
    - TBD-01 (REQ-1 PyMuPDF Hebrew) → Plan 03 Wave 1
    - TBD-02 (REQ-4 RTL helpers) → Plan 03 Wave 1
    - TBD-03 (REQ-2 sys_id namespace) → Plan 02 Wave 1
    - TBD-04 (D-13 parser compat) → Plan 02 Wave 1
    - TBD-05 (D-08 post-dedup merge) → Plan 05 Wave 2
    - TBD-06 (REQ-3 side-index merge / RRF) → Plan 05 Wave 2
    - TBD-07 (REQ-3 LAB merge + invalidation) → Plan 06 Wave 2
    - TBD-08 (REQ-5 mtime cache) → Plan 03 Wave 1
    - TBD-09 (D-21 two-phase commit) → Plan 03 Wave 1
    - TBD-10 (D-20 delete-by-uid) → Plan 03 Wave 1
    - TBD-11 (D-25 indexer mutex) → Plan 07 Wave 3
    - TBD-12 (D-37 open-fallback) → Plan 05 Wave 2
    - TBD-13 (D-40 unavailable folder) → Plan 07 Wave 3
    - TBD-14 (D-17 folder overlap) → Plan 03 Wave 1
    - TBD-15 (D-42 canonical filepath) → Plan 02 Wave 1
    - TBD-16 (REQ-6 filter cascade) → Plan 08 Wave 3
    - TBD-17 (D-39 filter persistence) → Plan 08 Wave 3
    - TBD-18 (REQ-9 no /api leak) → Plan 04 Wave 1
    - TBD-19 (REQ-9 no Lists leak) → Plan 04 Wave 1
    - TBD-20 (REQ-9 no corrections leak) → Plan 04 Wave 1
    - TBD-21 (D-46 web library options) → Plan 09 Wave 4
    - TBD-22 (D-45 export handling) → Plan 09 Wave 4
    - TBD-23 (D-35 schema evolution) → Plan 03 Wave 1
    - TBD-24 (D-43 PyInstaller smoke) → Plan 09 Wave 4 + this plan (.spec already updated)
    - TBD-25 (REQ-10 ceiling enforcement) → Plan 07 Wave 3
    - TBD-26 (REQ-7/REQ-8 badge + tab) → Plan 07 Wave 3 + Plan 08 Wave 3

    Update the `## Validation Sign-Off` checkboxes:
    - [x] All tasks have `<automated>` verify or Wave 0 dependencies
    - [x] Wave 0 covers all MISSING references
    - [x] No watch-mode flags
    - [ ] Sampling continuity (verified in execute)
    - [ ] Feedback latency < 130s (verified in execute)
    - [x] `nyquist_compliant: true` set in frontmatter
  </action>
  <verify>
    <automated>python -c "import re; s=open('.planning/phases/95-my-library/95-VALIDATION.md',encoding='utf-8').read(); assert 'nyquist_compliant: true' in s; assert 'wave_0_complete: false' in s, 'LOW-1: wave_0_complete must remain false at Wave-0; Plan 09 closeout flips it'; print('OK')"</automated>
  </verify>
  <acceptance_criteria>
    - VALIDATION.md frontmatter has `nyquist_compliant: true` (LOW-1 review fix: ONLY this flag flips at Wave-0).
    - VALIDATION.md frontmatter still has `wave_0_complete: false` after this task (LOW-1 review fix: Plan 09 closeout flips it after stub-pickup confirmation).
    - Per-task map's `Plan` column has 02, 03, 04, 05, 06, 07, 08, or 09 (no `TBD` remaining).
    - Validation Sign-Off section has at least 4 checked boxes.
  </acceptance_criteria>
  <done>VALIDATION.md nyquist_compliant flipped; wave_0_complete deferred to Plan 09 per LOW-1 review fix.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| User filesystem → indexer | Untrusted file paths and contents enter via Add Folder dialog (D-16) |
| LOCAL sys_id → cloud-write surfaces | Personal IDs must NEVER cross into `/api/search`, Lists sync, corrections submit |
| Tantivy on-disk index → process memory | Cleartext personal text on disk; D-33 disclosure (no encryption boundary in v1) |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-95-01 | Information disclosure | Wave-0 fixtures may include real personal Hebrew PDFs accidentally committed | mitigate | Task 1 user-checkpoint requires Word-authored, not scanned, Hebrew PDF; reviewer confirms no personal data |
| T-95-02 | Tampering | PyInstaller .spec edits could break the build for the entire desktop app | mitigate | Task 3 `<automated>` verify imports `ast.parse` so the spec stays syntactically valid; no other lines touched |
| T-95-07 | Tampering | Malicious PDF triggers PyMuPDF parse exception | accept (mitigated downstream) | PyMuPDF parse errors caught in Wave 1 indexer per `error_msg` field in `local_files` (D-35); Wave 0 just ships the dep |
</threat_model>

<verification>
After all tasks complete:
- `python -m pytest tests/test_local_*.py tests/test_my_library_tab.py tests/test_canonical_filepath.py tests/test_folder_overlap_detection.py tests/test_web_library_options_no_local.py tests/test_export_dossier_local_handling.py tests/test_side_index_merge.py --collect-only -q` exits 0 with ≥ 26 stubs collected.
- `python -m pytest tests/ -q --co` exits 0 (full suite still collectable — no regressions from our additions).
- `python -m ruff check tests/ requirements.txt GenizahSearchPro.spec` exits 0.
- `grep -c "pymupdf" requirements.txt` returns 1.
- `grep -c "collect_all('pymupdf')" GenizahSearchPro.spec` returns 1.
- VALIDATION.md `nyquist_compliant: true`.
</verification>

<success_criteria>
- 26 red-stub test files exist, collectable, RED (skip or NotImplementedError).
- `requirements.txt` includes `pymupdf>=1.24,<2.0`.
- `GenizahSearchPro.spec` collects PyMuPDF binaries.
- Hebrew PDF fixture + expected.txt present (or D-44 xfail-deferred per user choice).
- Conftest fixtures (`temp_local_index_dir`, `mock_supabase_client`, `local_indexer_fixtures_dir`) discoverable.
- LOCAL sys_id fixture constants importable.
- VALIDATION.md frontmatter `nyquist_compliant: true`, `wave_0_complete: false at Wave-0; flipped to true by Plan 09 closeout (per LOW-1).`
</success_criteria>

<output>
After completion, create `.planning/phases/95-my-library/95-01-SUMMARY.md` documenting:
- 26 test stub paths
- requirements.txt + .spec diffs (summarize, don't paste)
- Fixture decisions (D-44 fixture source)
- VALIDATION.md state
- Any deviations or open questions for downstream waves
</output>
