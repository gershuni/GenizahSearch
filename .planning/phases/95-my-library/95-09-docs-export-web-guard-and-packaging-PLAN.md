---
phase: 95
plan: 09
type: execute
wave: 5
depends_on: [02, 03, 04, 07, 08]
files_modified:
  - web/pages/help.py
  - Help.html
  - genizah_app.py
  - shared/export_dossier.py
  - web/pages/search.py
  - web/pages/browse.py
  - tests/test_export_dossier_local_handling.py
  - tests/test_web_library_options_no_local.py
  - tests/test_local_pyinstaller_smoke.py
  - docs/OPEN_ISSUES.md
  - CHANGELOG.md
  - CLAUDE.md
autonomous: false
requirements: [REQ-9]
must_haves:
  truths:
    - "Help page (web + desktop) has a 'My Library' section EN + HE covering D-31 + D-33 cleartext-on-disk disclosure"
    - "About dialog (web + desktop) has Seewald attribution line EN + HE (D-32)"
    - "shared/export_dossier.py row builders accept skip_local: bool kwarg; web sets True, desktop sets False (D-45)"
    - "Desktop xlsx export INCLUDES LOCAL rows; web xlsx export EXCLUDES LOCAL rows (D-45)"
    - "Web library-options builders filter out 'LOCAL' (D-30 web side); pinned via static AST guard (D-46)"
    - "PyInstaller packaging smoke test passes when run against dist/GenizahSearchPro.exe (D-43 gated @pytest.mark.packaging)"
    - "OPEN_ISSUES.md, CHANGELOG.md, CLAUDE.md updated with Phase 95 entries"
  artifacts:
    - path: "web/pages/help.py"
      provides: "My Library section EN + HE + D-33 cleartext-on-disk disclosure + D-32 Seewald attribution"
      contains: "My Library"
    - path: "shared/export_dossier.py"
      provides: "skip_local kwarg on row builders (D-45)"
      contains: "skip_local"
    - path: "tests/test_web_library_options_no_local.py"
      provides: "D-46 static AST guard for web LIBRARY_CODES consumers"
      contains: "ast.parse"
    - path: "tests/test_local_pyinstaller_smoke.py"
      provides: "D-43 packaging smoke (gated @pytest.mark.packaging)"
      contains: "pytest.mark.packaging"
  key_links:
    - from: "shared/export_dossier.py"
      to: "skip_local kwarg propagated to all row builders"
      via: "web sets True; desktop sets False"
      pattern: "skip_local"
    - from: "tests/test_web_library_options_no_local.py"
      to: "every web/pages/*.py file"
      via: "AST scan for LIBRARY_CODES iteration without LOCAL guard"
      pattern: "LIBRARY_CODES"
---

<objective>
Close out Phase 95 with documentation, defense-in-depth web guards, export path correctness, and packaging smoke. Four concerns:

**(A) Documentation (D-31 + D-32 + D-33):**
- Help page (`web/pages/help.py` + desktop `Help.html`) gets a "My Library" section bilingual (EN + HE) covering: what gets indexed, where data lives, privacy guarantee + three cloud-write gates, three-state filter usage, hostname-rename caveat. Includes D-33 cleartext-on-disk disclosure line.
- About dialog (both apps) gets Seewald attribution line EN + HE per D-32.

**(B) Export-path handling (D-45):**
- `shared/export_dossier.py` row builders gain `skip_local: bool = False` kwarg.
- Desktop xlsx export (`genizah_app.py:export_results('xlsx')`) sets `skip_local=False` — LOCAL rows ARE included in the user's local file (desktop-initiated, NOT cloud-bound).
- Web xlsx export (`web/export_service.py`) sets `skip_local=True` — defensive (web Tantivy has no LOCAL today, but the helper is shared).

**(C) Web library-options defense (D-30 + D-46):**
- Any web library-filter dropdown that iterates `LIBRARY_CODES` must filter out `'LOCAL'`. Apply to `web/pages/search.py` + `web/pages/browse.py` consumers.
- Static AST guard `tests/test_web_library_options_no_local.py` scans every `.py` under `web/pages/` for `LIBRARY_CODES` iteration without LOCAL filter.

**(D) Packaging smoke (D-43):**
- `tests/test_local_pyinstaller_smoke.py` runs against `dist/GenizahSearchPro.exe`: imports `fitz`, opens a Hebrew PDF, asserts text returned. Gated `@pytest.mark.packaging` — runs in release CI only.

Plus: project bookkeeping per CLAUDE.md (OPEN_ISSUES.md + CHANGELOG.md + CLAUDE.md "Recently Changed").

Output: Modified docs + exporter + web guards + packaging smoke + 2 GREEN test files + 1 marked-packaging test.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/PROJECT.md
@.planning/phases/95-my-library/95-CONTEXT.md
@.planning/phases/95-my-library/95-PATTERNS.md
@CLAUDE.md
@web/pages/help.py
@shared/export_dossier.py
@tests/test_pgp_filter_cascade.py

<interfaces>
D-31 Help section content (EN — verbatim or paraphrased):
- What gets indexed: .docx, .pdf, .txt files in folders you add via My Library tab.
- Where data lives: `%LOCALAPPDATA%\GenizahSearchPro\Index\LocalIndex\` (Windows; portable mode: next to EXE).
- Privacy guarantee: LOCAL documents are NEVER uploaded. Three cloud-write gates prevent leak: /api/search drops LOCAL items defensively; Lists sync aborts when any item is LOCAL; corrections submission rejects LOCAL document_ids.
- Three-state filter: button cycles All / Only Local / No Local on each result surface. Hidden when no LOCAL hits.
- Hostname-rename caveat: changing your computer's hostname invalidates the SQLite cache → next scan re-extracts all files.

D-33 disclosure line:
- EN: `"Your indexed text is stored on disk in cleartext inside the local index — it is never uploaded to GenizahSearch's servers. Use OS-level disk encryption (BitLocker / FileVault) if you need at-rest encryption."`
- HE: `"הטקסט המאונדקס נשמר בקובץ אינדקס מקומי בטקסט גלוי — הוא לעולם לא מועלה לשרתי GenizahSearch. השתמש בהצפנת דיסק ברמת מערכת ההפעלה (BitLocker / FileVault) אם נדרשת הצפנה במנוחה."`

D-32 Seewald attribution:
- EN: `"My Library feature inspired by Yehuda Seewald's GenizahLocal prototype"`
- HE: `"תכונת הספרייה שלי בהשראת אב-טיפוס GenizahLocal של יהודה זיוואלד"` (per CONTEXT — translation pending user confirmation).

D-46 static AST guard pattern (mirror tests/test_pgp_filter_cascade.py):
- Scan every `.py` under `web/pages/`.
- For each function that iterates `LIBRARY_CODES` (`.values()`, `.items()`, `.keys()`, dict expansion, etc.), assert it contains a sibling `code == 'LOCAL'` / `code != 'LOCAL'` / `if k == 'LOCAL': continue` guard.
- EXEMPT_FUNCTIONS = {} initially.

D-43 packaging smoke fixture:
- `tests/fixtures/local_indexer/hebrew_sample.pdf` (already created in Plan 01 Task 1).
- Test imports `fitz` (from the PACKAGED EXE context if possible — actually `subprocess.run([dist/GenizahSearchPro.exe, '-m', 'pytest', ...])` is overkill; simpler: assert `fitz` is importable in the venv built from `requirements.txt`, and run the same Hebrew extraction call as `test_pymupdf_hebrew_extraction_quality`).
- Mark `@pytest.mark.packaging` so it's excluded from default `pytest tests/` runs.
</interfaces>
</context>

<tasks>

<task type="auto">
  <name>Task 1: Add My Library Help section + Seewald attribution (web + desktop, EN + HE)</name>
  <read_first>
    - web/pages/help.py (existing English + Hebrew content sections — `_create_english_content`, `_create_hebrew_content`)
    - Help.html (desktop static help — `Config.HELP_FILE` at genizah_core.py:2015)
    - .planning/phases/95-my-library/95-CONTEXT.md (D-31, D-32, D-33)
    - .planning/phases/95-my-library/95-PATTERNS.md ("web/pages/help.py modifications" + "Desktop Help dialog modifications")
  </read_first>
  <action>
    **A) Web Help (`web/pages/help.py`):**

    1. In `_create_english_content`, find the TOC list (`toc_items`) and add an entry:
    ```python
    ('my-library', 'My Library — Local Documents'),
    ```

    2. Below the TOC, add a new section card (find an analogous existing section like `intro` or `search` for the card pattern, mirror it):
    ```python
    # Phase 95 D-31 — My Library section.
    with ui.card().classes('w-full p-6').props('id=my-library'):
        h2('My Library — Local Documents', classes='text-xl font-bold')
        ui.markdown("""
        The **My Library** tab lets you index your own `.docx`, `.pdf`, and `.txt`
        files alongside the Genizah corpus.

        **What gets indexed:** Word documents, PDFs (text-layer PDFs only — scanned
        PDFs without OCR are skipped), and plain text files. Other formats are
        ignored silently.

        **Where data lives:** `%LOCALAPPDATA%\\GenizahSearchPro\\Index\\LocalIndex\\`
        on Windows. Portable installations keep their LOCAL data with the install
        folder.

        **Privacy guarantee:** Your local documents are NEVER uploaded to
        GenizahSearch's servers. Three boundaries enforce this:
        - The `/api/search` JSON endpoint drops LOCAL items defensively.
        - The Lists cloud sync aborts entirely if any list item is LOCAL.
        - The corrections submission rejects LOCAL document IDs with a clear error.

        Your indexed text is stored on disk in cleartext inside the local index —
        it is never uploaded to GenizahSearch's servers. Use OS-level disk
        encryption (BitLocker / FileVault) if you need at-rest encryption.

        **Three-state filter:** Each result surface (Search, Composition Search,
        Parallels) has a filter button cycling All → Only Local → No Local → All.
        The button is hidden when the current result set has no LOCAL hits.

        **Hostname-rename caveat:** If you rename your computer's hostname, the
        SQLite cache is invalidated and the next scan re-extracts all files. This
        is rare; documented for completeness.

        *My Library feature inspired by Yehuda Seewald's GenizahLocal prototype.*
        """)
    ```

    3. Mirror in `_create_hebrew_content` with the Hebrew translations:
    ```python
    ('my-library', 'הספרייה שלי — מסמכים מקומיים'),
    ```
    Section text (Hebrew — planner may refine):
    ```
    הספרייה שלי מאפשרת לכם לאנדקס מסמכי Word, PDF וטקסט משלכם לצד גניזת קהיר.
    הטקסט המאונדקס נשמר בקובץ אינדקס מקומי בטקסט גלוי — הוא לעולם לא מועלה
    לשרתי GenizahSearch. השתמש בהצפנת דיסק ברמת מערכת ההפעלה (BitLocker /
    FileVault) אם נדרשת הצפנה במנוחה.
    תכונת הספרייה שלי בהשראת אב-טיפוס GenizahLocal של יהודה זיוואלד.
    ```

    **B) Desktop Help dialog (`Help.html`):**

    Read `Help.html` to identify its bilingual structure. Add an `<h2>My Library</h2>` section (EN) AND a Hebrew `<h2>הספרייה שלי</h2>` section. Mirror the content from web help section. Same D-32 attribution + D-33 disclosure.

    **C) About dialog (D-32):**

    Find the About dialog in BOTH apps (search via `grep -rn "About\\|create_about_dialog" web/ genizah_app.py | head -10`). Add the Seewald attribution line at the bottom (or in the credits section).
  </action>
  <verify>
    <automated>python -c "h=open('web/pages/help.py',encoding='utf-8').read(); assert 'My Library' in h; assert 'cleartext' in h; assert 'Seewald' in h or 'זיוואלד' in h; h2=open('Help.html',encoding='utf-8').read(); assert 'My Library' in h2 or 'הספרייה שלי' in h2; print('OK')"</automated>
  </verify>
  <acceptance_criteria>
    - `grep -c "My Library" web/pages/help.py` returns ≥ 2 (TOC + section).
    - `grep -c "cleartext\\|BitLocker" web/pages/help.py` returns ≥ 1 (D-33 disclosure).
    - `grep -c "Seewald\\|זיוואלד\\|GenizahLocal" web/pages/help.py` returns ≥ 1 (D-32 attribution).
    - `grep -c "My Library\\|הספרייה שלי" Help.html` returns ≥ 1 (desktop help has the section).
    - About dialog has Seewald attribution: `grep -c "Seewald\\|זיוואלד" web/ genizah_app.py -r | head -5` returns multiple matches.
    - `python -m ruff check web/pages/help.py` exits 0.
  </acceptance_criteria>
  <done>Help section + About attribution shipped on both apps in EN + HE.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Export-path handling for LOCAL hits — skip_local kwarg in shared/export_dossier.py (D-45)</name>
  <read_first>
    - shared/export_dossier.py (row builder functions — likely `build_main_row`, `build_manuscripts_row`, etc. from Phase 94)
    - genizah_app.py:export_results — find via `grep -n "export_results\\|export.*xlsx" genizah_app.py | head -10`
    - web/export_service.py (or web export caller — find via `grep -rnE "build_main_row|build_manuscripts_row" web/ | head -10`)
    - tests/test_export_xlsx_cross_parity.py (cross-app parity precedent — analog for the new test)
    - .planning/phases/95-my-library/95-CONTEXT.md (D-45 — exact decision tree)
    - .planning/phases/95-my-library/95-PATTERNS.md ("Modification 2: COL_SRC LOCAL badge..." — D-45 audit note + "tests/test_export_dossier_local_handling.py")
  </read_first>
  <behavior>
    Test `test_skip_local_false_includes_local_rows` (desktop default):
    - Construct fake results with 2 Genizah + 1 LOCAL row.
    - Call the row builder with `skip_local=False`.
    - Assert all 3 rows appear in the output.
    - LOCAL row's Source column reads "LOCAL"; Library column reads "My Library".
    - Manuscripts sub-sheet may contain the LOCAL row (it has a sys_id), but PGP-URL / PGP-Description / NLI-Description / Library-Viewer-URL cells are EMPTY.

    Test `test_skip_local_true_excludes_local_rows` (web defense):
    - Same fake results.
    - Call the row builder with `skip_local=True`.
    - Assert LOCAL row is ABSENT from output (only 2 rows).
  </behavior>
  <action>
    1. In `shared/export_dossier.py`, audit the row builder functions. Each one that constructs a result row from a dict gets a `skip_local: bool = False` kwarg:
    ```python
    def build_main_row(result_data, ..., skip_local: bool = False):
        """Build the main 'Search Results' sheet row.

        Phase 95 D-45: skip_local=True drops LOCAL rows (web defense-in-depth).
        Desktop passes False (LOCAL rows ARE exported to user's local xlsx file).
        """
        display = result_data.get('display', {}) or {}
        if skip_local and display.get('source') == 'LOCAL':
            return None  # caller filters None values out
        ...
    ```

    Same for `build_manuscripts_row`, `build_bibliography_row`, `build_credits_row` (whichever names exist per Phase 94 closeout).

    2. Update callers to filter `None` returns:
    ```python
    rows = [build_main_row(r, ..., skip_local=skip_local) for r in results]
    rows = [r for r in rows if r is not None]
    ```

    3. In `genizah_app.py:export_results('xlsx')`, pass `skip_local=False` explicitly (defaulting to False is also OK but explicit is clearer):
    ```python
    rows = build_main_row(r, ..., skip_local=False)  # desktop: LOCAL is included
    ```

    4. In `web/export_service.py` (or wherever web xlsx export builds rows), pass `skip_local=True`:
    ```python
    rows = build_main_row(r, ..., skip_local=True)  # web: defense-in-depth
    ```

    5. For the Manuscripts sub-sheet, LOCAL rows have empty PGP / NLI / Library-Viewer URLs (since LOCAL has no upstream metadata):
    ```python
    def build_manuscripts_row(result_data, ..., skip_local=False):
        display = result_data.get('display', {}) or {}
        is_local = display.get('source') == 'LOCAL'
        if skip_local and is_local:
            return None
        # LOCAL rows: clear the upstream metadata columns.
        if is_local:
            row['pgp_url'] = ''
            row['pgp_description'] = ''
            row['nli_description'] = ''
            row['library_viewer_url'] = ''
        return row
    ```

    6. Implement `tests/test_export_dossier_local_handling.py` per behavior block. Use the cross-app parity test pattern from `tests/test_export_xlsx_cross_parity.py` for assertion style.
  </action>
  <verify>
    <automated>python -m pytest tests/test_export_dossier_local_handling.py -x -q</automated>
  </verify>
  <acceptance_criteria>
    - `grep -c "skip_local" shared/export_dossier.py` returns ≥ 4 (kwarg on multiple row builders).
    - `grep -c "skip_local=False" genizah_app.py` returns ≥ 1 (desktop caller).
    - `grep -c "skip_local=True" web/` (recursive) returns ≥ 1 (web caller). If web has no explicit xlsx export caller yet, set `skip_local=True` wherever the row builders are called from web.
    - `python -m pytest tests/test_export_dossier_local_handling.py -x -q` exits 0.
    - REGRESSION: `python -m pytest tests/test_export_xlsx_cross_parity.py -x -q` exits 0 (Phase 94 invariant preserved).
    - `python -m ruff check shared/export_dossier.py tests/test_export_dossier_local_handling.py` exits 0.
  </acceptance_criteria>
  <done>skip_local kwarg propagated; desktop includes LOCAL; web excludes LOCAL; tests green; Phase 94 cross-parity preserved.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: Web library-options static AST guard (D-30 + D-46)</name>
  <read_first>
    - tests/test_pgp_filter_cascade.py (verbatim AST scanner template)
    - web/pages/search.py + web/pages/browse.py (audit for LIBRARY_CODES iteration — per PATTERNS.md "may be a NO-OP today")
    - genizah_core.py:1857 (`for name in LIBRARY_CODES.values():` — find what consumer iterates the full dict)
    - .planning/phases/95-my-library/95-CONTEXT.md (D-30 + D-46)
    - .planning/phases/95-my-library/95-PATTERNS.md ("web/pages/search.py + web/pages/browse.py modifications (D-30 + D-46)")
  </read_first>
  <behavior>
    Test `test_no_web_page_iterates_library_codes_without_local_guard`:
    - Scan every `.py` under `web/pages/` via `pathlib.Path("web/pages").rglob("*.py")`.
    - AST-parse each file.
    - For every function definition that contains a call/iteration involving `LIBRARY_CODES` (heuristic: function body contains `LIBRARY_CODES` AST node referenced via Attribute or Name):
      - Verify the same function ALSO contains a guard: an `If` node where the test compares `code == 'LOCAL'` or `code != 'LOCAL'` (or generally a string comparison against `'LOCAL'`).
      - OR the function is in EXEMPT_FUNCTIONS set (initially empty).
    - Assert: no offenders.

    Test `test_existing_web_pages_have_no_library_codes_iteration_or_have_guard`:
    - REGRESSION: confirm pre-existing web/pages/*.py code is either guard-clean or doesn't iterate LIBRARY_CODES.
  </behavior>
  <action>
    1. AUDIT: `grep -nE "LIBRARY_CODES" web/pages/*.py web/*.py 2>/dev/null | head -20`. If no consumers exist (RESEARCH.md confirmed grep returns zero matches), the modification to existing web files is a NO-OP — only the test guard is added.

    2. If any consumer DOES iterate `LIBRARY_CODES`, add a LOCAL guard. Example:
    ```python
    # Before
    options = [(code, name) for code, name in LIBRARY_CODES.items()]

    # After (D-30)
    options = [(code, name) for code, name in LIBRARY_CODES.items() if code != 'LOCAL']
    ```

    3. Create `tests/test_web_library_options_no_local.py` (replace Wave-0 stub). Mirror `tests/test_pgp_filter_cascade.py` shape:
    ```python
    # -*- coding: utf-8 -*-
    """Phase 95 D-46 — static AST guard: no web/pages/*.py iterates LIBRARY_CODES
    without filtering out 'LOCAL'.

    Mirrors tests/test_pgp_filter_cascade.py AST scanner; scans every web page
    module instead of one file.
    """
    import ast
    import pathlib

    WEB_PAGES_DIR = pathlib.Path(__file__).parent.parent / "web" / "pages"

    EXEMPT_FUNCTIONS: set[str] = set()  # No exemptions today.


    def _function_contains_library_codes_iteration(func_node) -> bool:
        """Heuristic: function contains a reference to LIBRARY_CODES in iteration."""
        for node in ast.walk(func_node):
            # Direct name lookup
            if isinstance(node, ast.Name) and node.id == "LIBRARY_CODES":
                return True
            # Attribute access (e.g., module.LIBRARY_CODES)
            if isinstance(node, ast.Attribute) and node.attr == "LIBRARY_CODES":
                return True
        return False


    def _function_contains_local_guard(func_node) -> bool:
        """Heuristic: function body contains a string comparison against 'LOCAL'."""
        for node in ast.walk(func_node):
            if isinstance(node, ast.Compare):
                # `something == 'LOCAL'` or `something != 'LOCAL'`
                for operand in (node.left, *node.comparators):
                    if isinstance(operand, ast.Constant) and operand.value == "LOCAL":
                        return True
            elif isinstance(node, ast.Constant) and node.value == "LOCAL":
                # Looser fallback (string literal anywhere in function — catches dict-key omission patterns)
                return True
        return False


    def _iter_function_defs(tree):
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                yield node


    def test_no_web_page_iterates_library_codes_without_local_guard():
        offenders = []
        for py_file in WEB_PAGES_DIR.rglob("*.py"):
            try:
                tree = ast.parse(py_file.read_text(encoding="utf-8"))
            except SyntaxError:
                continue
            for func in _iter_function_defs(tree):
                if func.name in EXEMPT_FUNCTIONS:
                    continue
                if not _function_contains_library_codes_iteration(func):
                    continue
                if not _function_contains_local_guard(func):
                    offenders.append((str(py_file.relative_to(WEB_PAGES_DIR.parent.parent)), func.name, func.lineno))
        assert not offenders, (
            f"Phase 95 D-46 violation — web/pages/*.py functions iterate LIBRARY_CODES "
            f"without a 'LOCAL' guard:\n"
            + "\n".join(f"  {f}:{n} ({l})" for f, n, l in offenders)
        )
    ```
  </action>
  <verify>
    <automated>python -m pytest tests/test_web_library_options_no_local.py -x -q</automated>
  </verify>
  <acceptance_criteria>
    - `python -m pytest tests/test_web_library_options_no_local.py -x -q` exits 0.
    - File exists with `ast.parse` import: `grep -c "ast.parse" tests/test_web_library_options_no_local.py` returns ≥ 1.
    - EXEMPT_FUNCTIONS set defined: `grep -c "EXEMPT_FUNCTIONS" tests/test_web_library_options_no_local.py` returns ≥ 1.
    - `python -m ruff check tests/test_web_library_options_no_local.py` exits 0.
  </acceptance_criteria>
  <done>Static AST guard for D-46 passes; future regressions when LIBRARY_CODES consumers are added will fail CI.</done>
</task>

<task type="auto">
  <name>Task 4: PyInstaller packaging smoke test (D-43 — gated @pytest.mark.packaging)</name>
  <read_first>
    - GenizahSearchPro.spec (already updated in Plan 01 Task 3)
    - tests/fixtures/local_indexer/hebrew_sample.pdf (Plan 01 Task 1)
    - .planning/phases/95-my-library/95-CONTEXT.md (D-43)
    - .planning/phases/95-my-library/95-PATTERNS.md ("Tests with no analog" — D-43 packaging smoke)
  </read_first>
  <action>
    Replace the Wave-0 stub in `tests/test_local_pyinstaller_smoke.py` with the real test:

    ```python
    # -*- coding: utf-8 -*-
    """Phase 95 D-43 — PyInstaller packaging smoke for PyMuPDF.

    Gated @pytest.mark.packaging. Runs in release CI only — NOT default pytest tests/.

    Without GenizahSearchPro.spec's collect_all('pymupdf') call, the packaged EXE
    raises ModuleNotFoundError: fitz._fitz at runtime. This test imports fitz and
    runs a Hebrew PDF extraction to prove the dep is correctly bundled.

    Note: the test exercises the SAME venv where dev tests run — not the packaged
    EXE directly. The packaged-EXE smoke is the manual verification step in
    .planning/phases/95-my-library/95-VALIDATION.md 'Manual-Only Verifications'.
    The unit test here pins the requirements.txt + GenizahSearchPro.spec contract
    affirmatively (fitz importable + Hebrew extraction works); the manual smoke
    verifies that PyInstaller's collect_all actually bundles the binaries.
    """
    import os
    import pathlib

    import pytest


    pytestmark = pytest.mark.packaging


    @pytest.fixture
    def hebrew_pdf_fixture():
        path = pathlib.Path(__file__).parent / "fixtures" / "local_indexer" / "hebrew_sample.pdf"
        if not path.exists():
            pytest.skip("D-44 Hebrew PDF fixture not available (Plan 01 Task 1 deferred)")
        return str(path)


    def test_fitz_importable():
        """fitz (PyMuPDF) must be importable — confirms requirements.txt pin works."""
        import fitz
        assert fitz.VersionBind, "fitz imported but VersionBind missing"
        # Pin: D-43 requires >= 1.24
        major, minor, *_ = fitz.VersionBind.split(".")
        assert int(major) >= 1 and (int(major) > 1 or int(minor) >= 24), \\
            f"PyMuPDF version {fitz.VersionBind} below the >=1.24 contract"


    def test_packaged_exe_extracts_hebrew_pdf(hebrew_pdf_fixture):
        """Open Hebrew PDF via fitz.get_text('blocks'), assert text returned.

        This is the SAME extraction call as test_pymupdf_hebrew_extraction_quality
        in test_local_indexer.py — but here gated @pytest.mark.packaging so it
        runs in release CI to catch packaging regressions specifically.
        """
        import fitz
        doc = fitz.open(hebrew_pdf_fixture)
        try:
            assert doc.page_count >= 1, "Hebrew PDF fixture has no pages"
            page = doc[0]
            blocks = page.get_text("blocks")
            text_parts = [b[4].strip() for b in blocks if b[6] == 0 and b[4].strip()]
            text = "\\n\\n".join(text_parts)
            assert text, "PyMuPDF returned empty text from Hebrew PDF — packaging or extraction broken"
            # Sanity check: text should contain at least some Hebrew letters.
            hebrew_chars = sum(1 for ch in text if "\\u0590" <= ch <= "\\u05FF")
            assert hebrew_chars > 0, "Extracted text has zero Hebrew characters — extraction broken"
        finally:
            doc.close()


    def test_spec_file_collects_pymupdf():
        """Affirmative check: GenizahSearchPro.spec calls collect_all('pymupdf')."""
        spec_path = pathlib.Path(__file__).parent.parent / "GenizahSearchPro.spec"
        if not spec_path.exists():
            pytest.skip("GenizahSearchPro.spec not in this environment")
        content = spec_path.read_text(encoding="utf-8")
        assert "collect_all('pymupdf')" in content or 'collect_all("pymupdf")' in content, \\
            "GenizahSearchPro.spec missing collect_all('pymupdf') call — D-43 regression"
    ```

    Also ensure pytest configuration recognizes the `packaging` mark. In `pyproject.toml` (or `pytest.ini`), verify `packaging` is in `markers`:
    ```toml
    [tool.pytest.ini_options]
    markers = [
        "slow: marks slow tests",
        "packaging: D-43 packaging smoke tests (release CI only)",
    ]
    ```

    If `packaging` not already declared, add it (find the existing markers block via `grep -n "markers" pyproject.toml`).
  </action>
  <verify>
    <automated>python -m pytest -m packaging tests/test_local_pyinstaller_smoke.py -x -q</automated>
  </verify>
  <acceptance_criteria>
    - `python -m pytest -m packaging tests/test_local_pyinstaller_smoke.py -x -q` exits 0 (PASS or SKIP for fixture-deferred case).
    - `python -m pytest tests/ -q` does NOT run the packaging tests by default (verify via grep that `@pytest.mark.packaging` properly excludes them — `python -m pytest tests/ --collect-only 2>&amp;1 | grep test_local_pyinstaller_smoke | head -3` should show them but they should not execute under default `pytest tests/`).
    - `pyproject.toml` markers list includes `"packaging"` (or pytest does not warn about unknown marker).
    - `grep -c "pytestmark = pytest.mark.packaging" tests/test_local_pyinstaller_smoke.py` returns 1.
    - `python -m ruff check tests/test_local_pyinstaller_smoke.py` exits 0.
  </acceptance_criteria>
  <done>Packaging smoke test in place; D-43 contract pinned; gated marker active.</done>
</task>

<task type="auto">
  <name>Task 5: Project bookkeeping — OPEN_ISSUES.md + CHANGELOG.md + CLAUDE.md updates</name>
  <read_first>
    - CLAUDE.md (Documentation Maintenance section — explicit policy)
    - docs/OPEN_ISSUES.md (current state — find via Read)
    - CHANGELOG.md (current state)
  </read_first>
  <action>
    1. **docs/OPEN_ISSUES.md** — Add Phase 95 entries. Per CLAUDE.md policy:
       - Add to "Recently Closed Bugs" (or equivalent):
         - "Phase 95 My Library — first-class desktop indexer for .docx/.pdf/.txt files (REQ-1..REQ-10). Codex P0 fixes folded: D-08 post-dedup merge, D-13 parser generalization, D-30 lists_sync gate placement. ✅ Closed YYYY-MM-DD."
       - Add to "Known Open Issues" if anything was deferred during execute (Plan 03's D-07 may have surfaced cp1255 need; D-12 audit decisions; D-44 fixture quality).
       - Update "Last Updated" timestamp to current date.

    2. **CHANGELOG.md** — Add a `## [Unreleased]` section (or extend if it exists) with Phase 95 entry:
       ```markdown
       ## [Unreleased]

       ### v7.14 — My Library (Phase 95)

       Desktop-only first-class feature: index your own `.docx` / `.pdf` / `.txt` files
       into a separate Tantivy side-index that surfaces inline in normal search,
       Composition Search, and Parallels results with a clear `LOCAL` badge and a
       three-state filter (`All` / `Only Local` / `No Local`). Personal corpora never
       leave the device: three regression tests pin the cloud-write boundaries
       (`/api/search` serializer, `lists_sync.sync_item_to_cloud`, corrections submit).

       New tab: **My Library** (7th tab, desktop). Multi-folder management; per-file
       status panel; cancellation responsive mid-file; 5,000 files / 2 GB scale ceiling
       with pre-scan dialog. Auto-rescan at app start.

       PyMuPDF (`fitz`) is now a desktop dependency for PDF extraction. Installer size
       grows by ~25 MB. The shared `Transcriptions.txt` / `libraries.csv` are still
       READ-ONLY from this phase's perspective.

       Inspired by Yehuda Seewald's external prototype. Credit in About + Help.
       ```

    3. **CLAUDE.md** "Recently Changed" — Add one-line entry at the top of the list:
       ```markdown
       - **v7.14 Phase 95 — My Library CLOSED (2026-MM-DD)** — internal milestone. First-class desktop indexer for .docx/.pdf/.txt. New `MyLibraryTab` (7th tab), side-index merged via RRF k=60 (Codex D-08 P0 — POST-dedup merge), three-state LOCAL filter mirroring Phase 93 PGP pattern, three cloud-write regression tests (lists_sync gate at TOP of `sync_item_to_cloud` per Codex D-30 P0). PyMuPDF dep + GenizahSearchPro.spec `collect_all('pymupdf')`. (desktop)
       ```

    4. Do NOT bump version yet — version bump happens at release time per CLAUDE.md `python scripts/bump_version.py X.Y.Z`. Phase 95 closes the v7.14 milestone but the release commit is a separate action.
  </action>
  <verify>
    <automated>python -c "import sys; ok=True
for f in ['docs/OPEN_ISSUES.md','CHANGELOG.md','CLAUDE.md']:
    c = open(f,encoding='utf-8').read()
    if 'Phase 95' not in c and 'My Library' not in c:
        print('MISSING in', f); ok=False
sys.exit(0 if ok else 1)"</automated>
  </verify>
  <acceptance_criteria>
    - All 3 docs files (docs/OPEN_ISSUES.md, CHANGELOG.md, CLAUDE.md) contain "Phase 95" or "My Library" entries.
    - docs/OPEN_ISSUES.md "Last Updated" timestamp is current date.
    - CHANGELOG.md has an Unreleased or v7.14 section with the Phase 95 entry.
    - CLAUDE.md "Recently Changed" has a Phase 95 line at the top.
    - DO NOT modify version.py / version_info.txt / .iss / README.md version line — version bump is a separate release-time action per CLAUDE.md.
  </acceptance_criteria>
  <done>3 docs files updated; no version bump (release-time concern).</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Web `LIBRARY_CODES` dropdown -> user-facing filter UI | D-30 + D-46 ensure LOCAL never surfaces as a filter option to web users |
| Desktop xlsx export -> user's local disk | Trusted boundary — LOCAL rows ARE included (D-45 desktop default) |
| Web xlsx export -> user's downloaded file | LOCAL filtered out defensively (D-45 web defense-in-depth) |
| PyInstaller-packaged EXE -> user's machine | PyMuPDF binary collection must work or feature is dead-on-arrival |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-95-34 | Information disclosure | Future web library-options consumer iterates LIBRARY_CODES without LOCAL guard | mitigate | D-46 static AST guard `tests/test_web_library_options_no_local.py`; CI fails on regression |
| T-95-35 | Information disclosure | Web xlsx export accidentally includes LOCAL via shared row-builder | mitigate | D-45 `skip_local=True` kwarg on all web caller invocations; pinned by `tests/test_export_dossier_local_handling.py` |
| T-95-36 | Denial of service | PyInstaller fails to bundle PyMuPDF binaries -> packaged EXE crashes on first PDF | mitigate | D-43 `collect_all(pymupdf)` in .spec (Plan 01); affirmative test `test_spec_file_collects_pymupdf` + manual packaging smoke verification |
| T-95-37 | Information disclosure | D-32 / D-33 attribution + cleartext disclosure missing -> users assume in-app encryption | mitigate | This plan adds D-33 disclosure line + D-32 Seewald attribution to Help + About in both languages |
</threat_model>

<verification>
- `python -m pytest tests/test_export_dossier_local_handling.py tests/test_web_library_options_no_local.py -x -q` exits 0.
- `python -m pytest -m packaging tests/test_local_pyinstaller_smoke.py -x -q` exits 0 (PASS or SKIP for fixture-deferred).
- `python -m pytest tests/ -q` exits 0 (full suite — no regressions). PHASE 95 COMPLETE — all 26 Wave-0 stubs now GREEN.
- `python -m ruff check .` exits 0 (per user memory: ruff is MANDATORY pre-release).
- `python scripts/check_docs.py` exits 0 (per CLAUDE.md).
- Documentation: grep returns Phase 95 entries in CLAUDE.md, CHANGELOG.md, docs/OPEN_ISSUES.md.
- Help section + About attribution present in BOTH apps in BOTH languages.
</verification>

<success_criteria>
- Help section + About attribution (web + desktop, EN + HE) ship.
- `shared/export_dossier.py` row builders accept `skip_local` kwarg.
- Desktop xlsx includes LOCAL rows; web xlsx excludes LOCAL rows (D-45).
- Web library-options guard pinned via static AST test (D-46).
- PyInstaller packaging smoke + .spec affirmative test ship (gated `@pytest.mark.packaging`).
- OPEN_ISSUES.md + CHANGELOG.md + CLAUDE.md updated.
- Phase 95 milestone CLOSED — all 26 Wave-0 stubs GREEN; full pytest + ruff clean.
- All 10 REQ-IDs satisfied (REQ-1..REQ-10).
</success_criteria>

<output>
After completion, create `.planning/phases/95-my-library/95-09-SUMMARY.md` documenting:
- Whether any pre-existing web/pages/*.py consumers needed LOCAL guards added (D-30/D-46 audit)
- Hebrew Seewald attribution final wording (D-32 follow-up)
- Final TXT encoding decision lock (D-07 — should match Plan 03 SUMMARY)
- D-44 fixture status (committed / xfail / deferred)
- Phase 95 close-out gate: all REQ-IDs satisfied; full pytest green; ruff clean.

After this SUMMARY, the next milestone-level action is the release-time version bump via `python scripts/bump_version.py 7.14.0` (separate from Phase 95 source edits per CLAUDE.md).
</output>
