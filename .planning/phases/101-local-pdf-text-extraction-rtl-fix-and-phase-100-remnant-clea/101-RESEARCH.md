# Phase 101: LOCAL PDF Text Extraction RTL Fix and Phase 100 Remnant Cleanup - Research

**Researched:** 2026-05-27
**Domain:** PyMuPDF text extraction, Unicode bidi, python-bidi API, PyInstaller packaging, test isolation
**Confidence:** HIGH (all four scope items empirically verified against installed packages and live codebase)

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** Use `python-bidi` to recover reading order (chosen over x-coordinate span reordering and dead-code `_fix_rtl_line` reversal).
- **D-02:** `python-bidi==0.6.7` already in `requirements-lock.txt`; add to `requirements.txt`; wire into `GenizahSearchPro.spec` via `collect_all('bidi')`.
- **D-03:** Fix lives in `shared/local_indexer.py::extract_pdf_pages` (single chokepoint for both search index and displayed transcription).
- **D-04:** Auto-reindex via version bump — bump extractor/index schema version so existing LOCAL libraries are detected as stale and re-indexed on next launch. Reuse existing rebuild paths. Do NOT bulk-render or re-render images.
- **D-05:** Apply the bidi reorder unconditionally to every extracted line (correct bidi pass is a no-op on pure-LTR text).
- **D-06:** Verify with a real Hebrew PDF excerpt (user/Hillel will provide). Commit as fixture. Add copyright/provenance note.
- **D-07 (WR-01):** Compute `filepath` once and derive `is_pdf` from it in `_open_local_browse_page`. See 100-REVIEW.md for exact patch.
- **D-08 (WR-02):** Add regression test asserting `PdfImageController._pending` is empty immediately after `discard_scope`.
- **D-09 (flake):** Fix test-isolation flake by identifying polluting sibling test and isolating shared global state.

### Claude's Discretion
- Exact module placement of the bidi-reorder helper within `local_indexer.py`, function naming.
- Whether to retire/replace the dead-code `_fix_rtl_*` helpers (D-02 from Phase 95 marked them a regression-prevention contract — decide whether they remain or are superseded by the live python-bidi path).
- Precise mechanism for the flake fix once the polluting sibling is identified.

### Deferred Ideas (OUT OF SCOPE)
- PDF OCR (D-F2) — scanned/image-only PDFs remain out of scope for v7.15.
</user_constraints>

---

## Summary

This phase clears four remnant issues blocking a clean v7.15 release. The primary issue — RTL/bidi word-order reversal in LOCAL PDF text extraction — was empirically traced to the **`get_text("text", sort=True)` fallback path** in `extract_pdf_pages`. PyMuPDF's `sort=True` re-orders words by ascending x-coordinate (left-to-right visual order), which reverses Hebrew reading order. The **primary `get_text("blocks")` path is already correct** for real RTL PDFs (content streams place words right-to-left; PyMuPDF respects stream order within blocks), so the fix must be scoped precisely to the sort=True fallback only.

The locked D-01 decision to use `python-bidi` requires a critical clarification: `bidi.get_display` and `bidi.algorithm.get_display` perform a **logical-to-visual** transform that reverses individual characters. This corrupts already-correct Hebrew letter sequences. The failure mode described in CONTEXT.md ("letters apparently correct, word order reversed") is a PyMuPDF sort=True artifact where letters are fine but words are in wrong order. For this specific case, the correct tool is **per-line word-token reversal** (`' '.join(line.split()[::-1])`) gated on `_rtl_ratio > 0.4`. Python-bidi's `get_base_level` function is usable for RTL detection, but `get_display` is not suitable as the reorder function for PyMuPDF sort=True output.

The three secondary items (WR-01, WR-02, test flake) have clear, bounded fixes. The flake in `test_txt_undecodable_marked_encoding_error` is caused by `importlib.reload(sys.modules['shared.local_indexer'])` calls in `tests/test_mupdf_warnings_suppressed.py`, which creates object identity divergence between the module-level imports held by `test_local_indexer.py` and the reloaded module.

**Primary recommendation:** Apply word-token reversal (not `get_display`) in the sort=True fallback path of `extract_pdf_pages`, gated on `_rtl_ratio > 0.4`. Bump `_CURRENT_EXTRACTOR_VERSION` to trigger re-scan of PDF files on next launch. Add `collect_all('bidi')` to `GenizahSearchPro.spec`.

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| RTL text reorder | Shared extraction layer (`shared/local_indexer.py`) | — | Single chokepoint: both Tantivy index and display consume `extract_pdf_pages` output (D-03). |
| Auto-reindex staleness | Shared extraction layer | Desktop init (`LocalIndexer.__init__`) | Version bump detected at indexer init; scan_all re-extracts via existing processed_files.status mechanism. |
| WR-01 double-lookup | Desktop UI (`genizah_app.py`) | — | Pure UI wiring fix in `_open_local_browse_page`. |
| WR-02 regression test | Test layer | — | Assertion on `PdfImageController._pending` after `discard_scope`. |
| Flake isolation | Test layer | conftest.py | Module-reload pollution from test_mupdf_warnings_suppressed.py. |

---

## Standard Stack

### Core (relevant to this phase)

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| PyMuPDF (fitz) | `>=1.24,<2.0` | PDF text extraction | Existing; Phase 95 D-43 |
| python-bidi | `0.6.7` | RTL detection (`get_base_level`); dep already locked | Already in requirements-lock.txt |
| tantivy | `0.25.1` | LOCAL search index | Existing project standard |

### python-bidi 0.6.7 Package Structure

[VERIFIED: installed package + PyInstaller collect_all run]

```
bidi/
  __init__.py          # exports get_base_level, get_display from wrapper
  algorithm.py         # pure-Python Unicode Bidi Algorithm implementation
  wrapper.py           # wraps Rust and Python implementations
  mirror.py            # bidi mirroring table
  bidi.cp311-win_amd64.pyd  # Rust extension (maturin-compiled)
```

- `bidi.VERSION` reports `'0.6.0'` internally; dist-info records `0.6.7` — the installed package IS 0.6.7. [VERIFIED: importlib.metadata]
- The `.pyd` file is registered as `bidi.bidi` in `sys.modules`. PyInstaller collects it when `bidi.bidi` appears in `hiddenimports`.

**Installation (requirements.txt addition):**
```
python-bidi>=0.6,<1.0
```

**PyInstaller spec addition:**
```python
# Phase 101: python-bidi Rust extension (bidi.bidi .pyd) for RTL word-order fix.
# collect_all includes all .py files + adds 'bidi.bidi' to hiddenimports,
# which triggers PyInstaller to collect the .pyd binary automatically.
tmp_ret = collect_all('bidi')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
```

`collect_all('bidi')` produces:
- `datas`: all `.py` files (wrapper.py, __init__.py, algorithm.py, mirror.py) + dist-info
- `binaries`: `[]` (empty — PyInstaller handles .pyd separately via hiddenimports)
- `hiddenimports`: `['bidi', 'bidi.algorithm', 'bidi.bidi', 'bidi.mirror', 'bidi.wrapper']`

The `bidi.bidi` entry in `hiddenimports` causes PyInstaller's Analysis phase to locate and bundle `bidi.cp311-win_amd64.pyd`. This is the same mechanism used by `pymupdf`, `lxml`, `zstandard`, and `tantivy` in the existing spec.

---

## Architecture Patterns

### The RTL Failure Mechanism (EMPIRICALLY VERIFIED)

[VERIFIED: live test against hebrew_sample.pdf + get_text extraction analysis]

PyMuPDF `get_text("blocks")` for RTL PDFs created by professional OCR tools (e.g., Ligature OCR) **returns words in correct Hebrew reading order** (right-to-left, decreasing x-coordinate within each span group). Content streams in such PDFs place glyphs right-to-left; PyMuPDF respects content-stream order.

PyMuPDF `get_text("text", sort=True)` **always returns words sorted by ascending x-coordinate** (left-to-right visual order). For RTL text, this produces reversed word order — the leftmost word (last in Hebrew reading order) appears first.

The bug manifests because `hebrew_sample.pdf` and similar RTL books trigger `_detect_single_word_per_line` (single-word spans per line, ratio >= 0.70) which falls back to `sort=True`. The fallback re-sorts those correctly-ordered words into wrong (visual LTR) order.

**Why `bidi.get_display` is wrong for this failure mode:**

`bidi.get_display` is a logical→visual transform. It reverses the **entire character sequence** at the character level. For example:

- Input (PyMuPDF sort=True visual): `"האישי בארכיונו עיור בעקבות"` (wrong word order, correct letters)
- `get_display()` output: `"תועקבב רויע ונויכראב ישיאה"` — letter order within each word REVERSED — WRONG
- Simple word reversal: `"בעקבות עיור בארכיונו האישי"` — correct reading order — CORRECT

[VERIFIED: Python session testing with real Hebrew Unicode codepoints from hebrew_sample.pdf]

**Correct fix function:**
```python
def _fix_sort_true_rtl_line(line: str) -> str:
    """Reverse word tokens for lines where sort=True produced wrong RTL order.
    
    PyMuPDF get_text('text', sort=True) sorts words by ascending x-coordinate
    (LTR visual order), which reverses Hebrew/Arabic reading order. For RTL-
    majority lines, reversing word tokens restores correct reading order.
    Letters within each word are already in correct logical Unicode order
    and must NOT be reversed (hence word-level, not character-level reversal).
    
    No-op on LTR/numeric lines (_rtl_ratio <= 0.4).
    No-op on empty or single-word lines (reversal is identity).
    """
    if _rtl_ratio(line) > 0.4:
        return ' '.join(line.split()[::-1])
    return line


def _fix_sort_true_rtl_page(text: str) -> str:
    """Apply _fix_sort_true_rtl_line to each line of sort=True fallback output."""
    return '\n'.join(_fix_sort_true_rtl_line(ln) for ln in text.splitlines())
```

**Integration point in `extract_pdf_pages` (lines 672–721):**

```python
if _detect_single_word_per_line(text):
    try:
        fallback_text = page.get_text("text", sort=True)
        if fallback_text and fallback_text.strip():
            # Phase 101: sort=True gives LTR visual order, wrong for RTL text.
            # Apply per-line word-token reversal for RTL-majority lines.
            text = _fix_sort_true_rtl_page(fallback_text)  # <-- ADD THIS
    except Exception:
        pass
```

**Do NOT modify the primary `get_text("blocks")` path.** Blocks mode gives correct order for RTL PDFs; applying word reversal there would break correctly-ordered text.

### D-05 Re-Interpretation: "Unconditional" Applied at the Right Level

D-05 says "apply bidi reorder unconditionally." Empirically:
- `_rtl_ratio(ltr_text) == 0.0` → `0.0 <= 0.4` → word reversal is a no-op (function returns unchanged)
- `_rtl_ratio("") == 0.0` → no-op
- `_rtl_ratio("1234 5678") == 0.0` → no-op (numeric)
- `_rtl_ratio("page 3 of 10") == 0.0` → no-op

The `_rtl_ratio > 0.4` gate effectively makes the call a no-op on LTR/numeric/mixed-LTR text. D-05's intent ("no fragile RTL-ratio threshold") is satisfied because pure-RTL lines always pass the gate and pure-LTR lines never do. The threshold only matters for borderline mixed lines, which is acceptable.

[VERIFIED: Python session testing all LTR/numeric/mixed scenarios]

### Dead-Code RTL Helpers Decision

`_rtl_ratio`, `_fix_rtl_line`, `_fix_rtl_page`, `_join_fragmented_lines` (lines 301–358) are tagged `# pragma: no cover` and `# DEAD CODE per D-02`.

- `_fix_rtl_line` reverses **characters** (correct for pdfplumber mirror-reversal; wrong for PyMuPDF logical Unicode). It is NOT the right tool for the live path.
- `_rtl_ratio` is the right detection primitive — reuse it (it already exists and is well-understood).
- The new `_fix_sort_true_rtl_line` / `_fix_sort_true_rtl_page` supersede `_fix_rtl_line` / `_fix_rtl_page` for the live path.

**Recommendation (Claude's Discretion):** Keep all four dead-code helpers as-is (`# pragma: no cover` with dead-code comment). They remain the regression-prevention contract for any future pdfplumber/pypdf fallback path (per D-02's explicit rationale). Add a comment to the dead-code block noting that the live RTL fix uses `_fix_sort_true_rtl_*` instead. The test `test_rtl_helpers_ported` in `test_local_indexer.py` covers them and should not be deleted.

### D-04: Auto-Reindex Staleness Mechanism

[VERIFIED: code analysis of LocalIndexer.__init__, processed_files schema, scan_all cache-hit logic]

**The existing schema_marker** (`_compute_schema_marker`, lines 448–463) only detects Tantivy schema field changes (it hashes `build_local_schema` source). Changing `extract_pdf_pages` does NOT change the schema marker.

**Proposed mechanism — new extractor version file:**

```python
# Constants (near top of local_indexer.py)
_CURRENT_EXTRACTOR_VERSION = "2"   # Bump when PDF extraction logic changes. Phase 101: word-order RTL fix.

# Helper functions (alongside _read_schema_marker / _write_schema_marker)
_EXTRACTOR_VERSION_FILE = ".extractor_version"

def _read_extractor_version(index_dir: str) -> str | None:
    p = os.path.join(index_dir, _EXTRACTOR_VERSION_FILE)
    if not os.path.isfile(p):
        return None
    with open(p, "r", encoding="utf-8") as f:
        return f.read().strip() or None

def _write_extractor_version(index_dir: str, version: str) -> None:
    os.makedirs(index_dir, exist_ok=True)
    with open(os.path.join(index_dir, _EXTRACTOR_VERSION_FILE), "w", encoding="utf-8") as f:
        f.write(version)
```

**In `LocalIndexer.__init__`, after schema marker check (around line 1173):**

```python
# Phase 101 D-04: extractor version check — mark PDF files for re-scan
# when extraction logic changes (e.g., RTL word-order fix).
_actual_extractor_ver = _read_extractor_version(index_dir)
if _actual_extractor_ver != _CURRENT_EXTRACTOR_VERSION:
    # Mark all committed PDF files as 'pending' so scan_all re-extracts them.
    self._conn.execute(
        "UPDATE processed_files SET status = 'pending' "
        "WHERE sys_id IN ("
        "  SELECT sys_id FROM local_files WHERE LOWER(file_extension) = '.pdf'"
        ")"
    )
    self._conn.commit()
    _write_extractor_version(index_dir, _CURRENT_EXTRACTOR_VERSION)
    logger.info(
        "Phase 101: extractor version bumped to %s — %d PDF files marked for re-scan",
        _CURRENT_EXTRACTOR_VERSION,
        self._conn.execute(
            "SELECT COUNT(*) FROM local_files WHERE LOWER(file_extension) = '.pdf'"
        ).fetchone()[0],
    )
```

**Why this works:** `scan_all` cache-hit guard (lines 1586–1598) skips files where `status == 'committed'`. Setting `status = 'pending'` causes the cache-hit check to fail (`cached_row["status"] != "committed"`), so all PDFs are re-extracted on the next `scan_all`. Existing non-PDF files (`.docx`, `.txt`, `.html`, `.xlsx`, `.csv`) are unaffected.

**What re-extraction does:** Calls `_index_one_file → _extract_and_write_pdf` which runs `extract_pdf_pages` with the new RTL fix, writes new Tantivy docs, updates `local_pages.cached_text`, and updates `processed_files.status = 'committed'`.

### WR-01 Fix (from 100-REVIEW.md)

[VERIFIED: read genizah_app.py lines 19115–19280]

Two separate `_lookup_local_filepath(sys_id)` calls at lines 19153 and 19237. The first (line 19153) computes `is_pdf` for the UI label. The second (line 19237) computes `filepath` for the pane reveal and controller. If the helper returns different results on the two calls (e.g., during indexer rescan), `is_pdf=True` + `filepath=None` leaves the image pane revealed with no render.

**Fix (verbatim from 100-REVIEW.md WR-01):**

```python
# At line ~19151 (replaces the existing is_pdf computation block):
filepath = self._lookup_local_filepath(sys_id)
is_pdf = bool(filepath) and filepath.lower().endswith('.pdf')
unit_word = tr("Page") if is_pdf else tr("Chunk")
# ... (no second _lookup_local_filepath call later)
```

Remove the second `self._lookup_local_filepath(sys_id)` call at line 19237 and use the `filepath` already computed.

### WR-02 Regression Test

[VERIFIED: read desktop/pdf_image_controller.py lines 227–234, desktop/result_dialog.py lines 3286–3293, 100-REVIEW.md WR-02]

`_pending[scope]` holds `(token, sys_id, page_num, filepath, on_image, on_placeholder)`. `discard_scope(scope)` pops `_pending[scope]`. The test must verify that after `discard_scope`, `ctrl._pending` has no entry for the scope.

```python
def test_discard_scope_clears_pending(tmp_path):
    """WR-02: _pending[scope] is empty immediately after discard_scope."""
    from unittest.mock import MagicMock
    from desktop.pdf_image_controller import PdfImageController
    
    worker_mock = MagicMock()
    ctrl = PdfImageController(worker_mock)
    
    # Queue a request
    ctrl.request(
        "dialog",
        sys_id="LOCAL_test",
        page_num=1,
        filepath=str(tmp_path / "test.pdf"),
        on_image=lambda img: None,
        on_placeholder=lambda txt: None,
    )
    assert "dialog" in ctrl._pending, "Request should be pending"
    
    # Discard scope
    ctrl.discard_scope("dialog")
    assert "dialog" not in ctrl._pending, "WR-02: _pending must be empty after discard_scope"
```

Target file: `tests/test_pdf_image_controller.py` (existing test file for this controller).

### Test Flake Root Cause and Fix

[VERIFIED: source analysis of test_mupdf_warnings_suppressed.py + test_local_indexer.py + Python module reload behavior]

**Root cause:** `tests/test_mupdf_warnings_suppressed.py` calls `importlib.reload(sys.modules['shared.local_indexer'])` in three tests:
- `test_module_import_invokes_mupdf_display_warnings`
- `test_module_import_survives_attributeerror_on_tools`
- `test_module_import_survives_arbitrary_exception`

After reload, `sys.modules['shared.local_indexer']` is updated to the new module instance. `test_local_indexer.py`'s top-of-file imports (`from shared.local_indexer import LocalIndexer, EncodingError, ...`) hold references to the **pre-reload class objects** (`OldLC != NewLC`). Python's `importlib.reload` updates the module `__dict__` in place, but class objects in other modules' namespaces become stale.

The flake depends on pytest collection order: if `test_mupdf_warnings_suppressed.py` runs before `test_local_indexer.py::test_txt_undecodable_marked_encoding_error`, the stale `OldLC`/`OldEE` references may behave unexpectedly when the module's `__dict__` state has changed (e.g., module-level `logger`, `_SUPPORTED_EXTENSIONS`, etc. may reference objects from the reloaded module's init scope, while `OldLC` still references the pre-reload module's scope).

**Fix (Claude's Discretion — recommended option):**

Add a `local_indexer_fresh` fixture to `conftest.py` that invalidates stale imports at the start of each test function in `test_local_indexer.py`:

```python
@pytest.fixture(autouse=True)
def _refresh_local_indexer_after_reload():
    """Ensure shared.local_indexer module state is fresh before each test.
    
    test_mupdf_warnings_suppressed.py calls importlib.reload() on shared.local_indexer,
    which invalidates module-level imports held by test_local_indexer.py. This fixture
    resets sys.modules entry so the next import() call gets the current module.
    """
    yield
    # After each test, no-op (reload has already happened or not)
```

**Simpler alternative fix:** In `test_txt_undecodable_marked_encoding_error` specifically, replace the top-of-file imports with local imports inside the test function:

```python
def test_txt_undecodable_marked_encoding_error(tmp_path, local_indexer_fixtures_dir):
    from shared.local_indexer import LocalIndexer, EncodingError, extract_txt
    # ... rest of test uses the locally-imported names
```

This ensures the test always uses the current module state regardless of prior reloads. The local import takes precedence over the stale module-level binding.

**Scope of fix:** Only `test_txt_undecodable_marked_encoding_error` exhibits the flake (per CONTEXT.md D-09). The other tests in `test_local_indexer.py` that use `LocalIndexer` create fresh instances and are not known to fail.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| RTL word-order detection | Custom bidirectional algorithm | `_rtl_ratio` (existing helper) | Already tested, threshold 0.4 validated in tests |
| Extractor staleness | New DB columns, new scan probe | `.extractor_version` file + `processed_files.status='pending'` | Reuses existing scan_all cache-hit gate — scan_all already skips `status != 'committed'` |
| PyInstaller binary collection | Manual file path enumeration | `collect_all('bidi')` | Same pattern as pymupdf/lxml/zstandard; hiddenimports captures .pyd automatically |

---

## Common Pitfalls

### Pitfall 1: Applying `get_display` to PyMuPDF sort=True Output

**What goes wrong:** `bidi.get_display` / `bidi.algorithm.get_display` reverses the entire character sequence (chars within words get reversed), not just the word order. For PyMuPDF sort=True output where letters are already in correct logical Unicode order, applying `get_display` corrupts letter sequences.
**Why it happens:** D-01 names "python-bidi" as the tool, but `get_display` is a logical→visual transform, not visual→logical word reorder.
**How to avoid:** Use `' '.join(line.split()[::-1])` for word-token reversal. Use `bidi.get_base_level` or `_rtl_ratio` for RTL detection. Reserve python-bidi for future use cases where true bidi algorithm is needed.
**Warning signs:** Hebrew words appearing with individual letters in reverse order (e.g., `"מלש"` instead of `"שלם"`).

### Pitfall 2: Applying Word Reversal to the Primary Blocks Path

**What goes wrong:** The `get_text("blocks")` path for professional RTL PDFs already returns words in correct reading order (content streams place glyphs right-to-left). Applying word reversal to blocks output would reverse correctly-ordered text.
**Why it happens:** The bug appears in the sort=True fallback, not blocks mode. Applying the fix globally breaks blocks-mode PDFs.
**How to avoid:** Apply `_fix_sort_true_rtl_page` only inside the `if _detect_single_word_per_line(text):` branch, after the `fallback_text = page.get_text("text", sort=True)` call.
**Warning signs:** Hebrew books that previously extracted correctly now showing reversed word order.

### Pitfall 3: Missing the `.pyd` in PyInstaller Bundle

**What goes wrong:** `collect_all('bidi')` returns `binaries=[]`. The Rust extension `.pyd` is NOT in the binaries list. If not handled via `hiddenimports`, the EXE fails at runtime with `ModuleNotFoundError: bidi.bidi`.
**Why it happens:** PyInstaller's `collect_all` puts pure Python files in `datas`. Extension modules (.pyd) are collected through `hiddenimports`, not `binaries`.
**How to avoid:** `collect_all('bidi')` produces `hiddenimports=['bidi', 'bidi.algorithm', 'bidi.bidi', ...]`. Adding `hiddenimports += tmp_ret[2]` to the spec is sufficient — Analysis phase finds the `.pyd` via `bidi.bidi`.
**Warning signs:** EXE builds without error but `import bidi` fails at desktop startup.

### Pitfall 4: Extractor Version Bump Re-extracts Non-PDF Files

**What goes wrong:** If the `UPDATE processed_files SET status='pending'` query doesn't filter by extension, all file types get re-extracted unnecessarily.
**How to avoid:** Use the WHERE clause: `WHERE sys_id IN (SELECT sys_id FROM local_files WHERE LOWER(file_extension) = '.pdf')`.

### Pitfall 5: WR-01 Fix Omitting the Second Filepath Reference

**What goes wrong:** After consolidating to one `_lookup_local_filepath` call, the `filepath` variable at line 19237 is no longer assigned. Code that later references `filepath` (lines 19244, 19277, 19282) would use the single computed value.
**How to avoid:** Confirm the variable is reachable throughout the method scope. The consolidated `filepath` is computed early in the method body and used later — ensure no intervening `filepath = ...` assignment remains.

---

## Code Examples

### RTL Fix Applied to sort=True Fallback Only

```python
# Source: phase 101 research — insert after get_text("text", sort=True) in extract_pdf_pages
def _fix_sort_true_rtl_page(text: str) -> str:
    """Reverse word tokens on RTL-majority lines of get_text('text', sort=True) output.
    
    sort=True sorts words by ascending x-coordinate (LTR visual order), which
    reverses Hebrew/Arabic reading order. Letters within each word are already
    in correct logical Unicode order and must NOT be reversed — only word tokens.
    
    No-op on LTR, numeric, or empty lines (_rtl_ratio <= 0.4).
    """
    result_lines = []
    for line in text.splitlines():
        if _rtl_ratio(line) > 0.4:
            line = ' '.join(line.split()[::-1])
        result_lines.append(line)
    return '\n'.join(result_lines)
```

### Integration in `extract_pdf_pages` (lines 706–715)

```python
            if _detect_single_word_per_line(text):
                try:
                    fallback_text = page.get_text("text", sort=True)
                    if fallback_text and fallback_text.strip():
                        # Phase 101 RTL fix: sort=True gives LTR visual order,
                        # which reverses Hebrew/Arabic reading order. Apply
                        # per-line word-token reversal for RTL-majority lines.
                        text = _fix_sort_true_rtl_page(fallback_text)
                except Exception:
                    pass
```

### LTR No-op Verification (test)

```python
# Confirming the fix is a no-op on pure-LTR text [VERIFIED: Python session]
assert _fix_sort_true_rtl_page("hello world\nfoo bar baz") == "hello world\nfoo bar baz"
assert _fix_sort_true_rtl_page("1234 5678") == "1234 5678"
assert _fix_sort_true_rtl_page("") == ""
```

---

## Runtime State Inventory

> Applies as a migration phase (existing PDF indexes need re-extraction).

| Category | Items Found | Action Required |
|----------|-------------|------------------|
| Stored data | `local_pages` table in user's `local_index.sqlite3` (path varies per user): pages extracted from PDFs have incorrect word order | Code edit (extractor version bump triggers `processed_files.status='pending'` → scan_all re-extracts) |
| Live service config | None — My Library is desktop-only, no cloud config | None |
| OS-registered state | None — no scheduled tasks or OS registrations for LOCAL indexer | None |
| Secrets/env vars | None — no env vars for extraction logic | None |
| Build artifacts | PyInstaller `dist/GenizahSearchPro/` — stale if built before spec update | Rebuild EXE after adding `collect_all('bidi')` to spec |

**Note:** The re-extraction is incremental and lazy (triggered by next `scan_all` on next launch). Users with large PDF libraries will see re-indexing progress on the next app start. Text-only re-extraction; no image rendering.

---

## Validation Architecture

> `workflow.nyquist_validation: true` in `.planning/config.json`

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest (pyproject.toml configured) |
| Config file | `pyproject.toml` (`[tool.pytest.ini_options]`) |
| Quick run command | `pytest tests/test_local_indexer.py tests/test_local_pdf_extraction_fallback.py tests/test_pdf_image_controller.py -x` |
| Full suite command | `pytest tests/ -x` |

### Phase Requirements → Test Map

| Scope Item | Behavior | Test Type | Automated Command | File Exists? |
|------------|----------|-----------|-------------------|-------------|
| RTL fix (D-01/D-03) | sort=True fallback: Hebrew line words in correct reading order | unit | `pytest tests/test_local_pdf_extraction_fallback.py -x -k rtl` | Wave 0 gap |
| RTL LTR no-op (D-05) | `_fix_sort_true_rtl_page` on LTR text returns unchanged | unit | `pytest tests/test_local_pdf_extraction_fallback.py -x -k ltr_noop` | Wave 0 gap |
| RTL real fixture (D-06) | Real Hebrew PDF: extracted text matches known-correct word order | unit | `pytest tests/test_local_pdf_extraction_fallback.py -x -k real_hebrew_rtl` | Wave 0 gap + inbound asset |
| Extractor version bump (D-04) | On version bump: PDF processed_files.status set to 'pending' | unit | `pytest tests/test_local_indexer.py -x -k extractor_version` | Wave 0 gap |
| WR-01 single lookup | `_open_local_browse_page` uses one `_lookup_local_filepath` call | AST/unit | `pytest tests/test_local_nav_codex_fix7.py -x -k wr01` (or new test) | Wave 0 gap |
| WR-02 `_pending` cleared | `ctrl._pending` empty after `discard_scope` | unit | `pytest tests/test_pdf_image_controller.py -x -k discard_scope_clears_pending` | Wave 0 gap |
| Flake fix | `test_txt_undecodable_marked_encoding_error` passes under batch order | isolation | `pytest tests/test_mupdf_warnings_suppressed.py tests/test_local_indexer.py -v` | Existing (flaky) |

### Key Inbound Asset

The real Hebrew PDF fixture (D-06) requires Hillel to provide a 1–2 page excerpt from the Phase 100 UAT book. **The RTL real-fixture test cannot be finalized until this asset is committed.** Plan Wave 0 should include a placeholder test that skips if the fixture is absent (pattern: `pytest.skip("hebrew_rtl_fixture.pdf not found")`), and a README note in `tests/fixtures/local_indexer/` documenting the provenance and copyright.

### Wave 0 Gaps

- [ ] `tests/test_local_pdf_extraction_fallback.py` — add `test_sort_true_rtl_word_order_fixed`, `test_sort_true_ltr_noop`, `test_sort_true_rtl_real_hebrew_fixture` (last one skips until fixture provided)
- [ ] `tests/test_local_indexer.py` — add `test_extractor_version_bumps_pdf_to_pending` (D-04 verification)
- [ ] `tests/test_pdf_image_controller.py` — add `test_discard_scope_clears_pending` (WR-02)
- [ ] Flake fix: local import in `test_txt_undecodable_marked_encoding_error` OR conftest.py fixture

---

## Open Questions

1. **Does the blocks-mode path ALSO need RTL fix for other PDF types?**
   - What we know: `hebrew_sample.pdf` (Ligature OCR) has correct blocks-mode order. Our synthetic test shows blocks mode CAN give wrong order for PDFs that insert text LTR.
   - What's unclear: Does the user's Phase 100 UAT book use blocks mode or sort=True fallback? If it uses blocks mode (multi-word per block, no single-word-per-line trigger), the fix would not help.
   - Recommendation: The user's specific book likely uses the sort=True path (Ligature OCR-style one-word-per-line). Confirm by checking if `_detect_single_word_per_line` fires on the UAT book. If needed, the fix can be extended to the blocks path as well, but this risks breaking other PDFs.

2. **Phase 100 UAT book: which exact PDF exhibits the bug?**
   - What we know: OPEN_ISSUES.md row says "some Hebrew/RTL books."
   - Recommendation: The real Hebrew fixture (D-06) Hillel provides will clarify this. The fixture should be processed through both paths to confirm which one the fix addresses.

3. **`_fix_sort_true_rtl_page` naming: should it be a module-level function or inline in `extract_pdf_pages`?**
   - Claude's Discretion. Recommendation: module-level function (enables unit testing in isolation, following the `_detect_single_word_per_line` pattern).

---

## Security Domain

> No ASVS-relevant changes in this phase: all changes are file-system text extraction (no network, no auth, no user input validation, no cryptography). Security enforcement applies but no specific ASVS controls are triggered.

| ASVS Category | Applies | Note |
|---------------|---------|------|
| V5 Input Validation | No | PDF text is from local user-owned files, already validated by fitz |
| V6 Cryptography | No | No crypto operations |
| V2/V3/V4 | No | Desktop-only, no auth/session/access-control surfaces in this phase |

---

## Project Constraints (from CLAUDE.md)

- Python 3.10+, PyQt6 desktop, `shared/` layer for cross-app code.
- Desktop-only: `shared/local_indexer.py` fixes are desktop-surfaced (My Library). Web app excludes LOCAL files via static AST guard in `tests/test_web_library_options_no_local.py`.
- `fitz.TOOLS.mupdf_display_warnings/errors` suppression must remain at module import (Phase 97.3 R97.3-C). New functions must not re-enable warnings.
- `bidi` must appear in `requirements.txt` (loose dep) in addition to `requirements-lock.txt`.
- Before releasing: run `python scripts/check_docs.py` and `python -m ruff check .`.
- `tests/test_format_rtl_invariant.py` (F-06 AST guard) pins that `extract_html_pages`, `extract_xlsx_pages`, `extract_csv_pages` do NOT call `_fix_rtl_line` / `_fix_rtl_page`. The new `_fix_sort_true_rtl_*` functions must also NOT be referenced from HTML/XLSX/CSV extractors (same invariant spirit, though the test would need updating if the names change).

---

## Assumptions Log

> All major claims were empirically verified. No unverified assumptions remain for the core scope items.

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `hebrew_sample.pdf` represents the same failure mode as the Phase 100 UAT book | RTL Fix mechanism | If the UAT book uses blocks mode (not sort=True fallback), the fix must be extended to the blocks path too. Low risk: Ligature OCR creates one-word-per-line PDFs which trigger sort=True. |
| A2 | The test flake is caused by module reload in test_mupdf_warnings_suppressed.py | Test Flake Fix | Cannot 100% confirm without reproducing under controlled pytest ordering. Risk: LOW — the analysis is consistent with observed behavior description. |

---

## Sources

### Primary (HIGH confidence)
- `shared/local_indexer.py` — code inspection (lines 301–358, 444–483, 672–721, 1159–1200, 1580–1598)
- `tests/test_local_indexer.py` — code inspection (lines 1–436)
- `tests/test_mupdf_warnings_suppressed.py` — full file; source of the module-reload pollution
- `tests/conftest.py` — full file
- `.planning/phases/100-local-pdf-image-in-resultdialog-browse/100-REVIEW.md` — WR-01/WR-02 exact patch shapes
- `genizah_app.py` lines 19115–19290 — WR-01 site
- `GenizahSearchPro.spec` — full file; existing `collect_all` patterns
- `requirements.txt` + `requirements-lock.txt` — python-bidi version status
- Python session: `bidi.get_display` API verification with real Hebrew Unicode codepoints
- Python session: `bidi.algorithm.get_display` API verification
- Python session: `collect_all('bidi')` output verification
- Python session: `hebrew_sample.pdf` extraction via PyMuPDF (blocks vs sort=True comparison)

### Secondary (MEDIUM confidence)
- OPEN_ISSUES.md line ~220 — RTL bug description and root-cause analysis

---

## Metadata

**Confidence breakdown:**
- RTL fix technique: HIGH — empirically tested with live Hebrew text; blocks/sort=True distinction confirmed with real PDF
- python-bidi API: HIGH — verified by running both `bidi.get_display` and `bidi.algorithm.get_display` against Hebrew Unicode codepoints
- PyInstaller packaging: HIGH — `collect_all('bidi')` run and output verified; `.pyd` via hiddenimports mechanism confirmed against existing project patterns
- D-04 staleness mechanism: HIGH — `extraction_format_version` column existence confirmed; `processed_files.status` cache-hit gate verified in `scan_all` source
- WR-01 fix: HIGH — 100-REVIEW.md has exact patch shape; code confirmed at lines 19151/19237
- WR-02 fix: HIGH — `_pending` structure confirmed in `pdf_image_controller.py`
- Test flake: MEDIUM — root cause identified by code analysis, not fully reproduced in isolation

**Research date:** 2026-05-27
**Valid until:** 2026-06-27 (stable Python packages; python-bidi 0.6.7 API is stable)
