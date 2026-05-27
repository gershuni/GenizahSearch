# Phase 101: LOCAL PDF text extraction RTL fix and Phase 100 remnant cleanup - Pattern Map

**Mapped:** 2026-05-27
**Files analyzed:** 6
**Analogs found:** 6 / 6

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `shared/local_indexer.py` (helpers + init) | utility / extraction logic | file-I/O, transform | `shared/local_indexer.py` existing helpers `_detect_single_word_per_line`, `_rtl_ratio`, `_read_schema_marker`/`_write_schema_marker` | exact (same file) |
| `genizah_app.py` (`_open_local_browse_page` WR-01) | desktop UI wiring | request-response | `genizah_app.py` lines 19151–19290 (existing function being simplified) | exact (same method) |
| `tests/test_local_pdf_extraction_fallback.py` | test | file-I/O | `tests/test_local_pdf_extraction_fallback.py` existing tests (`test_pathological_pdf_uses_fallback`, `test_good_pdf_stays_blocks`) | exact (same file, additive) |
| `tests/test_local_indexer.py` | test | file-I/O | `tests/test_local_indexer.py` existing tests + local-import flake fix | exact (same file, additive + one function edit) |
| `tests/test_pdf_image_controller.py` | test | request-response | `tests/test_pdf_image_controller.py` existing `test_discard_scope_removes_timer_entries` test | exact (same file, additive) |
| `tests/fixtures/local_indexer/` | fixture | file-I/O | `tests/fixtures/local_indexer/single_word_per_line.pdf` + existing `__init__.py` README convention | role-match |

---

## Pattern Assignments

---

### `shared/local_indexer.py` — new helpers `_fix_sort_true_rtl_line` / `_fix_sort_true_rtl_page` + wiring into `extract_pdf_pages`

**Role:** utility / extraction logic
**Data flow:** file-I/O, transform

**Analog 1:** `_detect_single_word_per_line` (lines 362–402) — closest match for a module-level helper that gates extraction behaviour on a per-line heuristic.

**Analog 2:** `_rtl_ratio` / `_fix_rtl_line` / `_fix_rtl_page` (lines 301–358) — dead-code helpers that show the style and docstring convention for RTL-related functions in this file. `_rtl_ratio` is REUSED (not new) as the detection gate.

**Analog 3:** `_read_schema_marker` / `_write_schema_marker` (lines 466–482) — exact pattern to mirror for the new `_read_extractor_version` / `_write_extractor_version` helpers.

---

**Function placement convention** (lines 301–403 block):

```python
# ---------------------------------------------------------------------------
# Phase 96 D-F4: detect pathological one-word-per-line PDF extraction
# ---------------------------------------------------------------------------
# This is the LIVE detection used by extract_pdf_pages (NOT dead code, unlike
# its dead-code cousin _join_fragmented_lines above).

_SINGLE_WORD_RATIO_THRESHOLD = 0.70  # Phase 96 D-F4
_SINGLE_WORD_MIN_SAMPLE = 5

def _detect_single_word_per_line(text: str) -> bool:
    """Phase 96 D-F4: return True if `text` looks like pathological
    one-word-per-line output from PyMuPDF's `get_text("blocks")` mode.
    ...
    """
    ...
```

New Phase 101 helpers should follow the same block comment pattern, placed AFTER the existing dead-code block (lines 301–358) and BEFORE `_detect_single_word_per_line` OR immediately after `_detect_single_word_per_line`. Either location is acceptable; immediately after the dead-code block (line ~360) with its own section header is the cleanest option:

```python
# ---------------------------------------------------------------------------
# Phase 101: sort=True RTL word-order fix
# ---------------------------------------------------------------------------

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

**Integration point — `extract_pdf_pages` sort=True fallback** (lines 705–715, existing):

```python
            # Phase 96 D-F4: detect pathological one-word-per-line output
            # and fall back to get_text("text", sort=True) for THIS PAGE only.
            if _detect_single_word_per_line(text):
                try:
                    fallback_text = page.get_text("text", sort=True)
                    if fallback_text and fallback_text.strip():
                        text = fallback_text          # <-- CURRENT (Phase 96)
                except Exception:
                    pass
```

Replace `text = fallback_text` with:

```python
                        # Phase 101 RTL fix: sort=True gives LTR visual order,
                        # which reverses Hebrew/Arabic reading order. Apply
                        # per-line word-token reversal for RTL-majority lines.
                        text = _fix_sort_true_rtl_page(fallback_text)
```

Critical constraint: apply `_fix_sort_true_rtl_page` ONLY inside the `if _detect_single_word_per_line(text):` branch. Do NOT touch the primary `get_text("blocks")` path above (line 701–703). See RESEARCH.md Pitfall 2.

---

**Extractor version constant + helpers — analog: `_read_schema_marker` / `_write_schema_marker`** (lines 466–482):

```python
def _read_schema_marker(index_dir: str) -> str | None:
    """Read the .schema_version marker from index_dir; returns None if absent."""
    p = os.path.join(index_dir, ".schema_version")
    if not os.path.isfile(p):
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            return f.read().strip() or None
    except OSError:
        return None


def _write_schema_marker(index_dir: str, marker: str) -> None:
    """Write .schema_version marker file alongside the index dir."""
    os.makedirs(index_dir, exist_ok=True)
    with open(os.path.join(index_dir, ".schema_version"), "w", encoding="utf-8") as f:
        f.write(marker)
```

New helpers follow the same file-read/write pattern with an analogous `.extractor_version` file. Add the constant and helpers in the same section as `_read_schema_marker` (after line 483):

```python
# ---------------------------------------------------------------------------
# Phase 101 D-04: extractor version marker (detect extraction-logic changes,
# trigger PDF re-scan — separate from schema_marker which tracks field changes).
# ---------------------------------------------------------------------------
_CURRENT_EXTRACTOR_VERSION = "2"   # Bump when PDF extraction logic changes.
                                    # Phase 101: word-order RTL fix.
_EXTRACTOR_VERSION_FILE = ".extractor_version"


def _read_extractor_version(index_dir: str) -> str | None:
    p = os.path.join(index_dir, _EXTRACTOR_VERSION_FILE)
    if not os.path.isfile(p):
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            return f.read().strip() or None
    except OSError:
        return None


def _write_extractor_version(index_dir: str, version: str) -> None:
    os.makedirs(index_dir, exist_ok=True)
    with open(os.path.join(index_dir, _EXTRACTOR_VERSION_FILE), "w", encoding="utf-8") as f:
        f.write(version)
```

**`LocalIndexer.__init__` wiring — analog: schema marker check** (lines 1162–1173):

```python
        _meta_exists = os.path.isfile(os.path.join(index_dir, "meta.json"))
        _schema_mismatch = False
        if _meta_exists:
            expected_marker = _compute_schema_marker(build_local_schema)
            actual_marker = _read_schema_marker(index_dir)
            if actual_marker != expected_marker:
                _schema_mismatch = True
```

After the schema marker check and the `_write_schema_marker` call at line 1173, add the extractor version check (mirror the same guard pattern):

```python
        # Phase 101 D-04: extractor version check — mark PDF files for re-scan
        # when extraction logic changes (e.g., RTL word-order fix).
        _actual_extractor_ver = _read_extractor_version(index_dir)
        if _actual_extractor_ver != _CURRENT_EXTRACTOR_VERSION:
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

Note: place this block AFTER `_write_schema_marker` and BEFORE the Tantivy index open/rebuild logic, so `self._conn` is already initialised (it is opened earlier in `__init__`).

---

### `genizah_app.py` — `_open_local_browse_page` WR-01 (double `_lookup_local_filepath` collapse)

**Role:** desktop UI wiring
**Data flow:** request-response

**Analog:** Same method, existing code at lines 19147–19157 (first lookup) and 19237 (second lookup):

```python
        # FIRST lookup (line ~19152) — used for is_pdf:
        is_pdf = False
        try:
            fp = self._lookup_local_filepath(sys_id) or ""
            is_pdf = fp.lower().endswith('.pdf')
        except Exception:
            pass
        unit_word = tr("Page") if is_pdf else tr("Chunk")

        # ... many lines of unrelated UI code ...

        # SECOND lookup (line ~19237) — used for filepath:
        filepath = self._lookup_local_filepath(sys_id)
```

**Target pattern (single lookup):** Compute `filepath` once at the top of the function body (at the position of the first lookup), derive `is_pdf` from it, and delete the second assignment:

```python
        # WR-01: single lookup — derive is_pdf from filepath to prevent divergence.
        try:
            filepath = self._lookup_local_filepath(sys_id)
        except Exception:
            filepath = None
        is_pdf = bool(filepath) and filepath.lower().endswith('.pdf')
        unit_word = tr("Page") if is_pdf else tr("Chunk")

        # ... all intervening UI code unchanged ...

        # line ~19237: DELETE the second `filepath = self._lookup_local_filepath(sys_id)` line.
        # The `filepath` variable computed above is already in scope here.
```

After the consolidation, the variables `filepath` and `is_pdf` are in scope for all downstream uses at lines 19244 (`os.path.basename(filepath)`), 19252 (`bool(filepath)`), 19277, 19282 — no additional changes needed.

---

### `tests/test_local_pdf_extraction_fallback.py` — new RTL tests

**Role:** test
**Data flow:** file-I/O

**Analog:** Existing tests in the same file, particularly `test_pathological_pdf_uses_fallback` (lines 42–68) and `test_good_pdf_stays_blocks` (lines 71–96).

**Imports and fixture-path pattern** (lines 1–31):

```python
import os
import pytest

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "local_indexer")
SINGLE_WORD_PDF = os.path.join(FIXTURES_DIR, "single_word_per_line.pdf")
CLEAN_PDF = os.path.join(FIXTURES_DIR, "clean_sample.pdf")
HEBREW_PDF = os.path.join(FIXTURES_DIR, "hebrew_sample.pdf")
```

New tests should follow the same pattern for the new fixture path:

```python
HEBREW_RTL_FIXTURE_PDF = os.path.join(FIXTURES_DIR, "hebrew_rtl_fixture.pdf")
```

**Helper import pattern** (lines 33–39):

```python
def _import_indexer_helpers():
    """Import the Phase 96 detection helper + extractor."""
    from shared.local_indexer import (
        extract_pdf_pages,
        _detect_single_word_per_line,
    )
    return extract_pdf_pages, _detect_single_word_per_line
```

Phase 101 tests need `_fix_sort_true_rtl_page` and `_fix_sort_true_rtl_line` as well:

```python
def _import_phase101_helpers():
    from shared.local_indexer import (
        _fix_sort_true_rtl_line,
        _fix_sort_true_rtl_page,
        extract_pdf_pages,
    )
    return _fix_sort_true_rtl_line, _fix_sort_true_rtl_page, extract_pdf_pages
```

**LTR no-op test pattern** (modelled on `test_small_sample_skipped`, lines 99–107):

```python
def test_sort_true_ltr_noop():
    """D-05: _fix_sort_true_rtl_page is a no-op on pure-LTR/numeric text."""
    _fix_rtl_line, fix_page, _ = _import_phase101_helpers()
    assert fix_page("hello world\nfoo bar baz") == "hello world\nfoo bar baz"
    assert fix_page("1234 5678") == "1234 5678"
    assert fix_page("") == ""
    assert fix_page("page 3 of 10") == "page 3 of 10"
```

**RTL word-order test pattern** (modelled on `test_pathological_pdf_uses_fallback`, lines 42–68):

```python
def test_sort_true_rtl_word_order_fixed():
    """D-01/D-03: _fix_sort_true_rtl_page reverses word tokens on RTL-majority lines."""
    _, fix_page, _ = _import_phase101_helpers()
    # Simulate sort=True output: words in LTR (wrong) visual order.
    wrong_order = "האישי בארכיונו עיור בעקבות"
    fixed = fix_page(wrong_order)
    tokens_wrong = wrong_order.split()
    tokens_fixed = fixed.split()
    assert tokens_fixed == list(reversed(tokens_wrong)), (
        f"Word tokens must be reversed for RTL line; got {tokens_fixed!r}"
    )
```

**Real-fixture skip pattern** (modelled on `pytest.skip` used for absent fixtures, lines 51–55):

```python
def test_sort_true_rtl_real_hebrew_fixture():
    """D-06: Real Hebrew PDF (Phase 100 UAT book excerpt) extracts with correct word order.

    Skips until the fixture is committed (prerequisite: Hillel provides excerpt).
    Copyright/provenance: see tests/fixtures/local_indexer/README.md.
    """
    _, _, extract_pdf_pages = _import_phase101_helpers()
    if not os.path.exists(HEBREW_RTL_FIXTURE_PDF):
        pytest.skip("hebrew_rtl_fixture.pdf not yet committed (inbound asset from user)")
    pages = list(extract_pdf_pages(HEBREW_RTL_FIXTURE_PDF))
    assert len(pages) >= 1
    _page_num, text, _title = pages[0]
    # Assert at least one line with correct RTL word order.
    # Specific known-good text comparison filled in once fixture is committed.
    assert text.strip(), "Fixture PDF must yield non-empty text"
```

---

### `tests/test_local_indexer.py` — new extractor-version test + flake fix

**Role:** test
**Data flow:** file-I/O

**Analog:** Existing tests in the same file. The flake fix targets `test_txt_undecodable_marked_encoding_error` at line 266.

**Top-of-file imports pattern** (lines 16–25) — the module-level imports that are polluted by `importlib.reload`:

```python
from shared.local_indexer import (
    LocalIndexer,
    EncodingError,
    _fix_rtl_line,
    _fix_rtl_page,
    _join_fragmented_lines,
    _rtl_ratio,
    extract_pdf_pages,
    extract_txt,
)
```

**D-09 flake fix — local import inside affected test** (RESEARCH.md simpler alternative fix):

```python
def test_txt_undecodable_marked_encoding_error(tmp_path, local_indexer_fixtures_dir):
    """MEDIUM-2: A file that fails both utf-8-sig and cp1255 gets extraction_status='encoding_error'.
    NO local_pages rows emitted. NO Tantivy docs added. error_msg contains both error messages.
    """
    # D-09 flake fix: local imports insulate against importlib.reload() pollution
    # from tests/test_mupdf_warnings_suppressed.py which reloads shared.local_indexer.
    from shared.local_indexer import LocalIndexer, EncodingError, extract_txt  # noqa: F811
    
    bad_fpath = os.path.join(local_indexer_fixtures_dir, "bad_encoding.txt")
    if not os.path.exists(bad_fpath):
        pytest.skip("bad_encoding.txt not found")
    # ... remainder of test unchanged ...
```

**Extractor version test pattern** (modelled on `test_pymupdf_hebrew_extraction_quality`, lines 37–50, and the `LocalIndexer(index_dir, lab_dir, db_path)` pattern at line 293):

```python
def test_extractor_version_bumps_pdf_to_pending(tmp_path, local_indexer_fixtures_dir):
    """D-04: When _CURRENT_EXTRACTOR_VERSION does not match the stored version,
    LocalIndexer.__init__ marks all PDF files in processed_files as 'pending'.
    Non-PDF files are unaffected.
    """
    from shared.local_indexer import LocalIndexer, _CURRENT_EXTRACTOR_VERSION
    import sqlite3, shutil

    index_dir = str(tmp_path / "idx")
    lab_dir = str(tmp_path / "lab")
    db_path = str(tmp_path / "test.sqlite3")
    os.makedirs(index_dir)
    os.makedirs(lab_dir)

    # First init: seeds the extractor version file.
    indexer = LocalIndexer(index_dir, lab_dir, db_path)
    indexer.close()

    # Manually overwrite the extractor version file to simulate stale version.
    ver_file = os.path.join(index_dir, ".extractor_version")
    with open(ver_file, "w", encoding="utf-8") as f:
        f.write("0")  # force mismatch

    # Manually seed processed_files with a PDF row at status='committed'.
    conn = sqlite3.connect(db_path)
    # ... insert row with file_extension='.pdf', status='committed' ...
    conn.close()

    # Second init: should detect version mismatch and set PDF rows to 'pending'.
    indexer2 = LocalIndexer(index_dir, lab_dir, db_path)
    indexer2.close()

    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT status FROM processed_files WHERE sys_id = ?"
        , (pdf_sys_id,)
    ).fetchone()
    conn.close()
    assert row is not None
    assert row[0] == "pending", f"PDF file must be marked 'pending' after version bump; got {row[0]!r}"
```

Note: the executor should adjust the exact SQL to match the actual `processed_files` schema (columns: `sys_id`, `status`, etc.) as seen in the codebase. The pattern is the one used in `test_txt_undecodable_marked_encoding_error` (lines 283–298) for spinning up a `LocalIndexer` in `tmp_path`.

---

### `tests/test_pdf_image_controller.py` — WR-02 `test_discard_scope_clears_pending`

**Role:** test
**Data flow:** request-response

**Analog:** `test_discard_scope_removes_timer_entries` (lines 502–523) in the same file — the closest existing test that verifies `discard_scope` cleanup behaviour.

**QApplication + worker setup pattern** (lines 45–68):

```python
def _make_fake_worker():
    """Return a (QApplication, _FakeWorker) pair."""
    app = _require_pyqt()
    from PyQt6.QtCore import QObject, pyqtSignal

    class _FakeWorker(QObject):
        render_succeeded = pyqtSignal(int, str, int, object)
        render_failed = pyqtSignal(int, str, int, object, str)

        def __init__(self):
            super().__init__()
            self.enqueued: list = []

        def enqueue(self, token, sys_id, page_num, filepath):
            self.enqueued.append((token, sys_id, page_num, filepath))
            return True

    return app, _FakeWorker()


def _make_controller(worker, debounce_ms=0, watchdog_ms=5000):
    from desktop.pdf_image_controller import PdfImageController
    return PdfImageController(worker, debounce_ms=debounce_ms, watchdog_ms=watchdog_ms)
```

**New WR-02 test** (mirrors `test_discard_scope_removes_timer_entries` structure):

```python
def test_discard_scope_clears_pending():
    """WR-02: _pending[scope] is cleared immediately after discard_scope.

    Regression guard for 100-REVIEW.md WR-02: if _pending retains the scope
    key after discard_scope, a late render_succeeded can land on a closed dialog.
    """
    app, worker = _make_fake_worker()
    ctrl = _make_controller(worker, debounce_ms=0)

    # Queue a request to populate _pending
    ctrl.request("dialog", "LOCAL_test", 1, _FAKE_PDF, lambda img: None, lambda txt: None)
    assert "dialog" in ctrl._pending, "Request should populate _pending"

    # Discard the scope
    ctrl.discard_scope("dialog")

    assert ctrl._pending.get("dialog") is None, (
        "WR-02: _pending must be None/absent after discard_scope"
    )

    # Idempotent: second call must not raise
    ctrl.discard_scope("dialog")
```

Note: the test uses `_FAKE_PDF = "/some/doc.pdf"` (the module-level sentinel defined at line 71) rather than a real PDF file. This matches the pattern used by all other tests in this file.

---

### `tests/fixtures/local_indexer/` — new Hebrew RTL fixture + provenance README

**Role:** fixture
**Data flow:** file-I/O

**Analog:** `tests/fixtures/local_indexer/single_word_per_line.pdf` — existing synthetic fixture committed in Phase 96. The `__init__.py` file in the same directory is empty (serves as a Python package marker).

**Fixture pattern:** The directory already has a flat structure — one PDF per fixture case, no subdirectories. The new fixture follows the same naming scheme:

- `tests/fixtures/local_indexer/hebrew_rtl_fixture.pdf` — the real Hebrew excerpt (inbound from user).

**Provenance README convention:** The directory currently has no README. Phase 101 should add one as a lightweight text file (or extend the `__init__.py` docstring). Content template:

```
tests/fixtures/local_indexer/

Fixtures in this directory:
...
hebrew_rtl_fixture.pdf — 1-2 page excerpt from [TITLE] (Phase 100 UAT book).
  Provided by Hillel Gershuni for Phase 101 RTL extraction regression test.
  Copyright: [PUBLISHER/AUTHOR]. Used for non-commercial research testing only.
  Source: [YEAR/EDITION details if known].
```

The test itself (`test_sort_true_rtl_real_hebrew_fixture`) should include a docstring referencing the fixture provenance. Use `pytest.skip("hebrew_rtl_fixture.pdf not yet committed")` if the file is absent, so CI does not block before the asset is committed.

---

## Shared Patterns

### `pragma: no cover` dead-code block convention

**Source:** `shared/local_indexer.py` lines 301–358
**Apply to:** Dead-code helpers that remain but are not on the live path

```python
def _rtl_ratio(text: str) -> float:  # pragma: no cover
    """Fraction of RTL chars among alpha chars. DEAD CODE per D-02."""
    ...

def _fix_rtl_line(line: str) -> str:  # pragma: no cover
    """Reverse a pdfplumber mirror-reversed RTL line. DEAD CODE per D-02."""
    ...
```

The new `_fix_sort_true_rtl_line` / `_fix_sort_true_rtl_page` are LIVE code (no `# pragma: no cover`). They must have test coverage in `test_local_pdf_extraction_fallback.py`. The dead-code block should gain a comment noting that the live RTL fix uses `_fix_sort_true_rtl_*`.

### Module-level `logger` usage

**Source:** `shared/local_indexer.py` (everywhere)
**Apply to:** Any new `logger.info` / `logger.warning` calls

```python
logger = logging.getLogger(__name__)
# Usage:
logger.info("Phase 101: extractor version bumped to %s — %d PDF files marked for re-scan",
            version, count)
```

All new log lines in `local_indexer.py` use `%`-style formatting (not f-strings) — this is the established pattern throughout the file.

### `except Exception: pass` defensive guard in `extract_pdf_pages`

**Source:** `shared/local_indexer.py` lines 712–715

```python
            if _detect_single_word_per_line(text):
                try:
                    fallback_text = page.get_text("text", sort=True)
                    if fallback_text and fallback_text.strip():
                        text = fallback_text
                except Exception:
                    # If the fallback itself errors, keep the blocks output —
                    # one-word-per-line is still better than no text at all.
                    pass
```

The `_fix_sort_true_rtl_page` call replaces `text = fallback_text` inside this same try/except block — no new try/except is needed. The existing guard already handles any exception from the RTL fix.

### Skip-if-absent fixture guard

**Source:** `tests/test_local_pdf_extraction_fallback.py` lines 51–55 and `tests/test_local_indexer.py` lines 46–48

```python
    if not os.path.exists(SOME_FIXTURE):
        pytest.skip("fixture not found")
    # OR for synthetic fixtures that MUST exist:
    if not os.path.exists(SOME_FIXTURE):
        pytest.fail("Phase N Wave 0 fixture missing: " + SOME_FIXTURE + "\nRun ...")
```

Use `pytest.skip` (not `pytest.fail`) for the real Hebrew fixture since it is an inbound asset from the user that may not be committed yet. Use `pytest.fail` for synthetic fixtures that should have been generated and committed already.

### `_make_controller` / `_make_fake_worker` reuse

**Source:** `tests/test_pdf_image_controller.py` lines 45–68
**Apply to:** WR-02 new test — use existing helpers rather than redefining them

The new `test_discard_scope_clears_pending` function calls `_make_fake_worker()` and `_make_controller(worker, debounce_ms=0)` directly — the helpers are already module-level in the same test file.

---

## No Analog Found

All six files have close analogs in the codebase. No gaps.

---

## Metadata

**Analog search scope:** `shared/`, `tests/`, `tests/fixtures/local_indexer/`, `genizah_app.py`, `desktop/pdf_image_controller.py`
**Files read:** `shared/local_indexer.py` (multiple ranges), `genizah_app.py` (lines 19140–19290), `tests/test_local_pdf_extraction_fallback.py` (full), `tests/test_pdf_image_controller.py` (full), `tests/test_local_indexer.py` (lines 1–50, 250–300), `tests/test_mupdf_warnings_suppressed.py` (full), `tests/conftest.py` (full)
**Pattern extraction date:** 2026-05-27
