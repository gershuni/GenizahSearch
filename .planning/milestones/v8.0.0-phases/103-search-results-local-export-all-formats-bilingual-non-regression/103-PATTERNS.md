# Phase 103: Search-Results LOCAL Export - Pattern Map

**Mapped:** 2026-06-01
**Files analyzed:** 4 files to create/modify (2 new helpers, 2 modified functions)
**Analogs found:** 4 / 4

---

## Line Number Verification

CONTEXT.md cited line numbers were verified against the live files. All are accurate:

| Cited | Actual | Location |
|-------|--------|----------|
| 19595 | 19595 | `export_results(self, fmt='xlsx')` — confirmed |
| 2531 | 2531 | `_build_search_results_xlsx_bytes(...)` — confirmed |
| 18804 | 18804 | `_lookup_local_filepath(self, sys_id)` — confirmed |
| 16726 | 16726 | LOCAL hit on-screen display pattern — confirmed |
| 7159 | 7159 | `_build_local_result_dict(...)` in `genizah_core.py` — confirmed |

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `shared/export_dossier.py` (new helpers) | utility | transform | `shared/export_dossier.py::main_header_row` / `build_manuscript_row` | exact-sibling |
| `genizah_app.py::_build_search_results_xlsx_bytes` (modified) | utility | transform | itself (current 4-sheet builder, lines 2531–2839) | self-analog |
| `genizah_app.py::export_results` CSV/TXT/DOCX branches (modified) | controller | request-response | itself (current branches, lines 19856–19938) | self-analog |
| new shared DOCX per-result block writer | utility | transform | `genizah_app.py::_add_docx_highlighted_runs` + current DOCX branch (lines 19875–19923) | role-match |

---

## Pattern Assignments

---

### 1. `shared/export_dossier.py` — New LOCAL bilingual helpers

**Role:** utility / transform — Qt-free, offline-testable, parallel to existing header/title helpers.

**Analogs:** `main_header_row`, `manuscript_header_row`, `bibliography_header_row`, `sheet_titles`, `build_manuscript_row`, `build_bibliography_rows` (all in `shared/export_dossier.py`).

#### Imports pattern (lines 134–144):
```python
import logging
from typing import Any, Callable, Dict, List, Optional

from shared.document_service import get_document_for_fragment
from shared.fjms_service import get_fjms_service
from shared.nli_crossref_service import get_nli_crossref_service

logger = logging.getLogger(__name__)
```

#### Module-level constant pattern — analogous to `_MAIN_HEADERS_EN`/`_HE` (lines 230–242):
```python
_MAIN_HEADERS_EN: List[str] = [
    "System ID", "Library", "Shelfmark", "Title",
    "Image/Page", "Source",
    "Snippet", "Full Text",
    "Has PGP", "Is Printed", "Domains", "Image URL",
]

_MAIN_HEADERS_HE: List[str] = [
    "מספר מערכת", "ספרייה", "מספר מדף", "כותרת",
    "תמונה/עמוד", "מקור",
    "קטע", "טקסט מלא",
    "יש PGP", "מודפס", "תחומים", "כתובת תמונה",
]
```

**New constants to add** (mirror exactly this pattern):
```python
_LOCAL_HEADERS_EN: List[str] = [
    "Filename", "Parent Folder", "Full Filepath", "Page", "Matched Text",
]

_LOCAL_HEADERS_HE: List[str] = [
    "שם קובץ", "תיקייה", "נתיב מלא", "עמוד", "טקסט תואם",
]

# Parallel to _SHEET_TITLES_EN/_HE dict additions:
# 'local_documents': "Local Documents"  (EN)
# 'local_documents': "מסמכים מקומיים"  (HE)
```

#### Header function pattern — analogous to `main_header_row` (lines 394–403):
```python
def main_header_row(lang: str = 'en') -> List[str]:
    """Return the 12 main-sheet column headers in the requested language.

    Phase 94 D-04 REVISED (2026-05-20): Hebrew when ``lang == 'he'``, English
    otherwise. The returned list is a fresh copy so callers cannot mutate the
    module constants.
    """
    if lang == 'he':
        return list(_MAIN_HEADERS_HE)
    return list(_MAIN_HEADERS_EN)
```

**New function to add** (copy this exact signature shape):
```python
def local_documents_header_row(lang: str = 'en') -> List[str]:
    """Return the 5 Local Documents sheet column headers in the requested language.

    Phase 103: Bilingual headers for the new "Local Documents" sub-sheet.
    Hebrew when ``lang == 'he'``, English otherwise. Fresh copy.
    """
    if lang == 'he':
        return list(_LOCAL_HEADERS_HE)
    return list(_MAIN_HEADERS_EN)   # <-- use _LOCAL_HEADERS_EN
```

#### Sheet titles function pattern — analogous to `sheet_titles` (lines 424–432):
```python
def sheet_titles(lang: str = 'en') -> Dict[str, str]:
    """Return a dict of localized sheet titles keyed by ``main`` / ``manuscripts``
    / ``bibliography`` / ``credits_info``."""
    if lang == 'he':
        return dict(_SHEET_TITLES_HE)
    return dict(_SHEET_TITLES_EN)
```

**Modification required:** add `'local_documents'` key to both `_SHEET_TITLES_EN` and `_SHEET_TITLES_HE` dicts (lines 376–391). New entries:
- `_SHEET_TITLES_EN['local_documents'] = "Local Documents"`
- `_SHEET_TITLES_HE['local_documents'] = "מסמכים מקומיים"`

#### Row builder pattern — analogous to `build_manuscript_row` (lines 989–1102):
```python
def build_manuscript_row(
    sys_id: str,
    meta_resolver: Optional[MetaResolver],
    lang: str = 'en',
    skip_local: bool = False,
) -> Optional[List[Any]]:
    """Build one Manuscripts sub-sheet row for a sys_id.
    ...
    Returns a list of exactly 14 Python primitives ... Missing data renders
    as empty strings (NOT 'N/A' / placeholders).
    """
    # Phase 95 D-45: skip LOCAL rows on web export path.
    if skip_local and sys_id:
        try:
            from shared.local_sys_id import is_local_sys_id
            if is_local_sys_id(sys_id):
                return None
        except Exception:
            pass
    ...
    return [
        sys_id or '',
        shelfmark,
        library_name,
        ...
    ]
```

**New function to add** (mirror this shape — Qt-free, no SQLite access, takes resolved values):
```python
def build_local_document_row(
    filename: str,
    parent_folder: str,
    full_filepath: str,
    page: str,
    matched_text_raw: str,
    sanitize_fn=None,
) -> List[Any]:
    """Build one Local Documents sheet row.

    Phase 103: Returns a list of exactly 5 Python primitives matching
    :func:`local_documents_header_row` column order. Missing data renders
    as empty strings. ``matched_text_raw`` retains ``*``-markers — caller
    applies ``build_rich_snippet_cell`` at write time (D-03).

    Qt-free, offline-testable (no SQLite, no indexer dependency).
    """
    _san = sanitize_fn or (lambda x: '' if x is None else str(x))
    return [
        _san(filename),
        _san(parent_folder),
        _san(full_filepath),
        _san(page),
        matched_text_raw or '',   # raw *-markers kept for rich-cell rendering
    ]
```

**Note on filepath resolution:** filepath lookup (`_lookup_local_filepath`) and `os.path.basename/dirname` must happen in the CALLER (`_build_search_results_xlsx_bytes`), not here. This helper is pure-data only (D-14 / Qt-free constraint).

---

### 2. `genizah_app.py::_build_search_results_xlsx_bytes` — Modified (lines 2531–2839)

**Role:** utility / transform (module-level pure function, Qt-free).

**Analog:** itself (current 4-sheet builder). The modification adds a conditional 5th sheet ("Local Documents") and flips two `skip_local` args.

#### Full current function signature (lines 2531–2567):
```python
def _build_search_results_xlsx_bytes(
    results,
    headers_main=None,
    meta_resolver=None,
    sanitize_fn=None,
    credit_text='',
    search_info_text='',
    transcription_sys_ids=None,
    printed_ids=None,
    result_domains=None,
    lang='en',
    full_text_fetcher=None,
    search_query=None,
    search_mode=None,
    search_gap=None,
    lab_mode_on=None,
    deep_scan_on=None,
    export_datetime=None,
    domain_name_map=None,
):
```

**Changes required:**

**A. New kwarg** — add `filepath_resolver=None` (or accept pre-resolved `local_filepath_map: dict = None`) to inject the filepath lookup without a Qt/SQLite dependency in the module-level function. Preferred: pass a pre-built `local_filepath_map: dict = None` (sys_id → filepath) mirroring how `result_domains` is passed (already resolved upstream by `export_results`).

**B. Import addition** — in the lazy import block at line 2617–2628:
```python
from shared.export_dossier import (
    build_manuscript_row, build_bibliography_rows,
    build_credits_info_sheet,
    main_header_row, manuscript_header_row, bibliography_header_row,
    sheet_titles,
    apply_manuscript_row_hyperlinks,
    build_image_url_for_row, apply_main_row_image_url_hyperlink,
    # NEW:
    local_documents_header_row, build_local_document_row,
)
from shared_export_utils import build_rich_snippet_cell
```

**C. Partition LOCAL rows** — add after `_domain_name_map = dict(domain_name_map or {})` (line 2639):
```python
_local_filepath_map = dict(local_filepath_map or {})
_local_sys_ids = set()
try:
    from shared.local_sys_id import is_local_sys_id as _is_local_sys_id
    for r in (results or []):
        sid = (r.get('display') or {}).get('id') or r.get('sys_id') or ''
        if sid and _is_local_sys_id(sid):
            _local_sys_ids.add(sid)
except Exception:
    pass
_has_local = bool(_local_sys_ids)
```

**D. Conditional Local Documents sheet creation** (after ws_credits creation, line 2667, before main-sheet header write):
```python
# D-06: Local Documents sheet only when ≥1 LOCAL hit present.
ws_local = None
if _has_local:
    ws_local = wb.create_sheet(title=_titles['local_documents'])
    ws_local.sheet_view.rightToLeft = rtl
# Credits sheet is always last — created before local so ordering is correct
# only if local is inserted before credits. See D-04 for sheet order:
# [Search Results, Manuscripts, Bibliography, Local Documents, Credits and Info]
```

**Sheet order constraint (D-04):** `Local Documents` must be at position 4 (index 3, before `Credits and Info`). Current code creates sheets in order: `ws_main` (active), `ws_manu`, `ws_bib`, `ws_credits`. To insert `ws_local` at position 4, create it BEFORE `ws_credits`:
```python
ws_manu = wb.create_sheet(title=_titles['manuscripts'])
ws_bib = wb.create_sheet(title=_titles['bibliography'])
ws_local = None   # placeholder
if _has_local:
    ws_local = wb.create_sheet(title=_titles['local_documents'])
    ws_local.sheet_view.rightToLeft = rtl
ws_credits = wb.create_sheet(title=_titles['credits_info'])
ws_credits.sheet_view.rightToLeft = rtl
```

**E. Flip `skip_local` on Manuscripts and Bibliography** (lines 2774, 2796):
```python
# Phase 95 D-45: CHANGE skip_local=False -> skip_local=True (D-07).
row = build_manuscript_row(sid, meta_resolver, lang=lang, skip_local=True)
...
for row in build_bibliography_rows(sid, meta_resolver, lang=lang, skip_local=True):
```

**F. Write Local Documents sheet** — add after the Bibliography sheet loop (line 2800), before the Credits sheet build:
```python
# --- Local Documents sheet (Phase 103 — only when _has_local) ---
if ws_local is not None:
    # Header row
    for col_idx, header in enumerate(local_documents_header_row(lang), 1):
        cell = ws_local.cell(row=1, column=col_idx, value=header)
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill(start_color="4F81BD", end_color="4F81BD", fill_type="solid")
    local_row = 2
    import os as _os
    for r in results:
        sid = (r.get('display') or {}).get('id') or r.get('sys_id') or ''
        if not (_is_local_sys_id(sid) if _is_local_sys_id else False):
            continue
        filename = (r.get('display') or {}).get('shelfmark') or ''
        fp = _local_filepath_map.get(sid) or ''
        parent_folder = ''
        if fp:
            try:
                parent_folder = _os.path.basename(_os.path.dirname(fp))
            except Exception:
                parent_folder = ''
        page = r.get('chunk_locator') or (f"p. {r.get('p_num', '')}" if r.get('p_num') else '')
        matched_text_raw = r.get('raw_file_hl', '') or ''
        row_vals = build_local_document_row(
            filename, parent_folder, fp, page, matched_text_raw,
            sanitize_fn=sanitize_fn,
        )
        for col_idx, val in enumerate(row_vals, 1):
            if col_idx == 5:   # Matched Text — rich snippet (D-03)
                ws_local.cell(
                    row=local_row, column=col_idx,
                    value=build_rich_snippet_cell(val, sanitize_fn),
                )
            else:
                ws_local.cell(row=local_row, column=col_idx, value=val)
        local_row += 1
    # Column widths for Local Documents sheet
    for col, width in zip('ABCDE', [45, 25, 80, 10, 70]):
        ws_local.column_dimensions[col].width = width
```

**G. Active sheet** — D-05 (LOCAL-only) vs D-04 (mixed). The existing line:
```python
wb.active = wb.index(ws_main)   # line 2834
```
must be conditionally overridden for LOCAL-only exports:
```python
if _has_local and not any(
    not ((r.get('display') or {}).get('source') == 'LOCAL')
    for r in (results or [])
):
    # LOCAL-only: make Local Documents the active sheet (D-05)
    wb.active = wb.index(ws_local)
else:
    wb.active = wb.index(ws_main)
```

**H. `export_results` caller must supply `local_filepath_map`** — the `_build_search_results_xlsx_bytes` call at line 19807 in `export_results` must pass the pre-primed cache:
```python
content = _build_search_results_xlsx_bytes(
    ...
    local_filepath_map=dict(self._local_filepath_cache),  # NEW
)
```

---

### 3. `genizah_app.py::export_results` — CSV and TXT branches (lines 19856–19938)

**Role:** controller / request-response (method on `GenizahGUI`).

**Analog:** itself (current CSV branch lines 19857–19872, TXT branch lines 19925–19938).

#### Current CSV branch (lines 19857–19872):
```python
elif fmt == 'csv':
    try:
        with open(path, 'w', encoding='utf-8-sig', newline='') as f:
            f.write(credit_text)
            f.write("\n" + search_info_text + "\n")
            writer = csv.writer(f)
            writer.writerow([])
            writer.writerow(headers)
            for row in data_rows:
                # Strip highlight markers for CSV
                clean_row = [str(val).replace('*', '') for val in row]
                writer.writerow(clean_row)
        self._save_last_folder(path)
        QMessageBox.information(self, tr("Saved"), tr("Saved to {}").format(path))
    except Exception as e:
        QMessageBox.critical(self, tr("Error"), f"Failed to save CSV:\n{str(e)}")
```

**Current `data_rows` builder** (lines 19638–19667): iterates `results_to_export`, builds 7-column rows `[sid, library_name, shelf, title, img, source, snippet]`. The `display` source discriminator is at `d.get('source', '')`.

**On-screen LOCAL display pattern** (lines 16726–16751, canonical reference for CSV mirroring):
```python
_is_local_hit = meta.get('source') == 'LOCAL'
if _is_local_hit:
    shelf = meta.get('shelfmark', '') or sid   # filename
    title = meta.get('title', '')
    library_code = ''
    _fp = self._lookup_local_filepath(sid)
    if _fp:
        _dir = os.path.dirname(_fp)
        _folder = os.path.basename(_dir)
        _parent = os.path.basename(os.path.dirname(_dir))
        if _parent:
            _local_library_display = f"{_parent}/{_folder}"
        else:
            _local_library_display = _folder
```

**Modifications to CSV branch:**

1. **Pre-export**: detect `_has_local_in_export = any((r.get('display') or {}).get('source') == 'LOCAL' for r in results_to_export)` at top of `export_results` (reuse across CSV/TXT/DOCX branches).

2. **Modify `data_rows` builder** (lines 19638–19667): for LOCAL rows, remap columns per D-08:
   - Shelfmark col = `display['shelfmark']` (filename)
   - Library col = parent folder (from `_lookup_local_filepath`)
   - Source = `"LOCAL"`
   - Snippet = `raw_file_hl` (stripped of `*`)

3. **Conditional extra columns** — when `_has_local_in_export`, append `Filepath` and `Page` columns:
```python
# D-08: conditional extended headers for CSV
if _has_local_in_export:
    csv_headers = headers + [
        tr("Filepath") if CURRENT_LANG == 'en' else "נתיב מלא",
        tr("Page") if CURRENT_LANG == 'en' else "עמוד",
    ]
else:
    csv_headers = headers
```

#### Current TXT branch (lines 19925–19938):
```python
else:
    try:
        with open(path, 'w', encoding='utf-8') as f:
            f.write(credit_text)
            f.write("\n" + search_info_text + "\n\n")
            for r in results_to_export:
                snippet = r.get('raw_file_hl', '').strip().replace('\n', ' ').replace('\r', '')
                f.write(f"=== {r['display']['shelfmark']} | {r['display']['title']} ===\n{snippet}\n\n")
        self._save_last_folder(path)
        QMessageBox.information(self, tr("Saved"), tr("Saved to {}").format(path))
    except Exception as e:
        QMessageBox.critical(self, tr("Error"), f"Failed to save TXT:\n{str(e)}")
```

**Modification to TXT branch** (D-09): per-result LOCAL block pattern replaces the Genizah `=== shelfmark | title ===` line for LOCAL rows:
```python
for r in results_to_export:
    d = r.get('display') or {}
    snippet = r.get('raw_file_hl', '').strip().replace('\n', ' ').replace('\r', '')
    snippet_clean = snippet.replace('*', '')
    if d.get('source') == 'LOCAL':
        sid = d.get('id') or r.get('sys_id') or ''
        filename = d.get('shelfmark') or sid
        fp = self._lookup_local_filepath(sid) or ''
        parent = os.path.basename(os.path.dirname(fp)) if fp else ''
        p_num = r.get('chunk_locator') or r.get('p_num') or ''
        page_str = f"(page {p_num})" if p_num else ''
        f.write(f"=== {filename} | {parent} ===\n")
        f.write(f"Path: {fp}  {page_str}\n")
        f.write(f"{snippet_clean}\n\n")
    else:
        f.write(f"=== {d.get('shelfmark','')} | {d.get('title','')} ===\n{snippet_clean}\n\n")
```

---

### 4. `genizah_app.py::export_results` — DOCX branch (lines 19874–19923) — full replacement

**Role:** utility / request-response (method on `GenizahGUI`).

**Analog — current DOCX table builder** (lines 19874–19923, to be replaced):
```python
elif fmt == 'docx':
    try:
        from docx import Document
        from docx.enum.text import WD_PARAGRAPH_ALIGNMENT
        doc = Document()
        for line in credit_text.split('\n'):
            ...
        headers = [tr("System ID"), tr("Library"), tr("Shelfmark"), tr("Title"),
                   tr("Image/Page"), tr("Source"), tr("Snippet")]
        table = doc.add_table(rows=1, cols=len(headers))
        self._set_table_width_pct(table, 100)
        hdr_cells = table.rows[0].cells
        for idx, header in enumerate(headers):
            hdr_cells[idx].text = header

        for row in data_rows:
            row_cells = table.add_row().cells
            for col_idx, val in enumerate(row):
                cell = row_cells[col_idx]
                if col_idx == 6:  # Snippet column
                    cell.text = ""
                    self._add_docx_highlighted_runs(cell.paragraphs[0], val)
                else:
                    cell.text = str(val).replace('*', '')
        ...
        doc.save(path)
```

**RTL + highlight helpers** (lines 19550–19593, used by the existing DOCX table writer and must be used by the new block writer):
```python
def _add_docx_highlighted_runs(self, paragraph, text):
    from docx.shared import RGBColor
    parts = str(text or "").split('*')
    for i, part in enumerate(parts):
        if not part:
            continue
        run = paragraph.add_run(part)
        if i % 2 == 1:
            run.font.color.rgb = RGBColor(0xFF, 0x00, 0x00)
            run.font.bold = True

def _set_paragraph_rtl(self, paragraph):
    from docx.enum.text import WD_PARAGRAPH_ALIGNMENT
    from docx.oxml import OxmlElement
    from docx.oxml.ns import qn
    paragraph.alignment = WD_PARAGRAPH_ALIGNMENT.RIGHT
    paragraph.paragraph_format.alignment = WD_PARAGRAPH_ALIGNMENT.RIGHT
    ppr = paragraph._p.get_or_add_pPr()
    bidi = ppr.find(qn("w:bidi"))
    if bidi is None:
        bidi = OxmlElement("w:bidi")
        ppr.append(bidi)
    bidi.set(qn("w:val"), "1")
```

**New shared DOCX block writer** (to be implemented in `shared/` or as a standalone module-level function in `genizah_app.py`). The CONTEXT.md (deferred section) says it should be "designed to be reusable by Phase 104's `export_comp_report`" — place it in a **new** `shared/docx_export.py` module (parallel to `shared_export_utils.py`) or as a module-level function in `genizah_app.py`. If in `genizah_app.py`, it MUST NOT take `self` as a parameter (module-level, not a method) so Phase 104 can call it without a `GenizahGUI` instance.

**Signature shape to implement** (mirroring `_build_search_results_xlsx_bytes` being pure / Qt-free):
```python
def _write_docx_result_block(doc, result_dict, filepath: str = '', lang: str = 'en') -> None:
    """Write one per-result block to a python-docx Document.

    Phase 103 D-10: replaces the former cramped 7-column table layout with a
    research-handout block:
      - Heading paragraph: ``Shelfmark — Title`` (Genizah) or ``Filename — Parent folder`` (LOCAL)
      - Metadata line: ``Library · Image/Page · Source`` (Genizah) or
        ``{full filepath} · page N · LOCAL`` (LOCAL)
      - Matched text paragraph with bold red *-highlights (D-11)
      - URL line: GenizahSearch URL (Genizah) or full filepath (LOCAL) (D-11)
      - Separator paragraph

    Designed to be reusable by Phase 104 export_comp_report (passes one result-like dict).
    Qt-free — no QWidget / Qt import.
    """
```

**New DOCX branch structure** — replaces lines 19875–19923:
```python
elif fmt == 'docx':
    try:
        from docx import Document
        from docx.enum.text import WD_PARAGRAPH_ALIGNMENT
        doc = Document()
        # Credits header (unchanged)
        for line in credit_text.split('\n'):
            if not line.strip():
                continue
            p = doc.add_paragraph(line.strip())
            if p.runs:
                p.runs[0].font.bold = True
        for line in search_info_text.split('\n'):
            if not line.strip():
                continue
            doc.add_paragraph(line.strip())
        doc.add_paragraph("")

        # NEW: per-result block layout (D-10)
        for r in results_to_export:
            sid = (r.get('display') or {}).get('id') or r.get('sys_id') or ''
            fp = self._lookup_local_filepath(sid) if (
                (r.get('display') or {}).get('source') == 'LOCAL'
            ) else ''
            _write_docx_result_block(doc, r, filepath=fp, lang=CURRENT_LANG)

        if CURRENT_LANG == "he":
            doc.styles["Normal"].paragraph_format.alignment = WD_PARAGRAPH_ALIGNMENT.RIGHT
            for p in doc.paragraphs:
                self._set_paragraph_rtl(p)

        doc.save(path)
        self._save_last_folder(path)
        QMessageBox.information(self, tr("Saved"), tr("Saved to {}").format(path))
    except Exception as e:
        QMessageBox.critical(self, tr("Error"), f"Failed to save DOCX:\n{str(e)}")
```

---

## Shared Patterns

### LOCAL row discrimination
**Source:** `genizah_app.py` (lines 16728, 18839) and `genizah_core.py` (line 7238)
**Apply to:** all four export branches + new Local Documents sheet writer

Primary discriminator:
```python
_is_local_hit = (r.get('display') or {}).get('source') == 'LOCAL'
```

Secondary guard (for `_build_search_results_xlsx_bytes`):
```python
from shared.local_sys_id import is_local_sys_id
if is_local_sys_id(sys_id):
    ...
```

### Filepath resolution — batch-primed cache
**Source:** `genizah_app.py` lines 18804–18849
**Apply to:** `export_results` (prime once before XLSX/CSV/TXT/DOCX dispatch), `_build_search_results_xlsx_bytes` (accept pre-built map via kwarg).

Critical pattern (DO NOT do per-row SQLite round-trips — BUG-6 lesson):
```python
def _lookup_local_filepath(self, sys_id: str):
    cache = getattr(self, '_local_filepath_cache', None)
    if cache is not None and sys_id in cache:
        return cache[sys_id]
    ...

def _prime_local_filepath_cache(self, results):
    self._local_filepath_cache = {}
    try:
        local_ids = [
            sid for r in (results or [])
            for sid in ((r.get('display', {}) or {}).get('id') or r.get('sys_id', ''),)
            if sid and (r.get('display', {}) or {}).get('source') == 'LOCAL'
        ]
        if not local_ids:
            return
        ...
        self._local_filepath_cache = indexer.get_filepaths(local_ids)
    except Exception:
        self._local_filepath_cache = {}
```

**Callers must prime the cache before iterating results** (call `_prime_local_filepath_cache(results_to_export)` at the top of `export_results`, or rely on the cache already primed at `on_search_finished`).

### Rich snippet cell (openpyxl)
**Source:** `shared_export_utils.py` lines 139–194 (`build_rich_snippet_cell`)
**Apply to:** Local Documents sheet "Matched Text" column (D-03)

```python
from shared_export_utils import build_rich_snippet_cell
# Usage:
ws_local.cell(row=local_row, column=5,
    value=build_rich_snippet_cell(matched_text_raw, sanitize_fn))
```

### `build_rich_snippet_cell` for DOCX (analogous)
**Source:** `genizah_app.py::_add_docx_highlighted_runs` (lines 19550–19559)
**Apply to:** DOCX matched-text paragraph in new block writer

```python
def _add_docx_highlighted_runs(self, paragraph, text):
    from docx.shared import RGBColor
    parts = str(text or "").split('*')
    for i, part in enumerate(parts):
        if not part:
            continue
        run = paragraph.add_run(part)
        if i % 2 == 1:
            run.font.color.rgb = RGBColor(0xFF, 0x00, 0x00)
            run.font.bold = True
```

The module-level `_write_docx_result_block` function must inline this logic (cannot call `self._add_docx_highlighted_runs`) since it is not a method.

### DOCX RTL / bidi handling
**Source:** `genizah_app.py::_set_paragraph_rtl` (lines 19561–19572)
**Apply to:** new DOCX block writer when `lang == 'he'`

```python
def _set_paragraph_rtl(self, paragraph):
    from docx.enum.text import WD_PARAGRAPH_ALIGNMENT
    from docx.oxml import OxmlElement
    from docx.oxml.ns import qn
    paragraph.alignment = WD_PARAGRAPH_ALIGNMENT.RIGHT
    paragraph.paragraph_format.alignment = WD_PARAGRAPH_ALIGNMENT.RIGHT
    ppr = paragraph._p.get_or_add_pPr()
    bidi = ppr.find(qn("w:bidi"))
    if bidi is None:
        bidi = OxmlElement("w:bidi")
        ppr.append(bidi)
    bidi.set(qn("w:val"), "1")
```

### Excel header row styling
**Source:** `genizah_app.py` lines 2675–2679 and 2757–2760
**Apply to:** Local Documents sheet header row

```python
for col_idx, header in enumerate(local_documents_header_row(lang), 1):
    cell = ws_local.cell(row=1, column=col_idx, value=header)
    cell.font = Font(bold=True, color="FFFFFF")
    cell.fill = PatternFill(start_color="4F81BD", end_color="4F81BD", fill_type="solid")
```

### Error handling pattern (export branches)
**Source:** `genizah_app.py` lines 19853–19854 and 19871–19872
**Apply to:** all modified branches

```python
except Exception as e:
    QMessageBox.critical(self, tr("Error"), f"Failed to save XLSX:\n{str(e)}")
```

### Page locator fallback (D-02)
**Source:** `genizah_core.py` lines 7187–7188 (`chunk_locator` field)
**Apply to:** Local Documents Page column + TXT/DOCX page rendering

```python
# chunk_locator is human-readable (e.g. "p. 3" for PDFs, "§ Intro" for DOCX/HTML)
# p_num is the 1-based page number as string
page = r.get('chunk_locator') or (f"p. {r.get('p_num', '')}" if r.get('p_num') else '')
```

---

## Non-Regression Constraint

### `tests/test_export_xlsx_cross_parity.py` (must stay green, no modification)

The cross-parity test builds workbooks with **Genizah-only** fixtures (sys_id `'99001234567890'` — no LOCAL rows). Because D-06 gates the Local Documents sheet on `≥1 LOCAL row`, the fixture produces the unchanged 4-sheet workbook. The test checks `wb.sheetnames` equality — the Local Documents sheet is absent on both sides.

**Key assertions** (lines 127–179):
```python
assert wb_web.sheetnames == wb_desktop.sheetnames   # ['Search Results', 'Manuscripts', 'Bibliography', 'Credits and Info']
assert wb_web.active.title == wb_desktop.active.title
# Header row byte-identity on Search Results, Manuscripts, Bibliography
```

**The `local_filepath_map` new kwarg** must default to `None` (or `{}`) in `_build_search_results_xlsx_bytes` so all existing test call sites (which pass positional/keyword args and do not know about this kwarg) continue to work unchanged.

---

## LOCAL Result Dict Shape (Reference)

From `genizah_core.py` lines 7220–7249:
```python
{
    "uid": unique_id,
    "full_text": content,
    "snippet": snippet,
    "raw_file_hl": raw_file_hl,        # *-marked, used by D-03 rich-cell
    "highlight_pattern": effective_pattern,
    "sys_id": sys_id,
    "p_num": p_num,                     # 1-based page number string
    "img": p_num,
    "score": float(score),
    "chunk_locator": chunk_locator,     # human-readable "p. 3" / "§ Intro"
    "display": {
        "id": sys_id,
        "source": "LOCAL",              # primary discriminator (D-14)
        "library_code": "LOCAL",
        "shelfmark": shelfmark,         # filename (D-14: filename ← display['shelfmark'])
        "img": p_num,
    },
    "full_header": full_header,
}
```

---

## No Analog Found

No files in this phase lack an analog. All surfaces have close existing patterns.

---

## Metadata

**Analog search scope:** `genizah_app.py`, `shared/export_dossier.py`, `shared_export_utils.py`, `shared/local_sys_id.py`, `genizah_core.py`, `tests/test_export_xlsx_cross_parity.py`, `tests/test_export_dossier_local_handling.py`
**Files scanned:** 7 primary files + 2 test files
**Pattern extraction date:** 2026-06-01
