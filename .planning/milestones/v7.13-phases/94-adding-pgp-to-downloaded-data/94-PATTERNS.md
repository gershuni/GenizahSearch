# Phase 94: Research-Grade Export Metadata — Pattern Map

**Mapped:** 2026-05-19
**Files analyzed:** 7 (1 NEW, 6 MODIFY)
**Analogs found:** 7 / 7

This pattern map produces concrete code excerpts (with file paths and line numbers) for each target file so the planner can write plans by reference, not by paraphrase. Every "Copy from" pointer below has been verified against the actual file at the cited lines on 2026-05-19.

---

## File Classification

| Target File | New / Modify | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|---|
| `shared/export_dossier.py` | NEW | shared service / projection module | per-sys_id point-lookup → narrow projection dict | `shared/browse_service.py` (`_pgp_sync`, `_nli_sync`, `_fjms_sync`) + `shared/search_serializer.py` (`_build_pgp_subset`) | exact (projection helpers); structural match (timeout wrapper omitted — D-A2 swaps it for in-helper try/except per D-09 perf) |
| `web/export_service.py:410 export_search_results_excel` | MODIFY | export-pipeline builder | result-list iter → multi-sheet openpyxl Workbook bytes | `web/export_service.py:641 export_parallels_excel` (same file — multi-section iterator-add_results inner helper); `web/export_service.py:577 export_list_excel` (per-row openpyxl + alignment) | exact (same module pattern); rich-text snippet pattern at `genizah_app.py:18000` |
| `shared/search_serializer.py:298 _serialize_item` | MODIFY | per-item serializer (additive 3 keys) | per-result dict → envelope dict | `shared/search_serializer.py:298 _serialize_item` itself (additive 'is_synthetic' Phase 85 precedent at `:360` is the canonical "add one bool key" template) | exact (same function, additive kwargs precedent in same file) |
| `web/export_state.py:set_search_export` | MODIFY | per-session payload writer (+3 kwargs, +1 new helper) | session payload dict mutate via safe_storage chokepoint | `web/export_state.py:511 update_search_export_results`, `:531 update_search_export_selection` (sibling helpers in same file — D-11 isinstance guard + D-12 copy-on-update) | exact (same file, established convention) |
| `web/pages/search.py` (3 sites + 1 new) | MODIFY | search-state plumbing | kwargs propagation only | `web/pages/search.py:2150-2152` (existing `update_search_export_selection` post-write site at the row-checkbox handler) | exact (sibling pattern in same file) |
| `web/api.py:2069 export_excel`, `:2212 export_json` | MODIFY | FastAPI endpoint payload-readers | session payload read → export_svc / serializer call | `web/api.py:2069 export_excel` itself (Phase 88-shaped read pattern); `:2212 export_json` itself (serializer-call pattern with `session_payload.get(...)` propagation) | exact (same file — additive kwarg propagation only) |
| `genizah_app.py:17895 export_results(fmt='xlsx')` | MODIFY | desktop export entry point | result-list iter → multi-sheet openpyxl Workbook saved to user-chosen path | `genizah_app.py:17984-18067` (current xlsx branch in same function — write_rich_cell + sheet construction); `web/export_service.py:283 create_excel_workbook` (the openpyxl wb-active pattern) | exact (same function — minimal restructure) |

**Web result-dict equivalents for desktop `d.get('img', '')` and `d.get('source', '')`** (D-01 Claude's Discretion item resolved here):
Web uses the SAME `display` dict shape — `genizah_core.MetadataManager.get_display_data()` at `genizah_core.py:4915-4931` returns `{shelfmark, title, img, source, id, library_code}`. Both apps share this dict. Web reads `result['display'].get('img', '')` and `result['display'].get('source', '')` — no field-identification ambiguity.

---

## Pattern Assignments

### 1. NEW `shared/export_dossier.py` (shared projection module)

**Module purpose:** 4 lookup helpers + 2 row-emitters + 2 header constants. Per CONTEXT D-08 + Codex MUST-FIX 1-4 + SHOULD-FIX 5-10.

#### Pattern 1.1 — Per-sys_id lookup helper structure

**Copy from:** `shared/browse_service.py:155-182` (`_pgp_sync`) and `:208-243` (`_nli_sync`)

```python
# shared/browse_service.py:155-182
def _pgp_sync(sys_id: str, p_num: int) -> Optional[dict]:
    """Fetch PGP doc + page-section transcription. Returns shaped dict or None."""
    from shared.document_service import (
        get_document_for_fragment, get_section_for_page,
    )
    doc = get_document_for_fragment(sys_id, p_num)
    if not doc:
        return None
    page_section_text: Optional[str] = None
    transcription = doc.get('transcription')
    if transcription:
        page_section_text = get_section_for_page(
            transcription, p_num,
            fragment_page_info=doc.get('_fragment_page_info'),
        )
    return {
        'description':           doc.get('description'),
        'tags':                  list(doc.get('tags') or []),
        'document_type':         doc.get('document_type'),
        'languages_primary':     list(doc.get('languages_primary') or []),
        'languages_secondary':   list(doc.get('languages_secondary') or []),
        'doc_date_original':     doc.get('doc_date_original'),
        'doc_date_standard':     doc.get('doc_date_standard'),
        'inferred_date_display': doc.get('inferred_date_display'),
        'pgpid':                 doc.get('pgpid'),
        'pgp_url':               doc.get('pgp_url'),
        'page_section_text':     page_section_text,
    }
```

**Apply to:** `pgp_subset_for_sys_id(sys_id)`.

**Deltas from analog:**
- DROP `page_section_text` — D-02 strict prohibition on transcription text in NEW dossier surfaces.
- DROP `page_num` parameter — Manuscripts row is per-sys_id, not per-folio.
- ADD try/except wrapper around the whole body — D-A2 exception-resilient (the analog deliberately propagates exceptions because `_wrap_with_timeout` at `shared/browse_service.py:250-270` is its outer catch; the dossier module has no such wrapper — Wave 1 catches inline).
- ADD `_split_pgp_languages(value)` helper for the comma-separated TEXT projection bug (D-08 helper 1). Languages column in pgp.db documents row is stored as comma-separated TEXT in some sidecar versions; `list(doc.get('languages_primary') or [])` iterates characters when the value is a string, not a list. Helper splits on `,` first.

**Copy `_nli_sync` shape (lines 208-243) for `nli_subset_for_sys_id`:**

```python
# shared/browse_service.py:208-243 (relevant subset — strip folio/p_num logic)
def _nli_sync(sys_id: str, p_num: int, fl_id: Optional[str] = None) -> Optional[dict]:
    from shared.nli_crossref_service import get_nli_crossref_service
    svc = get_nli_crossref_service(thread_safe=True)
    if not svc or not svc.is_available() or not sys_id:
        return None
    # ... folio logic dropped in dossier version ...
    return {
        'physical_metadata': crossref_data.get('physical_metadata'),
        'folio':             active_folio,
    }
```

**Apply to:** `nli_subset_for_sys_id(sys_id)` — replace `physical_metadata`/`folio` with `{catalog_entry, library_viewer_url}` per Codex MUST-FIX 2.

#### Pattern 1.2 — Service factory + availability gate

**Copy from:** `shared/fjms_service.py:3413-3422` and `shared/nli_crossref_service.py:1019`

```python
# shared/fjms_service.py:3413-3422 — singleton factory pattern
_default_service: Optional[FjmsService] = None

def get_fjms_service(thread_safe: bool = True) -> FjmsService:
    """Get or create the default FjmsService singleton."""
    global _default_service
    if _default_service is None:
        _default_service = FjmsService(thread_safe=thread_safe)
    return _default_service
```

**Apply to:** Every helper in `shared/export_dossier.py` calls `get_fjms_service(thread_safe=True)` (matches `_pgp_sync`'s pattern of importing the factory inside the function body — keeps Wave-1 unit-test mocking via `monkeypatch.setattr('shared.export_dossier.get_fjms_service', ...)` simple). Availability gate: `if not svc or not svc.is_available(): return None` — same as `browse_service._fjms_sync:188-190`.

#### Pattern 1.3 — Narrow projection helper (Codex SHOULD-FIX 12 — opinionated leaf shape)

**Copy from:** `shared/search_serializer.py:523-536` (`_build_pgp_subset`)

```python
# shared/search_serializer.py:523-536
def _build_pgp_subset(pgp: dict) -> dict:
    """R-07 stable shape (10 keys, never missing)."""
    return {
        'description':           pgp.get('description'),
        'tags':                  list(pgp.get('tags') or []),
        'document_type':         pgp.get('document_type'),
        'languages_primary':     list(pgp.get('languages_primary') or []),
        'languages_secondary':   list(pgp.get('languages_secondary') or []),
        'doc_date_original':     pgp.get('doc_date_original'),
        'doc_date_standard':     pgp.get('doc_date_standard'),
        'inferred_date_display': pgp.get('inferred_date_display'),
        'pgpid':                 pgp.get('pgpid'),
        'pgp_url':               pgp.get('pgp_url'),
    }
```

**Apply to:** Each lookup helper's return statement — stable narrow shape, every key always present (None when absent), never missing. Date fallback chain per CONTEXT D-08 helper 1: `inferred_date_display → doc_date_standard → doc_date_original → None`.

#### Pattern 1.4 — Service field availability (FJMS bibliography real schema)

**Copy from:** `shared/fjms_service.py:2562-2596` (the body of `get_bibliography`)

```python
# shared/fjms_service.py:2562-2596
for row in cursor:
    entry = {
        "running_title": row["RunningTitle"],
        "title_year": row["TitleYear"],
        "title_acronym": row["TitleAcronym"],
        "mention_page": row["MentionPage"],
        "from_page": row["FromPage"],
        "to_page": row["ToPage"],
        "volume": row["Volume"],
        "mention_type": row["MentionType"],
        "transcription_type": row["TranscriptionType"],
        "translation_type": row["TranslationType"],
        "article_name": row["ArticleName"],
        "article_author_eng": row["ArticleAuthorEng"],
        "article_author_heb": row["ArticleAuthorHeb"],
        "catalog_acronym": row["CatalogAcronym"],
    }
    # ... extended fields when sidecar supports them ...
```

**Apply to:** `bibliography_for_sys_id(sys_id) -> List[dict]` — wraps `fjms.get_bibliography(sys_id)` (no further projection — service already returns the real-name dicts; helper just passes through). CONTEXT D-08 helper 4 mandates the 6 fields {running_title, title_year, mention_page, article_name, article_author_eng, catalog_acronym} as the bib row schema. The 6 fields are a SUBSET of the 14 service fields above.

#### Pattern 1.5 — Catalog summary (Codex MUST-FIX 3 — use get_catalog_records NOT get_catalog_detail)

**Copy from:** `shared/fjms_service.py:2435-2524` (`get_catalog_records` — the WHOLE function body confirms the schema)

The schema returned per record (lines 2476-2493): `{title, title_heb, author_text, copy_date, copy_place, textual_frame_heb, textual_frame_eng, source_name, source_name_heb, unit_catalog_rec_id, num_folio, num_bifolio, num_column, num_row, genizah_title_org, genizah_title_eng}`.

**Apply to:** `catalog_summary_for_sys_id(sys_id) -> Optional[dict]` — projects 3-5 narrow fields from `get_catalog_records()` (returns a LIST per sys_id; helper picks the first non-empty record or aggregates). Documented rationale per CONTEXT D-08 helper 3: likely `{title, author_text, copy_date, textual_frame_eng}` — pick 3-5 of the 16 fields above, NEVER reach into `get_catalog_detail()` (which loads `full_texts` and is the D-02 violation).

#### Pattern 1.6 — Module-level header constants (Codex SHOULD-FIX 7)

**Copy from:** No exact analog — closest is `web/export_service.py:31-36` (CREDITS_TEXT module-level list constant) and `web/export_service.py:115-125` (the `_SEARCH_ROW_ALLOWLIST = frozenset((...))` pattern).

```python
# web/export_state.py:115-125
_SEARCH_ROW_ALLOWLIST = frozenset((
    'uid', 'sys_id', 'sort_score', 'snippet', 'match_terms', 'raw_header',
))
```

**Apply to:** Define `MANUSCRIPT_HEADERS: List[str]` and `BIBLIOGRAPHY_HEADERS: List[str]` as module-level constants (NOT frozenset — header lists are ordered and consumed positionally with `ws.append(MANUSCRIPT_HEADERS)`). Both apps `from shared.export_dossier import MANUSCRIPT_HEADERS, BIBLIOGRAPHY_HEADERS` and pass them to `style_excel_header(ws, MANUSCRIPT_HEADERS)`. Per CONTEXT D-08, the headers match the row order returned by `build_manuscript_row` / `build_bibliography_rows`. Tests assert `len(MANUSCRIPT_HEADERS) == len(build_manuscript_row(...))` and same for bibliography.

#### Pattern 1.7 — Metadata resolver callable (Codex SHOULD-FIX 8 — replaces opaque `meta_mgr`)

**Copy from:** `web/export_service.py:76-176` (`_resolve_result_display` function — it constructs the `(shelfmark, title, library_code, library_name)` tuple from meta_mgr; the row builders need a similar but simpler primitive resolver)

```python
# web/export_service.py:76-176 (subset showing the contract)
def _resolve_result_display(result: Dict[str, Any], meta_mgr) -> tuple:
    """Return (shelfmark, title, library_code, library_name) for an export row."""
    # ... tier logic ...
    try:
        meta = meta_mgr.get_meta_for_id(sys_id)
        if isinstance(meta, tuple) and len(meta) >= 2:
            shelfmark = meta[0] or f'ID: {sys_id}'
            title = meta[1] or ''
        # ...
    try:
        library_code = meta_mgr.get_library_for_id(sys_id) or ''
    except Exception:
        library_code = ''
    # ...
    return (shelfmark, title, library_code, library_name)
```

**Apply to:** `build_manuscript_row(sys_id, meta_resolver, lang='en')` — `meta_resolver` is a callable that each app constructs at the call site:

```python
# Web (web/export_service.py) constructs:
def _meta_resolver(sys_id: str) -> Optional[dict]:
    if not sys_id or not self.meta_mgr: return None
    shelf, title = self.meta_mgr.get_meta_for_id(sys_id)
    lib_code = self.meta_mgr.get_library_for_id(sys_id) or ''
    from genizah_core import get_library_display
    lib_name = get_library_display(lib_code, short=False, lang='en') if lib_code else ''
    return {'shelfmark': shelf, 'title': title, 'library_code': lib_code, 'library_name': lib_name}

# Desktop (genizah_app.py) constructs the SAME shape from self.meta_mgr (same module: genizah_core.MetadataManager)
```

**Why callable not object:** Per Codex SHOULD-FIX 8, an opaque `meta_mgr` object risks silent web/desktop drift if either app evolves its meta interface. A primitive 4-key dict return is the cross-app contract.

#### Pattern 1.8 — D-04 English-only library name (Codex SHOULD-FIX 9)

**Copy from:** `genizah_core.get_library_display(library_code, short=False, lang='en')` direct hard-pin (NOT routing through `web.translations.get_language()`).

The web's `web/export_service.py:399-408 get_library_display` instance method DOES route through `get_language()` — that's the LEGACY path for non-export callers. For the dossier path:

```python
# CORRECT (D-04 hard-pin English):
from genizah_core import get_library_display as core_get_library_display
library_name = core_get_library_display(library_code, short=False, lang='en') or library_code
```

Pre-existing analog precedent: `shared/search_serializer.py:325 _safe_library_name` uses the same hard-pin English routing for the public JSON envelope.

**Apply to:** `_meta_resolver` (constructed at each app's call site) AND the `Library` column in `_build_main_sheet` (web's main sheet adopts the same English-only library resolution). The `lang` parameter on row builders is ONLY for downstream sheet-view direction — documented in module docstring.

#### Pattern 1.9 — Wave 1 unit-test fixture pattern (no openpyxl in shared/ tests)

**Copy from:** `tests/test_export_state_selection.py:29-73` (SimpleNamespace stub) and Wave-1 plan (SUPERSEDED-v2) showed `FakeFjmsService` / `FakeNliService` classes that monkeypatch the factory function.

**Apply to:** Each helper gets its own `_FakeService` class with `is_available()`, the specific method (`get_document_for_fragment`, `get_catalog_entry`, etc.), and a `raises=False` switch for the exception-resilience test. Test coverage matches CONTEXT D-08 + Codex SHOULD-FIX 12: missing sidecars, comma-split languages, empty tags, service exceptions, no-transcription-text guarantee (regex-assert `'page_section_text'` NEVER in helper output keys).

---

### 2. MODIFY `web/export_service.py:410 export_search_results_excel`

**Restructure** the single-sheet function into a 3-sheet builder. Keep the existing function signature `(self, results, search_query="")` so `web/api.py:2094` callers don't break.

#### Pattern 2.1 — Multi-sheet workbook construction

**Copy from:** `web/export_service.py:283-296 create_excel_workbook` + `wb.create_sheet(...)` pattern.

```python
# web/export_service.py:283-296
def create_excel_workbook(
    sheet_name: str = "Results",
    rtl_sheet: bool = True
) -> tuple:
    """Create a new Excel workbook with basic configuration.
    Returns (workbook, worksheet).
    """
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = sheet_name[:31]  # Excel sheet name limit
    if rtl_sheet:
        ws.sheet_view.rightToLeft = True
    return wb, ws
```

**Apply to:** Restructured `export_search_results_excel`:

```python
# Pattern: per CONTEXT D-04 conditional RTL + D-03 sheet order
lang = get_language()  # web/translations.get_language()
rtl = (lang == 'he')
wb, ws_main = create_excel_workbook("Genizah Results", rtl_sheet=rtl)
ws_manu = wb.create_sheet(title="Manuscripts")
ws_bib = wb.create_sheet(title="Bibliography")
ws_manu.sheet_view.rightToLeft = rtl
ws_bib.sheet_view.rightToLeft = rtl
# ... build each sheet ...
wb.active = wb.index(ws_main)  # D-03 default-active sheet
```

#### Pattern 2.2 — Header styling + column widths (REUSE existing helpers verbatim)

**Copy from:** `web/export_service.py:299-316` (`style_excel_header`, `set_excel_column_widths`)

```python
# web/export_service.py:299-310
def style_excel_header(ws, headers: List[str]) -> None:
    """Apply standard header styling to the first row."""
    ws.append(headers)
    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill(start_color="4F81BD", end_color="4F81BD", fill_type="solid")
    header_alignment = Alignment(horizontal="center", vertical="center")
    for col_idx, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col_idx)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = header_alignment
```

**Apply to:** All 3 sheets call `style_excel_header(ws, headers_for_this_sheet)`. Column widths set via `set_excel_column_widths(ws, {'A': 18, 'B': 25, ...})` — planner chooses widths (CONTEXT Claude's Discretion).

#### Pattern 2.3 — Existing main-sheet body iteration (the REPLACEMENT target)

**Currently at:** `web/export_service.py:439-499` (the `for res in results:` loop)

```python
# web/export_service.py:439-499 (current — to be REPLACED)
for res in results:
    shelfmark, title, _library_code, library_name = _resolve_result_display(res, self.meta_mgr)
    snippet = clean_text_single_line(remove_highlight_markers(res.get('snippet', '')))
    full_text = clean_text_single_line(_resolve_result_full_text(res))[:32000]
    # ... sys_id resolution tier logic ...
    row = [
        sanitize_text_for_excel(shelfmark),
        sanitize_text_for_excel(library_name),
        sanitize_text_for_excel(title),
        sanitize_text_for_excel(sys_id_for_cell),
        str(res.get('sort_score', '')),
        sanitize_text_for_excel(snippet),
        sanitize_text_for_excel(full_text),
    ]
    ws.append(row)
    current_row = ws.max_row
    ws.cell(row=current_row, column=1).alignment = rtl_align
    # ... 6 more alignments ...
```

**Replace with** (per CONTEXT D-01 unified column order):

- DROP `Score` column.
- REORDER first 4 to `System ID | Library | Shelfmark | Title`.
- ADD `Image/Page` (col 5) ← `result['display'].get('img', '')`.
- ADD `Source` (col 6) ← `result['display'].get('source', '')`.
- Move `Snippet` → col 7 (rendered via `write_rich_cell` from Pattern 2.4 instead of plain text — D-14).
- Keep `Full Text` → col 8 (grandfathered per D-02 amendment).
- ADD 4 new appended cols: `Has PGP` (col 9, `"Yes"` or empty per D-06), `Is Printed` (col 10), `Domains` (col 11, pipe-joined per D-05), `IIIF Manifest` (col 12, optional per D-13).

#### Pattern 2.4 — Rich-text snippet rendering (D-14 — extracted from desktop)

**Copy verbatim from:** `genizah_app.py:17988-18021` (this is THE canonical pattern that web's main sheet adopts per CONTEXT D-14)

```python
# genizah_app.py:17988-18021
from openpyxl.cell.rich_text import TextBlock, CellRichText
from openpyxl.cell.text import InlineFont

# Fonts used for rich text snippets
font_red = InlineFont(color='FF0000', b=True)
font_normal = InlineFont(color='000000', b=False)

# Helper to write rich text cells
def write_rich_cell(row, col, text):
    safe_text = self._sanitize_for_excel(text)
    if '*' not in safe_text:
        ws.cell(row=row, column=col, value=safe_text)
        return
    # Split by asterisk markers
    parts = safe_text.split('*')
    rich_string = CellRichText()
    for i, part in enumerate(parts):
        if not part:
            continue
        # Odd indices represent highlighted text
        if i % 2 == 1:
            rich_string.append(TextBlock(font_red, part))
        else:
            # Even indices are plain text
            rich_string.append(TextBlock(font_normal, part))
    ws.cell(row=row, column=col, value=rich_string)
```

**Where to put the helper** (CONTEXT D-14 Claude's Discretion item): planner chooses ONE of:
- (a) Inline closure inside web's `_build_main_sheet` (mirrors desktop's current style — keeps state-machine-specific `self._sanitize_for_excel` semantics; web swaps to module-level `sanitize_text_for_excel`).
- (b) Module-level helper in `shared_export_utils.py` (DRY across web+desktop). Recommended — both apps reference it and snippet semantics are pure (text in, openpyxl rich object out).
- (c) Helper inside `shared/export_dossier.py` — REJECT: dossier sub-sheets are plain text per D-14, so the rich-text helper has no business living next to the dossier projections.

**Recommended (b):** `shared_export_utils.build_rich_snippet_cell(text, sanitize_fn=None) -> Union[str, CellRichText]`. Both apps call it identically. Sanitize callback lets desktop pass `self._sanitize_for_excel` and web pass the module-level `sanitize_text_for_excel`.

#### Pattern 2.5 — Sub-sheet build pattern (Manuscripts + Bibliography)

**Copy from:** `web/export_service.py:669-700` (`export_parallels_excel`'s inner `add_results(...)` helper — exact "inner helper iterates result list, appends rows with per-cell alignment" structure)

```python
# web/export_service.py:669-700
def add_results(results: List[Dict], start_idx: int, is_filtered: bool) -> int:
    for idx, item in enumerate(results, start_idx):
        shelfmark, title, _library_code, library_name = _resolve_result_display(item, self.meta_mgr)
        source_ctx = clean_text_single_line(remove_highlight_markers(item.get('source_ctx', '')))
        ms_text = clean_text_single_line(remove_highlight_markers(item.get('text', '')))
        row = [
            idx,
            sanitize_text_for_excel(shelfmark),
            sanitize_text_for_excel(library_name),
            sanitize_text_for_excel(title),
            item.get('score', 0),
            sanitize_text_for_excel(source_ctx),
            sanitize_text_for_excel(ms_text),
            'Yes' if is_filtered else '',
        ]
        ws.append(row)
        current_row = ws.max_row
        ws.cell(row=current_row, column=1).alignment = center_align
        ws.cell(row=current_row, column=2).alignment = rtl_align
        # ... per-cell alignment ...
```

**Apply to:**

**`_build_manuscripts_sheet(ws, results, meta_resolver, lang)`:**

```python
# D-12: ordered set of sys_ids (first-occurrence order)
seen = set()
unique_sys_ids = []
for res in results:
    sid = (res.get('display') or {}).get('id') or res.get('sys_id') or ''
    if sid and sid not in seen:
        seen.add(sid)
        unique_sys_ids.append(sid)
# Build rows via shared helper
for sid in unique_sys_ids:
    row = build_manuscript_row(sid, meta_resolver, lang=lang)  # 14 cells per CONTEXT D-08
    ws.append([sanitize_text_for_excel(str(v) if v is not None else '') for v in row])
    # ... per-cell alignment ...
```

**`_build_bibliography_sheet(ws, unique_sys_ids, meta_resolver)`:**

```python
for sid in unique_sys_ids:
    bib_rows = build_bibliography_rows(sid, meta_resolver)  # List[List[Any]], 0..N rows
    for row in bib_rows:
        ws.append([sanitize_text_for_excel(str(v) if v is not None else '') for v in row])
        # ... per-cell alignment ...
```

#### Pattern 2.6 — Filename + save (REUSE existing — unchanged)

**Copy from:** `web/export_service.py:502-503`

```python
filename = make_safe_filename(search_query) + ".xlsx"
return save_workbook_to_bytes(wb), filename
```

**Apply to:** Function tail — unchanged. The `(bytes, filename)` return shape is preserved.

---

### 3. MODIFY `shared/search_serializer.py:298 _serialize_item`

#### Pattern 3.1 — Additive boolean key precedent

**Copy from:** `shared/search_serializer.py:355-360` (the `is_synthetic` Phase 85 SYNTH-06 additive precedent in the SAME function)

```python
# shared/search_serializer.py:355-360 — the canonical "add one bool key" template
# Phase 85 SYNTH-06 / D-14 — top-level (NOT nested under locator per A3)
# additive boolean flag: True iff sys_id is a Phase-85 synthetic 18-digit
# "99 + InventoryId.zfill(10) + 000000" identifier. Skill consumers branch
# on this for browse-honesty annotations (no NLI metadata for synthetic).
# Schema version stays 1 (additive change per Phase 83 stability commitment).
'is_synthetic': is_synthetic_sys_id(final_sys_id),
```

**Apply to:** Add `is_printed` and `has_pgp` in the return dict at `shared/search_serializer.py:348-372`. Match the comment style (Phase 94 EXPORT-META-XX additive flag, schema version stays 1). Per CONTEXT D-06: BOTH fields are always boolean (never None) — `bool(sys_id and sys_id in printed_set)` ensures false when sys_id is empty.

#### Pattern 3.2 — Plumb the two id-sets through `_serialize_item` + `serialize_search_payload`

**Copy from:** `shared/search_serializer.py:232-248` (`_serialize_item` keyword-only signature) and `:419-456` (`serialize_search_payload` kwarg threading)

```python
# shared/search_serializer.py:232-238 — kwargs-only contract
def _serialize_item(
    result: dict,
    *,
    meta_mgr: Any,
    domain_batch: dict[str, list[dict]],
    catalog_batch: dict[str, dict],
) -> dict:
```

**Apply to:** Add two new kwargs `transcription_sys_ids: set[str]` and `printed_sys_ids: set[str]` (defaults `set()`). The `domains` key is already populated at `:327-331` from `domain_batch` — verify (per CONTEXT D-01) it's already on the envelope at `:365`. NO new lookup — both sets come from the caller (web/api.py:export_json passes through from `session_payload`).

**`serialize_search_payload` thread-through** (the calling function): add 2 kwargs and pass them into the `_serialize_item(...)` call in the list comprehension at `:458-466`. Existing precedent for additive kwargs: the `request_echo` parameter at `:431` was added Phase 81A as an optional additive kwarg in EXACTLY this style.

#### Pattern 3.3 — Parallels D-10 regression test (NEGATIVE assertion)

**Copy from:** No analog — this is a regression-test pattern not present yet.

**Apply to:** Wave 2 adds `tests/test_parallels_envelope_no_pgp_keys.py` asserting `_to_parallels_envelope_item` output does NOT contain `has_pgp` / `is_printed` keys. The function at `shared/search_serializer.py` (search for `_to_parallels_envelope_item`) wraps a synthetic dict and feeds `_serialize_item`; the test pins the contract that parallels JSON envelope is NOT inheriting the search-side additions.

---

### 4. MODIFY `web/export_state.py:set_search_export`

#### Pattern 4.1 — Kwarg signature extension (additive)

**Copy from:** `web/export_state.py:461-491` (current signature, the function to extend)

```python
# web/export_state.py:461-491 (current)
def set_search_export(
    results: List[Dict[str, Any]],
    query: str,
    mode: str = 'text',
    gap: Optional[int] = None,
    filters: Optional[Dict[str, Any]] = None,
    warnings: Optional[List[str]] = None,
    selected_uids: Optional[List[str]] = None,
) -> None:
    capped, truncated, original, _changed = _compact_results(
        results, _compact_search_result_row,
    )
    safe_user_set(_SEARCH_KEY, {
        'results': capped,
        'query': query,
        'mode': mode,
        'gap': gap,
        'filters': filters,
        'warnings': warnings or [],
        'selected_uids': selected_uids,
        'truncated': truncated,
        'total_count': original,
    })
```

**Apply to:** Add 3 new optional kwargs `transcription_sys_ids`, `printed_ids`, `result_domains`. Per CONTEXT D-06 the defaults are empty containers (`[]` / `[]` / `{}`), NOT None. Cast sets to sorted lists for JSON-safety (NiceGUI storage round-trips through JSON):

```python
# Pattern signature
def set_search_export(
    results, query, mode='text', gap=None, filters=None, warnings=None,
    selected_uids=None,
    # NEW per CONTEXT D-08 Wave 2:
    transcription_sys_ids: Optional[Iterable[str]] = None,
    printed_ids: Optional[Iterable[str]] = None,
    result_domains: Optional[Dict[str, List[str]]] = None,
) -> None:
    # ... existing capped/truncated logic ...
    safe_user_set(_SEARCH_KEY, {
        # ... existing keys ...
        'transcription_sys_ids': sorted(set(transcription_sys_ids or [])),
        'printed_ids': sorted(set(printed_ids or [])),
        'result_domains': dict(result_domains or {}),
    })
```

#### Pattern 4.2 — New sibling helper `update_search_export_enrichment`

**Copy from:** `web/export_state.py:511-528 update_search_export_results` and `:531-538 update_search_export_selection` (existing sibling helpers — the convention)

```python
# web/export_state.py:531-538 — the cleaner pattern (selection is small primitives)
def update_search_export_selection(selected_uids: Optional[List[str]]) -> None:
    """Patch only the ``selected_uids`` field (per-row checkbox sync)."""
    payload = safe_user_get(_SEARCH_KEY, None)
    if not isinstance(payload, dict):
        return
    payload = dict(payload)
    payload['selected_uids'] = selected_uids
    safe_user_set(_SEARCH_KEY, payload)
```

**Apply to:** New helper `update_search_export_enrichment` patches only the 3 enrichment fields. The pattern enforces:

- D-11 isinstance guard (line `:518`, `:533`) — `if not isinstance(payload, dict): return` — silent no-op when no payload exists (defends prune-race + new-search-not-yet-set).
- D-12 copy-on-update (line `:520`, `:536`) — `payload = dict(payload)` — defends against shared-reference races.
- Accept sets OR lists (cast internally) — both are valid live-UI shapes.

```python
def update_search_export_enrichment(
    transcription_sys_ids: Optional[Iterable[str]] = None,
    printed_ids: Optional[Iterable[str]] = None,
    result_domains: Optional[Dict[str, List[str]]] = None,
) -> None:
    payload = safe_user_get(_SEARCH_KEY, None)
    if not isinstance(payload, dict):
        return
    payload = dict(payload)
    if transcription_sys_ids is not None:
        payload['transcription_sys_ids'] = sorted(set(transcription_sys_ids))
    if printed_ids is not None:
        payload['printed_ids'] = sorted(set(printed_ids))
    if result_domains is not None:
        payload['result_domains'] = dict(result_domains)
    safe_user_set(_SEARCH_KEY, payload)
```

**Naming convention rationale:** Matches the existing `update_search_export_results` / `update_search_export_selection` sibling names — the verb is `update_search_export_<what>`.

#### Pattern 4.3 — Test fixture pattern (Wave 2 unit tests)

**Copy from:** `tests/test_export_state_selection.py:29-73` (instance-isolated SimpleNamespace stub per Phase 88 D-02 Refinement 6)

```python
# tests/test_export_state_selection.py:35-41
def _make_stub(initial_storage: dict):
    """Instance-isolated stub mirroring app.storage.user surface."""
    return SimpleNamespace(storage=SimpleNamespace(user=initial_storage))
```

**Apply to:** Wave 2 tests monkeypatch `web.safe_storage.app` to a SimpleNamespace stub with a fresh `dict()` per test. Tests then call `set_search_export(..., transcription_sys_ids={'a','b'})` and assert `get_search_export()['transcription_sys_ids'] == ['a','b']` (sorted-list cast verification).

---

### 5. MODIFY `web/pages/search.py` — 3 set_search_export call sites + 1 new update site

#### Pattern 5.1 — Pass new kwargs at all 3 existing call sites

**Copy from / location of analog calls:**

- `web/pages/search.py:3903-3911` (history-restore site)
- `web/pages/search.py:4223-4231` (partial-results site)
- `web/pages/search.py:4315-4323` (initial-completion site)

Existing site shape (the 3 are near-identical):

```python
# web/pages/search.py:4315-4323
from web.export_state import set_search_export
set_search_export(
    results=results,
    query=clean_query,
    mode=mode,
    gap=_current_search_gap,
    filters=_last_filters_applied,
    warnings=_last_search_warnings,
    selected_uids=None,
)
```

**Apply to:** Append 3 new kwargs at each call site. Initial values per CONTEXT D-08 / SUPERSEDED-v2 plan signal (these 3 sites all run BEFORE the post-enrichment block populates the real values):

```python
set_search_export(
    results=results, query=clean_query, mode=mode,
    gap=_current_search_gap, filters=_last_filters_applied,
    warnings=_last_search_warnings, selected_uids=None,
    # NEW (initial empty — enrichment patches via update_search_export_enrichment):
    transcription_sys_ids=set(),
    printed_ids=set(),
    result_domains={},
)
```

#### Pattern 5.2 — New `update_search_export_enrichment` call site

**Copy from:** `web/pages/search.py:2150-2152` (the existing `update_search_export_selection` post-write site at the checkbox-handler — the closest analog of "compute new value in search_state, then patch the export payload immediately")

```python
# web/pages/search.py:2150-2152
_selected_uids = compute_selected_uids(search_state)
from web.export_state import update_search_export_selection
update_search_export_selection(_selected_uids)
```

**Apply to:** Insert a call after the post-enrichment block at `web/pages/search.py:4619-4624` (where Stage-1 enrichment populates `search_state.transcription_sys_ids`, `search_state.printed_ids`, and `search_state.result_domains` via `_process_domain_data`). The Stage-2 background-chunk loop at `:4661-4664` does the SAME mutations — needs the same patch call.

```python
# After line :4624 (Stage 1) and after line :4666 (Stage 2 — inside the chunk loop's commit block):
from web.export_state import update_search_export_enrichment
update_search_export_enrichment(
    transcription_sys_ids=search_state.transcription_sys_ids,
    printed_ids=search_state.printed_ids,
    result_domains=search_state.result_domains,
)
```

The Stage-2 site updates inside the loop (or outside if planner prefers fewer storage writes — Claude's Discretion). Empty-payload defense is in the helper (D-11 isinstance guard) so calling before any `set_search_export` is a silent no-op.

---

### 6. MODIFY `web/api.py:2069 export_excel` + `:2212 export_json`

#### Pattern 6.1 — Read new payload kwargs from session and pass through

**Copy from:** `web/api.py:2078-2104` (the existing `export_excel` shape — Phase 88 session-read pattern) and `:2226-2261` (the existing `export_json` shape — explicit kwarg-by-kwarg propagation into the serializer)

```python
# web/api.py:2078-2104 (current — to be extended)
from web.export_state import get_search_export
payload = get_search_export()
if not payload or not payload.get('results'):
    return Response("No results to export", status_code=400)

all_results = payload['results']
query = payload.get('query') or ''
# ... selected_uids filtering ...
try:
    export_svc = get_export_service(state.meta_mgr)
    content, filename = export_svc.export_search_results_excel(_results, query)
```

**Apply to (`export_excel`):**

```python
# Extract new payload fields
transcription_sys_ids = set(payload.get('transcription_sys_ids') or [])
printed_ids = set(payload.get('printed_ids') or [])
result_domains = payload.get('result_domains') or {}

# Pass into the export pipeline (planner extends export_search_results_excel signature)
content, filename = export_svc.export_search_results_excel(
    _results, query,
    transcription_sys_ids=transcription_sys_ids,
    printed_ids=printed_ids,
    result_domains=result_domains,
)
```

**Apply to (`export_json`):** Per CONTEXT D-11 the JSON envelope keys are UNCHANGED at the top level — the 3 new per-item flags (`is_printed`, `has_pgp`, `domains`) live INSIDE each result via `_serialize_item`. So `export_json` reads `transcription_sys_ids` / `printed_ids` from the session payload and passes them as kwargs into `serialize_search_payload(...)`:

```python
# web/api.py:2240-2248 — extended
payload = serialize_search_payload(
    _results,
    meta_mgr=state.meta_mgr,
    query=session_payload.get('query') or '',
    mode=session_payload.get('mode') or 'text',
    gap=session_payload.get('gap'),
    filters=session_payload.get('filters'),
    warnings=session_payload.get('warnings') or [],
    # NEW Phase 94 per CONTEXT D-11 (envelope keys unchanged; only per-item flags added):
    transcription_sys_ids=set(session_payload.get('transcription_sys_ids') or []),
    printed_sys_ids=set(session_payload.get('printed_ids') or []),
)
```

The `result_domains` data is NOT needed in JSON — `_serialize_item` already populates `domains` from the batched FJMS lookup at `:330-331`. CONTEXT D-01 verified this. Only `transcription_sys_ids` + `printed_ids` thread through.

---

### 7. MODIFY `genizah_app.py:17895 def export_results(self, fmt='xlsx')`

#### Pattern 7.1 — Read desktop state directly (no export_state.py-equivalent)

**Copy from:** Existing direct-attribute reads in this function (`self.last_search_query` at `:17900`, `self.results_table` at `:17921`, `self._collect_sorted_results()` at `:17936`).

```python
# genizah_app.py:17895-17936 (current — state reads pattern)
def export_results(self, fmt='xlsx'):
    base_path = self._default_report_path(self.last_search_query, tr("Search_Results"))
    # ...
    results_to_export = self._collect_sorted_results()
```

**Apply to:** Read the 3 enrichment signals directly at the call site (NO desktop equivalent of `export_state.py`):

```python
# At entry of xlsx branch:
trans_ids = self._pgp_transcription_sys_ids  # :2547
printed_ids = self._printed_sys_ids  # :2550
result_domains = self._result_domain_map  # :5461 (already sys_id -> list of domain names)
lang = CURRENT_LANG  # from genizah_core (the canonical desktop locale)
rtl = (lang == 'he')
```

#### Pattern 7.2 — Restructure xlsx branch into 3-sheet builder

**Copy from:** `genizah_app.py:17984-18067` (current xlsx branch — the WHOLE structure)

```python
# genizah_app.py:17984-18067 (current xlsx branch — to be RESTRUCTURED)
if fmt == 'xlsx':
    try:
        import openpyxl
        from openpyxl.styles import Font, PatternFill
        from openpyxl.cell.rich_text import TextBlock, CellRichText
        from openpyxl.cell.text import InlineFont
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = tr("Search Results")
        ws.sheet_view.rightToLeft = True   # CHANGE: conditional on lang per D-04
        # ... write_rich_cell inner helper (extract per D-14) ...
        # ... credit + search-info header rows ...
        # ... data_rows iteration with col 7 = write_rich_cell ...
        # ... column widths ...
        wb.save(path)
```

**Apply to:** Restructure into a 3-sheet builder consuming `shared/export_dossier.py`:

```python
if fmt == 'xlsx':
    import openpyxl
    from openpyxl.styles import Font, PatternFill
    from shared.export_dossier import (
        MANUSCRIPT_HEADERS, BIBLIOGRAPHY_HEADERS,
        build_manuscript_row, build_bibliography_rows,
    )

    wb = openpyxl.Workbook()
    ws_main = wb.active
    ws_main.title = tr("Search Results")
    ws_main.sheet_view.rightToLeft = rtl  # D-04 conditional
    ws_manu = wb.create_sheet(title="Manuscripts")  # D-03 — locked English sheet name
    ws_manu.sheet_view.rightToLeft = rtl
    ws_bib = wb.create_sheet(title="Bibliography")
    ws_bib.sheet_view.rightToLeft = rtl

    # Desktop meta_resolver — adapter to self.meta_mgr
    def meta_resolver(sid):
        if not sid: return None
        shelf, title = self.meta_mgr.get_meta_for_id(sid)
        lib_code = self.meta_mgr.get_library_for_id(sid) or ''
        lib_name = get_library_display(lib_code, short=False, lang='en') if lib_code else ''
        return {'shelfmark': shelf, 'title': title, 'library_code': lib_code, 'library_name': lib_name}

    # Main sheet: existing rows + 4 new cols + Full Text col
    # write_rich_cell helper extracted per D-14 (planner picks (a)/(b)/(c) per Pattern 2.4)
    # ... credit/search-info header rows (preserve current behavior) ...
    # ... per CONTEXT D-01 12-col main sheet headers ...
    # ... iterate data_rows with new columns ...

    # Manuscripts + Bibliography sheets
    seen = set()
    unique_sys_ids = []
    for r in results_to_export:
        sid = (r.get('display') or {}).get('id') or ''
        if sid and sid not in seen:
            seen.add(sid)
            unique_sys_ids.append(sid)

    style_excel_header_desktop(ws_manu, [tr(h) for h in MANUSCRIPT_HEADERS] if False else list(MANUSCRIPT_HEADERS))
    # Headers stay English per CONTEXT D-04 — do NOT translate sheet/column names except via the lang param to row builder
    for sid in unique_sys_ids:
        row = build_manuscript_row(sid, meta_resolver, lang=lang)
        ws_manu.append([self._sanitize_for_excel(str(v) if v is not None else '') for v in row])

    style_excel_header_desktop(ws_bib, list(BIBLIOGRAPHY_HEADERS))
    for sid in unique_sys_ids:
        for row in build_bibliography_rows(sid, meta_resolver):
            ws_bib.append([self._sanitize_for_excel(str(v) if v is not None else '') for v in row])

    wb.active = wb.index(ws_main)  # D-03 default-active
    wb.save(path)
```

**Note on Snippet column rendering:** Desktop's existing `write_rich_cell` inner function at `:18000` is the CANONICAL pattern web adopts (per D-14). Whether to extract it into `shared_export_utils` is Pattern 2.4 Claude's Discretion. Either way the desktop xlsx branch keeps using its current rendering for the Snippet column — no behavior change on desktop's snippet rendering.

#### Pattern 7.3 — Full Text column source on desktop

**CONTEXT Claude's Discretion item** (D-01): "Exact field on desktop result dict for `Full Text` — planner verifies what desktop's `_collect_sorted_results()` exposes."

**Search analog:** The desktop result dict shape comes from `genizah_core.SearchEngine.execute_search` / `lab_composition_search` — both populate `r['display']` and `r['raw_file_hl']` (snippet), plus `r.get('full_text', '')` is the common Tantivy-indexed text field reused by web. Verification needed by planner via `genizah_core.py` search engine — quick `Grep` for `'full_text':` in `genizah_core.py` should resolve.

Lightweight pre-verification (from `web/export_service.py:55-73 _resolve_result_full_text`): the same result dict shape is shared between apps. Desktop result rows DO carry `full_text` when produced via the standard search path. Planner reads the field via `r.get('full_text') or r.get('full_text_excerpt') or ''` to mirror web's resolver fallback chain.

---

## Shared Patterns

### Shared Pattern A — Service-call exception resilience (D-A2)

**Source:** `shared/browse_service.py:155-243` (3 sibling `_pgp_sync` / `_fjms_sync` / `_nli_sync` helpers — the analog) BUT note the analog deliberately propagates exceptions to `_wrap_with_timeout` at `:250-270`.

**Apply to:** All 4 lookup helpers in `shared/export_dossier.py`. Since the dossier module has NO outer timeout wrapper (perf-acceptable per CONTEXT D-09), each helper catches inline:

```python
def pgp_subset_for_sys_id(sys_id: str) -> Optional[dict]:
    if not sys_id:
        return None
    try:
        from shared.document_service import get_document_for_fragment
        doc = get_document_for_fragment(sys_id)
        if not doc:
            return None
        return {
            'pgp_url':       doc.get('pgp_url'),
            'description':   doc.get('description'),
            # ... narrow projection ...
        }
    except Exception as e:
        logger.warning("pgp_subset_for_sys_id(%s) failed: %s", sys_id, e)
        return None
```

### Shared Pattern B — Logger import + naming

**Source:** All shared/ modules — `shared/document_service.py:18`, `shared/fjms_service.py`, `shared/browse_service.py`

```python
# Top of every shared/ module
import logging
logger = logging.getLogger(__name__)
```

**Apply to:** Top of `shared/export_dossier.py`.

### Shared Pattern C — Boolean rendering convention

**Source:** `web/export_service.py:689` (`'Yes' if is_filtered else ''`) — the project-wide convention for boolean cells

**Apply to:** All NEW boolean columns (`Has PGP`, `Is Printed`). Per CONTEXT D-06: `"Yes"` or EMPTY cell — NOT `"True"`/`"False"`, NOT `"No"`, NOT `"N/A"`.

```python
'Yes' if sys_id in transcription_sys_ids else ''
'Yes' if sys_id in printed_ids else ''
```

### Shared Pattern D — Pipe-joined multi-value cells (D-05)

**Source:** None directly; CONTEXT-locked decision. Closest analog: `web/export_service.py` does NOT currently pipe-join (lists are not currently exported). The pattern is new but trivially-shaped.

**Apply to:** `Domains` column on main sheet; `Languages` / `Tags` columns on Manuscripts sheet.

```python
'|'.join(domains) if domains else ''  # NO surrounding spaces per D-05
```

### Shared Pattern E — sanitize_text_for_excel everywhere on cell values

**Source:** `shared_export_utils.py:19-61` (module-level helper) + `web/export_service.py:472-479` (per-row application) + `genizah_app.py:18001` (`self._sanitize_for_excel = shared_sanitize_excel`)

**Apply to:** EVERY string-typed cell value in all 3 sheets on both apps. Numeric / boolean cells skip sanitization. The helper handles XML 1.0 control-char stripping + formula-injection prefix + Excel 32,767-char cell limit.

### Shared Pattern F — D-04 English-only library name resolution

**Source:** `shared/search_serializer.py:325` (the public JSON envelope's `_safe_library_name` — hard-pinned English) and `genizah_core.get_library_display(library_code, short=False, lang='en')`

**Apply to:** All 3 sheets, both apps. NEVER route through `web.translations.get_language()` for library names on the dossier path — that would translate based on UI lang and break D-04. Use:

```python
from genizah_core import get_library_display
library_name = get_library_display(lib_code, short=False, lang='en') or lib_code
```

The web's instance-method `web/export_service.py:399-408` IS the LEGACY UI-lang-aware variant — DO NOT use that one inside `_build_main_sheet` or `_build_manuscripts_sheet`.

---

## No Analog Found

| File / Concern | Why no analog |
|---|---|
| `IIIF Manifest` per-row main-sheet column (D-13 soft scope) | No existing main-sheet column reads from `nli_crossref_service.get_folio_images()`. CONTEXT D-13 explicitly notes the alternative is to defer to the Manuscripts sub-sheet via `library_viewer_url` — Claude's Discretion. Planner picks at Wave 3. |
| Per-task task-scoped resolver caching for repeated meta_mgr calls inside `build_manuscript_row` | No analog — Wave 1 ships per-sys_id calls; if smoke testing reveals latency, Wave 1 follow-up can add a `services=None` prefetch-map kwarg per Codex SHOULD-FIX 5. |
| Wave 2 parallels D-10 regression test | No existing negative-assertion test for the parallels envelope shape. New test file `tests/test_parallels_envelope_no_pgp_keys.py` per Pattern 3.3. |

---

## Metadata

**Analog search scope (read-only verification):**
- `shared/document_service.py:140-207, 950-973`
- `shared/fjms_service.py:2389, 2435-2524, 2531-2600, 3413-3422`
- `shared/nli_crossref_service.py:255, 448-499, 727-751, 790, 1019`
- `shared/browse_service.py:120-243, 250-270`
- `shared/search_serializer.py:200-485, 523-536`
- `shared_export_utils.py:1-96`
- `web/export_service.py:1-870 (full module)`
- `web/export_state.py:1-622 (full module)`
- `web/safe_storage.py:1-50`
- `web/translations.py:1-50`
- `web/api.py:2069-2310 (export_excel/word/json + parallels)`
- `web/pages/search.py:2148-2155 (selection update site), :3895-3911 (history-restore), :4216-4250 (partial-results), :4305-4325 (initial-completion), :4480-4670 (post-render enrichment)`
- `genizah_core.py:1820 (get_library_display), :2479-2511 (locale module-level), :4915-4931 (get_display_data — shared {img,source,id,...} shape)`
- `genizah_app.py:2540-2565 (state init), :5455-5466 (_result_domain_map init), :17843-17872 (sanitize), :17895-18067 (export_results)`
- `tests/test_export_state_selection.py:1-90 (test fixture pattern)`
- `tests/test_export_state_cap.py`, `tests/test_search_serializer.py` (sibling test patterns)

**Files scanned:** 17 production modules + 3 test modules + Wave-1/Wave-2 superseded plans (signal-only).

**Pattern extraction date:** 2026-05-19.

**Wave/plan-mapping suggestion to planner** (per CONTEXT D-15 4-wave structure):

- Wave 1 plan → Patterns 1.1-1.9 (all `shared/export_dossier.py`).
- Wave 2 plan → Patterns 3.1-3.3, 4.1-4.3 (`shared/search_serializer.py` + `web/export_state.py` + `web/pages/search.py` plumbing + parallels D-10 regression).
- Wave 3 plan → Patterns 2.1-2.6, 6.1, Shared C-F (web xlsx restructure + api.py kwarg propagation).
- Wave 4 plan → Patterns 7.1-7.3 (desktop xlsx parity + 3-sheet consumption of `shared/export_dossier.py`).
