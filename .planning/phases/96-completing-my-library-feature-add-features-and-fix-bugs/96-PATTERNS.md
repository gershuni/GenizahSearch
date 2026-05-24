# Phase 96: Completing My Library — Pattern Map

**Mapped:** 2026-05-24
**Files analyzed:** 6 modified files + 7 new test files + 1 new fixture
**Analogs found:** 13 / 14 (93% match coverage)

> Consumed by `gsd-planner`. Each row gives the planner the exact analog file + line range, an excerpt of the load-bearing pattern, "mirror this" guidance, and explicit divergences. Phase 96 is largely a **self-analog** phase: most patterns are extensions of Phase 95 code (`_apply_local_filter`, `_open_local_browse`, `_build_local_result_dict`, `_query_local_index`).
>
> **Key CONTEXT.md revision:** D-08 was REVISED 2026-05-24 — per-file opt-out persistence uses **session JSON** (`shared/session_persistence.py`), NOT QSettings. All analogs in this map point to the session-JSON pattern.

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `genizah_core.py` (modify) | search engine extension | request-response | self-analog `_build_local_result_dict` @ :6876-6907; `highlight` @ :7341-7366; Genizah `format_snippet` @ :6979-6999 and Genizah result-emit @ :7819-7820 / :8342 | **exact (in-file extension)** |
| `genizah_core.py` (new `get_local_browse_page`) | new engine method | request-response | self-analog `get_browse_page` @ :9131-9226; `_get_local_full_text_for_sys_id` @ 18507-18552 (aggregation primitive) | **role-match (port + adapt)** |
| `shared/local_indexer.py` (modify `extract_pdf_pages`) | indexer extension | file-I/O | self-analog `extract_pdf_pages` @ :302-326; self-analog `_join_fragmented_lines` @ :123-142 (dead-code detection heuristic) | **exact (in-file extension)** |
| `desktop/my_library_tab.py` (modify) | desktop UI extension | QWidget + persistence | self-analog `_build_ui` @ :285-345 (QVBoxLayout, NOT QSplitter — RESEARCH §3 layout discrepancy); QListWidget/QTableWidget convenience widgets @ :293/:329 | **role-match (extend layout)** |
| `desktop/result_dialog.py` (NEW-1 button removal) | UI widget delete | request-response | self-analog `btn_rd_open_browse` declaration @ :339-352; `_rd_open_in_browse` handler @ :1922-1943; visibility logic @ :1995-2014 | **exact (delete in-file)** |
| `desktop/result_dialog.py` (NEW-2 LOCAL nav) | UI dispatch | request-response | self-analog `load_page` @ :2169-2199 (Genizah nav primitive); `btn_compact_pg_prev/next` wiring @ :120/:130; `spin_page` @ :237 | **exact (extend dispatch)** |
| `genizah_app.py` (D-F1 cascade) | filter composition | request-response | self-analog `_apply_local_filter` @ :17321-17344; cascade joinpoints `_apply_results_table_filters` @ :17469-17478 and `_apply_comp_tree_filters` @ :17792-17811 | **exact (in-file extension)** |
| `genizah_app.py` (D-F1 session save/restore) | persistence | request-response | self-analog `_save_session` @ :23532-23613 (esp. lines 23553-23576 for LOCAL filter keys); `_restore_session` @ :23623-23704 (esp. lines 23702-23713 for LOCAL key restore) | **exact (extend keys block)** |
| `genizah_app.py` (NEW-2 View All separator) | text aggregation | transform | self-analog `_get_local_full_text_for_sys_id` @ :18507-18552 (specifically the `"\n\n".join(...)` at :18552); `_open_local_browse` @ :18554-18628 | **exact (extend join logic)** |
| `tests/fixtures/local_indexer/single_word_per_line.pdf` | new fixture | static data | RESEARCH §2 says **does NOT exist** despite CONTEXT/OPEN_ISSUES claiming otherwise — must be created in Wave 0 | **NO ANALOG** |
| `tests/test_local_hit_highlighting.py` (NEW) | unit test | static + behavioural | `tests/test_local_post_dedup_merge.py` :21-47 (engine instantiation stub) | **role-match** |
| `tests/test_local_pdf_extraction_fallback.py` (NEW) | unit test | I/O | `tests/test_local_indexer.py` :1-90 (extraction fixture pattern) | **exact (template)** |
| `tests/test_local_optout_persistence.py` (NEW) | unit test | round-trip | `tests/test_local_filter_persistence.py` :17-100 (session-dict round-trip — verbatim template) | **exact (template)** |
| `tests/test_local_optout_filter.py` (NEW) | unit test | composition | `tests/test_local_filter_cascade.py` :85-138 (`_Stub` pattern + filter unit test) | **exact (template)** |
| `tests/test_local_nav_page_chunk.py` (NEW) | unit test | engine call | `tests/test_local_post_dedup_merge.py` :21-47 + `tests/test_local_browse_panel.py` :87-107 | **role-match** |
| `tests/test_result_dialog_local_button_removed.py` (NEW) | static AST guard | static | `tests/test_local_filter_cascade.py` :39-72 (AST function walker — verbatim template); negated assertion form | **exact (template, negated)** |
| `tests/test_local_filter_cascade.py` (extend) | static AST guard | static | self-analog same file `test_local_filter_applied_within_results_cascade` @ :39-72 | **exact (in-file extension)** |
| `tests/test_local_browse_panel.py` (update) | static AST guard | static | self-analog same file `test_result_dialog_has_view_in_browse_button` @ :114-122 — **must flip to NEGATIVE assertion** for NEW-1 | **exact (in-file inversion)** |

---

## Pattern Assignments

Each section: read the analog at the cited file/line, copy that shape, apply the listed divergences.

---

### D-F5 — LOCAL Hit Dict Shape with Highlight Pattern

**Primary analog (Genizah result emit):** `genizah_core.py:7813-7846` — Genizah hits produce `snippet` with `*…*` markers and the `highlight_pattern` field via `self.highlight(content, regex, ...)`.

**Genizah hit emit excerpt** (lines 7813-7846):
```python
# Use standard highlight helpers with the match span
span = match_obj.span()
scope_list = self._get_field(doc, 'scope', ['page']) or ['page']
scope = scope_list[0]
boundaries = self._parse_boundaries(doc) if scope != 'page' else []

hl_c = self.highlight(content, regex, False)
hl_f = self.highlight(content, regex, True)

# ... later, append result with highlight_pattern stored:
results.append({
    'display': meta,
    'snippet': hl_c or "",
    'full_text': content,
    'uid': primary.get('uid') or doc['unique_id'][0],
    'raw_header': display_header,
    'raw_file_hl': hl_f or "",
    # 'highlight_pattern': pattern_str  (set at the simpler call site below)
})
```

**Simpler Genizah call site that stamps `highlight_pattern`** (lines 8340-8345):
```python
'display': meta, 'snippet': hl_c, 'full_text': content,
'uid': doc['unique_id'][0], 'raw_header': doc['full_header'][0],
'raw_file_hl': hl_f, 'highlight_pattern': pattern_str,
'scope': scope,
'score': float(score),
```

**The `highlight` helper** (`genizah_core.py:7341-7366`):
```python
def highlight(self, text, regex, for_file=False):
    m = regex.search(text)
    if not m: return None
    s, e = m.span()
    start = max(0, s - 60)
    end = min(len(text), e + 60)

    # Calculate indices relative to snippet
    rel_s = s - start
    rel_e = e - start

    # Grab raw snippet
    snippet = text[start:end]

    # Sanitize snippet to prevent interference with markers (replace with space to keep indices)
    snippet_safe = snippet.replace('*', ' ')

    # Insert Asterisks for Unified Highlighting
    hl_snippet = snippet_safe[:rel_s] + f"*{snippet_safe[rel_s:rel_e]}*" + snippet_safe[rel_s:rel_e].__class__.__name__ and snippet_safe[rel_e:]
    # (NB: above one-liner is illustrative — the real code is the literal at lines 7359-7363)
    if not for_file:
        return hl_snippet.replace('\n', ' ‖ ')
    return hl_snippet
```

**The format_snippet renderer** (`genizah_core.py:6979-6999`):
```python
@staticmethod
def format_snippet(text, style='html_class'):
    if not text:
        return ""
    # First escape HTML to prevent XSS
    escaped = html.escape(text)
    # Style ‖ line-break indicators
    escaped = escaped.replace('‖', '<span class="line-break-sep">‖</span>' if style == 'html_class'
                              else '<span style="color:#888; font-weight:bold;">‖</span>')
    # Convert *word* to highlighted span (after escaping, markers are safe)
    if style == 'html_class':
        return re.sub(r'\*(.*?)\*', r'<span class="highlight-match">\1</span>', escaped)
    else:
        return re.sub(r'\*(.*?)\*', r'<span style="color:#ff0000; font-weight:bold;">\1</span>', escaped)
```

**The current LOCAL hit (broken — root cause of D-F5):** `genizah_core.py:6876-6907`
```python
def _build_local_result_dict(self, doc, score) -> dict:
    unique_id = doc.get_first("unique_id") or ""
    full_header = doc.get_first("full_header") or ""
    content = doc.get_first("content") or ""
    shelfmark = doc.get_first("shelfmark") or ""
    # ... parse sys_id + p_num ...
    snippet = content[:200] if content else ""   # ← raw, no asterisks
    return {
        "uid": unique_id,
        "full_text": content,
        "snippet": snippet,                        # ← raw text — no *…* markers
        # ← MISSING: 'highlight_pattern'
        # ← MISSING: 'raw_file_hl'
        "sys_id": sys_id,
        "p_num": p_num,
        "score": float(score),
        "display": {
            "id": sys_id,
            "source": "LOCAL",
            "library_code": "LOCAL",
            "shelfmark": shelfmark,
        },
        "full_header": full_header,
    }
```

**ResultDialog uses `highlight_pattern` to apply markers on the fly** (`desktop/result_dialog.py:2051-2063`):
```python
ms_raw = data.get('full_text', '') or data.get('text', '')
pattern_str = data.get('highlight_pattern')  # ← LOCAL hits don't set this today

if pattern_str:
    try:
        flags = re.IGNORECASE
        if '\\n' in pattern_str or pattern_str.startswith('^') or '^\\' in pattern_str:
            flags |= re.MULTILINE
        regex = re.compile(pattern_str, flags)
        ms_raw = regex.sub(r'*\g<0>*', ms_raw)  # ← branch SKIPPED for LOCAL
    except re.error:
        pass
```

**Search-table render path** (`genizah_app.py:16668-16675`):
```python
_snip = res.get('snippet', '')
if self.refinement_chain and _snip:
    _snip = enrich_snippet_with_chain_terms(_snip, self.refinement_chain, self.query_input.text())
html_snippet = self.render_asterisks_to_html(_snip)   # ← calls format_snippet
lbl = QLabel(html_snippet)
lbl.setProperty("filter_text", res.get('snippet', ''))
```

**LOCAL query path that must be extended to thread regex through** (`genizah_core.py:6848-6874`):
```python
def _query_local_index(self, query_str: str, mode: str, gap: int, limit=None):
    if self.local_searcher is None or self.local_index is None:
        return []
    try:
        tantivy_q = self.local_index.parse_query(
            query_str, ["content", "content_head", "content_tail"]
        )
        search_limit = limit or Config.SEARCH_LIMIT
        res_obj = self.local_searcher.search(tantivy_q, search_limit)
        hits = res_obj.hits if hasattr(res_obj, "hits") else res_obj
        results = []
        for score, doc_address in hits:
            doc = self.local_searcher.doc(doc_address)
            results.append(self._build_local_result_dict(doc, score))  # ← must pass regex
        return results
    except Exception as e:
        LOGGER.warning("LOCAL index query failed: %r", e)
        return []
```

**Mirror this (Option A, RESEARCH §1 recommended):**
1. Compile the regex once in `_query_local_index` (using the same `query_str`, `mode`, `gap`) — OR accept a pre-compiled `regex` parameter from the caller in `genizah_core.py:8361-8363`.
2. Pass the compiled `regex` and `pattern_str` into `_build_local_result_dict(doc, score, regex, pattern_str)`.
3. Inside `_build_local_result_dict`, call `self.highlight(content, regex, for_file=False)` for `snippet` and `self.highlight(content, regex, for_file=True)` for `raw_file_hl`.
4. Add `'highlight_pattern': pattern_str` to the returned dict (same key Genizah uses at `genizah_core.py:8342`).
5. Defensive fallback when regex does not match `content` (Tantivy matched but regex didn't): set `snippet = content[:200]` and OMIT `highlight_pattern` (or set to empty) so the ResultDialog branch at `:2054` does not try to compile an empty pattern.

**Divergences from Genizah:**
- **Defensive fallback required (LOCAL-specific).** Genizah hits use the regex as the *filter* — if it didn't match, the hit was dropped before result-emit. LOCAL uses Tantivy `parse_query` without a regex re-filter, so the regex-didn't-match case must be handled explicitly (Genizah hits never hit this path).
- Do **NOT** introduce a `scope`/`boundaries` field for LOCAL — those are Genizah-specific multi-page span concepts.

---

### D-F4 — PDF Extraction Detect-then-Fallback

**Primary analog (current extractor):** `shared/local_indexer.py:302-326` — `extract_pdf_pages`.

**Current implementation:**
```python
def extract_pdf_pages(
    filepath: str,
) -> Iterator[tuple[int, str, str]]:
    """Extract text page-by-page using PyMuPDF (D-01 / D-03).

    Yields (page_num, text, title) - D-03 one-doc-per-page model.

    D-06: pages with < 10 chars after strip are skipped silently.
    D-05: caller must check total chars across all yielded pages; if < 50,
          file gets status='no_text_layer'.

    D-02: RTL helpers are NOT invoked in v1 (dead code).
    """
    doc = fitz.open(filepath)
    try:
        title = (doc.metadata or {}).get("title") or os.path.basename(filepath)
        for page_num, page in enumerate(doc, start=1):
            blocks = page.get_text("blocks")
            text_parts = [b[4].strip() for b in blocks if b[6] == 0 and b[4].strip()]
            text = "\n\n".join(text_parts)
            if len(text.strip()) < _EMPTY_PAGE_CHAR_THRESHOLD:
                continue  # D-06: skip empty pages
            yield page_num, text, title
    finally:
        doc.close()
```

**Detection-heuristic precedent (DEAD CODE — to be revived or duplicated):** `shared/local_indexer.py:123-142`
```python
def _join_fragmented_lines(text: str) -> str:  # pragma: no cover
    """Join pages where each word is on its own line. DEAD CODE per D-02."""
    lines = text.splitlines()
    non_empty = [line for line in lines if line.strip()]
    if len(non_empty) < 4:
        return text
    single = sum(1 for line in non_empty if len(line.split()) <= 1)
    if single / len(non_empty) < 0.60:
        return text
    paragraphs, current = [], []
    for line in lines:
        s = line.strip()
        if s:
            current.append(s)
        elif current:
            paragraphs.append(" ".join(current))
            current = []
    if current:
        paragraphs.append(" ".join(current))
    return "\n\n".join(paragraphs)
```

**Mirror this:**
1. Keep `get_text("blocks")` as primary (preserves all currently-working PDFs).
2. After block-extraction per page, run the detection heuristic: split on `\n`, count non-empty lines, compute `single_word_ratio = sum(1 for ln in lines if len(ln.split()) <= 1) / len(lines)`.
3. If `len(non_empty) < 5` (small-sample guard) → skip detection, accept blocks output.
4. If `single_word_ratio >= 0.70` (RESEARCH §2 — tightened vs Phase 95's 0.60 dead-code threshold) → trigger fallback: `page.get_text("text", sort=True)`.
5. The `sort=True` flag is **load-bearing** per PyMuPDF docs (RESEARCH §2 citations) — not just switching the mode.

**Divergences from `_join_fragmented_lines` (DEAD CODE pattern):**
- Phase 95's dead code uses 0.60 threshold; Phase 96 uses **0.70** per RESEARCH §2 tuning.
- Phase 95's helper rewrites the BAD text (joins lines); Phase 96 instead **re-extracts** via a different PyMuPDF mode (`get_text("text", sort=True)`). Cleaner separation.
- Planner choice (RESEARCH §2 final paragraph): (a) revive `_join_fragmented_lines` and use its detection as the trigger condition (reusing tested code but contradicting D-02 dead-code marker); or (b) write a fresh, smaller detection helper (preserves marker, duplicates ~10 lines of logic). Either is acceptable; prefer (b) for cleaner audit trail.

**Fixture caveat:** `tests/fixtures/local_indexer/single_word_per_line.pdf` **does NOT exist** despite CONTEXT/OPEN_ISSUES claims (RESEARCH §2 — verified via Glob + git log). Wave 0 must create it (user-supplied or synthetic via `fitz.Document` writing per-word `Tj` operators).

---

### D-F1 — Per-File Opt-Out: Session JSON Persistence (D-08 REVISED)

**Primary analog (session-JSON pattern for LOCAL filter state):** `genizah_app.py:23553-23576` (within `_save_session`).

**Excerpt** (lines 23553-23576):
```python
'local_filter': getattr(self, '_local_filter_state_search', 'all'),
'search_corpus_scope': getattr(self, '_search_corpus_scope', 'genizah'),
'printed_ids': sorted(getattr(self, '_printed_sys_ids', set())),
'excluded_sys_ids': sorted(getattr(self, 'excluded_sys_ids', set())),
'excluded_shelfmarks': sorted(getattr(self, 'excluded_shelfmarks', set())),
'excluded_raw_entries': getattr(self, 'excluded_raw_entries', []),
'exclusion_sources': serialize_sources(getattr(self, 'exclusion_sources', [])),
'results_filters': getattr(self, 'results_filters', {}),
# ...
'composition_search': {
    # ...
    'local_filter_composition': getattr(self, '_local_filter_state_composition', 'all'),
    'local_filter_parallels': getattr(self, '_local_filter_state_parallels', 'all'),
    'excluded_sys_ids': sorted(getattr(self, 'excluded_sys_ids', set())),
}
```

**Restore side** (`genizah_app.py:23702-23713`):
```python
self._domain_exclusions = set(reg.get('domain_exclusions', []))
self._printed_filter_state = reg.get('printed_filter', 'all')
# Phase 95 D-39 — restore LOCAL filter state per surface.
self._local_filter_state_search = reg.get('local_filter', 'all')
self._update_local_filter_btn_search()
# Phase 95-08 smoke-fix — restore corpus scope selector.
self._search_corpus_scope = reg.get('search_corpus_scope', 'genizah')
if hasattr(self, 'corpus_scope_combo'):
    _idx = self.corpus_scope_combo.findData(self._search_corpus_scope)
    if _idx >= 0:
        self.corpus_scope_combo.blockSignals(True)
        self.corpus_scope_combo.setCurrentIndex(_idx)
        self.corpus_scope_combo.blockSignals(False)
```

**Mirror this:**
1. Add a new key alongside the existing LOCAL filter keys. CONTEXT D-08 REVISED suggests `local_file_optouts` holding a list of canonical file paths (use `_canonical_filepath` from `shared/local_sys_id.py`).
2. Choose nesting: top-level (cross-surface, matches "all surfaces share the same opt-out set") OR inside `regular_search` (per-surface). Top-level is more aligned with D-F1 semantics ("a file is opted out everywhere").
3. Save in `_save_session` (line ~23553 block). Restore in `_restore_session` (line ~23702 block) with sensible default `[]`.
4. Storage type: **list of strings** (canonical paths). Use `sorted(...)` like other set-backed keys (e.g. `printed_ids`, `excluded_sys_ids`) for deterministic JSON output.

**Divergences from existing LOCAL filter keys:**
- Existing keys (`local_filter`, `local_filter_composition`, `local_filter_parallels`) are **per-surface** (search/composition/parallels are independent). The opt-out set is **single, cross-surface** — one source of truth shared by all three surfaces.
- Existing keys hold a 3-state enum (`'all' | 'only_local' | 'no_local'`). The opt-out set holds a sorted list of canonical paths.

---

### D-F1 — Cascade Composition (Filter Joinpoints)

**Primary analog (LOCAL three-state filter cascade):** `genizah_app.py:17321-17344` (`_apply_local_filter`).

**Existing filter** (lines 17321-17344):
```python
def _apply_local_filter(self, results, state):
    """Apply LOCAL three-state filter per D-10 / D-10 P1."""
    if state == 'all':
        self._local_filter_inactive_chip_visible = False
        return results
    has_local = any(
        (r.get('display', {}) or {}).get('source') == 'LOCAL'
        for r in results
    )
    if not has_local:
        # D-10 P1 NO-OP — preserve state but show inline chip.
        self._local_filter_inactive_chip_visible = True
        return results
    self._local_filter_inactive_chip_visible = False
    if state == 'only_local':
        return [r for r in results if (r.get('display', {}) or {}).get('source') == 'LOCAL']
    if state == 'no_local':
        return [r for r in results if (r.get('display', {}) or {}).get('source') != 'LOCAL']
    return results
```

**Cascade joinpoint A** (`genizah_app.py:17469-17478`, inside `_apply_results_table_filters`):
```python
# Phase 95 REQ-6 — LOCAL filter cascade joinpoint (search surface).
_local_state_search = getattr(self, '_local_filter_state_search', 'all')
_results_for_local = getattr(self, 'last_results', []) or []
_local_filtered = self._apply_local_filter(_results_for_local, _local_state_search)
_local_filter_active = _local_state_search != 'all'
_local_visible_sys_ids = {
    (r.get('display', {}) or {}).get('id') for r in _local_filtered
} if _local_filter_active and not self._local_filter_inactive_chip_visible else None
self._show_local_filter_chip('search', self._local_filter_inactive_chip_visible)
```

**Cascade joinpoint B** (`genizah_app.py:17795-17811`, inside `_apply_comp_tree_filters`):
```python
# Phase 95 REQ-6 — LOCAL filter cascade joinpoint (composition + parallels surface).
_from_parallels = getattr(self, '_comp_results_from_parallels', False)
if _from_parallels:
    _local_state_comp = getattr(self, '_local_filter_state_parallels', 'all')
    _local_chip_surface = 'parallels'
else:
    _local_state_comp = getattr(self, '_local_filter_state_composition', 'all')
    _local_chip_surface = 'composition'
_comp_raw = getattr(self, 'comp_raw_items', []) or []
_local_filtered_comp = self._apply_local_filter(_comp_raw, _local_state_comp)
_local_filter_comp_active = _local_state_comp != 'all'
_local_visible_sys_ids_comp = {
    (r.get('display', {}) or {}).get('id') or r.get('sys_id', '')
    for r in _local_filtered_comp
} if _local_filter_comp_active and not self._local_filter_inactive_chip_visible else None
self._show_local_filter_chip(_local_chip_surface, self._local_filter_inactive_chip_visible)
```

**Mirror this:**
1. Add a new method `_apply_local_optout_filter(self, results)` next to `_apply_local_filter`. It composes additionally: drops LOCAL hits whose `display.id` (canonical sys_id) maps to a filepath in `self._local_file_optouts`. Non-LOCAL hits pass through unchanged.
2. Use `self._lookup_local_filepath(sys_id)` (already exists at `genizah_app.py:18492-18505`) to resolve sys_id → canonical path.
3. **Apply at BOTH cascade joinpoints** (RESEARCH §3 + VALIDATION map). After the existing `_apply_local_filter` call in `_apply_results_table_filters` (line 17472) AND `_apply_comp_tree_filters` (line 17805).
4. Use the same `_local_visible_sys_ids` set-intersection mechanism — feed the post-opt-out subset into the existing visibility predicate at the row-iter (line ~17500+) and `_comp_data_matches_filters` (line ~17842).

**Static AST guard pattern:** `tests/test_local_filter_cascade.py:39-72` (verbatim template). Extend to also assert `_apply_local_optout_filter` is called inside both joinpoints (the existing test asserts `_apply_local_filter` only).

**Divergences from `_apply_local_filter`:**
- No 3-state cycle — opt-out is a **set predicate**, not an enum. No "all / only / no" labels.
- No no-op inactive-chip behaviour — opt-out is silent (a hit just doesn't appear).
- Filter operates per-row independently; no need for the `has_local` pre-check.

---

### D-F1 — Tree Widget (Tri-State Checkbox UI)

**Analog (existing layout — the file's UI style):** `desktop/my_library_tab.py:285-345` (`_build_ui`).

**Excerpt** (lines 285-345):
```python
def _build_ui(self) -> None:
    root = QVBoxLayout(self)
    root.setContentsMargins(8, 8, 8, 8)
    root.setSpacing(6)

    # ---- Section 1: folder list ----
    root.addWidget(QLabel(tr("Indexed folders:")))

    self._folder_list = QListWidget()
    self._folder_list.setSelectionMode(
        QAbstractItemView.SelectionMode.SingleSelection
    )
    root.addWidget(self._folder_list, stretch=1)

    folder_btns = QHBoxLayout()
    self._btn_add = QPushButton(tr("Add Folder…"))
    self._btn_remove = QPushButton(tr("Remove"))
    self._btn_add.clicked.connect(self._on_add_folder_clicked)
    self._btn_remove.clicked.connect(self._on_remove_folder_clicked)
    folder_btns.addWidget(self._btn_add)
    folder_btns.addWidget(self._btn_remove)
    folder_btns.addStretch()
    root.addLayout(folder_btns)
    # ... refresh row + progress bar ...

    # ---- Section 3: per-file status table ----
    root.addWidget(QLabel(tr("File status:")))
    self._status_table = QTableWidget(0, 3)
    self._status_table.setHorizontalHeaderLabels(
        [tr("Filename"), tr("Pages"), tr("Status")]
    )
    # ... header resize modes ...
    root.addWidget(self._status_table, stretch=2)
```

**RESEARCH §3 layout discrepancy:** The current layout is `QVBoxLayout`, NOT a `QSplitter`. CONTEXT D-07 says "Reuse the existing vertical split panel" — this is user-perceived (vertical stacking), not architectural. Planner choice:
- **Option 1 (minimal):** Replace the bottom `_status_table` with a `QSplitter(Qt.Orientation.Horizontal)` containing `[QTreeWidget, _status_table]`. The outer layout stays `QVBoxLayout`.
- **Option 2 (full splitter):** Promote outer to `QSplitter(Qt.Orientation.Vertical)`, then put a `QSplitter(Qt.Orientation.Horizontal)` inside the bottom region.

**Mirror this (tree widget shape):**
- Existing file uses **convenience widgets** (`QListWidget`, `QTableWidget`) — NOT model/view. There's no precedent in this file for `QTreeView+model`. **Use `QTreeWidget`** to match the file's style.
- Use `Qt.ItemFlag.ItemIsUserCheckable` and `Qt.ItemFlag.ItemIsAutoTristate` on folder nodes — Qt natively handles the "all / some / none" semantic CONTEXT D-07 asks for. No custom tri-state code needed.
- Files are leaf nodes with `Qt.CheckState.Checked` / `Qt.CheckState.Unchecked`.

**Divergences:**
- This is a NEW widget (no in-file precedent for `QTreeWidget`).
- Connect `itemChanged` signal to a method that recomputes `self._local_file_optouts`, persists via session JSON, and triggers a re-query (or just toggles a flag that re-render reads).
- **QMutex consideration (Phase 95 D-25, RESEARCH §5):** opt-out toggle is a UI state change, not a side-index mutation — does NOT need to acquire `self._indexer_mutex`. But if a rescan is in flight, the user could see stale results. RESEARCH §5 suggests a "Scanning…" indicator OR debouncing the toggle.

---

### NEW-1 — Remove `צפה בדפדוף` (View in Browse) Button

**Analog:** `desktop/result_dialog.py:339-352` (declaration), `:1922-1943` (handler), `:1995-2014` (visibility logic).

**Declaration to REMOVE** (lines 339-352):
```python
# Category 3 (user request): "View in Browse" button for LOCAL hits.
self.btn_rd_open_browse = QPushButton(tr("View in Browse"))
self.btn_rd_open_browse.setToolTip(
    tr("Open this LOCAL file in the Browse panel (text-only mode)")
)
self.btn_rd_open_browse.setStyleSheet(
    "QPushButton { background-color: #16a085; color: white; border-radius: 4px; padding: 2px 8px; }"
)
self.btn_rd_open_browse.setVisible(False)
self.btn_rd_open_browse.clicked.connect(self._rd_open_in_browse)
action_row.addWidget(self.btn_rd_open_browse)
```

**Handler to REMOVE** (lines 1922-1943):
```python
def _rd_open_in_browse(self):
    """Category 3: route the current LOCAL result into the Browse panel."""
    if not self._app or not hasattr(self._app, '_open_local_browse'):
        return
    data = self.data
    if not data:
        return
    sys_id = (data.get('display', {}) or {}).get('id', '')
    if not sys_id:
        return
    try:
        self._app._open_local_browse(sys_id, data)
    except Exception:
        return
    # Close the dialog so the user sees Browse without the modal on top.
    self.accept()
```

**Visibility branches to REMOVE** (lines 1995-2014):
```python
if _is_local_hit:
    # ...
    self.btn_rd_open_browse.setVisible(True)
else:
    # ...
    self.btn_rd_open_browse.setVisible(False)
```

**Mirror this:**
- Delete all three sections wholesale.
- Verify `_open_local_browse` (the parent app method at `genizah_app.py:18554`) is still reachable via the existing `עיין` Browse button (`btn_view_transcription` at `desktop/result_dialog.py:248`). Per CONTEXT.md `<specifics>`, this is the redundancy — `עיין` already covers the use case.

**Existing test to UPDATE (NOT delete):** `tests/test_local_browse_panel.py:114-122` — currently asserts the button EXISTS:
```python
def test_result_dialog_has_view_in_browse_button():
    """ResultDialog must declare btn_rd_open_browse and bind it to a handler."""
    src = _read_source("desktop/result_dialog.py")
    assert "btn_rd_open_browse" in src, (
        "Category 3: ResultDialog must declare btn_rd_open_browse"
    )
    assert "View in Browse" in src, (
        "Category 3: ResultDialog must label the button 'View in Browse'"
    )
```

**Flip to NEGATIVE assertion (NEW-1 enforcement):**
```python
def test_result_dialog_does_not_declare_view_in_browse_button():
    """NEW-1 (Phase 96): btn_rd_open_browse was removed — redundant with `עיין` Browse."""
    src = _read_source("desktop/result_dialog.py")
    assert "btn_rd_open_browse" not in src, (
        "NEW-1: btn_rd_open_browse must be removed (redundant with עיין Browse button)"
    )
```

Two adjacent tests (`test_result_dialog_has_open_in_browse_handler` at :125, `test_result_dialog_show_view_in_browse_for_local_only` at :136) must be deleted entirely.

---

### NEW-2 — LOCAL Next/Prev Navigation Primitive

**Primary analog:** `genizah_core.py:9131-9226` (`get_browse_page`).

**Excerpt — return shape** (lines 9216-9226):
```python
return {
    'uid': target_page['uid'],
    'p_num': target_page['p_num'],
    'full_header': target_page['full_header'],
    'text': text,
    'total_pages': len(pages),
    'current_idx': new_idx + 1, # Display is 1-based
    'internal_index': new_idx,  # 0-based for logic (NEW)
    'sys_id': sys_id,
    'volume_ie': active_ie or target_page.get('ie_id'),
}
```

**Excerpt — relative-nav logic** (lines 9194-9211):
```python
# Calculate New Index
new_idx = target_idx + next_prev

# Handle crossing to adjacent manuscripts when requested
if (new_idx < 0 or new_idx >= len(pages)) and allow_cross and next_prev != 0:
    direction = 1 if next_prev > 0 else -1
    adjacent_id = self.get_adjacent_sys_id_by_file_order(sys_id, direction)
    while adjacent_id:
        if adjacent_id in browse_map and browse_map[adjacent_id]:
            pages = browse_map[adjacent_id]
            sys_id = adjacent_id
            new_idx = 0 if direction > 0 else len(pages) - 1
            break
        adjacent_id = self.get_adjacent_sys_id_by_file_order(adjacent_id, direction)
    else:
        return None

if new_idx < 0 or new_idx >= len(pages): return None
```

**Secondary analog (page-aggregation primitive — building block):** `genizah_app.py:18507-18552` (`_get_local_full_text_for_sys_id`).

**Excerpt** (lines 18520-18552):
```python
try:
    q = local_index.parse_query(sys_id, ["full_header"])
    res = local_searcher.search(q, 5000)
except Exception as exc:
    logger.warning(
        "_get_local_full_text_for_sys_id: parse_query failed for %s: %s",
        sys_id, exc,
    )
    return ""
pages = []
for _score, doc_addr in res.hits:
    try:
        doc = local_searcher.doc(doc_addr)
        full_header = doc.get_first("full_header") or ""
        if not full_header.startswith(f"{sys_id}_LOCAL_P"):
            continue
        content = doc.get_first("content") or ""
        p_str = full_header.split("_LOCAL_P")[1].split("_F")[0]
        try:
            p_num = int(p_str)
        except (ValueError, IndexError):
            p_num = 0
        pages.append((p_num, content))
    except (KeyError, IndexError, TypeError):
        continue
pages.sort(key=lambda x: x[0])
return "\n\n".join(text for _p, text in pages if text)
```

**ResultDialog wiring** (`desktop/result_dialog.py:2169-2199`):
```python
def load_page(self, offset=0, target=None):
    if not self.current_sys_id: return
    self.cancel_image_thread()
    page_data = None

    if target is not None:
        # Jump by number (user typed in box)
        try: p = int(target)
        except (ValueError, TypeError): p = 1
        page_data = self.searcher.get_browse_page(self.current_sys_id, p_num=p, next_prev=0, allow_cross=True, volume_ie=self.current_volume_ie)
    else:
        # Relative Navigation (Next/Prev)
        idx_arg = self.current_internal_idx
        p_arg = int(self.current_p_num) if self.current_p_num is not None else None
        page_data = self.searcher.get_browse_page(
            self.current_sys_id,
            p_num=p_arg,
            next_prev=offset,
            absolute_index=idx_arg,
            allow_cross=True,
            volume_ie=self.current_volume_ie
        )
    if not page_data: return
```

**Mirror this (new `SearchEngine.get_local_browse_page` in genizah_core.py):**
1. Reuse the page-collection pattern from `_get_local_full_text_for_sys_id` to build the sorted `pages` list keyed by sys_id.
2. Cache the sorted list on the engine (`self._local_pages_by_sys_id[sys_id]`) — same purpose as `browse_map` for Genizah hits. Invalidate when `reload_local_indexes()` runs.
3. Accept the **same parameter signature** as `get_browse_page(sys_id, p_num=None, next_prev=0, absolute_index=None, allow_cross=False, volume_ie=None)` for drop-in dispatch.
4. Return the **same dict shape** (`uid`, `p_num`, `full_header`, `text`, `total_pages`, `current_idx`, `internal_index`, `sys_id`) so `desktop/result_dialog.py:load_page` can absorb LOCAL via a thin `if is_local_sys_id(...)` dispatch without rewriting state updates.
5. No `allow_cross`, no `volume_ie` semantics for LOCAL — return `None` at boundary (D-12: no wrap).

**ResultDialog dispatch shape:**
```python
def load_page(self, offset=0, target=None):
    if not self.current_sys_id: return
    from shared.local_sys_id import is_local_sys_id as _is_local
    if _is_local(self.current_sys_id):
        return self.load_local_page(offset=offset, target=target)  # new sibling
    # ...existing Genizah load_page body...
```

**Divergences from Genizah `get_browse_page`:**
- **No `browse_map` lookup** — LOCAL has no equivalent pre-built map; pages must be queried from the LOCAL Tantivy index each call (or cached on first call per sys_id).
- **No cross-manuscript navigation** (`allow_cross` is no-op for LOCAL) — files are independent units; no "next file" concept.
- **No `volume_ie`** — LOCAL files have a single virtual volume.
- **Disabled buttons at boundary, no wrap** (D-12).

---

### NEW-2 — View All with Page/Chunk Separators

**Primary analog:** `genizah_app.py:18550-18552` (`_get_local_full_text_for_sys_id` final lines):
```python
pages.sort(key=lambda x: x[0])
return "\n\n".join(text for _p, text in pages if text)
```

**Secondary analog (file-type lookup):** `genizah_app.py:18492-18505` (`_lookup_local_filepath`):
```python
def _lookup_local_filepath(self, sys_id: str):
    """Phase 95 D-28 — look up the canonical filepath for a LOCAL sys_id."""
    my_lib_tab = getattr(self, 'my_library_tab', None)
    indexer = getattr(my_lib_tab, '_indexer', None) if my_lib_tab else None
    if indexer is None:
        return None
    try:
        return indexer.get_filepath(sys_id)
    except Exception:
        return None
```

**Mirror this (D-14 format-aware separator):**
1. Extend `_get_local_full_text_for_sys_id` (or add `_get_local_full_text_with_separators`) to accept the file-type via filepath extension check.
2. Replace the bare `"\n\n".join(...)` with format-aware separator insertion.

**Suggested implementation (from RESEARCH §4):**
```python
def _aggregate_with_separators(self, pages, is_pdf):
    """Phase 96 D-14: page/chunk boundaries visible as labeled separators."""
    label = 'page' if is_pdf else 'chunk'
    label_he = 'דף' if is_pdf else 'מקטע'
    parts = []
    for p_num, text in pages:
        if text:
            if parts:
                # Use Hebrew or English depending on CURRENT_LANG
                sep_label = label_he if CURRENT_LANG == 'he' else label
                parts.append(f"\n\n— {sep_label} {p_num} —\n\n")
            parts.append(text)
    return "".join(parts)
```

3. Determine `is_pdf` via `os.path.splitext(filepath)[1].lower() == '.pdf'`.

**Divergences:**
- D-14 prescribes the separator label format: `— page N —` / `— chunk N —` (or Hebrew equivalents). Use this exact shape; do not invent.
- The aggregation is for **"View All" mode only** — when ResultDialog/Browse renders a single page, the new `get_local_browse_page` returns one page's `text` field without separators.
- Browse panel needs a NEW toggle UI (button or radio) for "View All" vs "Per-Page". CONTEXT.md doesn't pin placement — Claude's discretion (RESEARCH §10 open question #4).

---

## Test Pattern Analogs

### Test 1: `tests/test_local_hit_highlighting.py` (D-F5)

**Analog:** `tests/test_local_post_dedup_merge.py:21-47` (engine stub pattern).

**Excerpt** (lines 21-47):
```python
def _make_engine():
    from genizah_core import SearchEngine
    with patch("genizah_core.SearchEngine.reload_index", return_value=False):
        with patch.object(SearchEngine, "_open_local_searcher"):
            meta = MagicMock()
            meta.parse_full_id_components.return_value = {}
            engine = SearchEngine(meta, MagicMock())
    engine.local_searcher = None  # ensure clean state
    return engine


def _v08_hit(uid: str = "v8_uid") -> dict:
    return {
        "uid": uid,
        "full_text": "genizah text",
        "snippet": "genizah text",
        "display": {"source": "V0.8"},
    }


def _local_hit(uid: str = "local_uid") -> dict:
    return {
        "uid": uid,
        "full_text": "local text",
        "snippet": "local text",
        "display": {"source": "LOCAL"},
    }
```

**Mirror this:** Use `_make_engine()` to get a bare `SearchEngine` instance. Build a mock Tantivy `doc` (with `get_first` method) and pass to `_build_local_result_dict` along with a compiled regex. Assert the returned dict has `'highlight_pattern'` key, `'snippet'` contains `*` markers, `'raw_file_hl'` present, and the no-match fallback works.

**Divergences:** New test needs a mock for the Tantivy `doc` object — use `MagicMock()` with `get_first.side_effect = lambda field: {"content": "text with target word", "unique_id": "...", "full_header": "...", "shelfmark": "..."}.get(field, "")`.

---

### Test 2: `tests/test_local_pdf_extraction_fallback.py` (D-F4)

**Analog:** `tests/test_local_indexer.py:1-90` (PDF fixture loading pattern).

**Excerpt** (lines 1-90):
```python
import os
import pytest

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


FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "local_indexer")
HEBREW_PDF = os.path.join(FIXTURES_DIR, "hebrew_sample.pdf")
HEBREW_EXPECTED = os.path.join(FIXTURES_DIR, "hebrew_sample.expected.txt")


def test_pymupdf_hebrew_extraction_quality():
    """D-44 / D-02 Codex revision: real PyMuPDF Hebrew fixture quality check."""
    if not os.path.exists(HEBREW_PDF):
        pytest.skip("hebrew_sample.pdf fixture not found")
    # ...
    extracted_pages = list(extract_pdf_pages(HEBREW_PDF))
    assert len(extracted_pages) > 0, "No pages extracted from hebrew_sample.pdf"
```

**Mirror this:** Same `FIXTURES_DIR` constant; add `SINGLE_WORD_PDF = os.path.join(FIXTURES_DIR, "single_word_per_line.pdf")`. Use `pytest.skip(...)` for the fixture-missing case (consistent with Phase 95 pattern). Test the detection heuristic returns True/False as expected, and the fallback produces paragraph-shaped text.

**Divergences:** Must create the fixture file (Wave 0). RESEARCH §2 confirms it does NOT exist today.

---

### Test 3: `tests/test_local_optout_persistence.py` (D-F1)

**Analog:** `tests/test_local_filter_persistence.py:17-100` (session-dict round-trip — verbatim template).

**Excerpt** (lines 17-46):
```python
def _build_session_dict(search_local='all', comp_local='all', parallels_local='all'):
    """Build a minimal session state dict with LOCAL filter keys set."""
    return {
        'version': 1,
        'regular_search': {
            'printed_filter': 'all',
            'local_filter': search_local,
            'results': [],
        },
        'composition_search': {
            'printed_filter': 'all',
            'local_filter_composition': comp_local,
            'local_filter_parallels': parallels_local,
            'results': [],
            'filtered_results': [],
        },
    }


def _restore_from_session(state_dict):
    """Simulate the restore logic from _restore_session in genizah_app.py."""
    reg = state_dict.get('regular_search', {})
    comp = state_dict.get('composition_search', {})
    search_local = reg.get('local_filter', 'all')
    comp_local = comp.get('local_filter_composition', 'all')
    parallels_local = comp.get('local_filter_parallels', 'all')
    return search_local, comp_local, parallels_local
```

**Mirror this:** Build `_build_session_dict(local_file_optouts=[...])` and `_restore_from_session()` that returns the opt-out list. Add a `test_rescan_preserves` test that simulates the rescan flow: prune entries whose paths are no longer in `local_files` (the SQLite indexer table — mock).

**Divergences:** Phase 96 adds the rescan-preservation aspect (D-09) — opt-out list filtered against current `local_files` table. The template test doesn't have this concept.

---

### Test 4: `tests/test_local_optout_filter.py` (D-F1)

**Analog:** `tests/test_local_filter_cascade.py:85-138` (`_Stub` filter-composition pattern).

**Excerpt** (lines 94-114):
```python
class _Stub:
    _local_filter_inactive_chip_visible = False

    def _apply_local_filter(self, results, state):
        """Copy of the production method for isolated unit testing."""
        if state == 'all':
            self._local_filter_inactive_chip_visible = False
            return results
        has_local = any(
            (r.get('display', {}) or {}).get('source') == 'LOCAL'
            for r in results
        )
        if not has_local:
            self._local_filter_inactive_chip_visible = True
            return results
        self._local_filter_inactive_chip_visible = False
        if state == 'only_local':
            return [r for r in results if (r.get('display', {}) or {}).get('source') == 'LOCAL']
        if state == 'no_local':
            return [r for r in results if (r.get('display', {}) or {}).get('source') != 'LOCAL']
        return results
```

**Mirror this:** Build a `_Stub` with `_local_file_optouts` set and a `_lookup_local_filepath(sys_id)` mock. Add `_apply_local_optout_filter` (copy from production). Test combinations: (a) opt-out alone, (b) opt-out + `only_local` three-state, (c) opt-out + `no_local` three-state. Verify the two filters compose without conflicting.

---

### Test 5: `tests/test_local_nav_page_chunk.py` (NEW-2)

**Analog A:** `tests/test_local_post_dedup_merge.py:21-47` (engine stub pattern).
**Analog B:** `tests/test_local_browse_panel.py:87-107` (page-aggregation behaviour assertion).

**Excerpt B** (lines 87-107):
```python
def test_get_local_full_text_helper_defined():
    """_get_local_full_text_for_sys_id must exist."""
    src = _read_source("genizah_app.py")
    fn = _find_function(src, "_get_local_full_text_for_sys_id")
    assert fn is not None, (
        "Category 3: _get_local_full_text_for_sys_id helper required so "
        "_open_local_browse can aggregate pages when the search hit's "
        "full_text field is empty"
    )


def test_get_local_full_text_returns_aggregated_pages():
    """The helper sorts and joins pages from the LOCAL side-index."""
    src = _read_source("genizah_app.py")
    fn = _find_function(src, "_get_local_full_text_for_sys_id")
    fn_src = ast.get_source_segment(src, fn) or ""
    assert "local_searcher" in fn_src
    assert "page" in fn_src.lower() or "p_num" in fn_src.lower()
    assert "join" in fn_src or "\\n\\n" in fn_src or "sort" in fn_src.lower()
```

**Mirror this:** Test the new `get_local_browse_page(sys_id, p_num=None, next_prev=0)` returns the correct page; returns `None` at boundary (no wrap); View-All aggregation contains separators with correct labels (`page` for PDF, `chunk` for DOCX/TXT). Use a mocked `local_searcher` that returns synthetic `full_header` values matching the `{sys_id}_LOCAL_P{n}_F{file_id}` format.

---

### Test 6: `tests/test_result_dialog_local_button_removed.py` (NEW-1)

**Analog:** `tests/test_local_filter_cascade.py:39-72` (AST function walker — verbatim template, but negated assertion).

**Excerpt** (lines 39-72):
```python
def test_local_filter_applied_within_results_cascade():
    """REQ-6: static AST confirms LOCAL filter is called within ..."""
    source = GENIZAH_APP_PY.read_text(encoding='utf-8')
    tree = ast.parse(source)

    target_functions = {'_apply_results_table_filters', '_apply_comp_tree_filters'}
    found = {}
    for func in _iter_function_defs(tree):
        if func.name in target_functions:
            found[func.name] = func

    missing_from_source = target_functions - set(found.keys())
    assert not missing_from_source, ...

    offenders = []
    for fname, func in found.items():
        if not _function_contains_call(func, '_apply_local_filter'):
            offenders.append((fname, func.lineno))

    assert not offenders, (...)
```

**Mirror this (negated form):**
```python
def test_btn_rd_open_browse_removed():
    """NEW-1: btn_rd_open_browse and _rd_open_in_browse are gone."""
    src = (Path(__file__).parent.parent / 'desktop' / 'result_dialog.py').read_text(encoding='utf-8')
    assert "btn_rd_open_browse" not in src, "NEW-1: redundant button must be removed"
    assert "_rd_open_in_browse" not in src, "NEW-1: redundant handler must be removed"
    assert "View in Browse" not in src, "NEW-1: tooltip/label must be removed"
```

---

### Test 7: Extend `tests/test_local_filter_cascade.py` (D-F1)

**Self-analog:** `tests/test_local_filter_cascade.py:39-72`.

**Mirror this — add new test in same file:**
```python
def test_optout_filter_applied_within_both_cascades():
    """D-F1 (Phase 96): static AST confirms _apply_local_optout_filter is called
    within BOTH _apply_results_table_filters and _apply_comp_tree_filters."""
    source = GENIZAH_APP_PY.read_text(encoding='utf-8')
    tree = ast.parse(source)

    target_functions = {'_apply_results_table_filters', '_apply_comp_tree_filters'}
    found = {f.name: f for f in _iter_function_defs(tree) if f.name in target_functions}
    assert set(found.keys()) == target_functions

    offenders = []
    for fname, func in found.items():
        if not _function_contains_call(func, '_apply_local_optout_filter'):
            offenders.append((fname, func.lineno))

    assert not offenders, (
        "D-F1 cascade drift: opt-out filter not applied in: "
        + ', '.join(f'{n} (line {l})' for n, l in offenders)
    )
```

---

## Shared Patterns

### Pattern A — Phase 95 Invariants Carry Forward (DO NOT BREAK)

**Sources:**
- `tests/test_local_filter_cascade.py` — cascade joinpoint AST guard (extend, do not weaken)
- `tests/test_local_post_dedup_merge.py` — RRF POST-`_deduplicate` ordering pinned
- `tests/test_web_library_options_no_local.py` — web `LIBRARY_CODES` allowlist `[]`
- `tests/test_no_raw_storage_access.py` — Phase 87 multitenant allowlist `[]`
- `shared/search_serializer.py:582-585`, `corrections_client.py:627-630`, `lists_sync.py:699-713,752-766` — three cloud-write gates at TOP-of-function

**Apply to:** Every modification this phase makes. Especially:
- D-F5 modifies `_query_local_index` + `_build_local_result_dict`. Verify `tests/test_local_post_dedup_merge.py` stays green after these changes (RRF k=60 still merges POST-dedup).
- D-F1 modifies `_apply_results_table_filters` + `_apply_comp_tree_filters`. Verify `tests/test_local_filter_cascade.py` stays green (existing `_apply_local_filter` call unchanged; new `_apply_local_optout_filter` added alongside).
- NEW-1 / NEW-2 do not touch the three cloud-write files; freestyle work (D-15) must explicitly check before any such touch.

### Pattern B — `is_local_sys_id` Branch Dispatch

**Source:** `shared/local_sys_id.py:is_local_sys_id` (Phase 95). Used at three documented dispatch sites:
- `genizah_app.py:18475-18483` (browse_to_result LOCAL dispatch)
- `genizah_app.py:18570-18572` (`_open_local_browse` guard)
- `desktop/result_dialog.py:1995` (`_is_local_hit` visibility branch)

**Apply to:** NEW-2 ResultDialog dispatch in `load_page` (new sibling `load_local_page`). Reuse the same import pattern: `from shared.local_sys_id import is_local_sys_id as _is_local`.

### Pattern C — Session JSON Save/Restore Symmetry

**Source:** `genizah_app.py:23532-23613` (save) + `:23623-23800` (restore).

**Apply to:** D-F1 opt-out persistence. Every key added to `_save_session` must have a corresponding restore line in `_restore_session` with a sensible default for backward-compat with pre-Phase-96 session files.

### Pattern D — Mock Tantivy Doc

**Source:** `tests/test_local_post_dedup_merge.py` + `tests/test_local_filter_cascade.py` (`_Stub` pattern).

**Apply to:** All new unit tests that need to feed a "Tantivy doc" through `_build_local_result_dict` or `get_local_browse_page`. Use `MagicMock` with `get_first.side_effect = lambda field: {...}.get(field, "")`.

---

## No Analog Found

| File | Role | Reason |
|------|------|--------|
| `tests/fixtures/local_indexer/single_word_per_line.pdf` | new regression fixture | RESEARCH §2 verified: claimed to exist in CONTEXT/OPEN_ISSUES but does NOT exist. Wave 0 must create it (user-supplied PDF or synthetic via `fitz.Document` writing per-word `Tj` operators at distinct y-coordinates). No existing fixture has the pathological-extraction property needed. |

---

## Metadata

**Analog search scope:** `genizah_core.py`, `genizah_app.py`, `desktop/result_dialog.py`, `desktop/my_library_tab.py`, `shared/local_indexer.py`, `shared/session_persistence.py`, `tests/test_local_*.py`, `tests/test_pgp_filter_cascade.py`.
**Files scanned:** 12 source files + 23 test files + 1 docs cross-check.
**Pattern extraction date:** 2026-05-24
**Phase 96 codebase baseline:** v7.14.0 (commit `f115bd87`)

---

## PATTERN MAPPING COMPLETE
