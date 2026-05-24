# Phase 96: Completing My Library feature - Research

**Researched:** 2026-05-24
**Domain:** Desktop PyQt6 — search-result highlighting pipeline, PyMuPDF text extraction, QSettings/session-state persistence, ResultDialog & Browse navigation
**Confidence:** HIGH (all findings grounded in direct codebase reads + official PyMuPDF docs)

## Summary

Phase 96 takes v7.14.0 My Library from MVP to feature-complete by closing four
distinct problems and adding two UX gaps, all desktop-only. Every fix is bounded
to a small number of files with clearly identified analogs already in the
codebase. **All the load-bearing investigation called for by `<additional_context>`
has been resolved at code level:** the D-F5 highlight regression has a
single-line root cause; D-F4 has a documented PyMuPDF mode landscape; the
persistence boundary for D-F1 surfaces a CONTEXT D-08 vs. existing-pattern
conflict the planner must resolve; and NEW-2 navigation has a clean analog in
`ResultDialog.load_page` + `SearchEngine.get_browse_page`.

**Primary recommendation:** Take the **normalize-LOCAL-hit-dict-shape** path
for D-F5 (Option A in §1). It is one file, one helper, leaves the highlight
pipeline untouched, and converges Genizah/LOCAL parity which is exactly the
direction Phase 95 D-08 already pointed.

## User Constraints (from CONTEXT.md)

### Locked Decisions (verbatim)

**Scope Selection**
- **D-01:** Phase 96 ships D-F5 (P1) + D-F4 + D-F1 + NEW-1 + NEW-2. D-F2 (OCR) and D-F3 (side-by-side PDF rendering) are explicitly deferred to v7.15+ — keep them as OPEN_ISSUES entries.
- **D-02:** A freestyle / "fixed-as-encountered" bucket is allowed inside the phase for small bugs the user surfaces during smoke testing. The bucket is capped at "small fixes only" — anything that materially expands scope must become a new phase or `/gsd-plant-seed` entry. Planner should leave room for this in the wave structure (e.g., a trailing polish wave).

**D-F5 — LOCAL Highlighting (P1)**
- **D-03:** Approach is **investigate-first**. The planner/researcher MUST scout the highlight pipeline (both the search table and `ResultDialog`) to identify where Genizah-corpus hits get highlighted but LOCAL hits don't. Choose between "normalize LOCAL hit dict shape" vs. "per-source branch in highlight pipeline" AFTER the scout — record the choice in the plan, do not pre-commit now.
- **D-04:** Highlighting MUST be **regex-aware** for LOCAL — same two-phase (Tantivy candidates → regex filter+highlight) model the Genizah corpus uses. No substring-only shortcut. Consistency over ease.

**D-F4 — PDF Extraction Quality**
- **D-05:** Fix the one-word-per-line bug using a **detect-then-fallback** strategy: keep `get_text("blocks")` as primary, detect pathological output (e.g., >80% of lines have ≤1 word), fall back to `get_text("text")` (or other PyMuPDF mode chosen during planning). Preserves currently-working PDFs.
- **D-06:** Validate the fix against a small **representative sample of PDFs** — at minimum the existing `tests/fixtures/local_indexer/single_word_per_line.pdf` regression fixture PLUS a handful of user-supplied PDFs that currently extract cleanly (regression coverage in both directions: bad → good AND good → still good). Not a full audit-first sweep.

**D-F1 — Folder Drill-down (Per-file Opt-in/Out)**
- **D-07:** Reuse the **existing vertical split panel** in `MyLibraryTab` (top = folder list as today). The **bottom panel becomes a new horizontal split**: tree on left (with subfolders + files + tri-state checkboxes), the existing file-status output on the right.
- **D-08:** Per-file opt-out state persists via **QSettings** (in-app user state).
- **D-09:** When the indexer rescans a folder, opt-out state for files that still exist MUST be preserved. Removed files drop their state.
- **D-10:** Opt-out filtering is applied at **query time**, not index-build time. Files stay indexed; the filter excludes them from search results.

**NEW-1, NEW-2, NEW-3**
- **D-11:** The `צפה בדפדוף` button is only present on LOCAL hits today. Remove for LOCAL hits only. No project-wide audit.
- **D-12:** Next/prev navigation in LOCAL is **format-aware**: PDF → page, txt/docx → chunk. When file has only one page/chunk, buttons are disabled (no wrap).
- **D-13:** Navigation appears in **two places**: ResultDialog AND Browse panel. NOT in the search results table row.
- **D-14:** "View All" (הכל) for LOCAL in Browse = full file text, all chunks concatenated. Page/chunk boundaries should remain visible (thin separator labeled `— page 2 —` or `— chunk 2 —`).
- **D-15:** Small bugs surfaced during smoke testing can be fixed inline. Larger scope expansion must become a new phase.

### Claude's Discretion (verbatim)
- D-F5 normalize-vs-branch choice (after the scout — see D-03)
- D-F4 exact PyMuPDF fallback mode (`get_text("text")` is the first attempt — see D-05)
- Tree widget exact PyQt6 class (`QTreeWidget` vs `QTreeView+model`)
- Tri-state checkbox styling and label conventions
- Page/chunk separator visual style for "View All"

### Deferred Ideas (OUT OF SCOPE)
- **D-F2 — PDF OCR (Tesseract or cloud)** for scanned image-only PDFs. Belongs in its own phase with the OCR-engine choice as a primary discussion.
- **D-F3 — Side-by-side PDF page rendering** (PDF page image next to extracted text in Browse + ResultDialog). P3, polish, can wait.

## Phase Requirements

No REQ-IDs were minted for Phase 96 — scope is captured as D-XX decisions in
CONTEXT.md instead of REQUIREMENTS.md entries. The five tracked items map as:

| Item ID | Description | Research Support |
|---------|-------------|------------------|
| D-F5 | LOCAL highlight regression | §1 Highlight Pipeline Map — Option A recommended |
| D-F4 | PDF one-word-per-line | §2 PyMuPDF Extraction Fix — `sort=True` + heuristic |
| D-F1 | Per-file opt-in/out drill-down | §3 Persistence pattern — D-08 vs. session JSON conflict surfaced |
| NEW-1 | Remove `צפה בדפדוף` button | §4 `desktop/result_dialog.py:343` `btn_rd_open_browse` |
| NEW-2 | LOCAL next/prev + View All | §4 `searcher.get_browse_page` analog; LOCAL hit dict missing fields |
| NEW-3 | Freestyle / polish | Trailing wave; no specific code surface |

## Project Constraints (from CLAUDE.md)

- **Dual app discipline:** Phase 96 is desktop-only by scope (My Library is desktop-only since Phase 95). Web `LIBRARY_CODES` must NOT receive `LOCAL`. AST guard `tests/test_web_library_options_no_local.py` must stay green.
- **Multitenant invariants:** Phase 87 allowlist for `app.storage.user` is `[]`. Phase 96 is desktop work but any incidental web touch must not regress this — guard test is `tests/test_no_raw_storage_access.py`.
- **Cloud-write gates:** Three gates pinned at TOP of `shared/search_serializer.py`, `corrections_client.py`, `lists_sync.{sync_item_to_cloud, sync_list_to_cloud}`. Do NOT touch during freestyle work; if any of those files are touched for unrelated reasons, verify gates remain at TOP-of-function.
- **Version bumping:** Use `python scripts/bump_version.py X.Y.Z` to bump 4 files (+ manually CHANGELOG/CLAUDE.md/README "What's New").
- **MEMORY note:** `tests/test_release_artifacts.py::_TARGET_VERSION` is NOT auto-bumped by `bump_version.py` — must be edited manually each release or CI fails.
- **Pre-release MUST run ruff** explicitly (`python -m ruff check .`) — v7.12.0 CI failed on 18 F401 errors that the pre-flight missed.

---

## 1. Highlight Pipeline Map (D-F5)

### Root Cause — Single Smoking Gun

The highlight pipeline is **asterisk-driven**, not pattern-driven, on the search-results table surface. LOCAL hits never receive asterisks because `_build_local_result_dict` ships a raw `snippet`.

**Snippet creation for Genizah hits** [VERIFIED: `genizah_core.py:7819`]
```python
hl_c = self.highlight(content, regex, False)  # produces "*foo*bar" markers
hl_f = self.highlight(content, regex, True)
results.append({
    'display': meta,
    'snippet': hl_c,                  # already contains *...* markers
    'full_text': content,
    'raw_file_hl': hl_f,
    'highlight_pattern': pattern_str, # raw regex.pattern string
    ...
})
```

**Snippet creation for LOCAL hits** [VERIFIED: `genizah_core.py:6876-6907`]
```python
def _build_local_result_dict(self, doc, score) -> dict:
    ...
    content = doc.get_first("content") or ""
    snippet = content[:200] if content else ""   # ← no markers, no regex applied
    return {
        "uid": unique_id,
        "full_text": content,
        "snippet": snippet,        # ← raw, NOT highlighted
        # ← NO 'highlight_pattern' field
        # ← NO 'raw_file_hl' field
        ...
    }
```

**Search-table render path** [VERIFIED: `genizah_app.py:16668-16671`]
```python
_snip = res.get('snippet', '')
html_snippet = self.render_asterisks_to_html(_snip)
lbl = QLabel(html_snippet)
```

**`render_asterisks_to_html` → `SearchEngine.format_snippet`** [VERIFIED: `genizah_core.py:6979-6999`]
```python
@staticmethod
def format_snippet(text, style='html_class'):
    escaped = html.escape(text)
    # Convert *word* to highlighted span (after escaping, markers are safe)
    return re.sub(r'\*(.*?)\*', r'<span style="color:#ff0000; font-weight:bold;">\1</span>', escaped)
```

If `text` contains no asterisks, the regex matches nothing and the text is returned unchanged (escaped). That is exactly the LOCAL behavior the user sees.

**ResultDialog render path** [VERIFIED: `desktop/result_dialog.py:2051-2061`]
```python
ms_raw = data.get('full_text', '') or data.get('text', '')
pattern_str = data.get('highlight_pattern')  # ← LOCAL hits don't set this
if pattern_str:
    regex = re.compile(pattern_str, flags)
    ms_raw = regex.sub(r'*\g<0>*', ms_raw)    # ← skipped for LOCAL
```

This is the second surface. ResultDialog uses `highlight_pattern` to apply
markers on the fly to `full_text`. LOCAL hits have `full_text` but no
`highlight_pattern`, so the `if pattern_str:` branch never fires.

### Two Architectural Options

#### Option A — Normalize LOCAL hit dict shape at construction time (RECOMMENDED)

Inside `SearchEngine._query_local_index` (genizah_core.py:6849-6874), after
fetching Tantivy hits but before `_build_local_result_dict`, compile the same
`regex` the main search uses and pass it through to `_build_local_result_dict`.
Inside `_build_local_result_dict`, call `self.highlight(content, regex, False)`
and `self.highlight(content, regex, True)` to populate `snippet` and
`raw_file_hl`, and add `highlight_pattern = regex.pattern` to the returned
dict.

**Pros:**
- One file, one helper change. Zero touch in `genizah_app.py` and `desktop/result_dialog.py`.
- LOCAL hits behave identically to Genizah hits everywhere downstream — table render, ResultDialog, copy-to-clipboard via `filter_text` property, _all_ surfaces fixed at once.
- Convergent with Phase 95 D-04 (LOCAL parity with Genizah pipeline) and D-08 P0 (POST-dedup merge); same direction.
- Trivial to test: a unit assertion on `_build_local_result_dict` return shape.

**Cons:**
- Requires passing the `regex` object (or `query_str` + `mode` + `gap`) down into `_query_local_index`. Currently the local index path only knows `query_str`.
- The regex used for LOCAL must match the one used for Genizah — duplicates the compilation. Either pre-compile in the caller and pass down, or recompile inside `_query_local_index` (cheap; regex compilation is sub-millisecond).
- A second-order risk: if the regex returns no match against `content`, the snippet is empty (`highlight()` returns `None`). Need a fallback to `content[:200]` in that case so the row still has something to display. Genizah hits don't hit this fallback because the regex was the filter — if it didn't match, the hit was dropped. LOCAL hits use Tantivy parse_query directly without a regex re-filter, so we DO have to handle the "Tantivy matched but regex didn't" case explicitly.

#### Option B — Per-source branch in highlight pipeline

In `genizah_app.py:16668-16671`, detect `res['display']['source'] == 'LOCAL'`,
recompile the regex from the last search input, apply it inline to
`res['snippet']` before passing through `render_asterisks_to_html`. Same logic
for `desktop/result_dialog.py:2051-2061`.

**Pros:**
- LOCAL hit dict shape stays minimal — fewer fields, less data flowing through `_rrf_merge`.
- Doesn't require threading `regex` down into the local query path.

**Cons:**
- Two call sites must stay synchronized (search table + ResultDialog), each with its own LOCAL detection branch. Future-fragile.
- Recompiling the regex in the UI layer means the UI needs to know the query mode + gap to rebuild the exact regex the search used. That information is not currently held next to the result list.
- Diverges from the D-04 "same two-phase model" intent — LOCAL would have its own highlighting code path, not the same one Genizah uses.
- Breaks the "filter_text" property on QLabel at `genizah_app.py:16673` (it stores the asterisk-marker version of the snippet for filter UX); branch-style fix needs to populate that too.

**Recommendation: Option A.** One file touched, one new field on LOCAL hit
dict, zero divergence from Phase 95 architecture. The "regex didn't match on
LOCAL content even though Tantivy did" edge case is a real ~5-line piece of
defensive code (fall back to raw `content[:200]`), not a blocker.

### Verification Hint for Planner

A unit test on `SearchEngine._build_local_result_dict` (or its public-test
proxy) that asserts:
- `'highlight_pattern'` key present
- `'snippet'` contains `*` markers when the regex matches `content`
- `'snippet'` falls back to first-200-chars when regex doesn't match
- `'raw_file_hl'` key present

—is sufficient at the engine level. The two UI surfaces (table + ResultDialog)
become exercised by the existing render code without changes.

---

## 2. PyMuPDF Extraction Fix (D-F4)

### Mode Comparison [CITED: pymupdf.readthedocs.io/en/latest/textpage.html]

| Mode | Returns | Reading Order | Use For |
|------|---------|--------------|---------|
| `"text"` | Plain UTF-8 string | Document creation order; with `sort=True` reorders by (y, x) coords | Layout-agnostic plain-text dump |
| `"blocks"` | List of `(x0, y0, x1, y1, text, block_no, block_type)` tuples | Layout-block heuristic (font, size, rotation, proximity) | Structured paragraphs — current Phase 95 default |
| `"words"` | List of `(x0, y0, x1, y1, word, block_no, line_no, word_no)` | Per-word coordinates; caller reconstructs | Custom reading-order reconstruction |
| `"dict"` | Block→line→span→text dict | Hierarchical structure | Programmatic span-level access |
| `"html"` | HTML string with positioning + base64 images | Browser-display order | Not relevant for indexing |

### Why "blocks" Yields One-Word-Per-Line [CITED: pymupdf.readthedocs.io/en/latest/recipes-text.html]

> "This typically happens when the PDF creator inserted content in
> non-sequential steps (like adding headers separately), causing text to
> appear out of order despite displaying correctly in PDF viewers."

The MuPDF block-detection heuristic groups text by font, size, rotation, and
spatial proximity. For PDFs where text is laid out via individual XObjects or
Tj operators that emit one word at a time at distinct y-coordinates, each word
gets its own block and the join logic produces one block per word. Common in:
- PDFs generated from Word docs with embedded shapes/positioned text
- PDFs with Hebrew/Arabic RTL runs where each glyph cluster was emitted
  separately
- PDFs from scanned-then-OCR'd sources where the OCR layer is per-word

### Detection Heuristic Validation

CONTEXT D-05 proposes: ">80% of lines have ≤1 word".

**Verdict:** Sound but tighten the threshold. Recommend:
- After block-extraction, split the resulting text on `\n` and count non-empty lines.
- If fewer than ~5 non-empty lines total, skip detection (too small a sample — could be a 1-line title page).
- Compute `single_word_ratio = sum(1 for ln in lines if len(ln.split()) <= 1) / len(lines)`.
- Trigger fallback if `single_word_ratio >= 0.70`. (0.80 is too strict for documents with legitimate one-word paragraphs — chapter numbers, table cells. 0.70 is the threshold the Phase 95 dead-code `_join_fragmented_lines` helper uses at `local_indexer.py:154` and it was already vetted against Hebrew samples.)

Note: Phase 95 already ports a `_join_fragmented_lines` helper from
Seewald's prototype as dead code (see `local_indexer.py:148-167` per `95-PATTERNS.md:148`). It uses a `single / len(non_empty) < 0.60` threshold to
**decline** the join. The Phase 96 detection can use the inverse logic. The
helper itself is dead-code-marked per CONTEXT D-02 of Phase 95, but a Phase 96
plan could either (a) revive it as the post-processing pass under D-05 or (b)
write a fresh, smaller detection helper. Option (a) reuses already-tested code
but contradicts the "dead code" marker; option (b) preserves the marker but
duplicates logic. Planner choice.

### Recommended Fallback Mode

Based on PyMuPDF docs and the one-word-per-line root cause:

**Primary fallback:** `page.get_text("text", sort=True)`

This requests plain text with PyMuPDF's built-in spatial sort (top-left to
bottom-right). It is explicitly the documented remedy for "non-sequential
creation order" PDFs. The `sort=True` flag is the load-bearing piece — not
just switching from `"blocks"` to `"text"`.

**Tradeoffs:**
- Pros: Layout-agnostic, single API call, no custom reading-order code.
- Cons: Loses block-level structure (no paragraph boundaries). For Phase 95's per-page indexing model (one Tantivy doc per PDF page, D-03) this is fine — we don't use intra-page paragraph boundaries downstream.
- Hebrew RTL: PyMuPDF 1.27.x has [CITED: changes.md] improved RTL handling — `Page.get_text()` no longer mixes RTL/LTR character pairs within words. The fallback path benefits from this.

**Alternative if `"text"` is also pathological:** `page.get_text("words")` +
custom reconstruction grouping words by line-y-coordinate within tolerance.
This is more code; reserve for a v7.15+ deeper fix if the simple `text,
sort=True` fallback also fails on some user PDFs.

### Existing Regression Fixture Status

**Discrepancy flagged for planner.** CONTEXT.md `<code_context>` line 122 and
OPEN_ISSUES.md D-F4 both state `tests/fixtures/local_indexer/single_word_per_line.pdf`
**already exists**. [VERIFIED: ground truth via `Glob tests/fixtures/local_indexer/*`]
the directory contains 8 fixture files (`hebrew_sample.expected.txt`,
`hebrew_sample.pdf`, `bad_encoding.txt`, `cp1255_sample.txt`, `sample.docx`,
`sample.txt`, `unsupported.html`, `utf8sig_sample.txt`) but **NOT**
`single_word_per_line.pdf`. Git log confirms it has never been committed.

**Implication:** The plan MUST include creating this fixture before any
regression test can pass. The user-supplied PDFs referenced in D-06 are the
likely source — either commit one as the fixture, or generate a synthetic one
by re-encoding text via individual `Tj` operators at distinct y-coordinates.

### PyMuPDF 1.27.2.3 Pinned Quirks [VERIFIED: requirements-lock.txt]

- `requirements-lock.txt`: `pymupdf==1.27.2.3` (exact pin)
- `requirements.txt`: `pymupdf>=1.24,<2.0` (range)
- No known issues in 1.27.2.3 specific to `get_text("text", sort=True)`. The
  RTL ligature/word-boundary fix in recent 1.27.x releases is what the LOCAL
  indexer already relies on per Phase 95 D-44 (Hebrew fixture).

---

## 3. Per-file Opt-out Persistence + UI (D-F1)

### CRITICAL CONFLICT — D-08 vs. Existing Pattern

CONTEXT.md D-08 locks: *"Per-file opt-out state persists via QSettings (in-app user state)"*.

The existing pattern in this codebase for all other LOCAL filter persistence is
**session JSON via `shared/session_persistence.py`**, NOT QSettings.

[VERIFIED: `tests/test_local_filter_persistence.py:1-13`]
> "The desktop app uses a session-state JSON (shared/session_persistence.py)
> rather than QSettings — that is the established pattern for all other filter
> state in genizah_app.py (e.g. printed_filter, domain_exclusions)."

[VERIFIED: `genizah_app.py:23532-23613`] `_save_session()` serializes
`local_filter`, `local_filter_composition`, `local_filter_parallels`, plus 20+
other filter/exclusion keys, into the session JSON. QSettings is only used for
non-portable UI prefs (`desktop/my_library_tab.py:158-159` — `QSettings("Dicta", "GenizahSearchPro")` — sole usage in the entire `desktop/my_library_tab.py`).

**The conflict is real.** D-08 was decided in the discuss phase before the
existing pattern was surfaced. The planner has three options:

| Option | Effect | Risk |
|--------|--------|------|
| A. Honor D-08 (QSettings) | Per-file opt-out diverges from other LOCAL filter state | Two persistence patterns for the same feature family; user reasonably confused |
| B. Use session JSON (existing pattern) | Consistent with `local_filter_*` keys | Requires `/gsd-discuss-phase 96 --revise` to update D-08; or note as P0 deviation in plan |
| C. Hybrid: opt-out **set** in QSettings (so it persists across session restore) | Honors D-08 letter | Two stores to keep in sync; messy |

**Recommendation:** Surface this conflict explicitly during planning. The
session JSON approach (Option B) is the cleaner architectural fit AND aligns
with D-08's stated rationale: *"selections persist but are easy to toggle
per-search"*. The session JSON IS toggle-fast and IS persisted across restarts —
QSettings offers no additional benefit on either dimension.

If D-08 stays locked as-is, plan accordingly: use `QSettings("Dicta",
"GenizahSearchPro")` (the existing instance in `desktop/my_library_tab.py:159`),
key as `local/file_optouts` storing a JSON-encoded list of canonical file paths
(use `_canonical_filepath` from `shared/local_sys_id.py:103` for Windows
path normalization, already imported in `local_indexer.py:56-59`).

### Keying Strategy (D-09)

CONTEXT D-09 requires opt-out state preserved across rescans for files that
still exist. Key by **canonical file path** (`_canonical_filepath(p)` —
already used as the LOCAL sys_id seed, guarantees Windows path-normalization
parity).

- File still exists after rescan → state preserved (key still in the store, file still in `local_files` SQLite table).
- File removed from disk (or folder removed) → cleanup pass at end of `LocalIndexer.scan_all()` drops keys whose paths no longer appear in `local_files`.

Path-as-key is the obvious choice; hash/inode are over-engineering. The only
edge case is rename (moves the file, generates new sys_id, drops the old key).
That's correct behavior — a renamed file is conceptually a new file.

### Query-Time Filter Composition (D-10)

The existing three-state LOCAL filter applies at query time in
`_apply_local_filter` [VERIFIED: `genizah_app.py:17321-17344`]:

```python
def _apply_local_filter(self, results, state):
    # 'all' → passthrough; 'only_local'/'no_local' → predicate on display.source
    if state == 'all':
        return results
    has_local = any((r.get('display', {}) or {}).get('source') == 'LOCAL' for r in results)
    if not has_local:
        # D-10 P1 NO-OP — preserve state but show inline chip.
        return results
    if state == 'only_local':
        return [r for r in results if (r.get('display', {}) or {}).get('source') == 'LOCAL']
    if state == 'no_local':
        return [r for r in results if (r.get('display', {}) or {}).get('source') != 'LOCAL']
```

The per-file opt-out filter composes by stacking another predicate **on the
LOCAL hits subset only** (Genizah hits unaffected):

```python
# After _apply_local_filter:
opt_outs = self._load_local_file_optouts()  # set of canonical paths
if opt_outs:
    results = [r for r in results
               if (r.get('display', {}) or {}).get('source') != 'LOCAL'
               or self._lookup_local_filepath(r.get('display', {}).get('id', '')) not in opt_outs]
```

This composes cleanly with the three-state filter and is a pure additional
predicate — no race with the existing cascade discipline pinned by
`tests/test_local_filter_cascade.py`.

The filter must be applied at BOTH cascade joinpoints:
- `_apply_results_table_filters` (line 17474) — Search surface
- `_apply_comp_tree_filters` (line 17807) — Composition + Parallels surfaces

Same shape Phase 95 D-39 uses for `_apply_local_filter`.

### Tree Widget — QTreeWidget vs QTreeView+model

Existing `MyLibraryTab` uses `QListWidget` (folder list, line 293) and
`QTableWidget` (status table, line 329) — **convenience widgets**, not
model/view. There's no precedent in this file for `QTreeView+model`.

**Recommendation:** `QTreeWidget` with `Qt.ItemFlag.ItemIsUserCheckable` and
`Qt.ItemFlag.ItemIsAutoTristate` on folder nodes. Files are leaf nodes with
`Qt.CheckState.Checked` / `Qt.CheckState.Unchecked`. Folder auto-tristate is
exactly the "all / some / none" semantic CONTEXT.md D-07 asks for and Qt does
it natively — no custom logic.

### Layout Discrepancy Flagged

CONTEXT.md D-07 says "Reuse the existing vertical split panel in
`MyLibraryTab`". [VERIFIED: `desktop/my_library_tab.py:285-345`] the existing
layout is a `QVBoxLayout`, **not a `QSplitter`** — there is no draggable
splitter in the current MyLibraryTab. The user perceives it as "split" because
sections stack vertically with the status table at the bottom, but
architecturally it's a plain box layout.

Two options for the planner:
1. Interpret CONTEXT D-07 as "the existing top/bottom regions" — replace the
   bottom `_status_table` with a `QSplitter(Qt.Horizontal)` containing
   `[QTreeWidget, _status_table]`. The outer layout stays a `QVBoxLayout`.
2. Promote the outer layout to `QSplitter(Qt.Vertical)` so the top folder
   region and the bottom tree/status region are user-resizable, then put a
   `QSplitter(Qt.Horizontal)` inside the bottom region.

Option 2 better matches "vertical split panel" wording; option 1 minimizes
changes. Either fits within Claude's discretion (CONTEXT D-07 doesn't pin
this).

---

## 4. Next/Prev + View All Navigation (NEW-2)

### Genizah Navigation Analog [VERIFIED]

**ResultDialog folio nav** [`desktop/result_dialog.py:120,130,236-238,2169-2199`]:
```python
self.btn_compact_pg_prev.clicked.connect(lambda: self.load_page(offset=-1))
self.btn_compact_pg_next.clicked.connect(lambda: self.load_page(offset=1))
self.spin_page.editingFinished.connect(lambda: self.load_page(target=self.spin_page.value()))

def load_page(self, offset=0, target=None):
    if not self.current_sys_id: return
    if target is not None:
        # Jump-by-number
        page_data = self.searcher.get_browse_page(self.current_sys_id, p_num=p, next_prev=0, ...)
    else:
        # Relative ±1 — uses internal_index to prevent loops on non-contiguous p_num
        page_data = self.searcher.get_browse_page(
            self.current_sys_id, p_num=p_arg, next_prev=offset,
            absolute_index=self.current_internal_idx, allow_cross=True,
            volume_ie=self.current_volume_ie)
```

Central primitive: `SearchEngine.get_browse_page(sys_id, p_num, next_prev,
absolute_index, allow_cross, volume_ie)` returns
`{'sys_id', 'p_num', 'full_header', 'text', 'uid', 'internal_index',
'total_pages'}`.

**Browse panel folio nav** uses the same primitive — `browse_load()`
delegates through `get_browse_page` (line 18490).

### LOCAL Navigation Gap

[VERIFIED: `genizah_core.py:6876-6907`] `_build_local_result_dict` exposes:
- `uid` (unique_id from LOCAL Tantivy schema)
- `sys_id` (parsed from full_header `{sys_id}_LOCAL_P{page}_F{file_id}`)
- `p_num` (parsed from full_header — the current page/chunk)
- `full_header` (the raw header)
- `full_text` (the per-page content)

**Missing for navigation:**
- `total_pages` (how many pages/chunks the file has — needed to disable buttons when at boundary)
- `internal_index` (the Genizah analog for non-contiguous p_num handling — not strictly required for LOCAL because LOCAL pages ARE contiguous 1..N)
- A `next_prev` analog in the LOCAL query path

[VERIFIED: `genizah_app.py:18507-18552`] `_get_local_full_text_for_sys_id`
already exists for "View All" — it queries the LOCAL Tantivy index by sys_id
prefix and aggregates ALL pages of a sys_id sorted by p_num. This is the
"View All" primitive already in place for the Browse panel (used by
`_open_local_browse` at line 18584 when the search-hit `full_text` is absent).

### Implementation Path

**`SearchEngine.get_local_browse_page(sys_id, p_num=None, offset=0)`** —
new method, analog to `get_browse_page` but for LOCAL:
1. Query LOCAL index for ALL pages of this sys_id (reuses the same prefix
   query as `_get_local_full_text_for_sys_id`).
2. Sort by p_num. Compute total_pages = len(pages).
3. If `p_num` is None and offset=0: return page 1.
4. If `offset`: find the current page in the sorted list, return
   `pages[current_idx + offset]` or None if out of bounds.
5. Return `{sys_id, p_num, full_header, text, uid, total_pages,
   internal_index}` — same shape as Genizah `get_browse_page` so the
   downstream UI code is identical.

**ResultDialog wiring:** detect LOCAL hit in `load_page`, dispatch to a
sibling `load_local_page` that calls `get_local_browse_page` instead of
`get_browse_page`. Same button connect (`btn_compact_pg_prev`,
`btn_compact_pg_next`, `spin_page`) — they already exist and don't care which
primitive is fetching the page.

**Browse panel wiring:** `browse_load()` currently dispatches LOCAL through
`_open_local_browse` (line 18481), which renders all pages concatenated.
Phase 96 needs to split this:
- "View All" path → existing `_open_local_browse` (one continuous render with separators per D-14)
- Per-page nav path → new `_open_local_browse_page(sys_id, p_num)` that renders one page at a time
- Toggle: a button or radio "View All / Per-Page" near the existing browse controls.

### Page/Chunk Separator (D-14)

CONTEXT D-14 prescribes a thin separator labeled `— page 2 —` or `— chunk 2 —`
in "View All" mode. The existing aggregation in
`_get_local_full_text_for_sys_id:18552` does:
```python
return "\n\n".join(text for _p, text in pages if text)
```
Update to:
```python
def _aggregate_with_separators(self, pages, is_pdf):
    """Phase 96 D-14: page/chunk boundaries visible as labeled separators."""
    label = 'page' if is_pdf else 'chunk'
    parts = []
    for p_num, text in pages:
        if text:
            if parts:
                parts.append(f"\n\n— {label} {p_num} —\n\n")
            parts.append(text)
    return "".join(parts)
```
File-type detection (PDF vs DOCX/TXT) is available via the SQLite `local_files`
table — `get_filepath(sys_id)` already exists at
`my_library_tab.py:_indexer.get_filepath` (called from `genizah_app.py:18503`).
Extension check on the filepath determines `is_pdf`.

### Format-Aware Navigation Unit (D-12)

PDF: one page = one Tantivy doc = one nav unit (D-03 of Phase 95).
DOCX: one 20-paragraph chunk = one Tantivy doc = one nav unit (D-04 of Phase 95).
TXT: one chunk = one Tantivy doc = one nav unit.

The `full_header` field already encodes which we have:
`{sys_id}_LOCAL_P{page_or_chunk_num}_F{file_id}`. No additional file-type
detection needed for nav — the existing p_num field is the nav index. The
file-type detection is only needed for the UI label ("Page" vs "Chunk").
Suggested approach: store `file_type` (pdf/docx/txt) in `local_files` SQLite
table (already exists) and surface via `get_filepath` extension check, OR
expose a new `get_file_type(sys_id)` helper.

---

## 5. Phase 95 Invariants (DO NOT BREAK)

### LOCAL Merge Order [VERIFIED: `genizah_core.py:8354-8371`]

```python
deduped = self._deduplicate(results)
# Phase 95 D-08 (Codex P0): LOCAL hits merge AFTER _deduplicate.
if corpus_scope != "genizah" and getattr(self, "local_searcher", None) is not None:
    local_hits = self._query_local_index(query_str, mode, gap)
    if local_hits:
        deduped = self._rrf_merge(deduped, local_hits, k=60)
```

**Phase 96 implication:** Any opt-out filter (D-F1) applies at query time
**after** this merge, at the cascade joinpoints (`_apply_results_table_filters`
+ `_apply_comp_tree_filters`). Do NOT inject the opt-out filter inside
`_query_local_index` — that would couple the engine to a UI-layer concept
(opt-outs are a desktop QSettings/session-JSON state, not a search-engine
state). Keep the engine pure.

### Cloud-Write Gates [VERIFIED: `lists_sync.py:699-713,752-766`, `corrections_client.py:627-630`, `shared/search_serializer.py:582-585`]

All three gates are at TOP-of-function position. Phase 96 does NOT need to
touch any of these files. If the freestyle bucket (D-15) drifts toward
touching them, the planner must verify gates remain at top.

### Web LIBRARY_CODES Allowlist [VERIFIED: `tests/test_web_library_options_no_local.py`]

Static AST guard scans `web/pages/` for any iteration of `LIBRARY_CODES`
without a LOCAL guard. Currently NO-OP (no violators today). Phase 96 is
desktop-only by D-01; no web touch should be needed. Guard test must stay
green.

### Multitenant Storage Allowlist [VERIFIED: `tests/test_no_raw_storage_access.py`]

Allowlist for raw `app.storage.user` access under `web/` is `[]`. Desktop-only
phase, but pinned regardless.

### QThread + QMutex Serialization [VERIFIED: Phase 95 D-25]

If D-F1 opt-out toggle triggers re-query, ensure it doesn't race with an
in-flight `LocalIndexerWorker` rescan. The existing `self._indexer_mutex` at
`desktop/my_library_tab.py:151` already serializes side-index mutations; opt-out
toggle is a UI-layer state change, not a mutation, so it does not need to
acquire the mutex. But if the toggle triggers a re-query AND a rescan is in
progress, the user could see stale results — UI should at minimum show a
"Scanning..." indicator OR debounce the toggle until rescan completes.

---

## 6. Files Likely Modified

| File | Role | Phase 96 work |
|------|------|---------------|
| `genizah_core.py` | Search engine | D-F5 Option A: `_build_local_result_dict` enriched; `_query_local_index` accepts regex; new `get_local_browse_page` for NEW-2 |
| `shared/local_indexer.py` | PyMuPDF + DOCX + TXT extraction | D-F4: add fallback path in `extract_pdf_pages` with detection heuristic and `get_text("text", sort=True)` fallback |
| `desktop/my_library_tab.py` | MyLibraryTab UI | D-F1: bottom panel restructure (QSplitter horizontal + QTreeWidget); QSettings or session-JSON opt-out persistence; rescan-preserves-state logic |
| `desktop/result_dialog.py` | ResultDialog | NEW-1: remove `btn_rd_open_browse` (line 343); NEW-2: route LOCAL hits through `load_local_page` instead of `load_page` (or guard inside `load_page`) |
| `genizah_app.py` | Main app | D-F1: wire opt-out filter into `_apply_results_table_filters` + `_apply_comp_tree_filters`; NEW-2: Browse panel per-page nav + View-All toggle; D-14 separator helper; session-save extension if going Option B for D-08 |
| `gui_threads.py` | SearchThread + LocalIndexerWorker | Likely untouched — engine changes for D-F5 happen on the SearchEngine side, not in the thread wrapper |
| `tests/fixtures/local_indexer/single_word_per_line.pdf` | D-F4 regression | **NEW** — does not exist yet; must be created |
| `tests/test_local_pdf_extraction_fallback.py` | D-F4 regression | NEW |
| `tests/test_local_hit_highlighting.py` | D-F5 regression | NEW |
| `tests/test_local_optout_filter.py` | D-F1 opt-out filter composition | NEW |
| `tests/test_local_optout_persistence.py` | D-F1 round-trip | NEW |
| `tests/test_local_nav_page_chunk.py` | NEW-2 navigation | NEW |
| `tests/test_result_dialog_local_button_removed.py` | NEW-1 AST or widget guard | NEW |
| `docs/OPEN_ISSUES.md` | Issue tracker | Mark D-F1, D-F4, D-F5 as ✅ Fixed; leave D-F2 + D-F3 as deferred |
| `CHANGELOG.md` | Release notes | Add v7.14.1 (or v7.15.0) section |
| `version.py`, `version_info.txt`, `CompileScriptGenizah.iss`, `README.md` | Version bump | Run `python scripts/bump_version.py X.Y.Z` |
| `tests/test_release_artifacts.py` | `_TARGET_VERSION` | Manual edit (bump_version.py does NOT touch this — MEMORY note) |

**Files NOT touched (Phase 95 invariants):**
- `shared/search_serializer.py`, `corrections_client.py`, `lists_sync.py` — cloud-write gates
- `web/pages/*` — desktop-only feature
- `shared/local_sys_id.py` — namespace layer stable

---

## 7. Common Pitfalls

### Pitfall 1: Recompiling the regex in the UI layer (D-F5 Option B trap)

**What goes wrong:** Option B reconstructs the regex from `query_input.text()`
inside the UI render code. The reconstruction has to know mode + gap +
text_position + variant preset.

**Why it happens:** Underestimating how much state the regex compilation
depends on (genizah_core.py:7220-7340 has ~120 lines of regex construction).

**How to avoid:** Take Option A. Pass the compiled regex (or rebuild it in
`_query_local_index` where the search engine already has access to mode + gap +
all the parameters) and stamp `highlight_pattern` into the LOCAL hit dict.

### Pitfall 2: Forgetting `tests/test_release_artifacts.py::_TARGET_VERSION`

**What goes wrong:** Release commit fails CI because the constant is hardcoded
and `bump_version.py` doesn't touch it.

**Why it happens:** `bump_version.py` updates 4 files; this test isn't one of them.

**How to avoid:** Plan a manual edit step OR add `_TARGET_VERSION` to the bumper
script as a freestyle (D-15) bug fix.

### Pitfall 3: Pre-release skipping `ruff check`

**What goes wrong:** CI fails on F401 imports or similar lint that pytest
doesn't catch (v7.12.0 burned 18 errors this way per MEMORY).

**How to avoid:** Pre-flight checklist MUST include `python -m ruff check .`
as its own line item, not implied.

### Pitfall 4: D-F4 fixture doesn't exist yet

**What goes wrong:** Test references `tests/fixtures/local_indexer/single_word_per_line.pdf`,
test fails on import / collection.

**Why it happens:** CONTEXT.md and OPEN_ISSUES.md both incorrectly claim the
fixture exists; it doesn't.

**How to avoid:** Wave 0 of the plan creates the fixture before any regression
test references it. Source: either user-supplied PDF that exhibits the bug,
or generate synthetically with `fitz.Document` writing per-word `Tj` operators
at distinct y-coordinates.

### Pitfall 5: D-08 QSettings vs. session JSON conflict

**What goes wrong:** Per-file opt-out persists in QSettings but `local_filter`
state persists in session JSON. User loads a session, gets old session's filter
state but the previous machine's opt-outs.

**Why it happens:** Two persistence stores for the same user-facing feature.

**How to avoid:** Surface the conflict during planning (this research does);
either revise D-08 to use session JSON (preferred), OR accept the divergence
explicitly and document it in CHANGELOG / Help.

### Pitfall 6: Layout discrepancy — MyLibraryTab is NOT a QSplitter

**What goes wrong:** Plan says "modify the existing vertical splitter" but the
existing layout is `QVBoxLayout`, not `QSplitter`.

**How to avoid:** Plan explicitly creates a `QSplitter` if user-resizability is
desired; otherwise just refactor the bottom region without promoting the outer
layout.

---

## 8. Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest (per `pyproject.toml` `[tool.pytest.ini_options]`) |
| Config file | `pyproject.toml` |
| Quick run command (per feature) | `pytest tests/test_local_<feature>.py -x` |
| Full suite command | `pytest tests/` (~2532 tests as of v7.14.0) |

### Phase 96 Requirements → Test Map

| Item | Behavior | Test Type | Automated Command | File Exists? |
|------|----------|-----------|-------------------|-------------|
| D-F5 | LOCAL hit dict has `highlight_pattern` + `snippet` contains `*` markers when regex matches | unit | `pytest tests/test_local_hit_highlighting.py -x` | ❌ Wave 0 |
| D-F5 | LOCAL hit dict falls back to first-200-chars when regex doesn't match content | unit | `pytest tests/test_local_hit_highlighting.py::test_no_match_fallback -x` | ❌ Wave 0 |
| D-F5 | search-table render shows highlighted span for LOCAL hits | integration (widget) | `pytest tests/test_local_hit_highlighting.py::test_render_pipeline -x` (with QApplication fixture) | ❌ Wave 0 |
| D-F4 | extract_pdf_pages on `single_word_per_line.pdf` triggers fallback and returns paragraph-shaped text | unit | `pytest tests/test_local_pdf_extraction_fallback.py::test_pathological_pdf_uses_fallback -x` | ❌ Wave 0 |
| D-F4 | extract_pdf_pages on `hebrew_sample.pdf` (Phase 95 fixture) still uses blocks mode (no regression) | unit | `pytest tests/test_local_pdf_extraction_fallback.py::test_good_pdf_stays_blocks -x` | ❌ Wave 0 |
| D-F4 | detection heuristic returns False when <5 lines (small sample skip) | unit | `pytest tests/test_local_pdf_extraction_fallback.py::test_small_sample_skipped -x` | ❌ Wave 0 |
| D-F1 | QSettings (or session JSON) round-trips opt-out set | unit | `pytest tests/test_local_optout_persistence.py -x` | ❌ Wave 0 |
| D-F1 | rescan preserves opt-out for surviving files, drops removed files | unit | `pytest tests/test_local_optout_persistence.py::test_rescan_preserves -x` | ❌ Wave 0 |
| D-F1 | opt-out filter composes with three-state local filter (cascade discipline) | unit | `pytest tests/test_local_optout_filter.py -x` | ❌ Wave 0 |
| D-F1 | opt-out filter applied at BOTH cascade joinpoints (search + composition) | static AST | extend `tests/test_local_filter_cascade.py` | partial — extend existing |
| NEW-1 | `desktop/result_dialog.py` no longer creates `btn_rd_open_browse` widget | static AST | `pytest tests/test_result_dialog_local_button_removed.py -x` | ❌ Wave 0 |
| NEW-2 | get_local_browse_page returns correct page on offset=+1 | unit | `pytest tests/test_local_nav_page_chunk.py::test_next_page -x` | ❌ Wave 0 |
| NEW-2 | get_local_browse_page returns None at boundary (no wrap) | unit | `pytest tests/test_local_nav_page_chunk.py::test_no_wrap -x` | ❌ Wave 0 |
| NEW-2 | View-All aggregates all pages with page/chunk separator | unit | `pytest tests/test_local_nav_page_chunk.py::test_view_all_separators -x` | ❌ Wave 0 |
| NEW-2 | PDF file uses "page" label, DOCX/TXT uses "chunk" label | unit | `pytest tests/test_local_nav_page_chunk.py::test_format_aware_label -x` | ❌ Wave 0 |
| Phase 95 invariants | All existing LOCAL guard tests stay green | regression | `pytest tests/test_local_*.py tests/test_web_library_options_no_local.py tests/test_no_raw_storage_access.py -q` | ✅ exists |

### Sampling Rate
- **Per task commit:** `pytest tests/test_local_<feature>.py -x` (sub-second)
- **Per wave merge:** `pytest tests/test_local_*.py -q` + `tests/test_web_library_options_no_local.py` (Phase 95 regression bundle)
- **Phase gate:** Full suite green before `/gsd-verify-work` — `pytest tests/ -q` AND `python -m ruff check .` AND `python scripts/check_docs.py`

### Wave 0 Gaps
- [ ] `tests/fixtures/local_indexer/single_word_per_line.pdf` — D-F4 regression fixture, **does NOT exist** (CONTEXT.md error)
- [ ] `tests/test_local_pdf_extraction_fallback.py` — D-F4 detection + fallback tests
- [ ] `tests/test_local_hit_highlighting.py` — D-F5 hit dict shape + render pipeline
- [ ] `tests/test_local_optout_persistence.py` — D-F1 round-trip + rescan-preserve
- [ ] `tests/test_local_optout_filter.py` — D-F1 filter composition
- [ ] `tests/test_result_dialog_local_button_removed.py` — NEW-1 AST guard
- [ ] `tests/test_local_nav_page_chunk.py` — NEW-2 navigation primitive + View-All separator
- [ ] Extend `tests/test_local_filter_cascade.py` — D-F1 opt-out filter at both joinpoints

*(No framework install needed — pytest already configured.)*

---

## 9. Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `get_text("text", sort=True)` is the right fallback for pathological PDF blocks | §2 | If wrong, may need `"words"`-mode reconstruction; bigger code |
| A2 | 70% single-word-line threshold is the right detection bound | §2 | Tunable parameter; not a correctness issue |
| A3 | Path-as-key is sufficient for D-F1 opt-out persistence | §3 | Edge: rename = new file = new key (correct semantically per D-09 wording) |
| A4 | LOCAL pages are contiguous 1..N (no gaps from filter or delete) | §4 | If LOCAL indexer ever skips a page mid-file, navigation could feel non-contiguous; current PyMuPDF path emits 1..N for non-empty pages only — possible gap source |
| A5 | The user-perceived "split panel" in MyLibraryTab refers to the vertical stacking of folder list + status table (not a real QSplitter) | §3 | Layout interpretation — surfaced for planner |

---

## 10. Open Questions / Open Risks

1. **D-08 vs. existing-pattern conflict** (§3) — Planner MUST resolve before
   Wave 1 lands. Recommended: revise CONTEXT.md D-08 to session JSON.
2. **`single_word_per_line.pdf` fixture provenance** (§2 + Pitfall #4) —
   Planner needs to source the actual PDF. User-supplied? Synthetic? Both?
3. **LOCAL pages contiguity (A4)** — Worth a small spike: index a PDF with
   blank pages and verify whether `p_num` values are 1..N skipping blanks
   or 1..N renumbered. Affects NEW-2 boundary detection (`offset = -1` from
   p_num=3 should land on p_num=2, not "the previous existing page").
4. **Browse panel View-All toggle UI placement** — Where does the toggle live?
   A button near the existing browse controls? A radio group? CONTEXT.md
   doesn't pin this; Claude's discretion.
5. **`btn_rd_open_browse` removal — what about the existing
   `tests/test_local_browse_panel.py::test_result_dialog_has_view_in_browse_button`
   test?** (Found at line 114 in that file.) Plan must update the test —
   either rewrite it to assert the button is GONE, or delete the test outright
   if its only purpose was Phase 95 wiring verification.

---

## 11. Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| LOCAL hit dict shape + highlight pattern | Search Engine (`genizah_core.py`) | — | Engine owns result shape; UI consumes |
| PDF text extraction + fallback | Indexer (`shared/local_indexer.py`) | — | Indexer owns PyMuPDF interaction; query path unaware |
| Per-file opt-out persistence | Desktop UI (`desktop/my_library_tab.py`) | QSettings or shared/session_persistence | User-state UI concern; not an engine concept |
| Per-file opt-out filter application | Desktop UI cascade (`genizah_app.py`) | — | UI-layer filter, matching three-state local filter pattern |
| Tree widget rendering | Desktop UI (`desktop/my_library_tab.py`) | — | Qt widget concern |
| LOCAL next/prev primitive | Search Engine (`genizah_core.py`) | — | Engine query of LOCAL index; new public method `get_local_browse_page` |
| ResultDialog page nav wiring | Desktop UI (`desktop/result_dialog.py`) | — | Existing dialog absorbs LOCAL nav via thin dispatch |
| Browse panel View-All vs. per-page toggle | Desktop UI (`genizah_app.py`) | — | UI-layer state; renders via existing `_open_local_browse` + new per-page render |
| Page/chunk separator label | Desktop UI helper or shared util | — | Pure formatting; could go either side |

---

## Sources

### Primary (HIGH confidence)
- [VERIFIED: codebase Read] `genizah_core.py:6876-6907` — `_build_local_result_dict` shape
- [VERIFIED: codebase Read] `genizah_core.py:7341-7385,7819,8335` — `SearchEngine.highlight` and call sites
- [VERIFIED: codebase Read] `genizah_core.py:6979-6999` — `format_snippet`
- [VERIFIED: codebase Read] `genizah_core.py:8354-8371` — RRF POST-dedup merge
- [VERIFIED: codebase Read] `genizah_app.py:16668-16675` — search table render
- [VERIFIED: codebase Read] `genizah_app.py:17321-17371,23532-23613` — local filter + session save
- [VERIFIED: codebase Read] `genizah_app.py:18472-18634` — `_open_local_browse` and `_get_local_full_text_for_sys_id`
- [VERIFIED: codebase Read] `desktop/result_dialog.py:120,130,236-238,343,2051-2061,2169-2199` — ResultDialog nav + highlight + button
- [VERIFIED: codebase Read] `desktop/my_library_tab.py:1-345` — MyLibraryTab layout + QSettings instance
- [VERIFIED: codebase Read] `shared/local_indexer.py:302-326` — `extract_pdf_pages`
- [VERIFIED: codebase Read] `tests/test_local_filter_persistence.py:1-13` — session-JSON pattern is established, NOT QSettings
- [VERIFIED: Glob] `tests/fixtures/local_indexer/*` — `single_word_per_line.pdf` does NOT exist (CONTEXT.md error)
- [VERIFIED: git log] `single_word_per_line.pdf` never committed
- [VERIFIED: requirements-lock.txt] `pymupdf==1.27.2.3`

### Secondary (HIGH confidence — cited official docs)
- [CITED: pymupdf.readthedocs.io/en/latest/textpage.html] PyMuPDF `get_text()` modes (text/blocks/dict/words/html)
- [CITED: pymupdf.readthedocs.io/en/latest/recipes-text.html] Recommended fallback for one-word-per-line: `sort=True` on `get_text("text")`, `get_text("words")` reconstruction
- [CITED: github.com/pymupdf/PyMuPDF/issues/2199] RTL handling improvements in 1.27.x

### Tertiary (LOW confidence — surfaced for human verification)
- [ASSUMED] 70% single-word-line threshold is correct — taken from Phase 95 `_join_fragmented_lines` precedent at 60% inverse but tuned slightly more conservatively. May need empirical tuning against the actual `single_word_per_line.pdf` once obtained.

---

## Metadata

**Confidence breakdown:**
- Highlight pipeline map: HIGH — direct code reads at all relevant line numbers
- PDF extraction fix: HIGH for mode landscape (PyMuPDF docs); MEDIUM for exact threshold value (assumption A2)
- D-F1 persistence: HIGH for conflict identification; planner choice on resolution
- NEW-2 navigation: HIGH — analog primitive `get_browse_page` directly inspected; LOCAL gap precisely characterized

**Research date:** 2026-05-24
**Valid until:** 2026-06-23 (30 days — Phase 95 codebase is stable, PyMuPDF 1.27.x line stable, no upstream changes expected)

## RESEARCH COMPLETE
