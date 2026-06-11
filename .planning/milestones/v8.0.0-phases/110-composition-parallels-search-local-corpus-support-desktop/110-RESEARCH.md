# Phase 110: Composition / Parallels Search — LOCAL Corpus Support — Research

**Researched:** 2026-06-08
**Domain:** Desktop PyQt6 composition/parallels tab — corpus selector wiring + LOCAL-aware export
**Confidence:** HIGH (all findings verified directly from current source files)

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

- **D-01:** Defer ALL of Component B to post-v8.0.0 (JSA-01/02/03, JWB-05).
- **D-02:** Desktop-only. No web surface exists for composition/parallels or LOCAL.
- **D-03:** This is the last v8.0.0 phase. After verification → `/release` v8.0.0.
- **D-04:** Pre-search `Genizah / Local / ALL` dropdown on the composition tab, mirroring the Search-tab selector pattern.
- **D-05:** Pre-search scoping ONLY — do NOT activate the post-search LOCAL filter on the composition surface. The dormant `local_filter_btn_composition` stays inactive.
- **D-06:** Corpus is ORTHOGONAL to the composition search MODE — the selector governs which corpus is searched for both standard and Lab composition. "Lab Mode" must NOT be hardwired to LOCAL.
- **D-07:** `Local` = LOCAL corpus only; `ALL` = Genizah + LOCAL merged; `Genizah` = unchanged from today.
- **D-08:** Stale LOCAL LAB index must not silently drop LOCAL hits — surface a rebuild/staleness signal.
- **D-09:** Parallels inherits the selector (no separate wiring needed).
- **D-10:** `export_comp_report` becomes LOCAL-aware via Phase 103 helpers.
- **D-11:** i18n from line one — every new string is `tr()`-wrapped with keys in `genizah_translations.TRANSLATIONS`.
- **D-12:** No LOCAL data ever reaches the cloud — the three v7.14 cloud-write gates remain inviolate.
- **D-13:** Genizah default path is a strict non-regression baseline.

### Claude's Discretion

- Exact placement/label of `comp_corpus_scope_combo` on the composition tab (lean: next to Lab Mode / Deep Scan, matching Search-tab layout).
- Whether to hide/remove the dormant `local_filter_btn_composition` (verify it is separate from `local_filter_btn_parallels` before acting — they ARE separate, see RF-6 below).
- Staleness signal styling (banner vs inline note vs toast).
- Helper decomposition for `run_composition` / thread parameterization.

### Deferred Ideas (OUT OF SCOPE)

- Component B (JSA-01, JSA-02, JSA-03, JWB-05) — deferred to post-v8.0.0.
- Post-search LOCAL filter activation on the composition surface.
- Web composition/parallels LOCAL.
- Join Workbench (Phases 106–109 complete and untouched).
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| COMP-LOC-01 | Pre-search Genizah/Local/ALL corpus selector on comp/parallels tab, orthogonal to mode (standard + Lab both honor it; Lab Mode NOT hardwired to LOCAL) | RF-1 confirms Lab Mode has genuine extra semantics beyond LOCAL; RF-3 maps the end-to-end param chain |
| COMP-LOC-02 | Composition executes against selected corpus; ALL = merged; stale LOCAL LAB index surfaces a signal rather than silently omitting hits | RF-2 recommends RRF k=60 or score-interleaved merge; RF-4 identifies the staleness machinery |
| EXP-F3 | `export_comp_report` becomes LOCAL-aware across all four formats (xlsx/csv/txt/docx) using Phase 103 helpers | RF-5 maps every insertion point |
</phase_requirements>

---

## Summary

Phase 110 wires the existing LOCAL ("My Library") corpus into the composition/parallels search surface and makes `export_comp_report` LOCAL-aware. The Search-tab corpus selector is a fully-shipping template and most of this phase is "do the same thing on the composition tab."

The key finding from the code audit is that **the standard composition path (`search_composition_logic`) already has a half-wired LOCAL LAB hook** (lines 9071–9167 in `genizah_core.py`) that silently skips when the LAB index is stale (`_lab_fresh` gate). The Lab composition path (`lab_composition_search`) similarly has a LOCAL LAB hook (lines 1601–1704). Neither path has a `corpus_scope` parameter today — both unconditionally include LOCAL when fresh. The phase must add `corpus_scope` to both paths and expose a staleness signal to the UI.

**Lab Mode** has genuine extra semantics beyond "search LOCAL": it uses a separate n-gram fingerprint scoring algorithm (not BM25), a different scan strategy (`deep_scan`/`scan_limit`), and its own index (`lab_index` + `local_lab_searcher`). It is NOT merely a proxy for LOCAL corpus. The corpus selector and Lab Mode are orthogonal — D-06 is correct and implementable.

**Primary recommendation:** Add `comp_corpus_scope_combo` to `create_composition_tab` matching the Search-tab pattern exactly. Add `corpus_scope` param to both thread classes and both engine methods. For `ALL` scope on composition use score-interleaved merge (not RRF k=60 — composition scores are span-length-based, not BM25, so RRF's rank-normalization assumption does not hold). Emit a staleness warning label/banner when `corpus_scope in ('local', 'all')` and `local_lab_searcher_stale` is True.

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Corpus selector UI | Desktop / PyQt6 UI | — | QComboBox in `create_composition_tab`; no web surface |
| Corpus scope state persistence | Desktop session JSON | — | Mirrors `_search_corpus_scope` pattern in `_save_session` |
| Composition search routing (standard) | `SearchEngine.search_composition_logic` | CompositionThread | Engine owns the Tantivy query loop; thread is a thin wrapper |
| Composition search routing (Lab) | `LabEngine.lab_composition_search` | LabCompositionThread | LabEngine owns the fingerprint scoring loop |
| LOCAL-aware export | `export_comp_report` in `genizah_app.py` | `shared/export_dossier.py`, `shared/docx_export.py` | Partitioning logic in the app; per-row helpers are shared |
| Staleness detection | `SearchEngine._check_local_lab_freshness` / `LabEngine._check_local_lab_freshness` | UI warning label | Both engines already have the freshness check |
| Cloud-write gates | `shared/search_serializer.py` (line 582), `shared/corrections_service.py`, `lists_sync.py` | — | Pre-existing, must not be broken |

---

## Research Flags — Resolved

### RF-1: What does "Lab Mode" composition do beyond searching LOCAL?

**VERIFIED: Lab Mode has genuine extra semantics orthogonal to corpus.**

`LabCompositionThread` (`gui_threads.py:216`) calls `lab_engine.lab_composition_search(...)` with two parameters that `CompositionThread` does NOT have:

```python
# gui_threads.py:224-235
def __init__(self, lab_engine, text, mode, chunk_size=None, excluded_ids=None, filter_text=None,
             deep_scan=False, scan_limit=50000, ...):
    ...
    self.deep_scan = deep_scan    # NOT in CompositionThread
    self.scan_limit = scan_limit  # NOT in CompositionThread
```

Inside `lab_composition_search` (`genizah_core.py:1511-1522`):

```python
if deep_scan:
    # Executes _execute_batched_search(q_obj, limit_override=scan_limit)
    # — searches the ENTIRE LAB index in batches
    iterator = self._execute_batched_search(q_obj, ...)
else:
    # Top-5000 only
    res = self.lab_searcher.search(q_obj, 5000)
    iterator = res.hits
```

Additionally, Lab Mode uses a **fundamentally different scoring algorithm**: n-gram fingerprint matching (`text_to_fingerprint`) against the `lab_index` (built from the full corpus), NOT the standard BM25 + regex path. The LAB index schema (`fingerprint` / `fingerprint_dyn` fields) differs from the main Tantivy index (`content` field). Lab Mode also uses `_calculate_match_metrics` (custom proximity scoring) and `_is_phrase_statistically_weak` filtering.

**Conclusion for D-06:** Lab Mode is NOT just a proxy for LOCAL corpus. It is a distinct search algorithm (fingerprint-based vs BM25+regex) with distinct scan controls (deep_scan/scan_limit). The corpus selector (Genizah/Local/ALL) and the Lab Mode toggle are genuinely orthogonal axes. Do NOT retire the Lab Mode checkbox.

**Design for D-06:** When Lab Mode is ON and corpus_scope is:
- `genizah` → search only `lab_index` (Genizah LAB), no LOCAL LAB
- `local` → search only `local_lab_searcher` (LOCAL LAB), no Genizah LAB
- `all` → search both `lab_index` AND `local_lab_searcher`, merge results

Currently `lab_composition_search` always searches both (subject to `_check_local_lab_freshness`). Add a `corpus_scope` parameter to `lab_composition_search` and gate the two index queries on it.

**Landmine:** The `on_lab_mode_toggled_search` handler syncs the search-tab Lab Mode toggle to the composition tab (`btn_lab_mode_toggle_comp`) and vice versa (`on_lab_mode_toggled_comp` syncs to the search tab). This cross-sync is intentional and must be preserved. The corpus selector for composition is SEPARATE from the search-tab corpus selector — they should NOT cross-sync.

---

### RF-2: ALL-scope merge semantics for composition

**VERIFIED: RRF k=60 is NOT appropriate for composition; score-interleaved merge is recommended.**

The regular search RRF (`genizah_core.py:8823-8833`) works because it fuses two ranked lists from independent BM25 indexes:

```python
# genizah_core.py:8824
if corpus_scope != "genizah" and getattr(self, "local_searcher", None) is not None:
    local_hits = self._query_local_index(query_str, mode, gap, regex=regex)
    if local_hits:
        deduped = self._rrf_merge(deduped, local_hits, k=60)
```

`_rrf_merge` fuses by rank because BM25 scores from two independent indexes are not on the same scale (`genizah_core.py:7365`).

**Composition scores are different.** In `search_composition_logic`, the score is `base_score = sum(e-s for s,e in merged)` — a span-length total. In `lab_composition_search`, the score is `data['total_score']` — a custom fingerprint proximity score. These are NOT BM25 scores; they are already on a meaningful absolute scale within each search. Ranks across the two populations are comparable by score magnitude.

**Recommendation for ALL scope in `search_composition_logic`:** Concatenate Genizah and LOCAL LAB hits into a single `doc_hits` map (which is already a `defaultdict` keyed by uid), then sort by score in `build_items`. Since both Genizah and LOCAL LAB contributions write into the same `doc_hits[uid]` accumulator, they naturally interleave by score when `build_items` sorts at the end. This is the SAME as what already happens in the `_lab_fresh`-gated LOCAL LAB hook (lines 9071–9167) — the LOCAL results are simply merged into `doc_hits` alongside Genizah results.

**Recommendation for ALL scope in `lab_composition_search`:** Same pattern — LOCAL LAB hits merge into the same `results_map` dict (which already happens at lines 1601–1704). With `corpus_scope='all'`, both Genizah and LOCAL LAB queries run; with `corpus_scope='local'`, skip the Genizah `lab_searcher` loop; with `corpus_scope='genizah'`, skip the LOCAL LAB loop.

**Conclusion:** Score-interleaved merge (the existing accumulator pattern) is the right approach for composition. No RRF needed. The planner should add a `corpus_scope` parameter that gates which index queries run — the existing merge logic already handles the combination correctly.

**Reconciling the two existing LOCAL paths:**

The standard path (`search_composition_logic`) has a LOCAL LAB hook gated on `_lab_fresh` (line 9088). The Lab path (`lab_composition_search`) has its own LOCAL LAB hook also gated on `_check_local_lab_freshness` (line 1609). These are the SAME mechanism — both check `.meta.json` against the current weights hash. After adding `corpus_scope`, the gate logic changes:

| corpus_scope | Standard path | Lab path |
|---|---|---|
| `genizah` | Skip LOCAL LAB hook entirely | Skip LOCAL LAB loop |
| `local` | Skip Genizah Tantivy loop; run LOCAL LAB only (needs new local-only branch) | Skip Genizah lab_index loop; run LOCAL LAB only |
| `all` | Run both (existing behavior, gated on freshness) | Run both (existing behavior, gated on freshness) |

The known Phase-97 weights-hash mismatch (see RF-4) affects only `all` scope on standard composition. The fix is in RF-4.

---

### RF-3: End-to-end parameter thread

**VERIFIED: Complete chain for `corpus_scope` parameter.**

**Search-tab template (proven, shipping):**

```
corpus_scope_combo.currentData()           # genizah_app.py:16973-16974
  → SearchThread(corpus_scope=_corpus_scope) # gui_threads.py:86
    → searcher.execute_search(corpus_scope=...) # gui_threads.py:112
      → [local-only branch at :8430, RRF merge at :8824]
```

**Composition tab — new chain to build (mirroring the above):**

```
comp_corpus_scope_combo.currentData()      # new combo in create_composition_tab
  → run_composition() reads it             # genizah_app.py:21654 (modified)
    → CompositionThread(corpus_scope=...)  # gui_threads.py:171 (new param)
       → searcher.search_composition_logic(corpus_scope=...) # gui_threads.py:201 (modified call)
         → [LOCAL LAB gate controlled by corpus_scope]     # genizah_core.py:8892+

    → LabCompositionThread(corpus_scope=...) # gui_threads.py:224 (new param)
       → lab_engine.lab_composition_search(corpus_scope=...) # gui_threads.py:258 (modified call)
         → [Genizah loop gated, LOCAL LAB loop gated]     # genizah_core.py:1402+
```

**Concrete changes needed:**

1. `create_composition_tab` (`genizah_app.py:6489`): add `self.comp_corpus_scope_combo = QComboBox()` with items `("גניזה"/"Genizah", "genizah")` / `("מקומי"/"Local", "local")` / `("הכול"/"ALL", "all")` — same `CURRENT_LANG` guard as the Search-tab combo (`genizah_app.py:5953`). Wire `currentIndexChanged` to a new `_on_comp_corpus_scope_changed` handler. Restore from session.

2. `_on_comp_corpus_scope_changed` (new handler): persist `self._comp_corpus_scope` and call `_save_session()` — mirrors `_on_corpus_scope_changed` at line 16846.

3. `run_composition` (`genizah_app.py:21654`): read `_corpus_scope = self.comp_corpus_scope_combo.currentData() or 'genizah'`. Pass to both thread constructors.

4. `CompositionThread.__init__` (`gui_threads.py:171`): add `corpus_scope='genizah'` param; store as `self.corpus_scope`. Pass to `search_composition_logic` call at line 201.

5. `LabCompositionThread.__init__` (`gui_threads.py:224`): add `corpus_scope='genizah'` param; store as `self.corpus_scope`. Pass to `lab_composition_search` call at line 258.

6. `search_composition_logic` (`genizah_core.py:8892`): add `corpus_scope: str = 'genizah'` param. Modify the LOCAL LAB hook (lines 9071–9167): when `corpus_scope == 'genizah'` skip the hook entirely; when `corpus_scope == 'local'` skip the Genizah Tantivy loop (lines 9960–9069) and run LOCAL-only; when `corpus_scope == 'all'` keep existing behavior (gated on freshness).

7. `lab_composition_search` (`genizah_core.py:1402`): add `corpus_scope: str = 'genizah'` param. Gate the Genizah `lab_searcher` loop (line 1481+) on `corpus_scope != 'local'`; gate the LOCAL LAB loop (lines 1601–1704) on `corpus_scope != 'genizah'`.

**Session persistence:** Add `'comp_corpus_scope': getattr(self, '_comp_corpus_scope', 'genizah')` to the `composition_search` dict in `_save_session` (alongside existing keys at line 25048). Restore in `_restore_session` (alongside the block at line 25283).

---

### RF-4: LAB staleness signal (D-08)

**VERIFIED: Two freshness-check methods exist, both work, the weights-hash mismatch is still live.**

**Existing machinery:**

- `SearchEngine._check_local_lab_freshness` (`genizah_core.py:7111`): reads `.meta.json`, compares `weights_hash` to `_current_lab_weights_hash()`. Sets `self.local_lab_searcher_stale = True` on mismatch.
- `LabEngine._check_local_lab_freshness` (`genizah_core.py:824`): identical logic on LabEngine. Sets `self.local_lab_searcher_stale = True`.
- `build_lab_side_index` (`shared/local_indexer.py:4285`) writes `.meta.json` with `weights_hash` at line 4448.

**The Phase-97 weights-hash mismatch (still live):**

`SearchEngine._current_lab_weights_hash` (`genizah_core.py:7079`) and `LabEngine._current_lab_weights_hash` (`genizah_core.py:808`) both hash `{dynamic_rank_map, use_dynamic_weights}` from their own instance state. The `.meta.json` is written by `build_lab_side_index`, which receives the `lab_weights` dict from the caller (typically `LabEngine.dynamic_rank_map`). When `SearchEngine.search_composition_logic` runs the freshness check, it calls `self._check_local_lab_freshness()` using **SearchEngine's** `dynamic_rank_map`, which may differ from the `LabEngine.dynamic_rank_map` that was used to build the index. This is the known mismatch.

**Fix for Phase 110:** The correct fix is to ensure `SearchEngine._current_lab_weights_hash` uses the same weights source as the index was built with. Since the index is always built via `LabEngine.rebuild_lab_index` → `build_lab_side_index`, the `.meta.json` always stores the LabEngine's hash. The fix is: when `SearchEngine.search_composition_logic` checks freshness, it should compare against the **LabEngine's** hash, not SearchEngine's. The simplest approach is to inject the `LabEngine` reference into `SearchEngine` so the standard path's freshness check can read the LabEngine's weights. Alternatively, store the weights hash as a deterministic function of the index content (already done via `.meta.json`) and ensure both engines normalize identically — which they do (both hash `{"dynamic_rank_map": ..., "use_dynamic_weights": ...}`). The actual bug is that `SearchEngine.dynamic_rank_map` is never set (it defaults to `None`) so `SearchEngine._current_lab_weights_hash()` always produces the hash of `{dynamic_rank_map: None, use_dynamic_weights: False}`, which will NEVER match the `.meta.json` hash if the LabEngine built the index with a non-None `dynamic_rank_map`. This means the standard composition path's freshness check ALWAYS returns False, and LOCAL LAB hits are ALWAYS silently dropped from standard composition.

**Concrete fix:** Pass the LabEngine's weights hash to the freshness comparison in the standard path. Options:
- (a) Add `lab_engine` reference to `SearchEngine` so `_check_local_lab_freshness` reads from it.
- (b) Add a `_lab_weights_hash_override` attribute to `SearchEngine` that is set by the app after LabEngine is initialized, used in `_current_lab_weights_hash`.
- (c) Simplest: in `search_composition_logic`, accept an optional `lab_engine` param and use it for the freshness check.

Option (b) is the least invasive: set `self.searcher._lab_weights_hash_override = self.lab_engine._current_lab_weights_hash()` in `GenizahGUI.__init__` after both engines are initialized, and whenever `LabEngine.rebuild_lab_index` completes. Then `SearchEngine._current_lab_weights_hash` checks for this override.

**D-08 staleness signal to the user:** When `corpus_scope in ('local', 'all')` AND `local_lab_searcher_stale` is True (after the freshness check runs), the UI should show a warning. A small inline label below the composition controls (e.g., `lbl_comp_local_stale`) with text `tr("LOCAL index is outdated — rebuild in My Library tab")` fits the composition tab layout. Show/hide it in `on_comp_scan_finished` based on the stale flag. Do NOT trigger a rebuild here — that stays on the background worker per the `feedback_no_auto_reindex_in_init` constraint.

**Landmine:** Never call `_check_local_lab_freshness()` on the UI thread without a try/except. The existing guard in `search_composition_logic` (lines 9077–9084) demonstrates the correct pattern.

---

### RF-5: EXP-F3 — mapping `export_comp_report` to Phase 103 helpers

**VERIFIED: Four formats all need LOCAL-aware branches. Insertion points identified.**

**Current state of `export_comp_report` (`genizah_app.py:20447`):**

The function collects composition items (`c_main`, `c_appx`, `c_filt`, `c_filt_appx`, `c_known`) and builds `table_rows` via `add_rows()`. All four formats (xlsx, csv, docx, txt) consume `table_rows` (or equivalent). The items are grouped manuscript-level objects (`type: 'part'`, `type: 'manuscript'`, or fallback), each containing a `pages` list. Individual pages have `raw_header`, `source_ctx`, `text`, `score`.

**LOCAL detection in composition items:**

Composition results from `search_composition_logic` and `lab_composition_search` return raw items with `uid`, `raw_header`, `src_lbl`, `source_ctx`, `text`, `score`. After grouping (`group_pages_by_manuscript`), items become `type: 'manuscript'` or `type: 'part'` with `pages`. LOCAL hits will have `uid` values starting with `LOCAL_` (per `is_local_sys_id`), `src_lbl == 'LOCAL'` (the `source` field from the LOCAL LAB side-index), and `raw_header` containing the LOCAL sys_id.

**Detection pattern:** `item.get('src_lbl') == 'LOCAL'` or `is_local_sys_id(item.get('sys_id', ''))` (for grouped items). For individual pages: check the page's `uid` via `is_local_sys_id`.

**Phase 103 helpers:**

- `build_local_document_row(filename, parent_folder, full_filepath, page, matched_text_raw)` → `shared/export_dossier.py:1239` — returns a 5-cell row `[filename, parent_folder, full_filepath, page, matched_text_raw]`
- `local_documents_header_row(lang)` → `shared/export_dossier.py:418` — returns `["Filename", "Parent Folder", "Full Filepath", "Page", "Matched Text"]`
- `write_docx_result_block(doc, result_dict, filepath, lang)` → `shared/docx_export.py:64` — works for both LOCAL and Genizah when `result_dict['display']['source'] == 'LOCAL'`

**Batch filepath resolution:** Call `_prime_local_filepath_cache(all_flat_items)` at the start of `export_comp_report`, mirroring the pattern in `export_results` (`genizah_app.py:20073`). The flat item list must contain the raw page-level items before grouping, or alternatively extract LOCAL sys_ids from `c_main`/`c_appx` etc. and call `indexer.get_filepaths(local_ids)` directly.

**Insertion points for EXP-F3:**

1. **Top of `export_comp_report` (before `table_rows` construction):** Prime `_local_filepath_cache` for all LOCAL sys_ids in the export set. Add `_has_local = any(is_local_sys_id(item_sys_id) for item in all_items)` flag.

2. **Inside `add_rows()` (the shared row-builder for xlsx/csv/docx):** When the item is a LOCAL manuscript (detected via `src_lbl == 'LOCAL'` on the item or its pages), call `build_local_document_row(...)` and use `local_documents_header_row()` columns instead of the standard 10-column schema.

3. **Alternatively (cleaner):** Use a dual-schema approach: Genizah items go into the standard 10-column `table_rows`; LOCAL items go into a parallel 5-column `local_table_rows`. When both are non-empty, write them as separate sheets (xlsx: "Report View" + new "Local Documents" sheet) or separate CSV/TXT sections. When LOCAL-only, write LOCAL schema only. This exactly mirrors the Phase 103 search export pattern.

4. **TXT format:** The `_fmt_ms_entry` local function (line 21117) needs a LOCAL branch: instead of `shelf/title`, output `filename/parent_folder/filepath/page`.

5. **DOCX format:** Instead of building `table_rows` and writing a table, call `write_docx_result_block(doc, page_as_result_dict, filepath=..., lang=CURRENT_LANG)` per LOCAL page. This helper handles both LOCAL and Genizah correctly.

**Genizah path unchanged:** When `_has_local` is False, `export_comp_report` is byte-for-byte identical to today.

**Landmine:** Composition items after grouping (`type: 'manuscript'` / `type: 'part'`) do NOT have a `display` dict — they have `sys_id`, `raw_header`, `src_lbl`. The `item['display']['source'] == 'LOCAL'` detection pattern from search export does NOT apply directly to composition grouped items. Use `is_local_sys_id(item.get('sys_id', ''))` or `item.get('src_lbl') == 'LOCAL'` instead.

**Landmine 2:** `build_local_document_row` expects `filename`, `parent_folder`, `full_filepath`, `page`, `matched_text_raw`. The composition export does not currently compute `matched_text_raw` for LOCAL hits — it has `source_ctx` (source text context) and `text` (manuscript text). Use `source_ctx` as `matched_text_raw` (the source passage with `*` highlights), which matches the Phase 103 search export semantics.

---

### RF-6: Parallels wrapper inherits the selector — confirmed

**VERIFIED: No separate wiring needed.**

`browse_search_parallels` (`genizah_app.py:10681`) calls `send_result_to_composition` (`genizah_app.py:19944`), which calls `self.tabs.setCurrentWidget(self.composition_tab)` and populates `comp_text_area`. It does NOT call `run_composition` directly. The user then manually clicks "Analyze" (or presses the run button). Therefore, whatever scope is currently in `comp_corpus_scope_combo` will be used when the search runs.

**Separate `local_filter_btn_parallels` vs `local_filter_btn_composition`:** CONFIRMED they are SEPARATE controls.
- `local_filter_btn_composition` is created at `genizah_app.py:6820` (inside `create_composition_tab`)
- `local_filter_btn_parallels` is created at `genizah_app.py:7023` (inside the parallels results area)
- They have separate state variables (`_local_filter_state_composition` / `_local_filter_state_parallels`)
- They are NOT the same widget

**Implication for D-05 (hiding the dormant comp filter):** Hiding `local_filter_btn_composition` (which is already `setVisible(False)` at line 6823) will NOT affect `local_filter_btn_parallels`. Safe to leave as-is or remove the scaffolding for `local_filter_btn_composition` — but verify the `_toggle_local_filter_composition` handler and `_update_local_filter_btn_composition` are cleaned up too if removed.

**Default scope for parallels-from-browse launch:** The current `comp_corpus_scope_combo` value is used. Since the default is `genizah` and the user can change it, no special handling is needed. Recommend leaving the default as `genizah` (same as Search tab default).

---

## Architecture Patterns

### System Architecture Diagram

```
[comp_corpus_scope_combo]
       |
       v
[run_composition()]  ←──── [browse_search_parallels → send_result_to_composition]
       |
       |─── Lab Mode ON? ──→ [LabCompositionThread(corpus_scope)]
       |                            |
       |                            v
       |                   [lab_engine.lab_composition_search(corpus_scope)]
       |                      |── corpus='genizah': query lab_index only
       |                      |── corpus='local':   query local_lab_searcher only
       |                      └── corpus='all':     query both, merge in results_map
       |
       └─── Lab Mode OFF? ──→ [CompositionThread(corpus_scope)]
                                    |
                                    v
                        [searcher.search_composition_logic(corpus_scope)]
                          |── corpus='genizah': query Tantivy only (today's behavior)
                          |── corpus='local':   query LOCAL LAB only (new branch)
                          └── corpus='all':     query Tantivy + LOCAL LAB, merge in doc_hits

[on_comp_scan_finished]
    |── stale check → show lbl_comp_local_stale if stale
    └── results → display_comp_results (unchanged)

[export_comp_report(fmt)]
    |── _prime_local_filepath_cache(all_items)
    |── partition: LOCAL items → local_table_rows, Genizah items → table_rows
    └── per format:
         xlsx: "Report View" + "Raw Data" (Genizah) + "Local Documents" (LOCAL, if any)
         csv: Genizah rows + LOCAL rows (annotated)
         txt: _fmt_ms_entry LOCAL branch
         docx: write_docx_result_block per item (already LOCAL-aware)
```

### Recommended Project Structure

No new files needed. All changes are in existing files:
- `genizah_app.py` — UI wiring, `run_composition`, `export_comp_report`
- `gui_threads.py` — `CompositionThread`, `LabCompositionThread` (new `corpus_scope` param)
- `genizah_core.py` — `search_composition_logic`, `lab_composition_search` (new `corpus_scope` param + staleness fix)
- `genizah_translations.py` — new i18n keys
- `tests/` — new test files for each requirement

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| LOCAL filepath batch lookup | Per-row `get_filepath()` SQLite calls | `_prime_local_filepath_cache` + `indexer.get_filepaths(ids)` | N SQLite calls on export freezes UI; batch is O(1) |
| LOCAL row schema for export | Custom column definitions | `build_local_document_row` + `local_documents_header_row` from `shared/export_dossier.py` | Phase 103 helpers are tested, cover all formats |
| DOCX LOCAL blocks | Custom paragraph/table logic | `write_docx_result_block(doc, result_dict, filepath, lang)` from `shared/docx_export.py` | Already handles LOCAL vs Genizah branching |
| LAB staleness detection | Custom hash comparison | `_check_local_lab_freshness()` + `local_lab_searcher_stale` on both `SearchEngine` and `LabEngine` | Both exist; wire them to the UI banner |
| Corpus scope combo | Language-conditional addItem loop | Reuse the exact same `CURRENT_LANG` guard pattern from `corpus_scope_combo` at `genizah_app.py:5953` | One copy of the pattern, proven in production |

---

## Common Pitfalls

### Pitfall 1: Dormant `local_filter_btn_composition` (D-05 trap)
**What goes wrong:** The button exists at `genizah_app.py:6820` with `setVisible(False)`. If a developer removes it thinking it's dead code, the session-persistence code at lines 25058–25301 that reads/writes `_local_filter_state_composition` will throw AttributeError.
**Why it happens:** The state (`_local_filter_state_composition`) is persisted separately from the button visibility.
**How to avoid:** If hiding/removing the button, also clean up `_toggle_local_filter_composition`, `_update_local_filter_btn_composition`, and the session keys.
**Warning signs:** AttributeError mentioning `local_filter_btn_composition` on startup or session restore.

### Pitfall 2: LOCAL items lack `display` dict after grouping
**What goes wrong:** `export_comp_report` gets grouped manuscript items (`type: 'manuscript'`) which have `sys_id` and `src_lbl` but NOT `display`. Using `item['display']['source'] == 'LOCAL'` (the search-export pattern) will KeyError.
**Why it happens:** Composition grouping strips the item shape down — `group_pages_by_manuscript` returns items with `sys_id`, `raw_header`, `score`, `pages`, not the raw search-result `display` dict.
**How to avoid:** Use `is_local_sys_id(item.get('sys_id', ''))` or `item.get('src_lbl') == 'LOCAL'` for LOCAL detection in `export_comp_report`. Import `is_local_sys_id` from `shared.local_sys_id`.

### Pitfall 3: session save/restore ordering for `comp_corpus_scope`
**What goes wrong:** `create_composition_tab` runs before `_restore_session`. If the combo reads `getattr(self, '_comp_corpus_scope', 'genizah')` before the session is restored, it shows `genizah` and then the restore overwrites the state field but doesn't update the combo.
**Why it happens:** Same issue as the Search-tab scope — see the `blockSignals` pattern at `genizah_app.py:24998-25003`.
**How to avoid:** In `_restore_session`, after setting `self._comp_corpus_scope`, call `self.comp_corpus_scope_combo.blockSignals(True)`, `setCurrentIndex(...)`, `blockSignals(False)` — identical to the search-tab scope restore.

### Pitfall 4: `search_composition_logic` LOCAL-only branch lacks the Tantivy loop skip
**What goes wrong:** For `corpus_scope='local'`, if only the LOCAL LAB hook is run (lines 9071–9167) but the Genizah Tantivy loop (lines 9960–9069) also runs, Genizah results appear alongside LOCAL results even in LOCAL-only mode.
**Why it happens:** The current code structure runs the Genizah Tantivy loop unconditionally (lines 9059–9069) and then optionally adds LOCAL LAB results afterward.
**How to avoid:** Wrap the Genizah Tantivy loop in `if corpus_scope != 'local':` and wrap the LOCAL LAB hook in `if corpus_scope != 'genizah':`.

### Pitfall 5: `_save_session` called before composition tab is created
**What goes wrong:** `_on_comp_corpus_scope_changed` calls `_save_session()`, which reads `self.comp_corpus_scope_combo.currentData()` (and other comp tab attributes). If called before `create_composition_tab` has run, AttributeError.
**Why it happens:** Qt combo signals can fire during setCurrentIndex during init.
**How to avoid:** Use `getattr(self, 'comp_corpus_scope_combo', None)` guards in the handler. The existing `_on_corpus_scope_changed` already uses `getattr` defensively.

### Pitfall 6: weights-hash mismatch causes silent LOCAL LAB drop (still live)
**What goes wrong:** `SearchEngine._current_lab_weights_hash()` always returns hash of `{dynamic_rank_map: None, use_dynamic_weights: False}` (SearchEngine never gets dynamic weights). `.meta.json` stores the LabEngine's hash (with actual weights). These never match. The `_lab_fresh` guard at `genizah_core.py:9088` always returns False. LOCAL LAB hits are ALWAYS silently dropped from standard composition.
**Root cause:** `SearchEngine.dynamic_rank_map` is never populated — it's used only for the regular search, not compositions. The mismatch was documented in the Phase-97 OPEN_ISSUES but not fixed.
**How to avoid:** Fix described in RF-4: inject the LabEngine's current weights hash into SearchEngine (Option b: `_lab_weights_hash_override` attribute). Wire it at GenizahGUI init and after each LabEngine rebuild.

---

## Code Examples

### Search-tab corpus selector (template to mirror)

```python
# genizah_app.py:5953 — corpus selector creation (VERIFIED)
self.corpus_scope_combo = QComboBox()
if CURRENT_LANG == 'he':
    self.corpus_scope_combo.addItem("גניזה", "genizah")
    self.corpus_scope_combo.addItem("מקומי", "local")
    self.corpus_scope_combo.addItem("הכול", "all")
else:
    self.corpus_scope_combo.addItem("Genizah", "genizah")
    self.corpus_scope_combo.addItem("Local", "local")
    self.corpus_scope_combo.addItem("ALL", "all")
self.corpus_scope_combo.setToolTip(tr("Select which corpus to search"))
self.corpus_scope_combo.setFixedWidth(90)
_saved_scope = getattr(self, '_search_corpus_scope', 'genizah')
_scope_idx = self.corpus_scope_combo.findData(_saved_scope)
if _scope_idx >= 0:
    self.corpus_scope_combo.setCurrentIndex(_scope_idx)
self.corpus_scope_combo.currentIndexChanged.connect(self._on_corpus_scope_changed)
```

### `_on_corpus_scope_changed` handler (template)

```python
# genizah_app.py:16846 — persist scope (VERIFIED)
def _on_corpus_scope_changed(self, _index):
    scope = self.corpus_scope_combo.currentData() or "genizah"
    self._search_corpus_scope = scope
    self._save_session()
```

### RRF merge in execute_search (reference, NOT for composition)

```python
# genizah_core.py:8824 — only for search, NOT composition (VERIFIED)
if corpus_scope != "genizah" and getattr(self, "local_searcher", None) is not None:
    local_hits = self._query_local_index(query_str, mode, gap, regex=regex)
    if local_hits:
        deduped = self._rrf_merge(deduped, local_hits, k=60)
```

### `search_composition_logic` LOCAL LAB hook (today's gating)

```python
# genizah_core.py:9076-9088 — CURRENT freshness gate (VERIFIED)
try:
    _lab_fresh = self._check_local_lab_freshness()
except Exception as _lab_fresh_exc:
    LOGGER.warning("search_composition_logic: _check_local_lab_freshness raised %r ...", _lab_fresh_exc)
    _lab_fresh = False
_scl_is_searchable = getattr(_scl_tab, "is_searchable", True) if _scl_tab else True
if not was_cancelled and _lab_fresh and _scl_is_searchable:
    # ... LOCAL LAB scan loop
```

After Phase 110, gate becomes:
```python
if not was_cancelled and corpus_scope != 'genizah' and _lab_fresh and _scl_is_searchable:
```

And the Genizah Tantivy loop (currently always runs) gets:
```python
if corpus_scope != 'local':
    # ... existing Tantivy scan
```

### `_prime_local_filepath_cache` (batch filepath for export)

```python
# genizah_app.py:19262 — batch prime pattern (VERIFIED)
def _prime_local_filepath_cache(self, results):
    self._local_filepath_cache = {}
    try:
        local_ids = [
            sid for r in (results or [])
            for sid in ((r.get('display', {}) or {}).get('id') or r.get('sys_id', ''),)
            if sid and (r.get('display', {}) or {}).get('source') == 'LOCAL'
        ]
        # ... indexer.get_filepaths(local_ids)
```

For composition items (which lack `display`), adapt to:
```python
local_ids = [
    item.get('sys_id', '')
    for item in all_items
    if is_local_sys_id(item.get('sys_id', ''))
]
```

### `build_local_document_row` signature

```python
# shared/export_dossier.py:1239 (VERIFIED)
def build_local_document_row(filename, parent_folder, full_filepath, page, matched_text_raw, sanitize_fn=None):
    # returns [filename, parent_folder, full_filepath, page, matched_text_raw]
```

### `write_docx_result_block` for LOCAL

```python
# shared/docx_export.py:64 — already handles LOCAL (VERIFIED)
# is_local = d.get("source") == "LOCAL"
# For composition: must construct a compatible result_dict with display.source='LOCAL'
write_docx_result_block(doc, result_dict, filepath=filepath_str, lang=CURRENT_LANG)
```

---

## D-12 Invariant — Cloud-Write Non-Regression

**VERIFIED: Three cloud-write gates are load-bearing and must not be broken.**

1. `shared/search_serializer.py:582` — filters LOCAL items from `/api/*` payloads: `results = [r for r in results if not _is_local_item(r)]`. This is a web-only path; composition is desktop-only so this gate is not directly relevant here.

2. `shared/corrections_service.py` — the corrections save path. Composition results are display-only; the only cloud writes that could be triggered from the composition surface are user actions (corrections, list adds). These are gated by existing session auth checks and do not receive composition result objects.

3. `lists_sync.py` top-of-function gates (per v7.14 CLAUDE.md). These are also user-action-triggered, not result-triggered.

**Conclusion for Phase 110:** No new code in this phase touches any of the three gates. The invariant is preserved by design (composition LOCAL results are display-only on the desktop; no sync path exists). State it as a non-regression assertion in the test: verify that after a LOCAL composition search, no Supabase client methods are called.

---

## Runtime State Inventory

This phase is a code/wiring change only — no rename, no migration. No runtime state to inventory.

**Nothing found in any category** — verified by phase scope review.

---

## Validation Architecture

Nyquist validation is enabled (`workflow.nyquist_validation: true` in `.planning/config.json`).

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest |
| Config file | pytest.ini or inferred |
| Quick run command | `pytest tests/test_comp_corpus_scope.py -x` (new file) |
| Full suite command | `pytest tests/ -x --tb=short` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| COMP-LOC-01 | `corpus_scope='genizah'` → Lab mode search skips LOCAL LAB loop | unit | `pytest tests/test_comp_corpus_scope.py::test_lab_comp_genizah_skips_local_lab -x` | Wave 0 |
| COMP-LOC-01 | `corpus_scope='local'` → Lab mode search skips Genizah LAB loop | unit | `pytest tests/test_comp_corpus_scope.py::test_lab_comp_local_skips_genizah_lab -x` | Wave 0 |
| COMP-LOC-01 | `corpus_scope='genizah'` → standard composition skips LOCAL LAB hook | unit | `pytest tests/test_comp_corpus_scope.py::test_std_comp_genizah_skips_local_lab -x` | Wave 0 |
| COMP-LOC-02 | `corpus_scope='all'` on standard composition includes LOCAL LAB hits | unit | `pytest tests/test_comp_corpus_scope.py::test_std_comp_all_includes_local_hits -x` | Wave 0 |
| COMP-LOC-02 | Stale LAB index sets `local_lab_searcher_stale=True` | unit | `pytest tests/test_comp_corpus_scope.py::test_stale_lab_sets_flag -x` | Wave 0 |
| COMP-LOC-02 | Genizah default path returns identical results to today | regression | `pytest tests/test_comp_corpus_scope.py::test_genizah_default_nonregression -x` | Wave 0 |
| EXP-F3 | LOCAL hit in composition export has LOCAL columns (filename/folder/path/page) | unit | `pytest tests/test_comp_export_local.py::test_xlsx_local_row_shape -x` | Wave 0 |
| EXP-F3 | Genizah-only composition export is unchanged (cross-parity) | unit | `pytest tests/test_comp_export_local.py::test_genizah_only_export_unchanged -x` | Wave 0 |
| D-12 | No Supabase client calls after LOCAL composition run | unit | `pytest tests/test_comp_corpus_scope.py::test_no_cloud_write_on_local_comp -x` | Wave 0 |

### Existing Tests to Extend

- `tests/test_corpus_scope_routing.py` — extend with composition-scope variants
- `tests/test_lab_composition_chunk_hits.py` — extend with `corpus_scope` param non-regression

### Sampling Rate

- **Per task commit:** `pytest tests/test_comp_corpus_scope.py tests/test_comp_export_local.py -x --tb=short`
- **Per wave merge:** `pytest tests/ -x --tb=short`
- **Phase gate:** Full suite green before `/gsd-verify-work`

### Wave 0 Gaps

- [ ] `tests/test_comp_corpus_scope.py` — covers COMP-LOC-01/02, D-12, Genizah non-regression
- [ ] `tests/test_comp_export_local.py` — covers EXP-F3 (LOCAL-hit export shape, cross-parity)

*(Existing test infrastructure covers the Lab Mode chunk_hits assertions and corpus_scope routing for `execute_search` — these do not need to be re-created.)*

---

## Security Domain

No authentication, no external API calls, no user-submitted content going to a network endpoint. The only security-relevant constraint is D-12 (cloud-write gates) which is already verified above. ASVS V4 (Access Control) is the only applicable category:

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V4 Access Control | YES — LOCAL data must not reach cloud | D-12 gate: `_is_local_item` filter in `search_serializer.py`; composition results never reach `/api/*` |
| V5 Input Validation | NO | Composition text is user-provided but processed locally, never sent to server |
| V2/V3/V6 | NO | No auth changes, no new crypto |

---

## Environment Availability

This phase is a desktop-internal wiring change. All dependencies are already in the project:

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| PyQt6 | QComboBox, QThread | Already in project | Existing | — |
| tantivy (Python) | LOCAL LAB index queries | Already in project | Existing | — |
| shared/export_dossier.py | EXP-F3 helpers | Exists in repo | Phase 103 | — |
| shared/docx_export.py | EXP-F3 DOCX | Exists in repo | Phase 103 | — |
| shared/local_sys_id.py | `is_local_sys_id()` detection | Exists in repo | Phase 95 | — |

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Composition item `src_lbl` field equals `'LOCAL'` for LOCAL LAB hits (matches what LOCAL LAB index stores in `source` field) | RF-5, Pitfall 2 | LOCAL export detection fails; fallback is `is_local_sys_id(sys_id)` |
| A2 | The `local_filter_btn_composition` dormant scaffolding can safely remain invisible (setVisible(False)) without affecting parallels LOCAL filter | RF-6 | Verified: separate buttons. Risk = zero. |

**All other claims in this research were verified directly from current source files.**

---

## Sources

### Primary (HIGH confidence)

- `C:/Genizahsearch/genizah_app.py` — create_search_tab:5877, create_composition_tab:6489, run_composition:21654, export_comp_report:20447, browse_search_parallels:10681, send_result_to_composition:19944, _on_corpus_scope_changed:16846, _prime_local_filepath_cache:19262, _save_session:25005, _restore_session:25283
- `C:/Genizahsearch/gui_threads.py` — SearchThread:80, CompositionThread:163, LabCompositionThread:216
- `C:/Genizahsearch/genizah_core.py` — LabEngine:693, lab_composition_search:1402, search_composition_logic:8892, execute_search:8412, _rrf_merge:7365, _current_lab_weights_hash (SearchEngine):7079, _check_local_lab_freshness (SearchEngine):7111, _current_lab_weights_hash (LabEngine):808, _check_local_lab_freshness (LabEngine):824
- `C:/Genizahsearch/shared/export_dossier.py` — build_local_document_row:1239, local_documents_header_row:418, _LOCAL_HEADERS_EN:245
- `C:/Genizahsearch/shared/docx_export.py` — write_docx_result_block:64
- `C:/Genizahsearch/tests/test_corpus_scope_routing.py` — existing corpus scope regression tests
- `C:/Genizahsearch/tests/test_lab_composition_chunk_hits.py` — existing Lab composition tests

### Secondary (MEDIUM confidence)

- `C:/Genizahsearch/genizah_translations.py` — i18n corpus selector strings: "גניזה"/"Genizah", "מקומי"/"Local", "הכול"/"ALL" are hardcoded in the combo (not via `tr()`) — match the existing Search-tab pattern exactly
- `C:/Genizahsearch/.planning/phases/110-composition-parallels-search-local-corpus-support-desktop/110-CONTEXT.md` — user decisions D-01..D-13, RF-1..RF-6

---

## Metadata

**Confidence breakdown:**

- Standard stack: HIGH — all symbols verified by direct file reads with line numbers
- Architecture: HIGH — data flow traced through actual code, no hypotheticals
- Pitfalls: HIGH — each pitfall identified from actual code patterns in current codebase
- RF resolutions: HIGH (RF-1,3,4,6) / MEDIUM (RF-2,5) — RF-2 merge recommendation and RF-5 detection pattern have one assumed claim (A1 above)

**Research date:** 2026-06-08
**Valid until:** 2026-07-08 (stable codebase; 30-day shelf life)

---

## RESEARCH COMPLETE

**Phase:** 110 - composition-parallels-search-local-corpus-support-desktop
**Confidence:** HIGH

### Key Findings

- **Lab Mode is NOT just "search LOCAL"** — it uses a distinct fingerprint-scoring algorithm (`text_to_fingerprint`), its own index (`lab_index`), and `deep_scan`/`scan_limit` controls. The corpus selector and Lab Mode are genuinely orthogonal. D-06 is sound.
- **Both composition paths already have LOCAL LAB hooks** (`search_composition_logic:9071-9167` and `lab_composition_search:1601-1704`) that merge LOCAL hits into the same accumulator. Adding `corpus_scope` is a gating change, not a new architecture.
- **The weights-hash mismatch is live and causes silent LOCAL LAB drops from standard composition.** `SearchEngine.dynamic_rank_map` is always None; the `.meta.json` always has LabEngine's non-None hash. Fix: inject `LabEngine._current_lab_weights_hash()` into `SearchEngine` via a `_lab_weights_hash_override` attribute.
- **EXP-F3 has two landmines:** grouped composition items lack `display` dicts (use `is_local_sys_id` instead), and `matched_text_raw` must be mapped from `source_ctx` (the parallel text context with `*` highlights).
- **Parallels inherits the selector automatically** — `browse_search_parallels` → `send_result_to_composition` only populates the text area; the user triggers `run_composition` manually, which reads `comp_corpus_scope_combo` at that point.
- **`local_filter_btn_composition` and `local_filter_btn_parallels` are SEPARATE controls** — hiding the former is safe and does not affect the parallels surface.

### File Created

`.planning/phases/110-composition-parallels-search-local-corpus-support-desktop/110-RESEARCH.md`

### Confidence Assessment

| Area | Level | Reason |
|------|-------|--------|
| RF-1 (Lab Mode semantics) | HIGH | Read all relevant code paths; deep_scan/scan_limit confirmed |
| RF-2 (ALL merge semantics) | HIGH | Score-interleaved merge is what the existing accumulator pattern already does |
| RF-3 (parameter chain) | HIGH | Traced end-to-end; function signatures verified |
| RF-4 (LAB staleness) | HIGH | Root cause of weights-hash mismatch found and documented |
| RF-5 (EXP-F3 insertion) | MEDIUM | Insertion points identified; LOCAL detection pattern for grouped items is assumed (A1) |
| RF-6 (parallels) | HIGH | Verified separate button controls; inheritance confirmed |
| Validation Architecture | HIGH | Mapped to specific function/param changes; existing test patterns reusable |

### Open Questions (RESOLVED)

1. **A1 — RESOLVED (resolved-by-design):** The original question — confirm `src_lbl == 'LOCAL'` on a composition item from `lab_composition_search` with LOCAL LAB hits — does NOT gate execution. Plan 04 uses `is_local_sys_id(sys_id)` as the **primary** LOCAL discriminator (LOCAL sys_ids are 18-digit `97…` values per `shared/local_sys_id.py:53-79`; LOCAL headers are built from that numeric sys_id, `shared/local_indexer.py:939-941`), with `src_lbl == 'LOCAL'` treated only as an optional fast path. Codex round-2 (live source) further narrowed the related C6 concern. So the answer is non-blocking either way (LOW risk, fallback verified).

### Ready for Planning

Research complete. Planner can now create PLAN.md files for Phase 110.
