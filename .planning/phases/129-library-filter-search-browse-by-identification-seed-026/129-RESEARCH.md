# Phase 129: Library Filter — Search + Browse-by-Identification (SEED-026) - Research

**Researched:** 2026-06-28
**Domain:** Filter UI + SQLite temp-table push-down (NiceGUI web + PyQt6 desktop)
**Confidence:** HIGH

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

- **D-01:** Human-readable library names, EN + HE, via `get_library_display(code, short=...)` (`shared/browse_map_utils.py`). No raw codes (CUL/JTS), no "name (code)". No English leak under Hebrew UI.
- **D-02:** Facet on web search (per-library result counts + hide libraries with 0 matches). Plain list on catalog (no per-library counts — avoids extra GROUP BY on paginated browse query).
- **D-03:** Compact "Filter by library" dropdown/menu-button with a checklist beside existing filter buttons. Active selections → removable chips. Empty selection = all (no chips shown).
- **D-04:** Desktop catalog Browse-by-Identification gets the same library filter NOW (full parity, LIBFILTER-03).

### Claude's Discretion

- Exact NiceGUI widget for the dropdown/checklist (ui.menu, ui.select with multiple, custom checklist).
- Exact Qt widget (QMenu with checkable actions, multi-select combo, QListWidget).
- Cheapest catalog push-down query shape (this research document resolves it below).

### Deferred Ideas (OUT OF SCOPE)

- API library-filter param (`/api/search`, `/api/browse`).
- Catalog facet counts (additive follow-up if cheap).
- Library filter on other pages (reading desk, Joins Lab, puzzle).
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| LIBFILTER-01 | Web `/search` multi-select filters by `library_code` over FULL result set BEFORE `[:200]` cap; persists via `safe_storage`; removable chips; i18n EN/HE | Confirmed: full result set available at filter time in `search_state.results`; `library_code` is in every result's `display` dict; `persist_value` + `_safe_get` pattern verified |
| LIBFILTER-02 | `library_codes` arg pushed into `shared/fjms_service.get_browse_results` BEFORE COUNT/LIMIT; additive; composes with SEED-023 PGP/Editions; persists via `safe_storage` | Resolved below: exact temp-table pattern, allowlist extension, signature change, worked SQL |
| LIBFILTER-03 | Desktop catalog Browse-by-Identification gains same library filter; existing desktop search-results library/shelfmark filtering untouched | `_CatalogRefreshWorker` + `_catalog_update_chips` + `_catalog_cycle_*` pattern confirmed |
| GUARD-02 | Zero behavior change — full pytest suite green at every phase boundary | Existing tests listed; new `library_codes=None/[]` = no-op proven by temp-table guard |
</phase_requirements>

---

## Summary

This is a well-bounded feature phase. The existing SEED-023 PGP/Editions filter is the canonical template — it solved the same push-down-before-pagination problem and left all the plumbing in place. LIBFILTER-02 is structurally identical to what SEED-023 already shipped: add one more temp table to `_FILTER_TEMP_TABLES`, one more `_ensure_filter_temp` call inside `get_browse_results`, and one more additive argument to the function signature. The library filter's "giant IN clause" risk is handled by the SAME temp-table mechanism SEED-023 already uses.

The key non-trivial question (the design crux) is: how does a set of `library_codes` translate into a set of `AlmaId`s (sys_ids) suitable for `_ensure_filter_temp`? The answer is resolved concretely below. `MetadataManager.csv_bank` is an in-memory dict of all ~255K manuscripts keyed by `sys_id`, each row containing `library_code`. A simple Python dict-comprehension reverse-lookup (`{sid for sid, row in meta_mgr.csv_bank.items() if row.get('library_code') in selected_codes}`) produces the set in O(N) time over the 255K-row in-memory map. This lookup happens once per filter-change in the background thread, is cached alongside the PGP/Editions sets, and is never repeated per page-turn. The resulting `library_sys_ids` set is then passed to `_ensure_filter_temp` which materializes it as a SQLite TEMP table — SAME mechanism as `_browse_filter_pgp` / `_browse_filter_edition`.

**Primary recommendation:** Follow the SEED-023 temp-table pattern exactly. Do not pass an `IN (...)` clause. Do not add a `library_code` column to the FJMS catalog table. Resolve `library_codes → sys_id set` in Python via `csv_bank`, cache the result, and push down via a new `_browse_filter_library` TEMP table with an EXISTS clause — identical to the PGP filter's mechanics.

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Library filter state + persistence (web search) | Frontend Server (NiceGUI) | — | Mirrors `pgp_filter`/`printed_filter` pattern in `search.py` |
| Library filter state + persistence (web catalog) | Frontend Server (NiceGUI) | — | Mirrors `catalog_pgp_filter`/`catalog_editions_filter` in `catalog_browse.py` |
| Library → sys_id set resolution | Frontend Server (io_bound worker) | — | Reads in-memory `csv_bank`; must run off event loop |
| Catalog SQL push-down | Database / Storage (`fjms_service`) | — | `_ensure_filter_temp` + EXISTS clause; NEVER post-filter the page |
| Filter UI controls (web) | Browser / Client | Frontend Server | NiceGUI renders controls server-side; chips update on state change |
| Filter UI controls (desktop) | Desktop (Qt) | — | `_CatalogRefreshWorker` dispatches to background; UI updates on `done` signal |

---

## Design Crux Resolution

> **⚠ CODEX GATE (2026-06-28) — APPROVE WITH CHANGES.** This crux was reviewed by the mandatory Codex-review-before-code gate (Success Criterion #4) against live source. Design APPROVED, with TWO required changes folded into the code blocks below and detailed in `129-CODEX-CRUX-REVIEW.md`: **(1) HIGH** — the `_ensure_filter_temp` token MUST be derived from the *selection content* (`hash(tuple(sorted(library_codes)))`), NOT `len(library_sys_ids)` — the library set is dynamic multi-select, so same-size different selections collide on a length token and reuse stale TEMP rows. **(2) MEDIUM** — handle "selected-but-resolves-to-empty" (normalize/validate persisted codes; `csv_bank` readiness policy) distinctly from "empty selection = all".

### The Push-Down Problem

`get_browse_results` is server-side paginated. A library filter applied AFTER `LIMIT/OFFSET` would corrupt `total` (it would reflect the page subset, not the full filtered set). This is the SEED-023 B3 lesson. The same fix applies: push the filter condition into the `WHERE` clause that feeds BOTH the `COUNT(DISTINCT c.AlmaId)` query and the results query.

### Why Not a JOIN or a New Column

The FJMS `catalog` table has `AlmaId` (= `sys_id`) but NOT a `library_code` column. Library is derived from `libraries.csv` loaded into `MetadataManager.csv_bank`. Adding a column to the FJMS DB (fjms_enrichment.db) would require a migration script, a schema change, and a re-deploy of the sidecar — that is out of scope for this feature. The temp-table mechanism is the correct approach.

### Why Not a Giant IN Clause

SEED-023's SHOULD-FIX explicitly called this out. CUL alone has ~128K records — passing 128K strings as bound params to SQLite is expensive and fragile. The existing `_ensure_filter_temp` / TEMP table pattern (used for PGP and Editions) already solves this: materialize the sys_id set into a per-connection TEMP table keyed by `AlmaId`, then use `EXISTS (SELECT 1 FROM "_browse_filter_library" t WHERE t.AlmaId = c.AlmaId)`. Same mechanism, same performance.

### The Reverse Lookup: `library_codes → sys_id set`

`MetadataManager.csv_bank` is an in-memory dict `{sys_id: {'library_code': ..., 'shelfmark': ..., ...}}` loaded from `libraries.csv` at startup (~255K rows). There is NO pre-built reverse map (confirmed by grepping all of `shared/`).

**Recommended approach:** compute the reverse lookup lazily, in the background thread that already resolves PGP/Editions sets, cache the result (same `_LIBRARY_FILTER_SETS` pattern), and invalidate only on sidecar reload.

**Concrete resolution function** (runs in io_bound / `_CatalogRefreshWorker`):

```python
# In catalog_browse.py (web) or genizah_app.py (desktop)
def _resolve_library_sys_ids(selected_library_codes: list[str], meta_mgr) -> set[str]:
    """Return the set of sys_ids whose library_code is in selected_library_codes."""
    if not selected_library_codes or meta_mgr is None:
        return set()
    code_set = set(selected_library_codes)
    return {
        sid
        for sid, row in meta_mgr.csv_bank.items()
        if row.get('library_code') in code_set
    }
```

This is O(255K) over an in-memory Python dict — comparable to what PGP/Editions set resolution does against a SQLite DB. For CUL (~128K sys_ids), this produces a set of ~128K strings. That set is then passed to `_ensure_filter_temp`, which materializes it in SQLite as a TEMP table with 128K rows. SQLite TEMP tables are fine at this scale (they're in-memory by default).

**Caching strategy:** cache the resolved `(library_codes_frozenset → sys_id_set)` mapping per user session, invalidated when the user changes the selection. Do NOT cache globally by library_code across users (different users can have different selections). The computation is fast enough (O(255K) dict iteration, < 50ms) that per-request computation is also acceptable as a fallback.

**Alternative (simpler) strategy:** Do NOT pre-cache at all. Resolve fresh on each catalog page turn (every `get_browse_results` call). The dict iteration is ~50ms; the page turn is already async. This is simpler code and avoids a caching layer. The planner should pick based on whether the SEED-023 pattern (compute once, pass precomputed) or inline resolution is preferred. Both are correct.

### Recommended `get_browse_results` Signature Change

```python
def get_browse_results(
    self,
    domain: str = None,
    author: str = None,
    work: str = None,
    offset: int = 0,
    limit: int = 50,
    date_from: int = None,
    date_to: int = None,
    include_undated: bool = False,
    text_all: list[str] = None,
    text_any: list[str] = None,
    text_not: list[str] = None,
    pgp_filter: str = None,
    pgp_sys_ids=None,
    editions_filter: str = None,
    edition_sys_ids=None,
    # NEW — LIBFILTER-02 (additive; None/empty = no-op):
    library_codes: list[str] = None,   # selected library codes (UI state)
    library_sys_ids=None,              # precomputed set from _resolve_library_sys_ids()
) -> dict:
```

The caller passes BOTH `library_codes` (for documentation/chip rendering) and `library_sys_ids` (the precomputed set). The function only uses `library_sys_ids` to build the TEMP table (same pattern as `pgp_sys_ids` / `edition_sys_ids`). If `library_codes` is empty/None OR `library_sys_ids` is empty/None, the filter is a no-op (fail-open, consistent with existing behavior).

### Extension to `_FILTER_TEMP_TABLES`

```python
# shared/fjms_service.py — extend the allowlist:
_FILTER_TEMP_TABLES = ("_browse_filter_pgp", "_browse_filter_edition", "_browse_filter_library")
```

### WHERE Clause Addition (inside `get_browse_results`)

Appended after the editions block, before `where = ...`:

```python
# LIBFILTER-02: library membership filter. Same temp-table pattern as SEED-023.
# Fail-open: if sys_id set is missing or TEMP build fails, skip the filter.
if library_codes and library_sys_ids:
    # CODEX GATE CHANGE 1 (HIGH): token derived from SELECTION CONTENT, not len() —
    # dynamic multi-select means same-size different selections must NOT collide.
    _lib_token = hash(tuple(sorted(library_codes)))
    if self._ensure_filter_temp(
        "_browse_filter_library", library_sys_ids, _lib_token
    ):
        conditions.append(
            'EXISTS (SELECT 1 FROM "_browse_filter_library" t '
            'WHERE t.AlmaId = c.AlmaId)'
        )
```

### Worked Query Example

With domain="Liturgy", `library_codes=["CUL"]`, `library_sys_ids={set of ~128K CUL AlmaIds}`, `pgp_filter=None`:

**TEMP table created once per connection:**
```sql
CREATE TEMP TABLE "_browse_filter_library" (AlmaId TEXT PRIMARY KEY);
INSERT OR IGNORE INTO "_browse_filter_library" (AlmaId) VALUES (?), (?), ...;
-- (~128K rows, per-thread, per-process-lifetime)
```

**Count query (BEFORE LIMIT/OFFSET):**
```sql
SELECT COUNT(DISTINCT c.AlmaId) as total
FROM catalog c
WHERE c.AlmaId IN (
    SELECT AlmaId FROM domains WHERE Domain = ?
    UNION SELECT AlmaId FROM domains WHERE ParentDomain = ?
)
AND EXISTS (SELECT 1 FROM "_browse_filter_library" t WHERE t.AlmaId = c.AlmaId)
```

**Results query:**
```sql
SELECT c.AlmaId, MAX(...) ...
FROM catalog c
WHERE [same conditions]
GROUP BY c.AlmaId
ORDER BY c.AlmaId
LIMIT 50 OFFSET 0
```

The library filter applies to BOTH queries identically (via shared `conditions` list + `where`), so `total` reflects the full filtered set and pagination is correct.

### Codex Review Gate Target

The Codex review should confirm:
1. `_FILTER_TEMP_TABLES` extended with `"_browse_filter_library"`.
2. Filter is fail-open (skipped when `library_sys_ids` is None or empty, never returns empty/wrong set).
3. The None/empty = no-op invariant is covered by a unit test (same test shape as `test_filter_skipped_when_set_missing` in `test_seed023_catalog_filters.py`).
4. The new arg is additive — calling `get_browse_results()` with no new args behaves identically to before (backward-compatible).
5. Composition with PGP + Editions filters works (all three TEMP tables coexist in the same WHERE clause, and all three conditions are ANDed together).

---

## Verified Code Anchors

### A. Web Search Post-Filter + Facet (LIBFILTER-01)

**File:** `web/pages/search.py`

- **Full result set at filter time:** `search_state.results` holds the complete (pre-`[:200]`) result list. Both `_apply_printed_filter` (line 3348) and `_apply_pgp_filter` (line 3363) iterate over `search_state.results` directly. The library filter must follow the same pattern — a new `_apply_library_filter(results_list)` function that filters by `search_state.library_filter` (a list of codes).

- **`library_code` on every result:** `result['display']['library_code']` is populated by `SearchEngine` (line 2200-2206 of `shared/search_engine.py`). All regular search results have it. The filter reads `r.get('display', {}).get('library_code', '')`. [VERIFIED: grep of search_engine.py]

- **Facet (D-02):** At filter time, the library filter can compute per-code counts from the full `search_state.results` list via a `collections.Counter(r.get('display',{}).get('library_code','') for r in results_list)`. This requires zero extra DB queries.

- **Persistence keys:** `search_printed_filter` at line 183, `search_pgp_filter` at line 184 via `_safe_get`. New key: `search_library_filter` (a list, not a 3-state string). `persist_value('search_library_filter', search_state.library_filter)` writes through `safe_storage`.

- **Filter row + chip bar:**
  - Filter buttons at lines 1474–1577. Library multi-select dropdown placed beside `pgp_filter_btn`.
  - `_update_chip_bar()` defined at line 1127. One chip per selected library code using `get_library_display(code, short=False, lang=get_language())` for the label.
  - `_chip_bar_ready` flag at line 105.

- **Cascade ordering:** The library filter slots into `_apply_printed_filter_and_render` (line 3383) after the PGP filter, before measurement post-filters. The function is called by the toggle handler when `search_state.results` is non-empty.

- **`[:200]` timing:** `render_results(...)` in `search.py` applies the `[:200]` cap on the FILTERED output. The library filter must run before `render_results` is called — same as PGP/printed. [VERIFIED: grep of search.py render_results + _apply_printed_filter_and_render]

### B. Web Catalog Template (LIBFILTER-02)

**File:** `web/pages/catalog_browse.py`

- **Filter sets persistence:** `safe_user_get('catalog_pgp_filter', 'all')` at line 109, `safe_user_get('catalog_editions_filter', 'all')` at line 110. New: `safe_user_get('catalog_library_filter', [])` (a list).

- **`_get_filter_sets()`:** Lines 44–74. Returns cached `(pgp_link_sys_ids, edition_sys_ids)` tuple. The library filter needs a parallel `_get_library_sys_ids(selected_codes, meta_mgr)` function — either a separate cached dict or inline resolution in `_fetch_results_blocking`.

- **`_fetch_results_blocking()`:** Lines 248–266. Passes `pgp_filter`, `pgp_sys_ids`, `editions_filter`, `edition_sys_ids` to `fjms.get_browse_results(...)`. Library filter slots in here:
  ```python
  library_sys_ids = None
  if current_library_filter['value']:
      from web.state import state
      library_sys_ids = _resolve_library_sys_ids(
          current_library_filter['value'], state.meta_mgr
      )
  return fjms.get_browse_results(
      ...,
      library_codes=(current_library_filter['value'] or None),
      library_sys_ids=(library_sys_ids or None),
  )
  ```

- **Row mapping at line 350:** `library_code = state.meta_mgr.get_library_for_id(sid) or ''` — this is for displaying the library column in results, not for filtering. Not changed.

- **Chip rendering:** Lines 693–734. Active PGP and Editions filters render as removable chips at lines 721–734. Library chips follow the same `_make_chip(f"Library: {label}", lambda: clear_filter('library'))` pattern, one chip per selected code. The `has_filters` set at line 684 gains `bool(current_library_filter['value'])`.

- **`clear_filter('library')`:** New branch in the existing `clear_filter` function sets `current_library_filter['value'] = []`, saves to safe_storage, resets page, triggers refresh.

- **Filter button/control:** The new control is a dropdown/checklist (D-03). The existing `pgp_filter_btn_ref` (line 132) and `editions_filter_btn_ref` (line 133) are buttons; the library control is a different widget type (multi-select). The ref follows the existing `ref` dict pattern.

### C. Desktop Catalog Template (LIBFILTER-03)

**File:** `genizah_app.py`

- **`_get_catalog_filter_sets()` at line 454:** Returns `(pgp_link_sys_ids, edition_sys_ids)`. Does NOT need changing for library filter (library resolution is different — it depends on the user's selection, not a corpus-wide set).

- **`_CatalogRefreshWorker` at line 488:** Constructor params at lines 497–517. `pgp_filter` and `editions_filter` at lines 516–517. New params: `library_filter: list[str] = None`. In `run()`, resolve `library_sys_ids` from `meta_mgr.csv_bank` (passed as a param or accessed via module-level meta_mgr ref), then pass to `fjms.get_browse_results(library_codes=..., library_sys_ids=...)`.

- **`_catalog_start_async_refresh()` at line 10117:** Creates the `_CatalogRefreshWorker` with `pgp_filter=self._catalog_pgp_filter` (line 10144) and `editions_filter=self._catalog_editions_filter` (line 10145). New: `library_filter=self._catalog_library_filter`.

- **State variable pattern:** `self._catalog_pgp_filter = 'all'` at line 9582, `self._catalog_editions_filter = 'all'` at line 9583. New: `self._catalog_library_filter = []` (a list of codes, empty = all).

- **Filter buttons:** `self._catalog_pgp_filter_btn` at line 9806, `self._catalog_editions_filter_btn` at line 9812, connected to `_catalog_cycle_pgp_filter` / `_catalog_cycle_editions_filter`. Library control: a new `QListWidget` or a dropdown with checkboxes (D-03; implementer's call per discretion). Placed in the left panel after the existing availability filter buttons (line 9816).

- **`_catalog_update_avail_filter_btns()` at line 10354:** Updates PGP/Editions button labels + colors. Library filter has its own refresh method (or is extended here).

- **`_catalog_cycle_*` pattern at lines 10378–10392:** The cycle pattern for 3-state buttons. Library is multi-select so it uses a different toggle: clicking a code toggles it in/out of `self._catalog_library_filter`. The refresh trigger is `self._catalog_start_async_refresh(refresh_authors=False, refresh_works=False)`.

- **`_catalog_update_chips()` at line 10469:** PGP chip at line 10553–10561, Editions chip at line 10563–10572. Library chips follow the same pattern: one chip per selected code with an "×" remove button. The remove handler calls `_catalog_remove_filter("library_CODE")` or a variant that removes a single code.

- **`_catalog_remove_filter()` at line 10394:** Handles PGP at line 10416, Editions at line 10419. New branch for `"library"` (clear all selected codes) or individual code removal.

- **NOTE — `_catalog_refresh()` vs `_catalog_start_async_refresh()`:** The desktop has TWO code paths. `_catalog_refresh()` (line 9902) calls `fjms.get_browse_results(...)` DIRECTLY on the main thread WITHOUT the worker and WITHOUT the PGP/Editions filters (lines 9910–9922). This is the non-SEED-023 legacy path. `_catalog_start_async_refresh()` uses `_CatalogRefreshWorker`. The library filter must be added to the worker path (`_catalog_start_async_refresh`). The synchronous `_catalog_refresh()` path is NOT called from any user action in the SEED-023 desktop parity code — the existing filter cycling goes through `_catalog_start_async_refresh`. Confirm which path is actually live and ensure the library filter is threaded through both if both are reachable.

- **`get_library_for_id` desktop usages confirmed:**
  - Line 9944: `library = self.meta_mgr.get_library_for_id(sys_id)` (in `_catalog_refresh()`)
  - Line 10182: same in `_catalog_on_async_refresh_done()` (the worker callback)
  - These are for DISPLAYING the library column, not filtering. Not changed.

### D. Shared Label Helper (D-01)

**File:** `shared/browse_map_utils.py`

- **`get_library_display(code, short=True, lang=None)` at line 180.** When `short=False`, returns the full name from `LIBRARY_CODES` (EN) or `LIBRARY_CODES_HE` (HE) depending on `lang` or `CURRENT_LANG`. When `lang=None`, reads `CURRENT_LANG` via a lazy import from `genizah_core` (GUARD-01 safe, intentional lazy per inline comment at line 197).
  - Usage for web: `get_library_display(code, short=False, lang=get_language())`.
  - Usage for desktop: `get_library_display(code, short=False)` — auto-detects from `CURRENT_LANG`.

- **`LIBRARY_CODES` dict at line 23:** Full list of ~60+ library codes. Canonical ordered list for the dropdown is `list(LIBRARY_CODES.keys())` — but filter the dropdown to ONLY show codes that actually appear in the current result set (web search facet, D-02) or the full corpus (catalog: show all codes that have any records, to avoid showing entries the user can never select meaningfully). The simplest approach for catalog: show all `LIBRARY_CODES` keys (plain list, D-02).

- **`LIBRARY_CODES_HE` at `genizah_translations.py:3779`:** Hebrew names keyed by library code. `get_library_display` already reads this.

- **i18n invariant:** D-01 requires no English leak under Hebrew UI. `get_library_display(code, short=False, lang='he')` returns `LIBRARY_CODES_HE.get(code, LIBRARY_CODES.get(code, code))` — the Hebrew name if available, else English fallback. This is the existing behavior; no new code needed for the label lookup.

### E. GUARD-02 Regression Set

Existing tests that cover the affected paths (must stay green):

| Test File | What It Covers |
|-----------|----------------|
| `tests/test_seed023_catalog_filters.py` | `get_browse_results` PGP/Editions filters + `_ensure_filter_temp` + total-reflects-full-set + fail-open + TEMP reuse + composition |
| `tests/test_catalog_availability_filter.py` | Desktop `_get_catalog_filter_sets()` + cache + `_CatalogRefreshWorker` PGP/Editions wiring |
| `tests/test_fjms_service.py` | General FJMS service methods |
| `tests/test_pgp_filter_cascade.py` | Web search PGP filter + safe_storage chokepoint (AST guard) |
| `tests/test_browse_api.py` | Browse API endpoints |
| `tests/test_no_raw_storage_access.py` | Phase 87 safe_storage allowlist invariant (CI guard) — adding `search_library_filter` and `catalog_library_filter` to safe_storage does NOT touch this guard (allowlist `[]` = no raw access permitted, and the new filter uses `safe_user_get`/`safe_user_set` through the chokepoint) |

---

## Standard Stack

No new external packages. All needed tools are already in the project.

| Component | Implementation | Location |
|-----------|---------------|----------|
| Library code dict | `LIBRARY_CODES` | `shared/browse_map_utils.py:23` |
| Hebrew labels | `LIBRARY_CODES_HE` | `genizah_translations.py:3779` |
| Display helper | `get_library_display(code, short=False, lang=...)` | `shared/browse_map_utils.py:180` |
| Reverse map (sys_ids per library) | `csv_bank` dict comprehension | `shared/metadata_manager.py:413` via `csv_bank` |
| Web persistence | `safe_user_get` / `safe_user_set` / `persist_value` | `web/safe_storage.py`, `web/components/filter_panel.py:220` |
| Catalog SQL push-down | `_ensure_filter_temp` + EXISTS | `shared/fjms_service.py:1992` |
| Desktop background worker | `_CatalogRefreshWorker(QThread)` | `genizah_app.py:488` |

## Package Legitimacy Audit

No new packages installed. Section not applicable.

---

## Architecture Patterns

### System Architecture Diagram

```
User selects library filter
           │
           ▼
   [Web search]                          [Web/Desktop catalog browse]
   filter state in search_state          filter state in current_library_filter / _catalog_library_filter
   persisted via persist_value()         persisted via safe_user_get / safe_user_set
           │                                         │
           ▼                                         ▼
   _apply_library_filter(results)        io_bound / _CatalogRefreshWorker.run()
   ┌───────────────────────────┐         ┌───────────────────────────────────────┐
   │ Filter search_state.results│        │ _resolve_library_sys_ids(codes, mgr)  │
   │ by r['display']['library_code']     │ → O(255K) csv_bank dict comprehension │
   │ against selected_codes set │        │ → set of ~N AlmaId strings            │
   └───────────────────────────┘         └───────────────────────────────────────┘
           │                                         │
           ▼                                         ▼
   Compute facet counts (D-02)           fjms.get_browse_results(
   show N results, hide 0-count codes       library_codes=[...],
           │                                library_sys_ids={...},
           ▼                                pgp_filter=..., ...
   render_results(filtered[:200])        )
   render chips for selected codes                   │
                                                     ▼
                                         _ensure_filter_temp("_browse_filter_library", ...)
                                         → CREATE TEMP TABLE (AlmaId TEXT PRIMARY KEY)
                                         → INSERT all sys_ids
                                                     │
                                                     ▼
                                         WHERE ... AND EXISTS (SELECT 1 FROM
                                           "_browse_filter_library" t
                                           WHERE t.AlmaId = c.AlmaId)
                                         → COUNT(...) reflects full filtered set
                                         → LIMIT/OFFSET applies after filter
                                                     │
                                                     ▼
                                         {"results": [...50 rows...], "total": N}
```

### Recommended Project Structure

No new files required. Changes are:
```
shared/fjms_service.py          # extend _FILTER_TEMP_TABLES + get_browse_results signature
web/pages/catalog_browse.py     # new filter state + _resolve_library_sys_ids + chips
web/pages/search.py             # _apply_library_filter + facet counts + chips
genizah_app.py                  # _catalog_library_filter state + widget + worker wiring
```

### Pattern 1: Temp-Table Filter Push-Down (from SEED-023)

```python
# Source: shared/fjms_service.py:2206–2224 (SEED-023 PGP/Editions implementation)
if pgp_filter in ("has_pgp", "no_pgp") and pgp_sys_ids:
    if self._ensure_filter_temp(
        "_browse_filter_pgp", pgp_sys_ids, len(pgp_sys_ids)
    ):
        op = "EXISTS" if pgp_filter == "has_pgp" else "NOT EXISTS"
        conditions.append(
            f'{op} (SELECT 1 FROM "_browse_filter_pgp" t '
            f"WHERE t.AlmaId = c.AlmaId)"
        )
```

Library filter (LIBFILTER-02) is structurally identical, using `EXISTS` always (no NOT EXISTS case — excluded libraries simply are not in the filter set passed by the caller; caller provides only the INCLUDED sys_ids):

```python
# New block, after editions block, before `where = ...`
if library_codes and library_sys_ids:
    # CODEX GATE CHANGE 1 (HIGH): token derived from SELECTION CONTENT, not len() —
    # dynamic multi-select means same-size different selections must NOT collide.
    _lib_token = hash(tuple(sorted(library_codes)))
    if self._ensure_filter_temp(
        "_browse_filter_library", library_sys_ids, _lib_token
    ):
        conditions.append(
            'EXISTS (SELECT 1 FROM "_browse_filter_library" t '
            'WHERE t.AlmaId = c.AlmaId)'
        )
```

### Pattern 2: Web Search Post-Filter (from printed_filter / pgp_filter)

```python
# Source: web/pages/search.py:3348-3381
def _apply_printed_filter(results_list):
    if search_state.printed_filter == 'all' or not search_state.printed_ids:
        return results_list
    filtered = []
    for r in results_list:
        sys_id = r.get('display', {}).get('id')
        is_printed = sys_id and sys_id in search_state.printed_ids
        if search_state.printed_filter == 'hide_printed' and is_printed:
            continue
        ...
        filtered.append(r)
    return filtered
```

Library filter analog (multi-select, not 3-state):

```python
def _apply_library_filter(results_list):
    """Filter results by selected library codes. Empty selection = no-op."""
    if not search_state.library_filter:  # empty list = all
        return results_list
    selected = set(search_state.library_filter)
    return [
        r for r in results_list
        if r.get('display', {}).get('library_code', '') in selected
    ]
```

Facet computation (D-02 — cheap, no extra query):

```python
from collections import Counter
def _compute_library_facets(results_list):
    """Return {library_code: count} for all results (pre-filter)."""
    return Counter(
        r.get('display', {}).get('library_code', '')
        for r in results_list
        if r.get('display', {}).get('library_code')
    )
```

### Pattern 3: Catalog Chip Rendering (from SEED-023)

```python
# Source: web/pages/catalog_browse.py:721-734
if current_pgp_filter['value'] != 'all':
    pgp_label = tr('Has PGP') if current_pgp_filter['value'] == 'has_pgp' else tr('No PGP')
    _make_chip(
        pgp_label,
        lambda: clear_filter('pgp'),
        color='green' if current_pgp_filter['value'] == 'has_pgp' else 'red',
    )
```

Library chips (one per selected code):

```python
for code in current_library_filter['value']:
    display_label = get_library_display(code, short=False, lang=lang)
    _make_chip(
        f"{tr('Library')}: {display_label}",
        lambda c=code: clear_library_code(c),
        color='blue',
    )
```

### Anti-Patterns to Avoid

- **Post-filter after LIMIT/OFFSET in catalog:** Corrupts `total` and pagination. Never. Filter must be inside `get_browse_results` WHERE clause.
- **Giant IN clause:** Do not pass a 128K-element `IN (...)` list as bound params. Always use `_ensure_filter_temp` + EXISTS.
- **Filtering only the visible 200 in web search:** The full `search_state.results` must be filtered, then `[:200]` applied to the filtered output.
- **Client-only library filter in NiceGUI:** NiceGUI runs server-side. "Client-only" means filtering in the page's Python closure, which IS on the server but over the full result set. This is correct and is what `_apply_printed_filter` does. Do not filter in JavaScript/Vue.
- **English library names under Hebrew UI:** Always use `get_library_display(code, short=False, lang=get_language())` for web and `get_library_display(code, short=False)` for desktop (reads `CURRENT_LANG` automatically).

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Large sys_id set in SQL | `WHERE AlmaId IN (id1, id2, ..., id128K)` | `_ensure_filter_temp` + EXISTS | SQLite param limit, performance |
| Library → sys_id mapping | A new DB table or column in fjms_enrichment.db | Dict comprehension over `meta_mgr.csv_bank` | csv_bank is already in memory; no schema change needed |
| Hebrew library name lookup | A new translation dict | `get_library_display(code, short=False, lang='he')` | Already handles HE fallback + EN fallback |
| Session persistence | Raw `app.storage.user` | `safe_user_get` / `safe_user_set` / `persist_value` | Phase 87 invariant; CI guard |
| Background DB query (desktop) | Direct call on main thread | `_CatalogRefreshWorker(QThread)` | Same pattern as PGP/Editions; avoids UI freeze |

---

## Common Pitfalls

### Pitfall 1: Adding library filter to `_catalog_refresh()` but not `_catalog_start_async_refresh()`

**What goes wrong:** The desktop has two code paths. `_catalog_refresh()` at line 9902 calls `fjms.get_browse_results(...)` directly on the main thread WITHOUT PGP/Editions (notice it has no `pgp_filter` arg). `_catalog_start_async_refresh()` uses the worker. All user interactions go through `_catalog_start_async_refresh()`. If you add the library filter only to the worker path, `_catalog_refresh()` stays unfiltered but is never called from user-visible actions — acceptable but inconsistent. Confirm which path is live and document the decision.

**How to avoid:** Check which callers invoke each method. If `_catalog_refresh()` is only a legacy path (never called from active UI), it's fine to leave it unmodified. Do NOT modify it just to be "complete" unless it's reachable.

### Pitfall 2: Resolving `library_sys_ids` on the NiceGUI event loop

**What goes wrong:** `_resolve_library_sys_ids()` iterates 255K dict entries. At ~50ms, this blocks the event loop if called synchronously in an async NiceGUI handler.

**How to avoid:** Call it inside `_fetch_results_blocking()` which runs via `await run.io_bound(...)`. Same pattern as `_get_filter_sets()` in SEED-023.

### Pitfall 3: Facet counts showing stale data after filter change

**What goes wrong:** If facet counts are computed from the already-filtered set (after `_apply_library_filter`), the counts drop to 1 for every selected library and 0 for everything else — useless.

**How to avoid:** Compute facet counts from `search_state.results` BEFORE applying the library filter. Pass the pre-filter `Counter` to the chip/dropdown renderer. Same pattern as how domain facets work.

### Pitfall 4: `_FILTER_TEMP_TABLES` allowlist not extended

**What goes wrong:** `_ensure_filter_temp` checks `if name not in self._FILTER_TEMP_TABLES: return False` (line 2001). Passing `"_browse_filter_library"` without extending the tuple will silently skip the filter (fail-open = no filter, not a crash). The behavior would appear correct (library filter is ignored) with no error log.

**How to avoid:** Extend `_FILTER_TEMP_TABLES = ("_browse_filter_pgp", "_browse_filter_edition", "_browse_filter_library")` as the first change in `fjms_service.py`. Unit test verifies the filter actually changes `total`.

### Pitfall 5: Empty `library_filter = []` treated as "filter all" instead of "show all"

**What goes wrong:** If the code does `if library_sys_ids:` and `library_sys_ids` is an empty set (no libraries selected), the filter correctly no-ops. But if the caller passes `library_sys_ids = set()` thinking "nothing matches", the temp table is empty and `EXISTS` returns false for everything — returns 0 results.

**How to avoid:** The caller must only pass `library_sys_ids` when `library_codes` is non-empty. The `get_browse_results` block is guarded `if library_codes and library_sys_ids:` — both must be truthy. An empty `library_codes` list is the "all" state and must result in `library_sys_ids=None` being passed.

### Pitfall 6: `catalog_library_filter` persisted as a string instead of a list

**What goes wrong:** `safe_user_get` with a default of `[]` works correctly, but if a previous session stored the value as a plain string (e.g., "CUL") instead of a list, loading it back would iterate characters. Supabase / session storage serializes lists as JSON.

**How to avoid:** Always cast: `_lib0 = safe_user_get('catalog_library_filter', [])` then `current_library_filter['value'] = _lib0 if isinstance(_lib0, list) else []`. Same defensiveness as line 111 for `pgp_filter`.

---

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest |
| Config file | None — run directly |
| Quick run command | `pytest tests/test_libfilter_*.py -x` (new test files) |
| Full suite command | `pytest tests/ -x --ignore=tests/gui` (bulk), plus GUI tests separately |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| LIBFILTER-01 | Web search library filter narrows full result set (not just visible 200) | unit | `pytest tests/test_libfilter_web_search.py -x` | No — Wave 0 |
| LIBFILTER-01 | Empty library_filter = no-op (all results pass) | unit | same | No — Wave 0 |
| LIBFILTER-01 | Facet counts computed from pre-filter full set; 0-count libraries hidden | unit | same | No — Wave 0 |
| LIBFILTER-01 | Filter state persists via safe_storage (AST check) | unit | `pytest tests/test_libfilter_web_search.py::test_persistence -x` | No — Wave 0 |
| LIBFILTER-02 | `library_codes` arg changes `total` correctly (full set, not page subset) | unit | `pytest tests/test_libfilter_catalog.py -x` | No — Wave 0 |
| LIBFILTER-02 | None/empty library_codes = no-op (same as existing test_filter_skipped_when_set_missing shape) | unit | same | No — Wave 0 |
| LIBFILTER-02 | Composes with PGP + Editions filters (3-way AND) | unit | same | No — Wave 0 |
| LIBFILTER-02 | `_FILTER_TEMP_TABLES` contains `"_browse_filter_library"` | unit | same | No — Wave 0 |
| LIBFILTER-03 | Desktop `_CatalogRefreshWorker` threads library_filter into `get_browse_results` | unit (gui-marked) | `pytest tests/test_libfilter_desktop.py -x` | No — Wave 0 |
| GUARD-02 | Existing SEED-023 + browse tests all pass (zero behavior change) | regression | `pytest tests/test_seed023_catalog_filters.py tests/test_catalog_availability_filter.py tests/test_fjms_service.py tests/test_pgp_filter_cascade.py -x` | Yes |

### Sampling Rate

- **Per task commit:** `pytest tests/test_seed023_catalog_filters.py tests/test_fjms_service.py -x` (GUARD-02 regression + shared service)
- **Per wave merge:** Full bulk suite + new `test_libfilter_*.py` files
- **Phase gate:** Full suite green before `/gsd-verify-work`

### Wave 0 Gaps

- [ ] `tests/test_libfilter_catalog.py` — covers LIBFILTER-02 (fjms_service push-down)
- [ ] `tests/test_libfilter_web_search.py` — covers LIBFILTER-01 (web search filter + facets)
- [ ] `tests/test_libfilter_desktop.py` — covers LIBFILTER-03 (desktop worker wiring); gui-marked

---

## Security Domain

No new authentication, session management, or cryptography surfaces. The library filter adds:
- A new user-controllable list value stored in `safe_storage` — safe (same chokepoint as all other per-user filter state).
- SQL query modification via temp table — the `_FILTER_TEMP_TABLES` allowlist prevents name injection (`_ensure_filter_temp` rejects names not in the allowlist).
- No new external network calls.

V5 Input Validation is satisfied by the `_FILTER_TEMP_TABLES` allowlist check and the `if library_codes and library_sys_ids:` guard (never passes untrusted user input directly as SQL identifiers).

---

## Open Questions / Risks

### OQ-1: `_catalog_refresh()` vs `_catalog_start_async_refresh()` — which is live?

The desktop has two browse-refresh paths: the synchronous `_catalog_refresh()` (line 9902) which does NOT use the `_CatalogRefreshWorker` and does NOT pass PGP/Editions, and the async `_catalog_start_async_refresh()` which does. Grep of callers is needed during implementation to confirm `_catalog_refresh()` is dead code for user-facing actions. If it IS reachable, it must also receive the library filter. **Risk: LOW** — the SEED-023 availability filter already only wires into `_catalog_start_async_refresh()`, and no regressions were reported, suggesting `_catalog_refresh()` is not reachable from the filter-toggle path.

### OQ-2: `meta_mgr` availability in `_CatalogRefreshWorker.run()`

`_resolve_library_sys_ids()` needs access to `meta_mgr.csv_bank`. In the web path, `state.meta_mgr` is available in `_fetch_results_blocking` via `from web.state import state`. In the desktop path, the worker's `run()` method currently doesn't reference `self` (the parent `GenizahGUI`). The cleanest approach: pass `meta_mgr` (or `csv_bank` directly) as a constructor param to `_CatalogRefreshWorker`. **Risk: LOW** — straightforward plumbing; confirm the passing convention during implementation.

### OQ-3: NiceGUI multi-select widget selection

D-03 allows the implementer to choose the NiceGUI widget. `ui.select(options, multiple=True)` is the most natural NiceGUI control for a multi-select with a dropdown. However, NiceGUI's `ui.select` with `multiple=True` renders inline, not as a compact button. A `ui.menu` with checkable `ui.menu_item` items is the compact dropdown-with-checklist. The planner should specify which approach and confirm it renders correctly in RTL. **Risk: LOW** — both work; visual confirmation needed.

### OQ-4: Library filter order in the catalog left panel

The existing SEED-023 availability filter buttons are in a "Transcriptions" section (line 9802). The library filter is a different kind of control (multi-select list, not 3-state button). Placement: add a "Library" section below "Transcriptions" in the left panel. Or add it to the right-side filter row (like on web search). The planner should specify placement for desktop. **Risk: NONE** — purely UI layout.

### OQ-5: Catalog "all libraries" signal — empty list vs. special sentinel

The web search uses `empty selection = all`. The catalog uses the same convention (`library_codes=None` or `library_codes=[]` = no filter). The implementation must consistently treat `library_codes=[]` as no-op everywhere. **Risk: LOW** — already handled by `if library_codes and library_sys_ids:` guard.

---

## Sources

### Primary (HIGH confidence)

- `shared/fjms_service.py` — `get_browse_results` implementation (lines 2025–2280), `_ensure_filter_temp` (lines 1992–2023), `_FILTER_TEMP_TABLES` (line 1990). Read directly.
- `web/pages/catalog_browse.py` — `_get_filter_sets` (44–74), `_fetch_results_blocking` (248–266), chip rendering (693–734), `safe_user_get` usage (109–112). Read directly.
- `genizah_app.py` — `_get_catalog_filter_sets` (454–477), `_CatalogRefreshWorker` (488–552), `create_catalog_browse_tab` (9564–9898), `_catalog_update_chips` (10469–10574), `_catalog_cycle_*` (10378–10392). Read directly.
- `shared/metadata_manager.py` — `get_library_for_id` (413–427), `csv_bank` structure (284–346). Read directly.
- `shared/browse_map_utils.py` — `get_library_display` (180–201), `LIBRARY_CODES` (23–120). Read directly.
- `web/pages/search.py` — `_apply_printed_filter` (3348–3361), `_apply_pgp_filter` (3363–3381), `_apply_printed_filter_and_render` (3383–3410), filter persistence (183–184). Read directly.
- `tests/test_seed023_catalog_filters.py` — shape of GUARD-02 regression tests. Read directly.
- `tests/test_catalog_availability_filter.py` — desktop pattern. Read directly.

### Secondary (MEDIUM confidence)

- `.planning/seeds/SEED-023-homepage-stats-and-catalog-pgp-edition-filters.md` — Codex corrections (B3, SHOULD-FIX on giant IN clause). Used as specification source.
- `.planning/phases/129-library-filter-search-browse-by-identification-seed-026/129-CONTEXT.md` — locked decisions D-01..D-04.

---

## Metadata

**Confidence breakdown:**
- Design crux resolution: HIGH — verified against live `fjms_service.py` source; temp-table pattern confirmed
- Standard stack: HIGH — no new packages; all patterns confirmed in existing code
- Architecture: HIGH — code anchors verified by direct file reading
- Pitfalls: HIGH — derived from direct code reading + SEED-023 Codex review history

**Research date:** 2026-06-28
**Valid until:** 60 days (fjms_service and catalog_browse are stable, slow-moving files)

---

## RESEARCH COMPLETE

**Phase:** 129 - Library Filter — Search + Browse-by-Identification (SEED-026)
**Confidence:** HIGH

### Key Findings

1. **Design crux RESOLVED:** Library filter push-down uses EXACTLY the SEED-023 temp-table pattern. Extend `_FILTER_TEMP_TABLES` with `"_browse_filter_library"`, add one `_ensure_filter_temp` + EXISTS call after the editions block. No schema changes to fjms_enrichment.db.

2. **Reverse lookup is in-memory:** `MetadataManager.csv_bank` (~255K rows in memory) is the source. No DB query needed. `{sid for sid, row in meta_mgr.csv_bank.items() if row.get('library_code') in selected_codes}` is the complete implementation. O(255K) iteration, run in background thread.

3. **`library_code` is already on every search result:** `result['display']['library_code']` is populated by `SearchEngine` (confirmed at `search_engine.py:2200-2206`). The web search filter iterates `search_state.results` directly — zero extra DB queries, facet counts are free.

4. **SEED-023 template is complete and copy-pasteable:** The `_fetch_results_blocking()` function in `catalog_browse.py` (lines 248-266) and `_CatalogRefreshWorker.run()` (lines 519-552) show exactly how to wire new args. The `_catalog_cycle_*` + `_catalog_update_chips` pattern (lines 10378-10574) shows the desktop chip/button pattern to replicate.

5. **Codex gate target confirmed:** The key correctness invariant is `if library_codes and library_sys_ids:` — both must be truthy. Empty selection = no filter = no temp table built = no change to `total`. A unit test shaped like `test_filter_skipped_when_set_missing` in `test_seed023_catalog_filters.py` is the critical regression guard.

### Files Created

`.planning/phases/129-library-filter-search-browse-by-identification-seed-026/129-RESEARCH.md`

### Confidence Assessment

| Area | Level | Reason |
|------|-------|--------|
| Design crux (catalog push-down) | HIGH | `fjms_service.py` read directly; pattern confirmed in SEED-023 source |
| Web search filter mechanics | HIGH | `search.py` `_apply_printed_filter` / `_apply_pgp_filter` read directly |
| Desktop catalog wiring | HIGH | `genizah_app.py` `_CatalogRefreshWorker` + chips read directly |
| `library_code` availability on results | HIGH | `search_engine.py:2200-2206` confirmed |
| `csv_bank` reverse lookup | HIGH | `metadata_manager.py:413-427` + no reverse map found by grep |

### Open Questions

- OQ-1: Whether `_catalog_refresh()` (non-worker path) is reachable from user actions — LOW risk.
- OQ-2: How to pass `meta_mgr` into `_CatalogRefreshWorker.run()` — trivial plumbing decision.
- OQ-3: NiceGUI widget choice for multi-select dropdown — implementer's discretion (D-03).

### Ready for Planning

Research complete. Planner can write concrete tasks from the verified anchors and design crux resolution above. The Codex pre-flight gate should review specifically: `_FILTER_TEMP_TABLES` extension, None/empty = no-op guard, and composition with SEED-023 filters.
