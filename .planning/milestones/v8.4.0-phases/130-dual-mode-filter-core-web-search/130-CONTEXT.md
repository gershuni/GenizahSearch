# Phase 130: Dual-Mode Filter Core — Web `/search` - Context

**Gathered:** 2026-06-30
**Status:** Ready for planning

<domain>
## Phase Boundary

Redesign the web `/search` library-filter dialog from an inclusion-only allowlist into a
**dual-mode** filter: **Show only selected** (allowlist) vs **Hide selected** (denylist),
persisting `(mode + set)` via `web/safe_storage.py` so each intent survives across searches.
This is the **foundational** phase — it settles the shared `(mode + set)` state shape, the
dialog UX, the button/label states, the legacy-allowlist migration, and the edge-state
semantics that Phases 131 (desktop catalog / Browse-by-Identification / `/parallels`) and 132
(public API) will mirror.

Covers requirements **DMF-01, DMF-02, DMF-03, DMF-04, DMF-05, DMF-06**, and the cross-cutting
guard **DMF-10**. Web `/search` ONLY — other surfaces are Phase 131/132.

</domain>

<decisions>
## Implementation Decisions

### Dialog library universe (the crux — applies to BOTH modes)
- **D-01:** The dialog uses ONE unified structure in both modes: (a) a **shortlist** of libraries
  present in the current results, **sorted by result count (descending)**, each with its count;
  then (b) an **expandable section** listing **all other canonical libraries** (those not in the
  current results), **sorted A–Z**; plus (c) a **text-search input** to filter the combined list
  quickly. This solves pre-hiding (any library is reachable via the expand section, even one not
  yet seen) while keeping the "what's actually here" affordance + counts on top. The full universe
  is the canonical library set **minus `'LOCAL'`** (DMF-10).
- **D-02:** Counts shown on the shortlist come from the existing result-derived facet computation
  (`_compute_library_facets` over the full pre-`[:200]` result set); the expand section's libraries
  have no current-result count (count 0 / omitted).

### Mode toggle + selection behavior
- **D-03:** The mode toggle is a **segmented control** ("Show only | Hide") at the top of the dialog.
- **D-04:** Flipping the mode **RESETS the checked set** (starts fresh/empty). This prevents silently
  inverting intent (e.g. "show only CUL" must NOT become "hide CUL" on a mode flip).

### Default mode + first-open state
- **D-05:** With **no saved filter**, the dialog defaults to **Hide mode with an empty hide-set
  (= show all)** — the exclude use-case is the primary motivation for this milestone. The neutral
  button (no active restriction) is shown until a real restriction is applied.
- **D-06:** A **migrated legacy `search_library_filter` allowlist** (non-empty, from v8.3.0) opens in
  **Show-only mode** with that set (DMF-05). So: fresh → Hide/empty; legacy non-empty allowlist →
  Show-only/that-set.

### Button/label wording (bilingual, no chips)
- **D-07:** **Button-only** — no removable chips (consistent with the v8.3.0 smoke decision that
  removed chips and put state on the button). Three states:
  - Neutral (no active restriction): `Filter by library` / `סינון לפי ספרייה`
  - Show-only active: `Showing N/total` / `מציג N מתוך total`
  - Hide active: `Hiding N` / `מסתיר N`
  ("active" = the filter actually changes the shown set; an empty Hide-set or all-checked Show-only
  is neutral.)

### Edge states (DMF-06)
- **D-08:** Empty selection in **Show-only** = "show all" (no collision with the all-unchecked
  sentinel — there is no separate `[]`-means-nothing state). A fully-populated **Hide** set
  (everything hidden) is handled predictably (yields an empty result set with a clear "0 results"
  rendering, not an error).

### Persistence shape (planner's discretion within these constraints)
- **D-09:** Persist BOTH the mode and the code set through the `web/safe_storage.py` chokepoint
  (Phase 87 invariant; allowlist stays `[]`). Whether that is a single richer value under the
  existing `search_library_filter` key or a `(mode, codes)` pair is the planner's call, BUT the
  migration in D-06 must read any pre-existing plain-list value and load it as Show-only without
  error (no crash, no silent data loss). The persisted value must round-trip across search + reload.

### Claude's Discretion
- Exact NiceGUI widget choices for the segmented toggle and the expand/search affordances, the
  internal storage key/shape (subject to D-09), and how `_apply_library_filter` branches on mode
  (Show-only = keep `code ∈ set`; Hide = keep `code ∉ set`). The 'LOCAL' guard must remain satisfied
  by construction (D-10/DMF-10).

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Requirements & milestone
- `.planning/REQUIREMENTS.md` — v8.4.0 requirements DMF-01..DMF-11 (this phase owns DMF-01..06 + DMF-10). Authoritative.
- `.planning/PROJECT.md` §"Current Milestone: v8.4.0 Dual-Mode Library Filter" — milestone goal + scope.

### Code to modify / mirror (web `/search`)
- `web/pages/search.py` — `_compute_library_facets` (~1633, result-derived counts), `_open_library_filter_dialog` (~1681, the checkbox dialog to redesign), `_update_library_btn` (~1648, button state), `apply_library_filter` (~1783, persist on Apply), `_apply_library_filter` (~3677, the actual filtering — must branch on mode), persist key `search_library_filter` (~187-190 restore + sanitize).
- `web/pages/catalog_browse.py` — `apply_catalog_library_filter` (already builds from the FULL `LIBRARY_CODES` universe minus LOCAL via `all_codes = [c for c in LIBRARY_CODES if c != 'LOCAL']`) — reuse this full-list pattern for the dialog's expand-all section.

### Patterns & invariants
- `web/safe_storage.py` — the per-user state chokepoint (Phase 87; allowlist `[]`); all mode+set persistence routes through it.
- `shared/browse_map_utils.py` — `LIBRARY_CODES` + `get_library_display(code, short=False)` (bilingual labels via UI language) for the canonical list + display names.
- `tests/test_web_library_options_no_local.py` + `tests/test_phase_97_invariants.py` — the D-46/D-NEW-7 `'LOCAL'`-exclusion AST guards (DMF-10) that MUST stay green. The Phase-129 fix added `and c != 'LOCAL'` to the validation comprehensions in `search.py` + `catalog_browse.py` — preserve.
- The existing "Filter by Domains" checkbox dialog (`_open_domain_filter_dialog` in `search.py`) — the structural mirror the v8.3.0 library dialog already followed; the segmented mode toggle is the new element.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `_compute_library_facets` already yields per-library counts over the full pre-render result set — directly feeds the shortlist (sorted by count desc) in D-01.
- `catalog_browse.py`'s full-canonical-list construction (`[c for c in LIBRARY_CODES if c != 'LOCAL']`) is the ready-made source for the dialog's expand-all-A–Z section.
- `get_library_display(code, short=False)` gives bilingual display names (auto by UI language) for both the shortlist and the expand list.
- `web/safe_storage.py` get/set already used for `search_library_filter` (and `search_printed_filter`/`search_pgp_filter`) — same chokepoint for the new mode+set value.

### Established Patterns
- v8.3.0 put filter state on the button (no chips) — D-07 continues this.
- Filters stack in a fixed cascade: printed → pgp → library (see `_apply_printed_filter_and_render` ~3689). The mode branch lives inside `_apply_library_filter`; the cascade position is unchanged.
- The 3-state button color convention (neutral primary-outline vs active negative-fill) from `_update_library_btn` — extend to the two active modes.

### Integration Points
- Restore/sanitize path at `search.py` ~187-190 (`search_state.library_filter = [c for c in _lib0 if c in LIBRARY_CODES and c != 'LOCAL']`) must become mode-aware (read mode + set, migrate legacy list → Show-only).
- `_apply_library_filter` (~3677) gains the mode branch (∈ for Show-only, ∉ for Hide).
- `clear_search_snapshot()` / defaults that reset `search_library_filter` (~2529) must also reset the mode.

</code_context>

<specifics>
## Specific Ideas

- The dialog design (D-01) — shortlist-by-count + expandable all-others-A–Z + text search — is the
  user's explicit refinement and is the defining UX of this phase. The text search must filter the
  combined list (shortlist + expanded) so a long canonical list stays usable.
- Default to **Hide** mode for fresh users (D-05) — a deliberate choice reflecting that "hide a noisy
  library" is the primary motivating use-case for the whole milestone.

</specifics>

<deferred>
## Deferred Ideas

- Desktop catalog `LibraryFilterDialog` parity, web Browse-by-Identification dual-mode, and the web
  `/parallels` library control → **Phase 131** (DMF-07/08/09). The shared `(mode + set)` model and
  dialog UX settled here are the template they mirror.
- Public API `mode` (include/exclude) on `/api/search` + `/api/parallels` → **Phase 132** (DMF-11).
- Cross-device sync of the filter preference → Future Requirements (out of scope; device-local only).
- Public API semantics for `filters.library` are NOT touched in this phase (Phase 132 only).

None of the discussion strayed outside the milestone scope.

</deferred>

---

*Phase: 130-dual-mode-filter-core-web-search*
*Context gathered: 2026-06-30*
