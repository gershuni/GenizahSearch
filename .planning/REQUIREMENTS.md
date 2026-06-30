# Requirements: v8.4.0 Dual-Mode Library Filter

> Milestone goal: Library filtering can express BOTH "show only these libraries" and
> "hide these libraries" intents, persisted so each survives across searches, at full
> web + desktop parity — closing the v8.3.0 gap where the inclusion-only allowlist (over a
> result-derived universe) could not represent a sticky "exclude library X".

## v8.4.0 Requirements

### Dual-Mode Filter — Web Search (lead)

- [ ] **DMF-01**: On web `/search`, the user can choose between two filter modes in the library filter dialog — **"Show only selected"** (allowlist) and **"Hide selected"** (denylist).
- [ ] **DMF-02**: In **Hide** mode, libraries that surface in a later search but are NOT in the hidden set are shown by default (the "hide RNL" intent persists across searches); in **Show-only** mode, only the selected libraries are shown.
- [x] **DMF-03**: The chosen mode AND the selected set persist across searches and reloads via the `web/safe_storage.py` chokepoint (Phase 87 invariant), so neither intent has to be re-entered each search.
- [ ] **DMF-04**: The `/search` library-filter button (and any chip/label) clearly communicates the active mode and count — e.g. "Hiding N" vs "Showing N/total" — and a neutral state when no filter is active.
- [x] **DMF-05**: Existing v8.3.0 persisted allowlist values (`search_library_filter`) are migrated cleanly into the new (mode + set) model without error (default interpretation: Show-only with the existing set).
- [x] **DMF-06**: Sensible edge-state handling — an empty selection in Show-only means "show all" (no collision with the all-unchecked sentinel), and a fully-populated Hide set (everything hidden) is handled predictably.

### Desktop Parity

- [ ] **DMF-07**: The desktop catalog `LibraryFilterDialog` (Browse-by-Identification) offers the same Show-only / Hide modes, persisted, at parity with web.

### Browse-by-Identification (web catalog)

- [ ] **DMF-08**: The web Browse-by-Identification catalog filter offers the same Show-only / Hide modes (its universe is the full canonical library list, so the allowlist is already stable — included for cross-surface consistency).

### Parallels Library Control

- [ ] **DMF-09**: The web `/parallels` page gains a library-filter control (same dual-mode model) that scopes results via the existing `restrict_sys_ids` path, persisted for the page — closing the v8.3.0 deferred gap.

### Public API

- [ ] **DMF-11**: The public API `POST /api/search` and `POST /api/parallels` accept an optional library-filter **mode** (include / exclude) alongside `filters.library`, so programmatic callers can express "hide these libraries" as well as "only these". Backward-compatible: an omitted mode defaults to **include** (the current allowlist behavior); **exclude** resolves to the complement (sys_ids whose `library_code` is not in the given set) intersected into `restrict_sys_ids`. Documented in `docs/SEARCH_API.md` + the skill `api_contract.md`.

### Invariants (guard, not new capability)

- [ ] **DMF-10**: `'LOCAL'` (My Library) never appears as a web library-filter option in ANY mode or surface — the D-46 / D-NEW-7 guard (`tests/test_web_library_options_no_local.py` + `tests/test_phase_97_invariants.py`) stays green.

## Future Requirements (deferred)

- Cross-device sync of the filter preference (currently device-local via safe_storage).

## Out of Scope

- Any change to the LOCAL/My-Library desktop search-results filter (desktop already filters local results by library/shelfmark; this milestone is the catalog/search/parallels library-code filter only).
- Re-litigating the v8.3.0 result-derived facet computation for `/search` beyond what dual-mode requires.

## Traceability

| REQ-ID | Phase | Status |
|--------|-------|--------|
| DMF-01 | Phase 130 | Pending |
| DMF-02 | Phase 130 | Pending |
| DMF-03 | Phase 130 | Complete |
| DMF-04 | Phase 130 | Pending |
| DMF-05 | Phase 130 | Complete |
| DMF-06 | Phase 130 | Complete |
| DMF-07 | Phase 131 | Pending |
| DMF-08 | Phase 131 | Pending |
| DMF-09 | Phase 131 | Pending |
| DMF-10 | Phase 130, Phase 131 (cross-cutting guard) | Pending |
| DMF-11 | Phase 132 | Pending |
