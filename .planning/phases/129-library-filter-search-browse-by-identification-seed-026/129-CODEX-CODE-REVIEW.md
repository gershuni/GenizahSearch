---
phase: 129-library-filter-search-browse-by-identification-seed-026
gate: codex-code-review (CODE gate — second of two Codex gates)
reviewer: codex (gpt-5.x via codex exec)
reviewed: 2026-06-28
verdict: APPROVE WITH CHANGES
findings: { blocker: 0, high: 0, medium: 1, low: 1 }
status: resolved
---

# Phase 129 — Codex CODE-Review (cross-AI gate)

Independent post-implementation review by Codex of the full phase-129 source diff
(`shared/fjms_service.py`, `web/pages/search.py`, `web/pages/search_state.py`,
`web/pages/catalog_browse.py`, `genizah_app.py`, `genizah_translations.py`,
`tests/*`). This is the CODE gate that complements the PLAN pre-flight gate
(`129-CODEX-PREFLIGHT-REVIEW.md`) per the v8.3.0 two-Codex-gates process.

**Verdict: APPROVE WITH CHANGES.** Codex confirmed the core design holds — push-down
applied before COUNT/pagination, content-derived TEMP token, fail-open on empty
resolution, off-thread/off-event-loop resolution, Phase 87 safe_storage compliance,
and additive backward-compatibility of `get_browse_results`. Two findings, both fixed.

## Findings

### MEDIUM — web catalog io_bound race (the web-side analog of WR-01)
`web/pages/catalog_browse.py:304` passed the live `current_library_filter['value']`
list by reference into `run.io_bound`, while `clear_library_code` (`:979`) mutated
that same list in place via `lst.remove(code)`. A chip removal during an in-flight
fetch could race the background resolver. The Claude review fixed this class on the
desktop side (WR-01) but missed the web catalog side.

**Fix (commit `537d1b2d`):** snapshot the list at the call site
(`list(current_library_filter['value']) or None`) and make `clear_library_code`
assign a NEW list (`[c for c in ... if c != code]`) instead of mutating in place.

### LOW — WR-04 indicator placed in an unreachable branch
`web/pages/search.py:4003`: the WR-04 `(Library filter)` count indicator was added
to the word-search `else` branch, but the `elif (... or bool(search_state.library_filter))`
at `:3993` intercepts every library-active case first — so the indicator never
rendered when a library filter was active.

**Fix (commit `48c5914e`):** build the `count_parts` list (with the `Library filter`
indicator) in the `elif` (library-active) branch, which is the actually-reachable path.

## Post-fix verification
- `python -m ruff check web/pages/catalog_browse.py web/pages/search.py` → clean.
- `pytest tests/test_libfilter_web_search.py tests/test_libfilter_catalog.py tests/test_no_raw_storage_access.py` → 19 passed.

No new BLOCKER/HIGH. Both findings resolved → gate satisfied.
