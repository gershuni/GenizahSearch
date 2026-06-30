# Phase 131 — Codex Cross-AI Review (Round 2)

**Date:** 2026-06-30 · **Verdict:** REQUEST CHANGES (strong convergence) · **Reviewer:** codex-cli 0.139.0 (full repo read)

R1 resolution: **7 RESOLVED** (#2,#3,#5,#6,#8,#9,#10), **3 PARTIAL** (#1,#4,#7 — folded into N1/N3/N5 below). 6 new precision findings — all tightening, no new scope.

| # | Sev | Plan/File | Finding | Disposition |
|---|-----|-----------|---------|-------------|
| N1 | HIGH | 131-04 + 131-02 | `get_browse_library_facets` used as a module-level import in Plan 04 but defined as a `FjmsService` instance method in Plan 02 → ImportError/wrong call path. (R1 #7 PARTIAL) | **FIX** — Plan 04 calls `fjms.get_browse_library_facets(...)` obtained via `get_fjms_service()` (mirror the existing `get_browse_results` call site), OR Plan 02 adds a thin top-level wrapper. Lock the call contract identically in both plans. |
| N2 | HIGH | 131-05 / parallels.py | `_library_apply_selection` is referenced but not defined in parallels — it's nested in `search.py:1670`; catalog has its OWN local helper → NameError on Show-only Apply. | **FIX** — Plan 05 adds a local apply/normalization helper (or inlines the all-selected→[] normalization); do not call search.py's nested function. |
| N3 | HIGH | 131-01 / test_libfilter_desktop.py | Stale handoff test `test_catalog_build_browse_filters_includes_library` (:301) still expects ANY active library filter handed off; Plan 03 makes handoff Show-only-only. Hide-suppression test (R1 #4) still missing. (R1 #1+#4 PARTIAL) | **FIX** — Plan 01 revises that test with `_catalog_library_mode='show_only'` AND adds Hide-mode coverage: no `filters['library']`, restrict recompute carries no library restriction, and the notice fires. |
| N4 | MED | 131-03 / genizah_app.py | Desktop button `total` uses `LIBRARY_CODES-{'LOCAL'}` while the dialog universe is `library_codes_with_manuscripts()-{'LOCAL'}` → button count can exceed selectable libraries. | **FIX** — compute the button `total` from the same `library_codes_with_manuscripts()-{'LOCAL'}` universe. |
| N5 | MED | 131-02 + 131-04 / facets | Facet method underspecified — live browse counts use `COUNT(DISTINCT c.AlmaId)`/`GROUP BY c.AlmaId`; Plan 04's page-local `_resolve_all` mapping isn't the right source. | **FIX** — Plan 02 locks `get_browse_library_facets` to `SELECT DISTINCT c.AlmaId`-based counting with a full-corpus `meta_mgr.get_library_for_id`/csv_bank library mapper (not page-local); add TDD tests for duplicate AlmaIds + libraries not on the current page. |
| N6 | MED | 131-RESEARCH.md | Stale design text contradicts the revised A2/A3: Pattern 3 (Hide via `restrict_sys_ids` subtraction), Pattern 4 (result-derived facets), and the Open-Questions Q1 header/Recommendation lines (~501-510) still say "result-derived facets". | **FIX** — planner scrubs Pattern 3/4 + the Q1/Q2 header + Recommendation lead-ins so they match the locked A2 (TRUE facets) / A3 (HYBRID) DECISION blocks. No obsolete guidance left for the executor. |

All accepted (tightening only). Re-plan 01/02/03/04/05 + scrub RESEARCH.md, then re-review (R3).
