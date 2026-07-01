# Phase 131 — Codex Cross-AI Review (Round 3)

**Date:** 2026-06-30 · **Verdict:** REQUEST CHANGES (strong convergence) · **Reviewer:** codex-cli 0.139.0

R2 resolution: **N1–N4 RESOLVED**, N5/N6 PARTIAL (facet result-derived fallback still present). 4 residual findings — precision + a missed PATTERNS.md scrub. No new scope.

| # | Sev | Plan/File | Finding | Disposition |
|---|-----|-----------|---------|-------------|
| F1 | MED | 131-04 + 131-RESEARCH.md | Page-local/result-derived facet FALLBACK "when no filters active" still present (Plan 04 ~199-200; RESEARCH ~296/506/517). Catalog is ALWAYS paginated → even with no filters, page-local counts miss off-page libraries. (N5/N6 residual) | **FIX** — ALWAYS use `fjms.get_browse_library_facets(...)` for shortlist counts; the ONLY fallback is a no-count shortlist UI on facet-query failure. Remove every "result-derived fallback when no filters active" line from Plan 04 + RESEARCH (A2 + Patterns + Open-Questions). |
| F2 | HIGH | 131-PATTERNS.md | Stale executor-facing snippets contradict the revised plans: desktop `total` uses `LIBRARY_CODES.keys()`; catalog facets shown as a current-page `Counter`; parallels says `restrict_sys_ids` is "not modified". Plans 01/03/04/05 explicitly tell executors to READ PATTERNS.md → they'd follow obsolete guidance. | **FIX** — update/remove those snippets: desktop total → `library_codes_with_manuscripts()-{'LOCAL'}`; catalog facets → TRUE `get_browse_library_facets`; parallels → HYBRID (Show-only intersects into `restrict_sys_ids` pre-query, Hide post-fetch before export). |
| F3 | MED | 131-02 + 131-04 | `sys_id_to_library` param contract ambiguous — Plan 02 tests a dict mapping; Plan 04 passes `state.meta_mgr.get_library_for_id` (a callable). | **FIX** — lock the param to a CALLABLE `sys_id_to_library: Callable[[sys_id], str|None]` (matches what Plan 04 passes). Plan 02's TDD test must exercise a callable mapper. State the exact form in both plans. |
| F4 | MED | 131-05 | Show-only HYBRID intersection insertion rule not strict enough for library-only + per-manuscript exclusions — resolution must NOT be gated behind `_has_active_filters()` (false when only a library filter is set) and must occur BEFORE the existing exclusion subtraction. | **FIX** — place library sys_id resolution outside/alongside `_has_active_filters()` and intersect into `restrict_sys_ids` BEFORE the existing exclusion subtraction; add tests for library-only and advanced+library intersection. |

All accepted. Re-plan 02/04/05 + scrub PATTERNS.md + RESEARCH.md, then re-review (R4).
