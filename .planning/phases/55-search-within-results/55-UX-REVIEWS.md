---
phase: 55
reviewers: [gemini, codex]
reviewed_at: 2026-03-29T14:00:00Z
plans_reviewed: [55-REFINEMENT-UX-PLAN.md]
---

# Cross-AI Plan Review — Phase 55 UX Revision

## Gemini Review

### Summary
The plan successfully pivots the UI to align with the underlying search architecture (manuscript-level restriction) through clear labeling, while introducing a post-filter to solve the "missing terms on page" frustration. The technical approach is lightweight and avoids expensive search engine modifications by leveraging UI-side set intersections.

### Strengths
- **Expectation Management:** Changing "Search within X results" to "Search within X manuscripts" is the single most effective way to resolve user confusion about why "new" pages appear in refined results.
- **Non-Destructive Refinement:** Keeping the default behavior broad (manuscript-level) is correct for Genizah research, where fragments are often physically separated but contextually linked.
- **Zero-Cost Replay:** Storing `_result_sys_ids` only during the active session and rebuilding them on replay is a smart way to keep the session JSON small and avoid versioning issues.
- **Snippet Enrichment:** Re-using the refinement chain for snippet highlighting ensures that even in "broad" mode, the user can see why a manuscript was included.

### Concerns
- **The "Same-Page" Logic Flaw (MEDIUM):** The plan suggests intersecting `_result_sys_ids` at the sys_id (manuscript) level. If Manuscript A has Term 1 on Page 1 and Term 2 on Page 2, MS A will be in both result sets. The intersection will still include MS A, and the UI will still show both pages. **To achieve a true "Same-Page" filter, you must intersect at the Page/Fragment ID level, not the Manuscript level.**
- **Non-Commutative Counts (LOW):** While labeling helps, users may still be puzzled that "A → B" returns 50 pages but "B → A" returns 40. This is inherent to the manuscript-restriction approach. Consider a tooltip or help-text explanation.
- **Checkbox Visibility (LOW):** Ensure it handles "Metadata only" chains gracefully — if a user searches a Shelfmark (1 result) then a text term, the filter is technically redundant but should probably remain visible to avoid UI flickering.

### Suggestions
- **ID Granularity:** Update `RefinementStep` to capture `_result_item_ids` (the most granular ID — page/fragment level). Use these for the "Only results with all terms" intersection.
- **Hybrid Intersection:** For Text Search steps use Page/Fragment IDs. For Metadata steps (Title/Shelfmark), either include all Page IDs belonging to those manuscripts, or exclude Metadata steps from the intersection logic.
- **Visual Feedback:** When checkbox is active, consider subtly changing the highlight color or adding a small icon to indicate "Full Chain Match."
- **Heuristic Labeling:** If intersection results in 0, message should suggest "Try unchecking 'Only results with all terms'" to guide user back to broader view.

### Risk Assessment: LOW
The plan is technically safe because it does not modify the search core. Primary risk is the logic error regarding Manuscript vs. Page ID intersection, which is easily corrected.

---

## Codex Review

Codex performed a codebase-level review rather than a plan review. Key finding:

### Bug Found (MEDIUM)
**Refinement counts switch units after replay/session restore.** Live refinement steps store `result_count=len(results)` (page-level), but `replay_chain()` rewrites `step.result_count` as `len(result_sys_ids)` (manuscript-level). Result: breadcrumb/search-within counts can silently change from pages to manuscripts after chip removal, stale-chain replay, or session restore. This reinforces the current UX confusion around search-within scope.

### Impact
This means the count displayed in the breadcrumb is inconsistent:
- After live search: shows 97 (page count)
- After chip removal/replay: shows 73 (manuscript count)
- After session restore: shows 73 (manuscript count)

---

## Consensus Summary

### Agreed Strengths
- Label change from "results" to "manuscripts" is the right primary fix
- Keeping manuscript-level restriction as default is correct for Genizah scholarship
- Not modifying search engine keeps risk low

### Agreed Concerns
1. **MEDIUM — Intersection must be at page/fragment level, not manuscript level.** Both reviewers flag that sys_id intersection won't achieve "same page" filtering. Need to use a more granular ID (unique_id/uid from Tantivy index).
2. **MEDIUM — Count unit inconsistency between live and replayed steps.** `replay_chain()` uses `len(result_sys_ids)` (manuscripts) while live uses `len(results)` (pages). Must be unified.

### Divergent Views
- Gemini suggests hybrid intersection (different logic for metadata vs text modes). This adds complexity — may be better to simply exclude metadata steps from the "all terms" filter.

### Action Items for Revised Plan
1. Change `_result_sys_ids` to `_result_page_ids` storing page-level unique IDs (uid from result items)
2. Fix `replay_chain()` to use `len(results)` for `result_count` (or consistently use manuscript count everywhere)
3. For "Only results with all terms" filter: intersect page IDs for text steps, skip metadata steps
4. Add tooltip explaining non-commutative behavior
