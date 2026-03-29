---
phase: 56
reviewers: [gemini, codex]
reviewed_at: 2026-03-29T19:30:00Z
review_round: 2
plans_reviewed: [56-01-PLAN.md, 56-02-PLAN.md, 56-03-PLAN.md]
---

# Cross-AI Plan Review — Phase 56 (Round 2: Post-Revision)

## Gemini Review (Round 2)

### Summary
The revised plans successfully shift the architectural weight into a shared service (exclusion_service.py) that handles the heavy lifting of shelfmark normalization and cross-referencing. By introducing the ResolvedEntry model, the plans now satisfy the requirement for transparent user feedback (D-04). The explicit enumeration of render/display paths for both Web and Desktop provides a clear roadmap for integration, ensuring that exclusions remain "sticky" throughout complex user workflows like history restoration and refinement chains.

### Previous HIGH Concerns

| Concern | Status | Notes |
|---------|--------|-------|
| D-01 entry points missing | **RESOLVED** | Plans now explicitly specify button placement in both Filter Panel and Results Area for both apps |
| Resolution report detail | **RESOLVED** | ResolvedEntry and resolved_entries enable detailed table views for D-04 |
| Web render/restore paths | **RESOLVED** | Plan 02 lists 7 specific code paths where filter must be applied |
| Desktop display paths | **RESOLVED** | Plan 03 identifies 5 paths including CompositionSearch and batch appending |
| Desktop D-04 table | **RESOLVED** | Plan 03 includes QTableWidget with columns and color-coding |

### Remaining Concerns
- **UI Performance with Large Imports (MEDIUM)**: Rendering resolution report with thousands of rows without virtualization may cause UI freezing
- **Memory Bloat in Session State (LOW)**: Storing full ResolvedEntry objects in memory for large imports
- **Ambiguity in "Status" (LOW)**: "Yellow" status (duplicate) needs clear tooltip for user clarity

### Suggestions
- Cap resolution report preview table to first 500-1,000 entries for large files
- Ensure resolved_entries are omitted from serialization (Plan 01 already handles this)
- Consider progress bar for bulk normalization in Desktop UI

### Risk Assessment: LOW
Risk downgraded from Medium to Low. Primary technical hurdle (shelfmark resolution) handled by shared TDD service. UI risks limited to standard implementation details. Explicit path mapping provides high confidence against state-management bugs. **Plans are ready for execution.**

---

## Codex Review (Round 2)

### Summary
The revised plans are substantially stronger and now cover the core execution risks that previously made the feature unsafe to implement. The shared model supports D-04 reporting, both UI plans include the two required entry points, and both name the relevant render/restore paths instead of assuming a single happy-path render. No round-1 HIGH blocker is still open. Remaining concerns are behavioral edge cases and performance/UX clarifications that can be handled during implementation.

### Previous HIGH Concerns

| Concern | Status | Assessment |
|---------|--------|-----------|
| D-01 two entry points | **RESOLVED** | Both web and desktop plans include both entry points |
| Resolution report detail | **RESOLVED** | ResolvedEntry + ExclusionSource.resolved_entries addresses missing per-row data |
| Web render/restore paths | **RESOLVED** | Unified exclusion helper + explicit path list |
| Desktop display paths | **RESOLVED** | Named result paths with _apply_manual_exclusions coverage |
| Desktop D-04 | **RESOLVED** | Concrete QTableWidget with columns and color-coding |

### Remaining Concerns
- **Multi-source overlap semantics (MEDIUM)**: Same sys_id excluded by multiple sources — header breakdown counting and per-source clear behavior when another source still excludes the same manuscript
- **Long-running import not fully offloaded (MEDIUM)**: Web offloads list fetch and shelf_map build but parse+resolve for large files should also be explicitly backgrounded. Desktop should state whether file parsing runs in a worker.
- **Restored presentation recomputation (MEDIUM)**: Since resolved_entries are transient, plans should make explicit that restored header count, excluded section, and clear buttons are recomputed from current results + persisted sys_ids
- **List-fetch failure states (LOW)**: Picker should have explicit UX for unauthenticated users, empty lists, and network failures

### Suggestions
- Define counting rule: unique total in header, per-source counts shown separately, clearing one source only restores manuscripts not still excluded by another
- Explicitly background full file-import pipeline in both apps
- Add regression tests for overlapping sources, per-source clear with overlap, backward-compatible session restore
- State fallback UX for list mode: signed-out, loading, empty, and error states
- Ensure exclusion helper returns both visible and excluded subsets from same source list so D-05 and D-07 cannot drift

### Risk Assessment: MEDIUM
Revised plans resolved original structural blockers and are execution-worthy. Remaining risks are behavioral edge cases and performance clarifications, not architectural holes. Safe to proceed with edge cases handled during implementation.

---

## Consensus Summary

### All Round-1 HIGH Concerns: RESOLVED

Both reviewers independently confirm all 5 HIGH-severity concerns from round 1 are now fully resolved:
1. D-01 two entry points — both apps now have both entry points explicitly planned
2. Resolution report detail — ResolvedEntry model provides per-row data for D-04
3. Web render/restore paths — 7 paths explicitly listed and wired
4. Desktop display paths — 5 paths identified with _apply_manual_exclusions coverage
5. Desktop D-04 — QTableWidget with columns, color-coding, and summary

### Agreed Strengths (Round 2)
- **Shared service with TDD** — both praise the ResolvedEntry + ExclusionSource architecture and test-first approach
- **Explicit path enumeration** — both highlight the 7 web + 5 desktop render path listing as a significant improvement
- **Transient vs persistent data separation** — both note resolved_entries excluded from serialization is correct

### Remaining MEDIUM Concerns (implementation-time)
1. **Multi-source overlap semantics** — Codex flagged: define counting behavior when sys_id excluded by multiple sources, and per-source clear when overlap exists. Simple rule: unique total in header, per-source raw counts beside, clearing one source only un-excludes items not covered by another source.
2. **Large file import performance** — Both flagged: background full parse+resolve pipeline, cap resolution table to 500-1000 rows for display.
3. **Restored state recomputation** — Codex flagged: ensure header counts and excluded section are rebuilt from current results + persisted sys_ids (not from transient resolved_entries).

### Risk Assessment
- **Gemini: LOW** — plans ready for execution
- **Codex: MEDIUM** — execution-worthy with edge cases handled during implementation
- **Consensus: LOW-MEDIUM** — all structural issues resolved, remaining concerns are implementation-detail level

### Verdict
**Plans are ready for execution.** The MEDIUM concerns (overlap semantics, large file performance, restored state recomputation) are implementer-solvable at coding time and do not require another planning iteration.

---

# Post-Implementation Code Review — Phase 56 (Round 3)

**Reviewed at:** 2026-03-29
**Review type:** Post-implementation (code review, not plan review)
**Reviewers:** Gemini (Codex unavailable — requires TTY)

## Gemini Review

That's `web/pages/parallels.py`. I need to check `genizah_app.py`.
I'll search for `COL_CHECKBOX` in `genizah_app.py` to understand checkbox handling.
The code review for **Phase 56 (Exclude Known Manuscripts)** is complete. Below is the assessment based on the requested files and specific technical questions.

### **Summary**
Phase 56 introduces a robust service for manuscript exclusion across both desktop and web platforms. It correctly leverages the existing shelfmark normalization logic and metadata banks to resolve arbitrary shelfmark strings or system IDs into exclusion sets. The implementation provides clear visual feedback (D-04 resolution reports) and persists exclusion sources across sessions.

---

### **Strengths**
- **Unified Service Layer:** `shared/exclusion_service.py` provides a clean, testable logic layer used by both apps, ensuring consistent shelfmark resolution behavior.
- **Variant Handling:** The exclusion logic correctly resolves manuscripts via both primary shelfmarks and variant `call_numbers_raw`, preventing "leaks" where an excluded item appears under a slightly different name.
- **Non-Destructive Filtering (Desktop):** The use of `setRowHidden` in the desktop app allows for instantaneous toggling of exclusions without expensive re-searches or losing UI state (like scroll position or expansion).
- **Graceful Error Handling:** Serialization/deserialization logic handles malformed session data without crashing, and the resolution report clearly identifies "not found" or "duplicate" entries.

---

### **Concerns & Risks**

#### **1. Desktop `setRowHidden` vs. Export/Bulk Actions**
*   **Risk Level: HIGH**
*   **Observation:** The `_rerender_with_exclusions` method correctly hides rows, but the **Export** (`export_results`) and **Bulk Add to List** (`search_add_selected_to_list`) functions do not check `isRowHidden(row)`.
*   **Edge Case:** If a user clicks "Select All," the `on_search_select_all_toggled` method checks **all** rows, including those hidden by the exclusion filter. Consequently, excluded manuscripts will still be exported or added to lists if the user performs a bulk action after filtering.
*   **Recommendation:** Update `export_results`, `search_add_selected_to_list`, and `on_search_select_all_toggled` in `genizah_app.py` to check `not self.results_table.isRowHidden(i)` before processing a row.

#### **2. Web Results Swap Safety & Export Pipeline**
*   **Risk Level: HIGH (for Export Consistency)**
*   **Observation:** The temporary swap of `search_state.results` in `_apply_manuscript_exclusions` is technically safe from race conditions because the operation is synchronous within the task. However, it fails to sync with the **Web Export API**.
*   **Issue:** `web/api.py` (e.g., `/api/export/excel`) relies on `state.last_results` (global state), which is only updated when a search first completes. The post-search exclusion filter does not update this global state. As a result, **Web exports will always include all excluded manuscripts**, regardless of active filters.
*   **Recommendation:** The `state.last_results` should be updated or the Export API should be changed to use the filtered `search_state.displayed_results` to ensure "What You See Is What You Export."

#### **3. List Picker Scalability**
*   **Risk Level: MEDIUM**
*   **Observation:** In the Web dialog, `get_all_lists` is called on open, and all lists are rendered as checkboxes in a scroll area.
*   **Issue:** For power users with hundreds or thousands of saved lists, this will cause significant lag when opening the "Exclude Manuscripts" dialog.
*   **Recommendation:** Consider adding a search/filter bar for lists within the dialog or using a virtualized list component (if available in the UI framework) to handle large numbers of user lists.

---

### **Suggestions for Improvement**
- **Surgical Selection:** In the Desktop `ExcludeDialog`, when "Load to Editor" is clicked for a list, it currently appends all `sys_ids` to the text area. Adding a "Count" or "Preview" before loading would improve UX for very large lists.
- **UI Consistency:** Ensure the "Exclude" button color/icon in the Web app matches the Desktop app's red theme to signify a "negative" filter.

### **Risk Assessment**
| Category | Risk | Impact |
| :--- | :--- | :--- |
| **Functional** | High | Excluded items "leak" into Excel/Word exports in both Desktop and Web. |
| **Performance** | Medium | UI hang when opening the exclusion dialog if the user has many saved lists. |
| **Stability** | Low | The service layer is well-tested and handles edge cases gracefully. |

**Final Verdict:** The exclusion logic is solid and the resolution service is excellent. However, the **Export** logic in both applications must be updated to respect the "hidden" or "filtered" state to meet user expectations for Phase 56.

---

## Consensus Summary (Single Reviewer)

### Key Concerns (from Gemini)

1. **HIGH: Export bypasses exclusions** — Both desktop `export_results` and web `/api/export/excel` (via `state.last_results`) include excluded manuscripts. Desktop doesn't check `isRowHidden`, web export uses unfiltered global state.
2. **HIGH: Bulk actions on hidden rows** — Desktop "Select All" + export/add-to-list will include excluded rows.
3. **MEDIUM: List picker scalability** — All lists + items fetched on dialog open. Could lag with many lists.

### Strengths (agreed)

- Clean shared service layer with good test coverage
- Variant shelfmark resolution via call_numbers_raw
- Non-destructive filtering (setRowHidden) preserves enrichment state
- Graceful error handling in serialization

### Action Items

1. [ ] Desktop: Add `isRowHidden` check to `export_results`, `search_add_selected_to_list`, `on_search_select_all_toggled`
2. [ ] Web: Update export API to use filtered `search_state.displayed_results` instead of `state.last_results`
3. [ ] Consider lazy-loading list items in the web picker dialog
