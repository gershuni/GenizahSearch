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
