---
phase: 56
reviewers: [gemini, codex]
reviewed_at: 2026-03-29T18:00:00Z
plans_reviewed: [56-01-PLAN.md, 56-02-PLAN.md, 56-03-PLAN.md]
---

# Cross-AI Plan Review — Phase 56

## Gemini Review

### Summary
The implementation plans for Phase 56 are architecturally sound and follow a logical progression from shared logic to platform-specific UI. By centralizing the shelfmark resolution and data structures in `shared/exclusion_service.py`, the design ensures consistency between the Web and Desktop applications—a critical requirement for scholarly tools. The decision to treat exclusions as a "post-search filter" (independent of the refinement chain) is a wise UX choice that prevents confusing interactions with existing filters. The plans are comprehensive and directly address all requirements (EXCL-01 through EXCL-04) and user decisions (D-01 through D-10).

### Strengths
- **Modularity**: Creating a dedicated `exclusion_service.py` prevents further bloating of the already large `genizah_core.py` (~8,300 lines) and facilitates testing.
- **Performance Awareness**: The use of `set` for `sys_ids` in Plan 01 ensures that the O(1) lookup time will keep search filtering fast, even with large exclusion lists.
- **Robust CSV Handling**: Inclusion of UTF-8-sig (BOM) support and keyword-based column detection shows empathy for researchers who often work with Excel-exported CSVs.
- **UX Consistency**: Reusing the breadcrumb/chip pattern for exclusions and the domain exclusion pattern for the results breakdown ensures a shallow learning curve for existing users.
- **Data Integrity**: Maintaining the list of `unresolved` shelfmarks allows for the "Resolution Report" required by the success criteria.

### Concerns
- **Large File DoS / Memory (LOW)**: Plan 01 doesn't specify a size limit for `parse_shelfmark_file`. A very large file could hang the UI thread (Desktop) or consume excessive memory (Web).
- **Supabase Latency/Availability (MEDIUM)**: Fetching from Supabase lists with 5,000+ items every time the dialog opens or session restores might be slow. UI stutter or "App Unresponsive" if not handled asynchronously.
- **Resolution Ambiguity (LOW)**: Some aliases can be ambiguous. A shelfmark might resolve to multiple sys_ids (though rare in this corpus).
- **Persistence Sync (MEDIUM)**: Web uses `app.storage.user`. If a user excludes a list on one browser and switches to another, the "session" persistence might behave differently.

### Suggestions
- **Batch Resolution**: Ensure that `resolve_shelfmarks` uses efficient lookups rather than querying in a loop.
- **Desktop UI Blocking**: Ensure file parsing and resolution in `genizah_app.py` happens in a QThread to prevent blocking.
- **Clearer Resolution Report**: Provide a "Copy to Clipboard" button for unresolved items.
- **Preview Before Apply**: In "From File" tab, show a quick preview before committing.
- **Efficient union**: Use `set().union(*(s.sys_ids for s in sources))` in `compute_excluded_ids`.

### Risk Assessment: LOW
The phase is well-defined and largely additive. The dependency on Plan 01 is clear and TDD mitigates logic errors. The most significant risk is UI performance with very large datasets, but sets and post-search filtering are correct mitigations.

---

## Codex Review

### Plan 01: Shared Exclusion Service

**Summary**: Right shape for Wave 1. Centralized parsing/resolution/serialization is the cleanest approach. Main weakness: data contract is too thin for the resolution report requirements, and shelfmark resolution is underspecified.

**Strengths**:
- Centralizes logic that must be identical across web and desktop
- Correctly reuses canonical `normalize_shelfmark()`
- Includes serialization early for session persistence
- TDD-first is appropriate because resolution rules regress silently

**Concerns**:
- **HIGH**: `ExclusionSource` may not capture enough for D-04 resolution report table (original input, matched shelfmark, matched sys_id, duplicates, ambiguous rows)
- **HIGH**: `build_shelf_map` underspecified — if it only indexes one shelfmark per manuscript, it misses variant behavior. Should index `call_numbers_raw` variants too.
- **MEDIUM**: Ambiguous matches not called out — "not found" vs "matched multiple" need separate handling
- **MEDIUM**: CSV handling doesn't address delimiter sniffing, headerless files, quoted cells, empty/comment rows, or oversized files
- **MEDIUM**: `serialize_sources` could become a storage problem with large payloads

**Risk**: MEDIUM

### Plan 02: Web Exclusion UI

**Summary**: Fits existing patterns conceptually, but underestimates how many render/restore paths exist in search.py. Missing one of two required entry points.

**Strengths**:
- Post-search filter matches D-08
- Reusing domain-exclusion pattern is the right direction
- State in SearchUIState is consistent
- Per-source clear and source-aware count display aligned with UX goals

**Concerns**:
- **HIGH**: D-01 not fully covered — plan mentions results-area button but not filter panel entry point
- **HIGH**: Many rerender paths beyond search completion (domain exclusion, printed filter, measurement, history restore, refinement replay, word-search). Without unified pipeline, excluded manuscripts will reappear.
- **HIGH**: D-05 collapsible excluded section not well-specified
- **MEDIUM**: List integration should use `state.lists_mgr`, not direct Supabase access. Need behavior for logged-out/no-lists users.
- **MEDIUM**: File parsing/resolution should run off event loop to avoid UI freeze

**Risk**: HIGH

### Plan 03: Desktop Exclusion UI

**Summary**: Directionally correct migration from flat to source-based state, but closer to partial migration than complete plan.

**Strengths**:
- Extending existing ExcludeDialog is pragmatic
- Shared ExclusionSource objects are the right long-term model
- Backward compatibility explicitly considered
- Session save/restore integration identified

**Concerns**:
- **HIGH**: D-01 incomplete — doesn't add second post-search entry point near results summary
- **HIGH**: D-04 underspecified — no resolution report table described for file import
- **HIGH**: Multiple result-display paths on desktop — plan doesn't specify where exclusions apply during initial load, batches, or restore
- **MEDIUM**: Migration from old flat fields needs explicit mapping of which paths remain, which are replaced
- **MEDIUM**: History restore not mentioned — desktop stores exclusion state in history structures

**Risk**: HIGH

### Overall
Wave ordering is correct. Main gap across all plans is contract/detail, not feature intent. Plans need: richer source/report model in Plan 01, unified post-search filter pipeline in Plans 02-03, and both required entry points explicitly covered.

---

## Consensus Summary

### Agreed Strengths
- **Architecture**: Both reviewers praise the Wave 1 shared service → Wave 2 parallel UI split as clean and logical
- **Post-search filtering**: Both agree D-08 (exclusions independent of refinement chain) is the correct approach
- **Performance**: Both note the use of `set()` for O(1) sys_id lookups is appropriate
- **Reuse of existing patterns**: Both highlight that following `_apply_domain_exclusions` and existing `ExcludeDialog` patterns reduces risk
- **TDD for service layer**: Both see value in test-first approach for the resolution logic

### Agreed Concerns
1. **D-01 Two entry points not fully planned (HIGH)** — Codex flagged this for both Plans 02 and 03. The filter panel button entry point is mentioned in CONTEXT.md D-01 but not explicitly wired in either UI plan. Gemini didn't flag this specifically.
2. **Resolution report detail insufficient (HIGH)** — Codex flagged that ExclusionSource model may be too thin for D-04's resolution report table. Gemini suggested "Preview before Apply" which is related.
3. **Multiple render/restore paths (HIGH, web)** — Codex identified that search.py has many paths that re-display results, and manuscript exclusions need to be applied consistently across all of them. Gemini didn't flag this specifically.
4. **Async handling for large operations (MEDIUM)** — Both agree that list fetching and file parsing must be async (web: run.io_bound, desktop: QThread) to avoid UI freezes.
5. **Session persistence edge cases (MEDIUM)** — Both raised concerns about persistence payload size and cross-device sync behavior.

### Divergent Views
- **Overall risk**: Gemini rates LOW overall, while Codex rates Plans 02 and 03 as HIGH risk due to insufficient pipeline centralization and missing entry points. The divergence is likely because Gemini reviewed at feature-level while Codex reviewed at implementation-level against the actual codebase patterns.
- **CSV handling depth**: Codex wants delimiter sniffing, headerless files, and comment rows handled; Gemini considers BOM + keyword detection sufficient. For scholarly use, Gemini's assessment is likely correct (researchers export simple CSVs from spreadsheets).
- **ExclusionSource model richness**: Codex wants per-row resolution records (input_value, normalized, status, matched_sys_ids, error_reason); Gemini finds the current model adequate. The truth is likely in between — a richer model helps the resolution report but shouldn't over-engineer for the MVP.
