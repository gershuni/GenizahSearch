---
phase: 55
reviewers: [gemini, codex]
reviewed_at: 2026-03-28T20:00:00Z
plans_reviewed: [55-01-PLAN.md, 55-02-PLAN.md, 55-03-PLAN.md]
---

# Cross-AI Plan Review — Phase 55

## Gemini Review

This review evaluates the implementation plans for **Phase 55: Search Within Results** for the GenizahSearch project.

### 1. Summary
The proposed plans (55-01 through 55-03) represent a robust and modular approach to implementing multi-step search refinement. By centralizing the logic in a shared `RefinementStep` dataclass and using a TDD approach for the core logic (Plan 55-01), the project ensures behavioral consistency between the NiceGUI web interface and the PyQt6 desktop application. The strategy strictly adheres to the architectural constraints—specifically avoiding the overloading of existing restriction mechanisms—and meticulously addresses the 18+ specific user decisions (D-01 to D-16) outlined in the roadmap.

### 2. Strengths
*   **Logical Decoupling:** Moving the refinement logic to `shared/refinement.py` ensures that complex chain operations (truncation, intersection, and label logic) are tested in isolation from UI side effects.
*   **State-Based Replay (D-14):** Persisting parameters rather than `sys_id` lists is a critical design choice that prevents memory bloat and ensures that sessions remain serializable and lightweight.
*   **Granular TDD (55-01):** The plan for 14 specific unit tests covering edge cases like chain truncation and mode-mixing labels ensures the foundation is solid before UI work begins.
*   **UX Consistency:** Both Web and Desktop plans explicitly implement the "stale indicator" (D-16) and "zero-result recovery" (D-14a), which are often overlooked in refinement features.
*   **Additive Filtering:** The use of `compute_effective_restrict()` correctly addresses the requirement to intersect refinement chains with pre-search filters (D-09) without destructive overwrites.

### 3. Concerns
*   **Performance of "Pop-back" Re-execution (D-13) [MEDIUM]:** The decision to not cache results and instead re-execute the entire chain means that removing the 4th chip in a 5-step chain triggers four sequential (or nested) searches. While this ensures data integrity, it may feel sluggish on the desktop app if the Tantivy index is under heavy load or if the result sets are extremely large.
*   **PyQt6 Horizontal Scroll Complexity (D-05) [LOW]:** Implementing a horizontally scrolling strip of dynamic widgets in PyQt6 (Plan 55-03) can be finicky regarding layout refreshes and "scroll-to-end" behavior when adding new steps.
*   **Search History Guard (D-15) [LOW]:** While the plan mentions a "history guard," care must be taken to ensure that "Search Within" actions don't inadvertently trigger the "Recent Searches" logic in the shared core, especially if `genizah_core.py` has automatic logging.
*   **Breadcrumb Overflow [LOW]:** With "unlimited nesting depth" (D-05), the UI needs to handle very long query strings within chips to prevent the horizontal scroll from becoming unwieldy.

### 4. Suggestions
*   **Intermediate Result Memoization:** Even though D-13 specifies "no caching" (likely referring to persistent/session caching), consider a short-lived *in-memory* cache for the `sys_ids` of the current session's chain to make "back" operations instantaneous.
*   **Chip Text Truncation:** For the breadcrumb UI, implement a maximum character length for each chip's display text (e.g., "Query: Very long search te...") with a tooltip showing the full query, to keep the refinement strip readable.
*   **Atomic Refinement Updates:** Ensure that `compute_effective_restrict()` is called inside the `SearchThread` (Desktop) or the async search task (Web) to prevent the UI from freezing if the intersection logic involves large sets of IDs.
*   **Visual Feedback during Replay:** When a chip is removed and the chain re-executes, show a specific "Re-evaluating refinement..." loading state rather than a generic search spinner to inform the user why multiple steps are processing.

### 5. Risk Assessment
**Risk Level: LOW**

The risk is low because the heavy lifting is being done in a TDD-validated shared module. The clear separation between "Refinement Parameters" (the chain) and "Effective Restrictions" (the computed IDs) follows clean architecture principles. The primary risks are UI-related (layout polish in PyQt6 and state synchronization in NiceGUI), which are mitigated by the non-autonomous "human-verify" checkpoints in the UI waves.

---

## Codex Review

### Plan 55-01: Shared RefinementStep dataclass + chain helpers

**Summary**
This is the right first wave and it matches the architecture constraint: put the chain model and intersection logic in shared code before touching either UI. The weakness is that the proposed `RefinementStep` shape looks too narrow for faithful replay, restore, and mixed-mode behavior, so the plan is directionally good but not yet complete enough to be a safe foundation.

**Strengths**
- Centralizes refinement semantics instead of duplicating them in web and desktop.
- Explicitly avoids overloading existing `restrict_sys_ids`.
- Adds tests up front, which is the right place to validate `None` vs empty-set behavior.
- Includes helpers for mode-label display and chain truncation, which directly map to D-10 and D-12.

**Concerns**
- HIGH: The proposed fields do not clearly cover all replay-critical search params. `query`, `mode`, `gap`, `exclude_words`, `text_position`, and `responsa_options` may not be enough for variant-mode settings or any future mode-specific knobs, which makes D-14 replay fragile.
- HIGH: `compute_effective_restrict()` needs an exact contract for `None` vs empty set vs active filters. If that contract is even slightly wrong, SRCH-04/D-09 breaks silently.
- MEDIUM: D-16 needs a stable "scope changed" signature, but that helper is not in this plan, so both UIs may reimplement it differently.
- MEDIUM: `result_count` is useful for display, but persisted counts can go stale after filter changes or replay and should not be treated as authoritative.
- LOW: Fourteen tests is probably light for a shared state model that both apps will depend on.

**Suggestions**
- Replace the narrow field list with either a mode-agnostic `search_params` payload or an explicitly versioned schema that can grow safely.
- Define `compute_effective_restrict()` behavior for all four cases: no base scope, no refinement scope, empty base scope, empty refinement scope.
- Add a shared helper for "scope signature" / "scope changed" detection.
- Add tests for mixed-mode chains, zero-result steps, stale `result_count`, and serialization compatibility.

**Risk Assessment**
`MEDIUM` because the abstraction direction is correct, but if the shared model is underspecified, both Wave 2 plans inherit the mistake.

### Plan 55-02: Web refinement UI

**Summary**
The web plan covers most user-facing requirements, but it packs too much behavior into one implementation task inside a very stateful page. The biggest gap is persistence: current web behavior restores saved results directly, while D-14 requires persisting only refinement params and replaying the chain. That mismatch needs to be designed explicitly before implementation starts.

**Strengths**
- Correctly depends on the shared wave first.
- Covers the major roadmap items: refine mode, breadcrumb strip, history guard, zero-result recovery, persistence, and stale-scope indicator.
- Includes a human verification checkpoint, which is appropriate for the UI flow.

**Concerns**
- HIGH: The plan does not explain how it will replace current result-restore behavior with chain replay.
- HIGH: "Current result set" is ambiguous on the web. There are raw results, displayed results, domain exclusions, printed filtering, word exclusions, and post-search measurement filters. The plan does not define which set becomes `refinement_restrict_sys_ids`.
- HIGH: One auto task is doing execution changes, UI state, persistence, history, replay, zero-result recovery, and stale-scope UX all at once.
- MEDIUM: There is no automated verification for history suppression, restore replay, chip truncation, or filter intersection semantics.
- MEDIUM: Zero-result recovery is listed, but the exact state transition is not defined.
- LOW: Focus/scroll behavior in NiceGUI can be sensitive to client context.

**Suggestions**
- Split this into separate tasks: search/persistence plumbing, breadcrumb/refine UI, and restore/history integration.
- Decide explicitly whether refinement scope is based on raw results or currently visible results, then encode that rule once.
- Add a small shared/pure helper layer so the page logic is not all embedded in one handler.
- Add at least targeted tests around replay, truncation, history guard, and "scope changed" detection.

**Risk Assessment**
`HIGH` because the restore/replay model and result-scope semantics are still underdefined.

### Plan 55-03: Desktop refinement UI

**Summary**
The desktop plan is concrete and mostly aligned with the approved UX, but it has the same core persistence ambiguity as the web plan and a more obvious completeness gap: D-14a zero-result recovery is not explicitly called out. Given how much state the desktop app already persists and restores, this needs to be broken down more carefully than a single implementation task.

**Strengths**
- Dedicated strip above the results table matches the desktop-specific decision well.
- Method list is concrete enough to show real implementation intent.
- Includes session persistence, history guard, chip removal, clear-all, and replay hooks.
- Properly depends on the shared foundation first.

**Concerns**
- HIGH: D-14a is not explicit here. No first-class "Back to previous step" recovery path in the task list.
- HIGH: The plan does not explain how current desktop session/history restore will be reconciled with D-14's params-only replay requirement.
- HIGH: Desktop also has multiple competing notions of "current results" — the plan does not define the refinement source set.
- MEDIUM: One task touching search-thread creation, finish handlers, persistence, restore, replay, and new UI strip in a very large file is high regression risk.
- MEDIUM: No automated checks are proposed for replay, truncation, clear-all, or stale-scope indication.
- MEDIUM: Unlimited-depth replay on startup could materially slow restore.

**Suggestions**
- Make zero-result recovery an explicit desktop subtask.
- Split the work into execution/persistence changes and UI-strip changes.
- Define exactly which desktop filters/exclusions affect refinement scope.
- Add tests around replay/persistence helpers.
- Plan visible restore status for replayed chains.

**Risk Assessment**
`HIGH` because the UX direction is solid, but restore model, scope semantics, and missing zero-result recovery detail leave too much room for incorrect implementation.

---

## Consensus Summary

### Agreed Strengths
- **Shared foundation first (Wave 1 → Wave 2)** — Both reviewers praise the centralized shared module approach
- **TDD for core logic** — Tests before UI ensures the contract is solid
- **Correct architecture: no overloading of restrict_sys_ids** — Clean separation of filter and refinement restrict concepts
- **Params-only persistence** — Avoids memory bloat, keeps sessions lightweight
- **Human-verify checkpoints for UI waves** — Appropriate for visual flows

### Agreed Concerns
- **"Current result set" is ambiguous (HIGH)** — Both reviewers note the plans don't define whether refinement scope is raw search results, displayed results, or post-filter visible results. This must be locked before implementation.
- **Restore/replay model underspecified (HIGH)** — Both note that D-14 params-only replay isn't explicitly designed against the existing result-restore behavior in both apps.
- **Tasks too large in Plans 02/03 (MEDIUM)** — Both suggest splitting the single auto task in web/desktop plans into smaller focused tasks.
- **Zero-result recovery not explicit in desktop plan (MEDIUM)** — Codex flags D-14a missing from Plan 03; Gemini doesn't call it out but it's a real gap.
- **Pop-back replay performance (MEDIUM)** — Re-executing full chain could be slow for deep chains; both suggest user feedback during replay.

### Divergent Views
- **Overall risk level:** Gemini rates LOW risk (confident in the shared module + checkpoints), Codex rates MEDIUM-HIGH (concerned about underspecified scope semantics). Codex's concern seems more well-founded — the scope definition gap is real.
- **RefinementStep schema sufficiency:** Codex worries the fields are too narrow for future modes; Gemini considers them adequate. Worth adding an `extra_params: dict` escape hatch.
- **In-memory caching:** Gemini suggests session-local memoization for faster pop-back; Codex doesn't mention this. D-13 explicitly says no caching, so this is a design deviation to weigh.
