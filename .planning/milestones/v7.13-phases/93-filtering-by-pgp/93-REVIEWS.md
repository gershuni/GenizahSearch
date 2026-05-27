---
phase: 93
reviewers: [gemini, codex]
reviewed_at: 2026-05-19T14:04:58Z
plans_reviewed: [93-01-PLAN.md]
self_skipped: claude (running inside Claude Code CLI)
---

# Cross-AI Plan Review — Phase 93 (PGP Filter on `/search`)

Internal `gsd-plan-checker` ran 2 iterations and returned `VERIFICATION PASSED` after fixing 1 BLOCKER (REQ-ID-to-Task mapping inversion) + 6 WARNINGS. This cross-AI review provides a second set of eyes against the revised `93-01-PLAN.md`.

## Gemini Review

# Plan Review: Phase 93-Filtering-by-PGP (Plan 01)

## Summary
This plan provides a high-quality, surgical implementation of a post-search PGP filter for the web search interface. It rigorously follows the established patterns used by the existing `printed_filter`, ensuring consistent UX and technical reliability. The plan correctly addresses multitenancy requirements by routing all persistence through `web/safe_storage.py` and maintains architectural integrity by placing the filter in the correct position within the render cascade.

## Strengths
- **Pattern Parity:** DIRECTLY models the new filter on the `printed_filter` implementation, which significantly reduces the risk of logic errors and ensures a familiar experience for power users.
- **Robust Cascade Logic:** The use of a `count_parts` list in `_apply_printed_filter_and_render` and `_apply_domain_exclusions` is a cleaner, more extensible approach for managing multiple stacked filters compared to nested if/else blocks.
- **Multitenant Compliance:** Every persistence write and bootstrap read is explicitly routed through the `safe_storage` chokepoint, upholding the Phase 87 invariants.
- **Intermediate Safety:** The inclusion of a no-op stub for `_update_pgp_filter_chip` in Task 2 is a thoughtful touch that prevents potential `NameError` exceptions if the button is interacted with before the chip logic is fully implemented.
- **Clear Documentation:** The rationale for skipping unit tests (page-local closures) is technically accurate and aligns with previous phase decisions (999.1), showing a consistent approach to testing constraints.

## Concerns
- **Cascade Delegation (LOW):** The plan assumes `_apply_manuscript_exclusions` delegates to either `_apply_domain_exclusions` or `_apply_printed_filter_and_render`. If `_apply_manuscript_exclusions` has a performance fast-path that calls `render_results` directly without going through these helpers, the PGP filter might be bypassed when manuscript exclusions are active.
- **Button Label Brevity (LOW):** Following Decision **D-05**, the "All" state uses the label `tr('All')` whereas the printed filter uses `tr('Filter Printed')`. While this honors the user's specific request for brevity, the visual inconsistency in the toolbar (one button saying "Filter Printed" and the other just "All") might be slightly jarring.

## Suggestions
- **Verify `_apply_manuscript_exclusions`:** Before executing Task 3, check the implementation of `_apply_manuscript_exclusions`. If it contains a direct call to `render_results(...)`, add an Edit to Task 3 to wrap that call with `_apply_pgp_filter`.
- **Tool-tip for "All":** Since the button label "All" is very brief, consider adding a tooltip `tr('Filter by PGP presence')` to the button construction in Task 2 to aid discoverability when the filter is in its default state.

## Risk Assessment
**Overall Risk: LOW**

The plan is exceptionally well-structured and maps directly to a proven existing pattern. The dependencies between tasks are clearly defined, and the verification steps are exhaustive (both automated AST checks and a detailed 13-step human smoke test). The architectural impact is localized to the web search page and does not touch the desktop app or the shared search core, further minimizing the blast radius.

---
**Verdict: VERIFICATION PASSED.** Proceed with execution.

---

## Codex Review

## Summary

The plan is strong on UI shape, safe-storage discipline, and copying the existing `printed_filter` pattern, but I would not treat it as verification-ready yet. The largest issue is cascade coverage: the plan wires PGP filtering into `_apply_printed_filter_and_render` and `_apply_domain_exclusions`, but the live `/search` page has additional render paths that bypass both. As written, persisted PGP filters, restored results, word-search exclusions, manuscript exclusions without printed/domain filters, and background enrichment re-renders can show unfiltered results while the button/chip says the filter is active.

## Strengths

- Mirrors the existing `printed_filter` UI and persistence pattern closely.
- Uses `persist_value(...)` / `_safe_get(...)`, so it preserves the Phase 87 no-raw-`app.storage.user` invariant.
- Forward-reference handling in Task 2 is mostly sound: Python late binding makes the button/update handler ordering safe after the UI is fully constructed.
- Scope is correctly web-only; no desktop or `/parallels` changes are planned.
- The human smoke test is unusually detailed and includes cascade-count invariants, RTL, persistence, and scope checks.

## Concerns

- **HIGH:** Task 3 only adds two `_apply_pgp_filter(...)` call sites and explicitly requires exactly two matches, lines `688-692`. The live code has other render branches: `_render_with_filters` at `web/pages/search.py:4456-4487`, `_apply_manuscript_exclusions` at `3180-3225`, and `_apply_word_search_exclusions_and_render` at `3564-3607`. Those can bypass PGP filtering entirely.

- **HIGH:** The plan says “Do NOT touch `_apply_manuscript_exclusions` directly” at plan line `675`, but that is incorrect. In the live code, manuscript exclusions only delegate to `_apply_printed_filter_and_render` when printed is active. With PGP-only active, it falls through to measurement/render and skips PGP.

- **HIGH:** Restored-session behavior is incomplete. Task 7 expects reload persistence at lines `1189` and smoke Check 6, but live `_deferred_transcription_restore` at `web/pages/search.py:4714-4729` only fetches `transcription_sys_ids` and calls `render_results(search_state.results, ...)`. It does not show the PGP button, update the chip, or apply the PGP filter.

- **HIGH:** `_render_with_filters` can undo the filter after enrichment. Task 5 adds `_apply_enrichment_to_ui()` updates at plan lines `873-879`, but stage 1/stage 2 then call `_render_with_filters(...)` in live code. If `pgp_filter` is active and printed/domain/exclusion filters are not, `_render_with_filters` currently falls to plain measurement/render.

- **MEDIUM:** The chip visibility violates the plan’s own must-have. Plan line `31` says the chip never appears when the result set has zero PGP hits, but Task 4’s implementation at lines `766-770` only checks `pgp_filter == 'all'`; it does not also gate on `bool(search_state.transcription_sys_ids)`.

- **MEDIUM:** New Search clearing should be centralized. Task 5 clears with `persist_value('search_pgp_filter', 'all')` at line `913`, but `persist_value` is gated by `session_persistence_enabled`. Existing snapshot clearing uses `clear_search_snapshot()` and safe setters. Add `search_pgp_filter` to the central clear/default path or use a non-gated safe reset for New Search.

- **MEDIUM:** Test coverage is too manual for the cascade risk. Task 6 defers unit tests at lines `968-981`, which is understandable for a page-local closure, but this plan needs at least static or small integration-style tests for “all render branches that apply printed/domain filters also apply PGP.”

- **LOW:** The plan’s New Search rationale says printed_filter does not reset, line `924`, but the live code already resets `search_state.printed_filter = 'all'` at `web/pages/search.py:2029-2030` and `clear_search_snapshot()` resets `search_printed_filter`.

- **LOW:** Several verify commands are Bash-specific (`tail`, `grep`, `test -f`) while the provided environment is PowerShell. That can make an otherwise valid execution fail for tooling reasons.

## Suggestions

- Expand Task 3 beyond lines `584-673`: update `_render_with_filters`, `_apply_manuscript_exclusions`, and `_apply_word_search_exclusions_and_render` so PGP is applied whenever `search_state.pgp_filter != 'all'`, even when printed/domain filters are inactive.

- Replace AC-3-2 at line `688`. Do not require exactly two `_apply_pgp_filter(...)` call sites. Require coverage of every render branch instead.

- In `_render_with_filters`, treat printed and PGP as one post-search-filter stage, e.g. branch when either printed or PGP is active and call a renamed helper like `_apply_post_search_filters_and_render(...)`.

- In Task 4 lines `766-770`, hide the chip when `not search_state.transcription_sys_ids`, or explicitly reset `pgp_filter` to `all` when enrichment completes with zero PGP hits.

- Add restore-path work near live `web/pages/search.py:4714-4729`: after fetching `transcription_sys_ids`, update PGP button/chip visibility and call the unified filtered render path instead of raw `render_results(...)`.

- Add `search_pgp_filter` to snapshot/clear tests in `tests/test_search_state.py`, or add an explicit test proving New Search clears stale PGP state even when session persistence is disabled.

- Convert verify commands to PowerShell-safe equivalents or state that execution must run from Git Bash.

## Risk Assessment

**Overall risk: MEDIUM-HIGH.** The UI/persistence pieces are straightforward and multitenant-safe, but the cascade model in the plan is incomplete relative to the live code. The most likely failure is a visible active chip/button with unfiltered results after reload, enrichment, or non-printed filter combinations. Fixing the missed render branches before execution would bring the risk down to LOW-MEDIUM.

---

## Consensus Summary

Gemini and Codex agree on the **UI/persistence layer** but **diverge sharply on cascade coverage**. The divergence is significant: Gemini rated risk LOW with the same plan that Codex rated MEDIUM-HIGH because Codex actually inspected the surrounding render branches in `web/pages/search.py` (`_render_with_filters`, `_apply_manuscript_exclusions`, `_apply_word_search_exclusions_and_render`, `_deferred_transcription_restore`) and found that wiring PGP filter into only `_apply_printed_filter_and_render` + `_apply_domain_exclusions` leaves **four bypass paths** where an active PGP filter shows unfiltered results.

This is the canonical value of cross-AI review: Gemini accepted the plan's own claim that the existing 2-site cascade is exhaustive; Codex did not, and grep-verified that other render paths exist.

### Agreed Strengths

- **Pattern parity with `printed_filter`** — both reviewers note the plan faithfully mirrors the existing 3-state filter pattern, reducing logic-error risk.
- **Multitenant invariant compliance** — both confirm zero raw `app.storage.user` access; all persistence routes through `persist_value` / `_safe_get` (Phase 87 chokepoint).
- **Forward-reference stub** — both treat the no-op `_update_pgp_filter_chip` stub in Task 2 / replacement in Task 4 as a sound mechanism.
- **Web-only scope discipline** — no bleed into desktop, parallels, or pre-search filter.

### Agreed Concerns

- **Manuscript-exclusion cascade gap** — Gemini flags as LOW ("verify before executing Task 3"); Codex confirms with grep evidence that `_apply_manuscript_exclusions` falls through to plain render when only PGP is active. Should be treated as **HIGH** per Codex's grep findings.

### Divergent Views (worth investigating before execution)

| Issue | Gemini | Codex |
|-------|--------|-------|
| Cascade coverage | LOW (suggests checking) | HIGH (grep-confirmed 4 bypass paths) |
| `_render_with_filters` undo after enrichment | Not flagged | HIGH (`_apply_enrichment_to_ui` adds the visibility flip, but stage1/stage2 then call `_render_with_filters` which doesn't apply PGP) |
| `_deferred_transcription_restore` reload behavior | Not flagged | HIGH (fetches transcription_sys_ids and renders raw without applying PGP filter) |
| Chip visibility when `transcription_sys_ids` is empty | Not flagged | MEDIUM (must-have line 31 says "chip never appears when zero PGP hits", but Task 4 chip render only checks `pgp_filter == 'all'`) |
| New Search clearing path | Not flagged | MEDIUM (uses `persist_value` which is gated by `session_persistence_enabled`; existing snapshot clearing path goes through `clear_search_snapshot()` + safe setters) |
| New Search rationale ("printed_filter doesn't reset") | Not flagged | LOW (claim is wrong; printed_filter IS reset at `:2029-2030` and via `clear_search_snapshot()`) |
| PowerShell vs Bash verify commands | Not flagged | LOW (Win11 env uses PowerShell; `tail`/`test -f` may fail) |

### Recommended Next Step

Codex's findings are specific (grep-verified line ranges), and the cascade gaps are exactly the failure modes the internal plan-checker's Task 7 cascade-correctness template was added to catch — but the template catches them **after** the smoke test fails, not before execution. Fixing them in-plan is materially cheaper than smoking and looping back.

**Recommended:** `/gsd-plan-phase 93 --reviews` to incorporate Codex's MUST-FIX items (4 HIGH + 3 MEDIUM) and Gemini's tooltip suggestion. Expected plan delta:

1. Task 3 expansion: add `_apply_pgp_filter` to `_render_with_filters` (`:4456-4487`), `_apply_manuscript_exclusions` (`:3180-3225`), `_apply_word_search_exclusions_and_render` (`:3564-3607`); replace AC-3-2 "exactly two call sites" with "every render branch that applies printed/domain filters also applies PGP".
2. Task 5 expansion: `_deferred_transcription_restore` (`:4714-4729`) must call the unified filtered render path AND update button/chip after fetching `transcription_sys_ids` from a restored session.
3. Task 4 chip visibility: gate render on `bool(search_state.transcription_sys_ids)` AND `pgp_filter != 'all'`.
4. Task 5 New Search clearing: route through `clear_search_snapshot()` + safe setters instead of the `persist_value(...)` path (which is gated by `session_persistence_enabled`).
5. Task 5 rationale fix: remove the false "printed_filter doesn't reset" claim.
6. Task 6 / Task 7 test reinforcement: add at least one static check (e.g., grep-based test in `tests/test_search_state.py`) asserting every render branch that imports `_apply_printed_filter` also imports `_apply_pgp_filter`, OR document the bypass-by-design with verified safety rationale.
7. Verify commands: either convert to PowerShell-safe alternatives or note that execution must run from Git Bash on Windows.

Alternative: proceed with execution and rely on Task 7 human smoke to catch the cascade gaps. The plan's `<resume-signal>` does include cascade-correctness templates, so the loop-back path exists — it's just more expensive than fixing in-plan now.
