---
phase: 114
reviewers: [codex]
reviewed_at: 2026-06-15T18:30:06Z
plans_reviewed: [114-01-PLAN.md, 114-02-PLAN.md, 114-03-PLAN.md]
---

# Cross-AI Plan Review — Phase 114

## Codex Review

**Summary**

The plans are unusually strong on privacy posture and sequencing intent, but I would not approve them as-is. The biggest remaining risks are plan↔code drift in live dispatch paths: PGP Tags bypasses the planned `start_search()` instrumentation, feature coverage misses live Puzzle/FJMS entry points, shutdown can still leak a regular-search cancel event, and the proposed AST guard is too coarse to pass against the current code. Identity design is mostly sound, but the coordinator's "exactly once" guard can also prevent re-identification after opt-out/re-opt-in in the same process.

**Strengths**

- Strong chokepoint discipline: all planned emissions route through `desktop.telemetry.track()` / `identify()` / `reset_identity()`, preserving consent, scrubber, allowlist, and fixed event-name guarantees.
- Correct identity source is emphasized repeatedly: `current_user._uuid`, not `current_user.id`; this matches the web merge target.
- Good handling of zero-result searches as completed searches with bucket `0`, separate from cancellation.
- Good composition shutdown awareness: Plan 02 correctly identifies the cooperative `requestInterruption()` window for composition.
- Visual Similarity was moved away from dead `_browse_view_visual_similarity()` and into the live `open_joins_workbench(..., source='visual')` path.
- The plans avoid sending query text, tag text, filenames, selected paths, or visible labels as telemetry properties.

**Concerns**

- **HIGH: PGP Tags search is not instrumented despite being in the mode map.** `toggle_search()` returns early for `MODE_PGP_TAGS` and calls `_execute_tag_search()` instead of `start_search()` (genizah_app.py:17140). Plan 02's `{7: 'pgp_tags'}` mapping inside `start_search()` is effectively dead for that mode. This violates D-05's "each mode_combo entry" requirement.

- **HIGH: regular search can still emit during app shutdown.** Plan 02 guards `stop_search()`, but `closeEvent()` directly sets `search_thread.cancel_flag`, waits, and may receive a queued `results_signal.emit([])` afterward (genizah_app.py:26384, gui_threads.py:116). Since `_emit_search_telemetry()` lacks the same first-line `_app_shutting_down` guard planned for composition, a shutdown cancel can still produce a post-`session_end` `desktop_search_executed`.

- **HIGH: the AST guard as described will likely fail on live code.** It flags any function that both emits telemetry and calls `.text()`, `.currentText()`, `.toPlainText()`, etc. But planned telemetry calls are added to functions that already use those accessors for non-telemetry work: `on_search_finished()` uses `query_input.text()` / `gap_input.text()` (genizah_app.py:17739), `export_results()` uses `mode_combo.currentText()` (genizah_app.py:20464), and `export_comp_report()` uses `comp_text_area.toPlainText()` / `comp_mode_combo.currentText()` (genizah_app.py:20883). The guard needs to inspect telemetry argument expressions, not whole functions.

- **HIGH: re-opt-in after opt-out in the same process may stay anonymous.** Plan 01's coordinator returns immediately if `_telemetry_session_started` is true. If a logged-in user starts consented, opts out, then opts back in, `set_consent(False)` clears identified state, and the coordinator no-ops on re-entry. That breaks D-13: opt-in should identify the logged-in `_uuid` before further usage events.

- **MEDIUM: tab telemetry only suppresses `_restore_session`, not other programmatic tab changes.** D-02 says code-driven jumps should not count. Live code uses `setCurrentWidget` / `setCurrentIndex` outside restore, e.g. `send_result_to_composition()` (genizah_app.py:20287), `_search_by_pgp_tag()` (genizah_app.py:18753), and catalog navigation (genizah_app.py:12191). Plan 02 only checks `_restoring_session`.

- **MEDIUM: FJMS catalog coverage misses ResultDialog.** Plan 03 instruments the Browse FJMS catalog open at (genizah_app.py:9249), but ResultDialog has its own live FJMS catalog constructor at (desktop/result_dialog.py:2881). If "FJMS catalog dialog opened" is the surface, both paths should emit.

- **MEDIUM: Fragment Puzzle coverage misses most live open paths.** Plan 03 instruments `_open_puzzle_window()`, but `add_to_puzzle()` also creates/shows/activates the puzzle window directly (genizah_app.py:15596) and is called from Browse, ResultDialog, lists, and VS paths. `_open_puzzle_window()` alone will undercount puzzle usage.

- **MEDIUM: export telemetry placement will count non-opened/non-completed exports.** Plan 03 says emit at the top of `export_comp_report()`, but that function returns before any file dialog if there is no data (genizah_app.py:20782). Also, emitting `action='export_xlsx'` before `QFileDialog.getSaveFileName()` counts cancelled save dialogs as export actions.

- **MEDIUM: startup producer race remains.** `on_startup_finished()` enables UI before the proposed 700ms coordinator. With persisted consent, a fast tab/feature/search event could fire before `_session_id` exists and before stale identity is corrected. This is especially risky because `desktop.telemetry._load_consent_state()` trusts persisted `IDENTIFIED_USER_KEY` until the coordinator runs.

- **LOW: source assertions are too regex-fragile for D-10.** `grep "identify(user.id)"` will not catch `identify(getattr(user, "id"))` or an alias variable. The behavior test is more valuable; add an AST guard around identity callsites if this remains a hard requirement.

**Suggestions**

- Add PGP Tags as a first-class search telemetry path: create a per-run state in `_execute_tag_search()` with `search_mode='pgp_tags'`, emit in `_on_tag_search_results()` before early returns, and never include the tag value.
- Put `if getattr(self, '_app_shutting_down', False): return` at the top of both `_emit_search_telemetry()` and `_emit_comp_search_telemetry()`.
- Split identity sync from session start. Let the coordinator always re-check logged-in `_uuid` when consent is true, but guard only `desktop_session_start` with `_telemetry_session_started`.
- Gate all usage producers on `self._telemetry_session_started` or invoke the coordinator synchronously for already-consented launches before UI interaction is enabled.
- Rework D-17 AST guard to inspect only telemetry payload expressions and helper-call keyword values, not entire functions.
- Instrument ResultDialog's `_show_rd_catalog()` and `add_to_puzzle()`/new-window activation paths, or explicitly narrow D-03 if those are intentionally excluded.
- For exports, emit `dialog_name='export'` before opening the save dialog, and emit `action='export_*'` only after a path is selected or after successful save.

**Risk Assessment: HIGH**

The privacy architecture is good, but the implementation plan still has enough live-code drift to miss required signals and potentially violate ordering/exactly-once guarantees. The highest-risk items are not theoretical: PGP Tags bypasses the planned search path, the AST guard will collide with existing code, and regular search shutdown lacks the same guard the plan correctly applies to composition.

---

## Consensus Summary

Single external reviewer (Codex). Overall verdict: **do not execute as-is — replan to close 4 HIGH plan↔code-drift / correctness gaps first.** The privacy/identity *architecture* is sound; the failures are in *where the producers are wired* against the live codebase.

### Agreed Strengths
- Chokepoint discipline (all emission through `desktop/telemetry.py`), correct `_uuid` identity source, zero-result-vs-cancel distinction, composition-shutdown awareness, the VS dead-code→live-path fix, and structural absence of content/labels.

### Agreed Concerns (priority order)
**HIGH (must fix before execute):**
1. **PGP Tags bypasses instrumentation** — `MODE_PGP_TAGS` dispatches via `_execute_tag_search()` (genizah_app.py:17140), not `start_search()`, so Plan 02's `{7:'pgp_tags'}` map is dead. Wire a per-run state in `_execute_tag_search()` / emit in `_on_tag_search_results()`. (D-05)
2. **Regular-search shutdown leak** — `_emit_search_telemetry()` needs the same first-line `if getattr(self,'_app_shutting_down',False): return` guard that composition got; `closeEvent` can deliver a queued `results_signal.emit([])` after `session_end`. (D-09/D-15)
3. **D-17 AST guard is too coarse** — flagging whole functions will FAIL against `on_search_finished`/`export_results`/`export_comp_report`, which already call `.text()`/`.currentText()`/`.toPlainText()` for non-telemetry work. Re-scope the guard to inspect only telemetry-call argument/keyword expressions.
4. **Re-opt-in stays anonymous** — coordinator no-ops when `_telemetry_session_started` is true, so opt-out→opt-in in one process never re-`identify()`s the logged-in `_uuid`. Split identity-sync from the one-shot `session_start` guard. (D-13)

**MEDIUM (should fix — coverage/accuracy):**
5. Tab D-02 guard only checks `_restoring_session`; other programmatic `setCurrentWidget/Index` paths (send_result_to_composition:20287, _search_by_pgp_tag:18753, catalog nav:12191) would mis-count as user navigation.
6. FJMS catalog also opens from ResultDialog (desktop/result_dialog.py:2881) — Browse-only instrumentation undercounts.
7. Fragment Puzzle also opens via `add_to_puzzle()` direct window show (genizah_app.py:15596) from Browse/ResultDialog/lists/VS — `_open_puzzle_window()` alone undercounts.
8. Export emit placement (top of `export_comp_report()`, before `QFileDialog`) counts no-data early-returns and cancelled save dialogs; emit `dialog_name='export'` before the dialog but `action='export_*'` only after a path/successful save.
9. Startup race: a fast user event after `on_startup_finished()` enables the UI but before the 700ms coordinator could fire pre-`_session_id` / pre-identity-correction. Gate producers on `_telemetry_session_started` or run the coordinator synchronously for already-consented launches.

**LOW:**
10. `grep "identify(user.id)"` source assertion is regex-fragile (misses `getattr`/alias); lean on the behavior test (or an identity-callsite AST guard).

### Divergent Views
None — single reviewer.
