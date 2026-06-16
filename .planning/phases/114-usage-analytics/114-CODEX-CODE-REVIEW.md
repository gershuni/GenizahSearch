---
phase: 114-usage-analytics
reviewer: codex-cli (gpt-5.x) 0.139.0
review_type: cross-AI code review (post-execution)
diff_base: a7545048
head: 9331f129
date: 2026-06-16
verdict: CHANGES-REQUESTED
findings: { blocker: 0, high: 2, medium: 3, low: 1 }
---

# Phase 114 Independent Code Review (Codex)

> Cross-AI adversarial code review of the Phase 114 desktop telemetry producers,
> requested after the internal code-review + verifier + secure-phase passes.
> Brief: `_tmp/codex-114-code-review-brief.md`. Raw run: `_tmp/codex-114-output.txt`.

No **BLOCKER** findings. No user-content/PII leak in the new event properties; telemetry identity
uses `current_user._uuid`, not `current_user.id`. The WR-01 ResultDialog gate fix is confirmed
complete (all six `ResultDialog(...)` construction sites pass `self` from `GenizahGUI`, so
`self._app._emit_feature_opened(...)` resolves to the gated host helper).

| ID | Severity | file:line | Issue | Suggested fix |
|---|---|---|---|---|
| CR-114-01 | HIGH | `genizah_app.py:19074` | PGP tag search installs a new `_current_pgp_tag_search_run` before draining any existing PGP worker. If the previous worker finishes while the UI thread is blocked in `wait()` (`:19080`), its queued `_on_tag_search_results` slot can run after the new run object exists and mark the new run emitted (`:19049-19062`). The old search is then attributed to the new run, and the real new completion is suppressed. | Don't share one mutable run object across workers. Use a per-run token/id passed through the worker signal/closure; ignore stale tokens. Drain/cancel/disconnect the previous worker before creating the next telemetry run. |
| CR-114-02 | HIGH | `genizah_app.py:17678` | The regular Search "New" reset path cancels an active search via `cancel_flag` but never sets `_search_was_cancelled` and never calls `_emit_search_telemetry('cancelled')`. When `SearchThread` emits `[]` on cancellation, `on_search_finished` sees `was_cancelled=False` (`:18051`) and emits `completed` with bucket `0` (`:18085-18088`). If the thread is terminated at `:17684-17686`, the run can be dropped entirely. | Mirror `stop_search`: set `_search_was_cancelled=True` and emit/capture the cancelled telemetry exactly once for the reset-cancellation path. Prefer a run token so stale queued `results_signal` can't mutate a later run. |
| CR-114-03 | MEDIUM | `genizah_app.py:22662` | Composition "New" reset cancels and may terminate an active composition worker (`:22665-22670`) without any fallback telemetry emission. Normal composition telemetry only fires from `on_comp_scan_finished` (`:23124-23132`), so a worker that doesn't cooperatively finish inside the wait window loses the required cancelled event. | Emit cancelled telemetry when reset initiates cancellation, guarded by the existing `emitted` flag, or guarantee the terminated path calls the same helper before killing the thread. |
| CR-114-04 | MEDIUM | `genizah_app.py:26849` | `desktop_session_end` is NOT gated by `_telemetry_ready()` and can send `session_id=''` if the app closes after consent is enabled but before the ~700ms startup coordinator creates `_session_id` (`:3568-3570`, `:3629-3644`). Orphan session-end event; violates the "all producers gated by `_telemetry_ready()`" invariant. (Same class as WR-01, for session_end.) | Only emit session_end when `_telemetry_ready()` is true and `_session_id` is truthy; set `_session_end_emitted` only after that decision. |
| CR-114-05 | MEDIUM | `genizah_app.py:26773` | Session restore can reopen Join Lab via `_restore_join_lab` → `open_join_workbench()` (`:26776-26780`), which now always emits `desktop_feature_opened` (`:15923-15925`). Deferred restore + the telemetry coordinator both run around startup → a `joins_lab` ghost event without a user gesture. | Add an `emit_telemetry=False` parameter for restore callers, or use a scoped restore/programmatic feature-open suppression flag for deferred restore work. |
| CR-114-06 | LOW | `genizah_app.py:26837` | The interrupted-composition resume flow switches tabs with bare `self.tabs.setCurrentWidget(self.composition_tab)` after `_restoring_session` is already false (`:26798-26800`). `_on_tab_changed` treats that as a user tab activation if telemetry is ready (`:4029-4050`). | Use `_set_active_tab(self.composition_tab)` here, like the other programmatic tab switches. |

## Subtle checks (Codex)

- WR-01 fix verified complete (see above).
- Known WR-02 + WR-03 still present; not re-listed as new findings.
- No content leak: query text, PGP tag text, shelfmarks, file paths, window/tab text, and selected
  file paths are not assigned to telemetry props in the reviewed producers.

## Overall verdict: CHANGES-REQUESTED

## Orchestrator note on overlap with internal findings

- CR-114-04 is the **session_end analog of WR-01** (the result_dialog gate gap we just fixed) — the
  internal verifier explicitly claimed session_end was fine; Codex caught that it's ungated.
- CR-114-02/CR-114-03 are concrete, deeper instances of the **WR-03 family** (cancellation/restore
  count accuracy) — they pin specific dropped/miscounted paths the internal review only gestured at.
- CR-114-01 is a genuinely **new thread-safety bug** (shared mutable run object across PGP workers).
- CR-114-05/CR-114-06 are **new ghost-event paths** via session restore (joins_lab + comp tab).
