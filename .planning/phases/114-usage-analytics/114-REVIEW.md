---
phase: 114-usage-analytics
reviewed: 2026-06-15T20:09:22Z
depth: standard
files_reviewed: 5
files_reviewed_list:
  - desktop/result_dialog.py
  - desktop/telemetry.py
  - genizah_app.py
  - tests/test_no_dynamic_telemetry_strings.py
  - tests/test_telemetry_phase114.py
findings:
  critical: 0
  warning: 4
  info: 5
  total: 9
status: issues_found
---

# Phase 114: Code Review Report

**Reviewed:** 2026-06-15T20:09:22Z
**Depth:** standard
**Files Reviewed:** 5
**Status:** issues_found

## Summary

Reviewed the Phase 114 desktop usage-analytics telemetry changes against diff base `a7545048`: the new producers in `genizah_app.py` (search/composition/PGP-tag/feature-opened/tab/active-ping emitters, the startup coordinator, and identity wiring), the two direct `track()` calls added to `desktop/result_dialog.py`, the 3-line `result_count_bucket`/`duration_bucket_ms` allowlist addition in `desktop/telemetry.py`, and the two new test files.

The privacy hard-invariant holds up well under adversarial scrutiny. I traced every new property that leaves the chokepoint and could not find a path that carries free-text/user content: search modes and corpus scopes are sourced from hardcoded enum maps and `currentData()` codes (never `currentText()`/`tabText()`), result counts are coarse-bucketed before transmission, the PGP tag string is explicitly never passed into props, identity uses `current_user._uuid` (verified `User.id` is an `int` and `_uuid` is the Supabase UUID string), and `session_id` is allowlisted. The structural scrubber + `_ALLOWED_PROPS` allowlist + `DesktopEvent` registry provide three independent defense layers, and the AST guard backs the "no dynamic strings" invariant. Producers are wrapped in `try/except` and gated by `_telemetry_ready()` / `is_enabled()` / `_app_shutting_down`, so I found no path where a telemetry failure can raise into the UI/search path.

No BLOCKERs found. The findings below are correctness/consistency gaps (WARNING) and accuracy/maintainability nits (INFO).

## Warnings

### WR-01: ResultDialog telemetry emits bypass the `_telemetry_ready()` producer gate

**File:** `desktop/result_dialog.py:53-61` and `desktop/result_dialog.py:2892-2902`
**Issue:** Both ResultDialog telemetry sites call `telemetry.track(DesktopEvent.FEATURE_OPENED, ...)` directly, while every analogous producer in `genizah_app.py` routes through `_emit_feature_opened()`, whose FIRST guard is `if not self._telemetry_ready(): return` (REVIEWS MEDIUM-9). The startup coordinator runs on a 700 ms `QTimer.singleShot` (`genizah_app.py:3568`), and `_session_id` is only minted inside it. A ResultDialog opened during that startup window (e.g. a restored-session result, or a fast double-click before 700 ms) will emit a `desktop_feature_opened` event with `session_id=''` and *before* `desktop_session_start` has fired — violating the "no usage event before the coordinator runs" ordering invariant that MEDIUM-9 was created to enforce, and producing an orphan event PostHog cannot tie to a session. The events are still consent-gated (no privacy leak), so this is correctness/consistency, not a BLOCKER.
**Fix:** Route both ResultDialog emits through the parent's gated helper so the readiness gate and empty-session-id handling stay in one place:
```python
# in __init__ and _show_rd_catalog, instead of telemetry.track(...):
app = self._app  # (parent in __init__)
if app is not None and hasattr(app, '_emit_feature_opened'):
    app._emit_feature_opened(dialog_name='result_detail')   # / 'fjms_catalog'
```
`_emit_feature_opened` already owns the try/except, the `_telemetry_ready()` gate, and the `session_id` injection.

### WR-02: `run_composition` early-return paths leave a never-emitted `_current_comp_search_run`

**File:** `genizah_app.py:22905-22910` (LAB engine guard) and `genizah_app.py:22945-22949` (searcher guard)
**Issue:** `self._current_comp_search_run = {... 'emitted': False}` is assigned (`genizah_app.py:22896`) *before* the two validation guards that `return` early when `self.lab_engine` / `self.searcher` is missing (after showing a `QMessageBox` and calling `reset_comp_ui()`). On those paths the comp thread never starts, so `on_comp_scan_finished` never fires and the run object is left with `emitted=False`. It is harmless for double-counting (the next `run_composition` overwrites it, and the dangling object is not emitted), but it means a composition the user *attempted* and that failed pre-flight records no telemetry at all, while a subsequent successful run inherits a clean object — so the failure is silently invisible in analytics. More subtly, if `on_comp_scan_finished` were ever reached with a stale object from a prior aborted run (it is not today, but the coupling is fragile), the emitted state would be wrong.
**Fix:** Move the `_current_comp_search_run` assignment to *after* both validation guards (just before `self.comp_thread.start()`), so the run object only exists once a thread is actually launched. Alternatively, clear it to `None` in the early-return branches.

### WR-03: Restored-session searches at startup can emit `desktop_search_executed` for non-user-initiated runs

**File:** `genizah_app.py:18219` (and the empty path at `genizah_app.py:18086`), reached via `_restore_regular_search_from_state` → `start_search()` at `genizah_app.py:26060` / `_on_restore_filter_finished` at `genizah_app.py:17140`
**Issue:** Tab activation got an explicit `_programmatic_tab_change` / `_restoring_session` suppression (MEDIUM-5/D-02), but the search producer has no equivalent restore guard — it gates only on `_telemetry_ready()` and `_app_shutting_down`. A user-clicked history entry re-running a search is legitimately a user action (acceptable to count). However, if the 200 ms `_restore_session` path or a deferred `_on_restore_filter_finished` calls `start_search()` *after* the 700 ms coordinator has set `_telemetry_session_started=True`, a programmatically re-run search will emit `desktop_search_executed` even though the user did not initiate it this session — an inflated search count. The timing race (200 ms restore vs 700 ms coordinator) makes this intermittent and hard to reproduce. Note this is inconsistent with the deliberate tab-restore suppression.
**Fix:** Apply the same restore/programmatic suppression the tab producer uses. Either short-circuit `_emit_search_telemetry` when `getattr(self, '_restoring_session', False)` is True, or set a transient `_programmatic_search` flag around the restore-driven `start_search()` calls and check it in `_emit_search_telemetry`.

### WR-04: Privacy disclosure text states `user.id` is sent, but code sends `user._uuid`

**File:** `desktop/consent_dialog.py:306-308` (EN) and `desktop/consent_dialog.py:339-340` (HE), vs `genizah_app.py:3624` (`telemetry.identify(user._uuid)`)
**Issue:** The user-facing privacy disclosure (the legally/ethically load-bearing text the user consents to) says: "the only identity attached is your bare Supabase `user.id` — a pseudonymous identifier, the same one the website already uses." The implementation correctly identifies with `current_user._uuid` (the raw Supabase UUID string), *not* `user.id` — and `User.id` is actually an `int` "for compatibility" (`supabase_corrections_client.py:102`), so the disclosure literally names the wrong field. The behavior is the privacy-correct one (the `_uuid` is the pseudonymous identifier the web app uses; the int `.id` would arguably be worse to disclose). This is a disclosure-accuracy defect: the consent text must accurately describe what is collected. Flagging as WARNING because consent dialogs are a compliance surface, not cosmetic.
**Fix:** Reword both language blocks to describe the actual identifier, e.g. EN: "the only identity attached is your pseudonymous Supabase account identifier (UUID) — the same one the website already uses." Keep the HE copy in sync. (The AST guard `test_no_dynamic_telemetry_strings.py::_is_allowed_identity_source` already enforces `_uuid`-shaped sources at the call site, so the code side is correct and guarded.)

## Info

### IN-01: Dead enum entry `7: 'pgp_tags'` in `_SEARCH_MODE_ENUM`

**File:** `genizah_app.py:17486-17489`
**Issue:** `_SEARCH_MODE_ENUM` maps index `7: 'pgp_tags'`, but `toggle_search` (`genizah_app.py:17443-17445`) routes `MODE_PGP_TAGS` (index 7) to `_execute_tag_search()` and returns *before* `start_search()` ever builds `_current_search_run`. The PGP-tag path uses its own `_emit_pgp_tag_search_telemetry` with a hardcoded `'pgp_tags'` mode. The index-7 entry is therefore unreachable.
**Fix:** Remove the `7: 'pgp_tags'` key (or add a comment that it is intentionally redundant for defensiveness). Not harmful, just dead.

### IN-02: `duration_bucket_ms` allowlisted but no producer emits it in Phase 114

**File:** `desktop/telemetry.py:302`
**Issue:** The allowlist addition introduces `result_count_bucket` (used by all three search emitters) and `duration_bucket_ms`. No Phase 114 producer emits `duration_bucket_ms` — it appears to be forward-provisioned for Phase 115 performance events. Allowlisting ahead of a producer is harmless (nothing can leak through an allowlisted-but-unused key), but it is dead surface area until 115 lands.
**Fix:** Optional — defer the `duration_bucket_ms` allowlist entry to Phase 115 when its producer ships, or leave a comment noting it is provisioned for 115.

### IN-03: `_set_active_tab` relies on synchronous Qt signal delivery for the suppression flag

**File:** `genizah_app.py:4061-4076`
**Issue:** `_set_active_tab` sets `self._programmatic_tab_change = True`, calls `setCurrentIndex/setCurrentWidget`, then resets the flag in a `finally`. This is correct *only* because Qt delivers `currentChanged` synchronously within `setCurrentIndex`. If a future Qt version or a queued-connection refactor made delivery async, `_on_tab_changed` would read the flag as already-reset `False` and emit a programmatic tab switch as a user switch. The current behavior is correct; this is a latent coupling worth a comment.
**Fix:** Add a one-line comment on `_set_active_tab` documenting the synchronous-delivery assumption (and that `currentChanged` does not fire when the target tab is already current, which is why no spurious reset occurs).

### IN-04: Three emit sites in `_on_tag_search_results` rely on the `emitted` guard for exclusivity

**File:** `genizah_app.py:19089` (empty), `genizah_app.py:19109` (no-valid-results), `genizah_app.py:19167` (success)
**Issue:** `_on_tag_search_results` calls `_emit_pgp_tag_search_telemetry('completed', ...)` at three points. The empty and no-valid-results branches `return` immediately, so only one fires per call today. Correctness depends on (a) those early `return`s and (b) the `run['emitted']` exactly-once guard. The defense-in-depth is fine, but the triple call site is fragile — a future edit that removes one of the early returns would double-emit (the `emitted` guard saves it, but the count semantics — success bucket vs zero bucket — would depend on call order).
**Fix:** Optional — consolidate to a single emit at the end of the method computing the count from the relevant branch, or add a comment that the early `return`s are load-bearing for emit exclusivity.

### IN-05: `_app_shutting_down` / `_session_end_emitted` only initialized inside `closeEvent`

**File:** `genizah_app.py:26847` and `genizah_app.py:26851-26852`
**Issue:** `_app_shutting_down` and `_session_end_emitted` are first assigned inside `closeEvent`, never in `__init__`. Every reader uses `getattr(self, '_app_shutting_down', False)` / `getattr(self, '_session_end_emitted', False)`, so the missing initializer is currently safe. It is a maintainability nit: a future reader doing a bare `self._app_shutting_down` (without `getattr`) would `AttributeError` before the first `closeEvent`.
**Fix:** Initialize both to `False` in `__init__` (alongside `_programmatic_tab_change`, which already is) so the attributes always exist.

---

_Reviewed: 2026-06-15T20:09:22Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
