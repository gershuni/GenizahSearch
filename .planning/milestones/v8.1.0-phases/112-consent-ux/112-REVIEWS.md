---
phase: 112
reviewers: [codex]
reviewed_at: 2026-06-14T18:30:00Z
plans_reviewed: [112-01-PLAN.md, 112-02-PLAN.md, 112-03-PLAN.md]
codex_model: default (codex-cli 0.136.0)
overall_risk: HIGH
---

# Cross-AI Plan Review — Phase 112 (Consent UX)

> Reviewed by Codex (full-permissions `codex exec`). Claude CLI skipped (self — running inside Claude Code). The review is grounded against the live codebase, not just the plan text.

## Codex Review

**Summary**

The plans have the right architecture: a single consent-gated telemetry engine, a standalone dialog module, startup wiring after existing modals, and Settings wiring through `set_consent()`. But as written they leave several consent-critical edge cases under-specified or incorrectly tested. Main risks: real Qt key/close behavior, stale Settings state because `SettingsDialog` is constructed once before first-run consent, and incomplete bilingual About coverage because Hebrew About content lives in `genizah_translations.py`, not only `genizah_app.py`.

**Strengths**

- Correctly keeps transport access behind `desktop/telemetry.py`; the PRIV-03 guard matches this design.
- Correctly identifies `save_app_config()` as additive-merge (`genizah_core.py:2882`), so stripping telemetry keys from `_config_snapshot` is a sound D-07b fix.
- Startup sequencing is mostly grounded: the recovery modal runs synchronously during `MyLibraryTab` construction before `on_startup_finished()`.
- The personal first-run copy (D-11) does include the basic what-is / what-is-not points, so D-11 is not inherently in conflict with CONSENT-02.
- Plans avoid new dependencies and reuse Phase 111's `set_consent()`/`is_enabled()` API.

**Concerns**

- **HIGH — Escape/close handling not guaranteed.** Plan 01 allows `closeEvent()` plus an *optional* note about `reject()`/`done()`. In `QDialog`, Escape commonly routes through `reject()`/`done()`, not reliably through `closeEvent()`. The plan must REQUIRE a single `done()` finalizer that writes `FIRST_RUN_SHOWN_KEY=True` and applies opt-out unless explicit opt-in already happened.
- **HIGH — Enter-key opt-in test too weak.** Posting Return to the dialog does not prove real behavior when a child button has focus — a focused `QPushButton` can consume Return before `ConsentDialog.keyPressEvent()`. `setDefault(False)`/`setAutoDefault(False)` is necessary but not sufficient. Highest-risk consent-bypass blind spot.
- **HIGH — Settings checkbox can be stale.** `SettingsDialog` is constructed once at startup (`genizah_app.py:3438`), BEFORE the first-run consent prompt runs. If the user opts in from the first-run dialog, the already-created Settings checkbox still shows the old state when opened via `_open_settings_dialog()` (`genizah_app.py:15558`). Plan 03 must refresh `chk_telemetry` from `is_enabled()` immediately before every `exec()`.
- **HIGH — Hebrew About path missing from `files_modified`.** Hebrew About content comes from `tr("ABOUT_HTML")`, backed by `genizah_translations.py`, while Plan 03 lists only `genizah_app.py`. English-only HTML in `genizah_app.py` will NOT satisfy bilingual PRIV-05 for Hebrew users unless the plan also modifies `genizah_translations.py` or adds a separate bilingual block outside the translated HTML path.
- **MEDIUM — "anonymous usage data" is privacy-imprecise.** Logged-in desktop users are identified by Supabase `user.id` — that is *pseudonymous*, not anonymous. First-run + Settings copy should say pseudonymous/anonymous depending on login state, and the full disclosure must explicitly mention the bare Supabase user id for logged-in users.
- **MEDIUM — Modal stacking only partially addressed.** Citation sequencing is good, but `_maybe_show_first_run_prompt()` should check `QApplication.activeModalWidget()` and reschedule if another modal is open (covers Settings opened quickly, index-missing flows, any future sync prompt).
- **MEDIUM — Test monkeypatching may miss `consent_dialog.save_app_config`.** Plan 01 imports `save_app_config` directly into `desktop/consent_dialog.py`, but the copied fixture patches `genizah_core.save_app_config` and `desktop.telemetry.save_app_config`, not necessarily the already-imported dialog binding → can write real config during tests unless the import style or fixture changes.
- **MEDIUM — Missing positive first-run test.** There's a skip-if-shown test but no required test that `show_first_run_prompt()` constructs and execs `ConsentDialog` exactly once when `FIRST_RUN_SHOWN_KEY` is absent.
- **LOW — Test metadata/commands drift.** No `pytest.ini`; markers live in `pyproject.toml`, and `qt` is not registered → `pytest.mark.qt` may warn/fail under strict-marker runs. Several env-var commands use Bash syntax while this repo's shell context is PowerShell.

**Suggestions**

- Make `ConsentDialog.done()` the single exit-path finalizer. Track an internal `_explicit_choice`/`_accepted_telemetry` flag so accept writes opt-in, every other exit writes opt-out, and all exits write `FIRST_RUN_SHOWN_KEY=True`.
- Add real key tests: focus Enable then send Return (assert no opt-in unless an explicit click); send Escape (assert `set_consent(False)` + shown flag); close with X (same).
- In `_open_settings_dialog()`, refresh `self.settings_dialog.chk_telemetry` from `is_enabled()` with signals blocked before `exec()`.
- Add `genizah_translations.py` to Plan 03 `files_modified`, OR add one bilingual telemetry/privacy block below the About browser for all languages (avoids translation drift).
- Change "anonymous usage data" → "privacy-preserving"/"pseudonymous usage data", with explicit mention that logged-in users use the same bare Supabase user id as the web app.
- `_maybe_show_first_run_prompt()` should reschedule if an active modal exists.
- Register the `qt` marker or don't use it; prefer existing Qt patterns from `tests/conftest.py`.

**Risk Assessment**

**HIGH.** Broad design is solid, but literal execution can miss three success criteria: Enter may still opt in through focused-button behavior, Escape may fail to persist the shown flag, and Settings may display stale consent state after first-run opt-in. Fixable before implementation, but these are consent/privacy correctness risks, not polish.

---

## Consensus Summary

Single external reviewer (Codex). No cross-reviewer consensus to compute, but the findings cluster cleanly:

### Highest-priority concerns (consent/privacy correctness — should block execution until addressed)
1. **Single `done()` finalizer** for the first-run dialog so the shown-flag + opt-out fire on EVERY exit path (Escape/X/reject), not just `closeEvent` (HIGH).
2. **Real Enter-key test** with a button focused — `setAutoDefault(False)` alone doesn't prove a focused button won't consume Return (HIGH).
3. **Refresh `chk_telemetry` before each Settings `exec()`** — the dialog is built once at startup, before first-run consent, so it can show stale state (HIGH).
4. **Hebrew About via `genizah_translations.py`** — Plan 03's `files_modified` omits it, so PRIV-05 bilingual coverage would silently fail for HE users (HIGH).

### Worth fixing in the same pass (MEDIUM)
- Pseudonymous-vs-anonymous wording for logged-in users (ties to D-10's "bare Supabase user id" point — the copy I drafted for D-11 says "anonymous," which is imprecise when logged in).
- `activeModalWidget()` reschedule guard in `_maybe_show_first_run_prompt()`.
- Test fixture must patch the `consent_dialog`-local `save_app_config` binding (import-style sensitivity).
- Add a positive "constructs + execs exactly once" first-run test.

### Low
- Register the `qt` pytest marker (markers are in `pyproject.toml`; there is no `pytest.ini` — VALIDATION.md/plan references to `pytest.ini` are themselves drift); PowerShell vs Bash env-var command syntax.

### Verification note
Several of these are concrete, verifiable codebase claims (the `genizah_translations.py` About path, the single `SettingsDialog` construction at startup, no `pytest.ini`). They should be confirmed against the live tree before/while replanning, then folded in via `/gsd:plan-phase 112 --reviews`.
