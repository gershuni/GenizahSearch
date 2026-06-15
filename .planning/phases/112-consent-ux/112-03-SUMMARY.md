---
phase: 112-consent-ux
plan: "03"
subsystem: desktop-telemetry-ui
tags: [telemetry, consent, privacy, settings, qt, bilingual, checkbox]
dependency_graph:
  requires:
    - phase: 112-consent-ux/112-01
      provides: PrivacyDialog constructor + ConsentDialog
    - phase: 112-consent-ux/112-02
      provides: show_first_run_prompt() wired in startup
  provides:
    - genizah_app.py SettingsDialog._TELEMETRY_SNAPSHOT_EXCLUDE + stripped _config_snapshot
    - genizah_app.py SettingsDialog._build_general_tab self.chk_telemetry + _on_telemetry_changed + Privacy-details link
    - genizah_app.py GenizahGUI._open_settings_dialog open-time chk_telemetry refresh (REVIEWS HIGH-3)
    - genizah_app.py SettingsDialog._build_about_tab bilingual telemetry disclosure block (REVIEWS HIGH-4)
    - tests/test_telemetry_consent_ux.py 5 new tests (cancel-desync + 4 toggle/refresh)
  affects: [Phase 113 crash hooks, Phase 114 usage events, Phase 115 perf events]
tech-stack:
  added: []
  patterns:
    - _TELEMETRY_SNAPSHOT_EXCLUDE frozenset strips consent keys from SettingsDialog snapshot (D-07b additive-merge)
    - blockSignals(True/False) around setChecked for spurious stateChanged prevention (Pitfall 5)
    - QMessageBox.question Yes|No confirm-on-change before set_consent() (D-07a)
    - _open_settings_dialog open-time refresh of chk_telemetry (REVIEWS HIGH-3)
    - Language-agnostic bilingual widget below About QTextBrowser for ALL languages (REVIEWS HIGH-4)
key-files:
  created: []
  modified:
    - genizah_app.py
    - tests/test_telemetry_consent_ux.py
key-decisions:
  - "Strip telemetry keys from _config_snapshot at __init__ (not _on_cancel) — additive-merge guarantee means omitted keys survive Cancel"
  - "Confirm-on-change routes ONLY through set_consent() — D-08 grep gate enforced"
  - "Open-time refresh in _open_settings_dialog (not showEvent) — simpler, single-path, try/except-wrapped"
  - "About disclosure as separate QTextBrowser widget below the browser — avoids translation drift vs modifying about_html_en or tr(ABOUT_HTML)"
  - "Privacy-preserving + pseudonymous wording throughout; no bare 'anonymous usage data' in file"
patterns-established:
  - "Snapshot exemption: frozenset of owner-written keys stripped from SettingsDialog snapshot to protect immediate-apply settings from Cancel restore"
  - "Language-agnostic bilingual disclosure: single widget with dir=ltr and dir=rtl blocks, independent of CURRENT_LANG, below translated content browser"
requirements-completed: [CONSENT-04, PRIV-05]
duration: 30min
completed: 2026-06-15
---

# Phase 112 Plan 03: Settings Telemetry Toggle + About Disclosure Summary

**Settings telemetry checkbox (confirm-on-change → set_consent(), stale-checkbox refresh on open, Cancel-desync prevention) and language-agnostic bilingual About disclosure block with PrivacyDialog opener.**

## Performance

- **Duration:** ~30 min
- **Started:** 2026-06-15
- **Completed:** 2026-06-15
- **Tasks:** 3
- **Files modified:** 2 (genizah_app.py, tests/test_telemetry_consent_ux.py)

## Accomplishments

- Telemetry keys stripped from `_config_snapshot` via `_TELEMETRY_SNAPSHOT_EXCLUDE` frozenset — `save_app_config` additive-merge in `_on_cancel` cannot desync consent state (D-07b / T-112-CancelDesync)
- `self.chk_telemetry` checkbox in `_build_general_tab` reads initial state from `is_enabled()` under blockSignals; `_on_telemetry_changed` shows QMessageBox confirm and calls `set_consent()` only on Yes; reverts on No — with "Privacy details" flat button beside it opening `PrivacyDialog` (CONSENT-04 / D-07a / D-08)
- `_open_settings_dialog` refreshes `chk_telemetry` from `is_enabled()` with signals blocked before every `exec()` — stale startup-built checkbox fixed (REVIEWS HIGH-3 / T-112-StaleCheckbox)
- Single language-agnostic bilingual telemetry disclosure block rendered below the About `QTextBrowser` for ALL languages — not inside `about_html_en` or `tr("ABOUT_HTML")`, so EN and HE users both see it without translation drift (REVIEWS HIGH-4 / PRIV-05)
- 5 new tests: `test_settings_cancel_does_not_desync_telemetry` (headless) + 4 Qt-offscreen (initial state, applies on confirm, reverts on cancel-confirm, refreshes on open)

## Task Commits

1. **Task 1: Exempt telemetry keys from _config_snapshot (D-07b)** - `52949556` (feat)
2. **Task 2: Add telemetry checkbox + confirm handler + Privacy-details + open-time refresh** - `9ff8c958` (feat)
3. **Task 3: Bilingual About-tab telemetry disclosure (PRIV-05/HIGH-4)** - `36d29f91` (feat)

## Files Created/Modified

- `C:\Genizahsearch\genizah_app.py` — `_TELEMETRY_SNAPSHOT_EXCLUDE` frozenset + stripped `_config_snapshot`; `self.chk_telemetry` + `_on_telemetry_changed` + `btn_privacy` in `_build_general_tab`; open-time refresh in `_open_settings_dialog`; bilingual telemetry disclosure block + PrivacyDialog opener in `_build_about_tab`
- `C:\Genizahsearch\tests\test_telemetry_consent_ux.py` — `test_settings_cancel_does_not_desync_telemetry` (headless D-07b proof) + 4 Qt-offscreen toggle/refresh tests

## Decisions Made

- Strip telemetry keys from snapshot at `__init__` (not in `_on_cancel`): cleanest fix — `save_app_config` is additive-merge, so omitted keys survive the restore untouched. `_on_cancel` left unchanged.
- Open-time refresh lands in `_open_settings_dialog` (not `showEvent` override): simpler, single code path, wrapped in `try/except Exception: pass` so a missing attribute never blocks Settings.
- About disclosure as a separate `QTextBrowser` below the main browser: avoids any translation drift between `about_html_en` (English) and `tr("ABOUT_HTML")` (Hebrew) — a single widget is rendered for all users regardless of `CURRENT_LANG`.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Qt GC deletes QCheckBox when page widget not retained in tests**

- **Found during:** Task 2 verify (test_settings_toggle_initial_state)
- **Issue:** `_build_general_tab()` returns a `page` QWidget that becomes the parent of the `chk_telemetry` QCheckBox. When the returned page is not kept alive by the test, Qt GC deletes the widget hierarchy, raising `RuntimeError: wrapped C/C++ object of type QCheckBox has been deleted`.
- **Fix:** All four Settings toggle tests now store the return value of `_build_general_tab()` in `_page` to keep the widget hierarchy alive.
- **Files modified:** `tests/test_telemetry_consent_ux.py`
- **Committed in:** `9ff8c958` (Task 2 commit)

**2. [Rule 1 - Bug] _FakeMainWin.__getattr__ returning None fails QComboBox signal connect**

- **Found during:** Task 2 verify (test_settings_toggle_initial_state)
- **Issue:** `SettingsDialog._build_general_tab` calls `self.main_win._on_language_combo_changed` as a `connect()` target. The fake `__getattr__` returning `None` caused `TypeError: argument 1 has unexpected type 'NoneType'`.
- **Fix:** `_FakeMainWin` provides explicit method stubs (`_on_language_combo_changed`, `check_updates_manual`, `run_indexing`) and `__getattr__` returns `lambda *a, **kw: None` (callable) for all other names.
- **Files modified:** `tests/test_telemetry_consent_ux.py`
- **Committed in:** `9ff8c958` (Task 2 commit)

---

**Total deviations:** 2 auto-fixed (both Rule 1 - test infrastructure bugs)
**Impact on plan:** Both fixes needed only in test code; production code unaffected. No scope creep.

## Issues Encountered

None in production code. Two test infrastructure issues auto-fixed (documented above).

## Known Stubs

None — all disclosure blocks are fully wired. The bilingual About disclosure text is best-effort Hebrew, flagged for translation-workflow review (RESEARCH Open Question 2). English copy is authoritative.

## Wording Compliance (REVIEWS MED / D-11)

- Checkbox label: "privacy-preserving usage data / שומרי-פרטיות" — NOT "anonymous"
- Confirm dialog copy: "privacy-preserving" wording — NOT "anonymous usage data"
- About disclosure: "privacy-preserving usage data" + "pseudonymous" — NOT "anonymous"
- Grep gate `grep -c "anonymous usage data" genizah_app.py` = 0 (verified)
- Grep gate `grep -c "save_app_config({'telemetry_enabled'" genizah_app.py` = 0 (verified, D-08)

## Threat Flags

No new network endpoints, auth paths, file access patterns, or schema changes. All changes are pure UI wiring within the existing SettingsDialog/About tab. PRIV-03 AST guard (tests/test_telemetry_no_direct_posthog.py) verified green — no `shared.posthog_server` import introduced in any `desktop/` file.

## Next Phase Readiness

- Phase 112 Plan 03 complete — all 3 plans in Phase 112 done
- CONSENT-04 and PRIV-05 requirements satisfied
- Settings toggle routed through `set_consent()` sole write path; Cancel cannot desync consent state; About shows bilingual disclosure for all languages
- Phase 113 (crash hooks) can proceed — consent gate + UI surface both complete

## Self-Check: PASSED

Files verified:
- `genizah_app.py` — FOUND (modified; parse-ok)
- `tests/test_telemetry_consent_ux.py` — FOUND (modified)

Commits verified:
- `52949556` — FOUND
- `9ff8c958` — FOUND
- `36d29f91` — FOUND

Tests verified:
- `pytest tests/test_telemetry_consent_ux.py -x -q` → 17 passed
- `pytest tests/test_telemetry_no_direct_posthog.py -x -q` → 6 passed

---
*Phase: 112-consent-ux*
*Completed: 2026-06-15*
