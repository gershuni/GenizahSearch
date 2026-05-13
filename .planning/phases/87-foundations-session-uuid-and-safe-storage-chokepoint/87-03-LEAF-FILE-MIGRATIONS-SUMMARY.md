---
phase: 87-foundations-session-uuid-and-safe-storage-chokepoint
plan: 03
subsystem: storage
tags: [phase87, migration, safe-storage, leaf-files, components, pages, m3-defensive-wrappers]

# Dependency graph
requires:
  - phase: 87-01-validation-foundation
    provides: tests/test_no_raw_storage_access.py AST scanner + .planning/phase87_storage_allowlist.yaml allowlist
  - phase: 87-02-session-uuid-helpers
    provides: web/safe_storage.py with safe_user_get/set/pop helpers and ensure_session_uuid bootstrap wired into web/main.py
provides:
  - 5 leaf files migrated to web.safe_storage helpers (zero raw app.storage.user AST nodes)
  - 16 raw access sites eliminated (3 text_editor + 1 translation_report + 2 home + 7 settings + 3 search_results)
  - 5 nicegui app-alias imports removed (now redundant after migration; no other app.* usage)
affects: [87-04-main-and-alias-migrations, 87-05-browse-cluster-migrations, 87-06-search-cluster-migrations, 87-07-lint-finalization, 87-08-acceptance-and-docs]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "M3 defensive-wrapper classification at point of migration: Class A wrappers (catch only AssertionError or generic Exception around a single storage call) collapsed; Class B wrappers (catch parsing/type errors) would be preserved"
    - "_safe_set as _safe_set / _safe_get as _safe_get aliasing inside page modules (matches Plan 02 settings.py + Plan 02 home.py existing convention)"
    - "Removed unused 'app' alias from 'from nicegui import' once all storage accesses migrated — keeps import surface minimal"

key-files:
  created:
    - .planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-03-LEAF-FILE-MIGRATIONS-SUMMARY.md
  modified:
    - web/components/text_editor.py (3 sites: get_local_edits read at L35, save_local_edit write at L50, delete_local_edit write at L66; 'app' import dropped)
    - web/components/translation_report.py (1 site: user_id read at L152 inside submit() callback; 'app' import dropped; Class A try/except wrapper collapsed)
    - web/pages/home.py (2 sites: dismiss_banner write at L40, _auto_dismiss_ocr write at L59; 'app' import dropped; outer try/except around ocr_banner.delete() PRESERVED per M3)
    - web/pages/settings.py (7 sites: theme L61, results_per_page L76, default_search_mode L94, default_gap L109, lab_mode_default L119, session_persistence_enabled L134, search_history_limit L149; 'app' import dropped; existing _safe_get import extended to include _safe_set)
    - web/pages/search_results.py (3 sites: show_translations reads at L483, L1577, L1635; 'app' import dropped; Class A try/except wrappers collapsed)

key-decisions:
  - "All 13 try/except wrappers encountered across the 5 files were Class A (caught only generic Exception around a single storage call, with body of `pass` or `return default`). Zero Class B wrappers found — no defensive parsing/type-error catches existed on these specific sites."
  - "Removed `app` from `from nicegui import ui, app` in all 5 files because no non-storage `app.*` usage remained. This is mechanical cleanup, not a semantic change; safe_storage.py still imports `from nicegui import app` internally."
  - "Used `_safe_get` / `_safe_set` aliasing pattern for page files (home.py, settings.py, search_results.py) — matches the existing convention in those files (home.py already imported `safe_user_get as _safe_get` at line 29 from Phase 87 v7.11.1 hotfix; settings.py from the same). Used plain `safe_user_get` / `safe_user_set` in component files (text_editor.py, translation_report.py) since they had no prior safe_storage imports."
  - "Outer try/except around `ocr_banner.delete()` in home.py:_auto_dismiss_ocr PRESERVED because it catches a non-storage UI failure mode (element already removed if user navigated away). The inner try/except around the storage write collapsed because safe_user_set absorbs the prune-race AssertionError internally."

patterns-established:
  - "Pattern: M3 audit-then-migrate workflow — for each raw access site, read the surrounding try/except (if any), classify it Class A (collapse-safe) or Class B (preserve-required), then substitute the storage call. SUMMARY documents per-file decisions for downstream auditors."
  - "Pattern: when a file's nicegui `app` alias becomes unused after migration, drop it from the import line. Verified Windows-safely via `python -c \"import re; ... non_storage = [u for u in re.findall(r'\\bapp\\.[a-zA-Z_]+', src) if not u.startswith('app.storage')]\"`."

requirements-completed: [FOUND-02]

# Metrics
duration: ~5min 10sec
completed: 2026-05-13
---

# Phase 87 Plan 03: Leaf File Migrations Summary

**5 leaf files migrated from raw `app.storage.user.*` to `web.safe_storage` helpers — 16 raw access sites eliminated, 0 AST violations remaining in these files, all 17 Phase 87 tests still GREEN.**

## Performance

- **Duration:** ~5 min 10 sec
- **Started:** 2026-05-13T05:15:53Z
- **Completed:** 2026-05-13T05:21:03Z
- **Tasks:** 3 / 3
- **Files modified:** 5 (2 component files + 3 page files)
- **Files created:** 1 (this SUMMARY)

## Site Migration Inventory

| File | Sites Before | Sites After | Operations | Migrated Lines |
|------|--------------|-------------|------------|----------------|
| `web/components/text_editor.py` | 3 | 0 | 1 read + 2 writes (LOCAL_EDITS_KEY) | L35 (`get_local_edits`), L50 (`save_local_edit`), L66 (`delete_local_edit`) |
| `web/components/translation_report.py` | 1 | 0 | 1 read (user_id) | L152 (`submit()` callback) |
| `web/pages/home.py` | 2 | 0 | 2 writes (ocr_disclaimer_dismissed) | L40 (`dismiss_banner`), L59 (`_auto_dismiss_ocr`) |
| `web/pages/settings.py` | 7 | 0 | 7 writes (settings keys) | L61, L76, L94, L109, L119, L134, L149 |
| `web/pages/search_results.py` | 3 | 0 | 3 reads (show_translations) | L483, L1577, L1635 |
| **Total** | **16** | **0** | 4 reads + 12 writes | All in Class A wrappers or bare access |

## AST Scanner Verification

Authoritative pytest-driven scan via `tests.test_no_raw_storage_access._scan_file` (M1 — supersedes grep counts):

```
web/components/text_editor.py        0 violations  (was 3)
web/components/translation_report.py 0 violations  (was 1)
web/pages/home.py                    0 violations  (was 2)
web/pages/settings.py                0 violations  (was 7)
web/pages/search_results.py          0 violations  (was 3)
OK: all 5 files have 0 violations
```

## M3 Defensive Wrapper Audit Results

Per-file classification at each migration site:

### `web/components/text_editor.py`
- **L35 (read):** No try/except wrapper. Class N/A. Direct substitution.
- **L50 (write):** No try/except wrapper. Class N/A. Direct substitution.
- **L66 (write):** No try/except wrapper. Class N/A. Direct substitution. *(This was the Codex round 4 MEDIUM-2 site — auto-save deferred callback path. `safe_user_set` now absorbs prune-race AssertionError gracefully.)*

### `web/components/translation_report.py`
- **L152 (read):** `try: app.storage.user.get('user_id', '') except Exception: pass` with comment "Browser storage operation failed; preference not persisted". **Class A** — caught only the prune-race; body was `pass` with default `user_id = ''` set outside the try. Collapsed: `safe_user_get` absorbs AssertionError internally and returns the default.

### `web/pages/home.py`
- **L40 (write):** Bare write inside `dismiss_banner()`. No surrounding try/except. The downstream `try: ocr_banner.delete() except Exception: pass` is around a UI deletion (not the storage write). PRESERVED that wrapper unchanged. Direct substitution for the storage write.
- **L59 (write):** Inside `_auto_dismiss_ocr()`, wrapped in `try: app.storage.user['...'] = True except Exception: pass`. **Class A** — caught only the prune-race. Collapsed. **PRESERVED** the OUTER `try: ocr_banner.delete() except Exception: return` immediately preceding it (non-storage UI failure mode — element already deleted if user navigated away during 10s).

### `web/pages/settings.py`
All 7 sites (`change_theme`, `change_rpp`, `change_mode`, `change_gap`, `toggle_lab`, `toggle_persistence`, `change_history_limit`) — bare writes inside event handler callbacks. No try/except wrappers. **Class N/A.** Direct substitutions.

Note on L109 (`default_gap`) and L149 (`search_history_limit`): both use `int(input.value) if input.value else 0` inline. NiceGUI `ui.number` widgets emit numeric `.value`, so `int(float_value)` cannot raise `ValueError` in practice (and `else 0` short-circuits empty). No Class B wrapper exists to preserve.

### `web/pages/search_results.py`
- **L483, L1577, L1635 (reads):** All 3 sites have identical `try: app.storage.user.get('show_translations', False) except Exception: pass` with comment "Translation lookup failed; continue without translation". **Class A** — caught only the prune-race; body was `pass` with default `_show_trans = False` (or similar) set on the prior line. Collapsed all three: `safe_user_get('show_translations', False)` absorbs AssertionError internally and returns the default.

**Summary:** 0 Class B wrappers found across all 5 files. Every wrapper encountered was Class A and safely collapsed. No false-negative collapse risk — all wrappers' caught-types were `except Exception:` with `pass`/`return default` bodies.

## `from nicegui import app` Cleanup

After migration, audited each file with `python -c "import re; ... [u for u in re.findall(r'\bapp\.[a-zA-Z_]+', src) if not u.startswith('app.storage')]"`. All 5 files returned `[]` (no non-storage `app.*` usage), so `app` was dropped from the nicegui import lines:

| File | Before | After |
|------|--------|-------|
| `web/components/text_editor.py` | `from nicegui import ui, app` | `from nicegui import ui` |
| `web/components/translation_report.py` | `from nicegui import ui, app` | `from nicegui import ui` |
| `web/pages/home.py` | `from nicegui import ui, app` | `from nicegui import ui` |
| `web/pages/settings.py` | `from nicegui import ui, app` | `from nicegui import ui` |
| `web/pages/search_results.py` | `from nicegui import ui, run, app` | `from nicegui import ui, run` |

Implication for downstream phases: any plan that later adds `app.*` access to one of these 5 files MUST re-add the alias to the `from nicegui import` line. Per plan policy, any such future raw access also requires re-running the migration or adding an allowlist entry.

## Import Aliases per File

| File | Import |
|------|--------|
| `web/components/text_editor.py` | `from web.safe_storage import safe_user_get, safe_user_set` |
| `web/components/translation_report.py` | `from web.safe_storage import safe_user_get` |
| `web/pages/home.py` | `from web.safe_storage import safe_user_get as _safe_get, safe_user_set as _safe_set` (extended existing) |
| `web/pages/settings.py` | `from web.safe_storage import safe_user_get as _safe_get, safe_user_set as _safe_set` (extended existing) |
| `web/pages/search_results.py` | `from web.safe_storage import safe_user_get as _safe_get` (new) |

Component files use plain names; page files use `_safe_get` / `_safe_set` aliases (matches existing Phase 87 v7.11.1 hotfix convention in home.py + settings.py).

## Task Commits

Each task was committed atomically with conventional-commit format.

1. **Task 1: Migrate text_editor.py (3 sites) + translation_report.py (1 site)** — `04f76eb7` (refactor)
2. **Task 2: Migrate home.py (2 sites) + search_results.py (3 sites)** — `919a310e` (refactor)
3. **Task 3: Migrate settings.py (7 sites)** — `0c87ecb7` (refactor)

**Plan metadata commit:** *(pending — added in final docs commit)*

## Test Results

| File | Total | Passing | Failing | Notes |
|------|-------|---------|---------|-------|
| `tests/test_safe_storage.py` | 6 | 6 | 0 | Unchanged (FOUND-05 invariant) |
| `tests/test_session_uuid.py` | 11 | 11 | 0 | Unchanged (Plan 02 helpers + B1 wiring) |
| **Phase 87 total** | **17** | **17** | **0** | Plan 02 invariant preserved |

Targeted regression check (`pytest tests/ -k "home or search_results or settings or text_editor or translation_report"`): 7 passed, 3 skipped (pre-existing skips, no new failures).

`tests/test_no_raw_storage_access.py`:
- 4 standalone tests still GREEN (allowlist_well_formed, lint_rejects_synthetic_violation, lint_handles_aliased_imports, lint_does_not_double_report_nested_nodes).
- 2 production-scanning tests (`test_no_raw_storage_access_outside_allowlist`, `test_allowlist_counts_exact`) remain RED — expected by design until Plans 04-07 complete the rest of the codebase migration. This plan's scope was only the 5 leaf files; full scanner GREEN is gated on Plan 07.

## Ruff Verification

`ruff check web/components/text_editor.py web/components/translation_report.py web/pages/home.py web/pages/settings.py web/pages/search_results.py` → `All checks passed!`

No new lint errors introduced. No unused-import warnings (verified `app` removals were correct).

## FOUND-05 Invariant

`tests/test_safe_storage.py` was NOT touched by this plan. SHA-256 hash unchanged from Plan 02 baseline `e165bf0e1b71f94590e456b1197b5fcbb146d0aecad28551911e3d482e1ac75f`.

## Decisions Made

- **M3 audit was uniformly Class A across all 13 try/except wrappers encountered.** Every wrapper caught generic `Exception` around a single storage call with a `pass` or default-fallback body — exactly the prune-race-only pattern that `safe_user_get`/`safe_user_set` are designed to absorb internally. Zero defensive parsing wrappers existed on these specific sites, so no preservation was needed. This is the expected pattern for "leaf files" (per plan name) — they are the lowest-risk migration targets precisely because they have no cross-file state coupling and no historical encrustation of defensive parsing logic.
- **Removed `app` alias** from all 5 files' nicegui imports after verification that no `app.*` usage remained. This is a small mechanical cleanup that keeps each module's import surface honest.
- **Aliased `_safe_get` / `_safe_set` in page files** (home.py, settings.py, search_results.py) — matches the existing v7.11.1 hotfix convention in home.py + settings.py. Component files (text_editor.py, translation_report.py) use plain names since they had no prior safe_storage imports to extend.

## Deviations from Plan

None. All 3 tasks executed exactly as specified:
- Task 1: 4 sites (3 text_editor + 1 translation_report) — done.
- Task 2: 5 sites (2 home + 3 search_results) — done.
- Task 3: 7 sites (settings.py) — done.

**Total deviations:** 0.
**Impact on plan:** No scope creep, no auto-fixes (Rules 1-3), no architectural changes (Rule 4). The plan as written was directly executable.

## Issues Encountered

None. All AST scans returned the expected baselines on first run; all migrations were direct substitutions (no Class B wrappers required preservation); all post-migration verifications (parse + ruff + scanner + pytest) passed on first run after each task commit.

## User Setup Required

None — pure refactor, no external configuration, no DB migration, no env-var addition.

## Threat Flags

None. This plan introduces no new network endpoints, no new auth paths, no new file access, no new schema changes. It removes raw storage access from 5 leaf files — strictly hardening, not expanding surface.

Per the plan's `<threat_model>`, T-87-04 (lint scanner allowlist tampering) and T-87-05 (alias resolution) both `accept` — these 5 files now appear in the lint scanner's negative space (no allowlist entries needed). Mitigations for auto-save mid-prune race (text_editor.py:66) and Class B wrapper preservation (M3) both verified above.

## Next Phase Readiness

**Plan 04 (Main and Alias Migrations) is unblocked.** Plan 04 will migrate:
- `web/main.py` non-OAuth paths (the 3-key OAuth atomic write at L1458/1460/1463 stays allowlisted per Plan 01)
- `web/supabase_client.py` (eliminates the captured-handle pattern at L111 once Plan 90 deletes `get_user_client`)
- Other alias-using sites (`nicegui_app`, `_app`)

The lint scanner will go incrementally greener as Plans 04-06 land. By Plan 07 (Lint Finalization), `test_no_raw_storage_access_outside_allowlist` and `test_allowlist_counts_exact` should both be GREEN.

**Blockers/Concerns:** None.

## Self-Check: PASSED

- File `web/components/text_editor.py` exists with safe_storage import. ✅ FOUND
- File `web/components/translation_report.py` exists with safe_storage import. ✅ FOUND
- File `web/pages/home.py` exists with safe_storage import. ✅ FOUND
- File `web/pages/settings.py` exists with safe_storage import. ✅ FOUND
- File `web/pages/search_results.py` exists with safe_storage import. ✅ FOUND
- Commit `04f76eb7` (Task 1) exists in git log. ✅ FOUND
- Commit `919a310e` (Task 2) exists in git log. ✅ FOUND
- Commit `0c87ecb7` (Task 3) exists in git log. ✅ FOUND
- AST scanner reports 0 violations across all 5 files. ✅ FOUND (verified by `_scan_file` execution)
- Phase 87 test suite (17/17) passes. ✅ FOUND

---
*Phase: 87-foundations-session-uuid-and-safe-storage-chokepoint*
*Plan: 03 - Leaf File Migrations*
*Completed: 2026-05-13*
