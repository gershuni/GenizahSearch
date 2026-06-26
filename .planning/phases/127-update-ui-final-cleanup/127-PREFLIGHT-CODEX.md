# Phase 127 — Codex PLAN Pre-Flight (resolutions)

**Gate:** Codex PLAN pre-flight (read-only, against the live codebase) — the gate that caught real
BLOCKERs in Phases 124 & 126. Run BEFORE execution to catch plan↔code drift the internal plan-checker
(which reasons from plan text) cannot see.

**Round 1 verdict:** REVISE — 5 findings (3 HIGH, 1 MEDIUM, 1 LOW). Codex independently VERIFIED the
safe claims: [A] all 9 D1 classes ARE used internally (noqa-drop is F401-safe), [B] all 4 update_ui
classes used (no-noqa shim correct), [D] EN privacy strings live in desktop/settings_dialogs.py (flip
valid), [F] the skip-until-exists branch exists in the core template (green-at-HEAD valid).

Output: `scratchpad/127-codex/planflight-r1.txt` (full), `…/planflight-brief.txt`.

## Findings & resolutions (all LOCKED into the plans)

| # | Sev | Finding (Codex evidence) | Resolution |
|---|-----|--------------------------|------------|
| 1 | HIGH | 127-02 import hint+action prescribed `load_app_config`/`save_app_config`/`APP_VERSION`/`SidecarDownloadThread` for `desktop/update_ui.py`, but the 4 class bodies use NONE of them; `APP_VERSION` is not even exported by genizah_core (it's in `version.py:3`) → module-level import = ImportError. Actual module-level set = `tr, CURRENT_LANG` only; the rest stay lazy in-function (UpdateDownloaderThread@446, reset_*@531-536, stdlib tempfile/os/subprocess/sys). | 127-02: corrected BOTH the "import set" hint block AND the Task-1 action text AND the `key_links` `via:` field to the Codex-verified derived set; explicit DO-NOT-import list; reaffirmed "derive from bodies, ruff F401 is the gate". |
| 2 | HIGH (blocker-class) | 127-02 delete range `364-~597` overshoots: `UpdateProgressDialog` body ENDS at line 593; `BATCH_SIZE = 500` at line 596 (used at genizah_app.py:16459) sits between it and `LabPanel@598`. "Delete to the next `^class`" would sweep a live module constant. | 127-02: range fixed to `364-593` in read_first + action; explicit ⚠ to STOP at the end of UpdateProgressDialog's body (NOT the next `^class`) and PRESERVE `BATCH_SIZE`; new acceptance criterion `grep ^BATCH_SIZE` returns one hit + still referenced at ~16459. |
| 3 | HIGH | 127-01 said copy imports incl. `import os` into `test_no_back_edges_desktop.py`, but `os` is used ONLY by the core-only `test_config_paths_resolve_to_repo_root` (not copied) → unused `import os` fails the new file's own ruff F401. | 127-01: interfaces + action now say copy ONLY `ast`, `pathlib`, `pytest` + REPO_ROOT; OMIT `import os`. |
| 4 | MED | 127-01 coordination test: `_download_next_sidecar` empty-queue branch calls `QMessageBox.information(self,…)` @24434; a bare `GenizahGUI.__new__` object isn't a real QWidget → would raise unless patched. Plan didn't name the patch. | 127-01 Task-2 action: if exercising the empty-queue branch, patch `genizah_app.QMessageBox.information`; otherwise test the non-empty branch (queue pops + SidecarDownloadThread fires). |
| 5 | LOW | "20 facade names" prose (×7) vs the 27 identity assertions in `test_no_back_edges_core.py`. The enumerated name-list was already complete (27); only the count label was stale → risk the executor stops at 20. | 127-01: all "20" facade-count references → "27"; acceptance now says "assert EVERY enumerated name — the count is 27, not 20". |

## Status
All 5 resolved + cross-checked against the exact live-code evidence Codex cited (9 D1 uses, 4 update_ui
uses, BATCH_SIZE@596/16459, os-usage, 27 identity asserts). Round 2 (focused confirmation) follows.
