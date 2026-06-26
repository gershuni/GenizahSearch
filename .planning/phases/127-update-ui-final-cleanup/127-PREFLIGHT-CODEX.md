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

## Round 2 (confirmation) — REVISE, 3 residuals
Codex confirmed the substantive fixes (HIGH-2 delete-range/BATCH_SIZE, HIGH-3 omit-`os`, MED-4 QMessageBox
patch) all landed. Caught 3 leftover string-instances of the already-decided fixes (not new code risks):
(1) HIGH-1 leftover — `127-02` frontmatter `key_links` gui_threads `via:` still listed `SidecarDownloadThread`/`module-level`;
(2) LOW-5 leftover — one stale "20-name table" at `127-01:135`;
(3) NEW nit — `127-03` should also update the now-stale live comment at `genizah_app.py:952`
(`# noqa: F401 shim` → `plain import shim`, since the D1 noqa is retired this phase).
All 3 applied. Output: `scratchpad/127-codex/planflight-r2.txt`.

## Round 3 (final convergence) — CLEARED
Codex: items 2 & 3 CONFIRMED; **"No additional blocking drift found"** (re-verified live: classes
184/243/295/364, BATCH_SIZE@596 used@16459, coordination on GenizahGUI, only lazy desktop→genizah_app
import is join_workbench.py, tr/CURRENT_LANG exported by genizah_core, no F401 risk). One last cosmetic
ask: the `127-02:35` `via:` line should not even mention `SidecarDownloadThread` → applied verbatim
(`via: "UpdateDownloaderThread — lazy inside UpdateProgressDialog.start_download"`). Output: `scratchpad/127-codex/planflight-r3.txt`.

## GATE STATUS: CLEARED ✅
Every functional finding CONFIRMED-fixed against live code across 3 rounds; the residual REVISEs were
leftover string-instances of already-decided fixes (caught by convergence discipline), and r3 explicitly
found no blocking drift. Final cosmetic metadata line applied to Codex's verbatim spec. Proceed to execute
(127-01 → 127-02 → 127-03, sequential, USE_WORKTREES=false) with the source-integrity gate between waves.
