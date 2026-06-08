---
phase: 109-visual-similarity-merge-soft-retire
plan: "13"
subsystem: planning/UAT + desktop/join_workbench + genizah_app
tags: [uat, human-verify, gap-closure, deprecation, parity-signoff]
dependency_graph:
  requires:
    - phase: 109-08
      provides: "Gap-round-3 i18n keys"
    - phase: 109-09
      provides: "Single eye badge + visibly-ON toggle"
    - phase: 109-10
      provides: "G-07 VS buttons removed"
    - phase: 109-11
      provides: "Triage undo + merged folio/triage row + VS hint"
    - phase: 109-12
      provides: "G-08 JoinsDialog plain-open reroute"
  provides:
    - "Round-4 parity UAT signed off (parity_sign_off: APPROVED)"
    - "_show_vs_dialog deprecation marker flipped LIVE (removable) — D-11/D-14b"
  affects:
    - "Future cleanup phase: physical removal of _show_vs_dialog + _on_vs_fetch_complete + _enrich_vs_suggestions"
tech_stack:
  added: []
  patterns:
    - "Forced-LTR nav buttons so RTL bidi does not mirror < / > glyphs"
    - "Single reusable Join Lab window (D-02) — hidden, not recreated, on reopen"
    - "jw-None _save_session carries forward prior join_lab (no wipe across restart)"
    - "Client-side compare-pane zoom (cached full pixmap scaled by zoom in a pannable scroll area)"
    - "Crash-safe QThread teardown: retain a running _EnrichWorker until finished() (Windows 0xC0000409)"
key_files:
  created:
    - .planning/phases/109-visual-similarity-merge-soft-retire/109-13-SUMMARY.md
  modified:
    - .planning/phases/109-visual-similarity-merge-soft-retire/109-HUMAN-UAT.md
    - genizah_app.py
    - desktop/join_workbench.py
    - genizah_translations.py
    - tests/test_join_workbench_vs.py
decisions:
  - "Plan 07 (round-2/3 UAT) consolidated into this single round-4 UAT per user decision (2026-06-08)"
  - "_show_vs_dialog RETAINED (not deleted) this cycle per D-11; marker flipped to REMOVABLE on sign-off"
metrics:
  duration: "multi-turn UAT loop (2026-06-08)"
  completed: "2026-06-08"
  tasks_completed: 3
  tasks_total: 3
  files_changed: 5
requirements-completed: [JWB-12]
---

# Phase 109 Plan 13: Round-4 Parity UAT + Deprecation-Marker Flip

**The consolidated round-4 human UAT for the Visual-Similarity merge / soft-retire is signed off
(`parity_sign_off: APPROVED`, 2026-06-08). The `_show_vs_dialog` deprecation marker is now
live-removable (D-11/D-14b); the dead method + its orphaned helpers are retained one cycle for an
atomic cleanup phase. Six round-4 UAT findings were fixed and re-verified along the way.**

## Tasks

| # | Task | Outcome |
|---|------|---------|
| 1 | Run automated gate + author round-4 UAT scenarios (A2–A8 + K/L/M) | Gate green; `109-HUMAN-UAT.md` round-4 scaffold written (commit `5d1b30c3`) |
| 2 | Human re-UAT checkpoint on the live desktop app | **APPROVED** by Hillel after 6 fix iterations |
| 3 | On approval: flip UAT frontmatter + `_show_vs_dialog` marker live | `status: complete` / `parity_sign_off: APPROVED`; marker → REMOVABLE (method retained per D-11) |

## Round-4 UAT findings fixed (all re-verified)

| ID | Issue | Fix commit |
|----|-------|-----------|
| F-R4-1 | Toggle Visual Similarity OFF after a search → hard crash `0xC0000409` (a running `_EnrichWorker` QThread dropped mid-run) | `bf0a6353` — crash-safe `_retire_enrich_worker` (cancel + disconnect + retain-until-finished) |
| F-R4-2 | Eye 👁 badge + Y/?/N triage missing from the Compare window and Table mode | `26a57088` — shared `_candidate_shelf_badge`; Compare `_mark` no longer overrides `paint()` |
| F-R4-3 | Compare: zoom no-op, no anchor text, candidate text was whole-MS, Y/N glyphs, narrow nav | `453bbcf1` — client-side zoom (pannable), page-scoped text, ✓/?/✗ glyphs, wider nav |
| F-R4-4 | Compare nav arrows bidi-mirrored (both showed `>`) | `9e1113e1` — force LTR buttons |
| F-R4-5 | Join Lab state lost on close/reopen within a session | `e94f6540` — single reusable instance (reuse hidden window) |
| F-R4-6 | Join Lab state lost across app restart (jw-None save wiped `join_lab`) | `6c52a3b9` — `_save_session` carries forward prior `join_lab` |

(Compare nav final orientation `<הקודם` / `הבא>` per Hillel — commits `c67acae3` / `e94f6540`.)

## Deprecation flip (D-11 / D-14b)

- `genizah_app.py::_show_vs_dialog` marker changed from "pending parity sign-off" → **REMOVABLE
  (signed off 2026-06-08)**. The method is NOT physically deleted this cycle (D-11). Its orphaned
  helpers `_on_vs_fetch_complete` and `_enrich_vs_suggestions` carry the same removable marker.
  A future cleanup phase deletes the whole cluster atomically.
- `109-HUMAN-UAT.md`: `status: complete`, `parity_sign_off: APPROVED`.

## Verification

- Final automated gate (`test_join_workbench_vs` + `_i18n` + `_no_private` + `_visual_similarity_dialog`,
  plus `_construct` offscreen): green (59 incl. round-4 regression tests; 55 in the non-Qt subset).
- `python -m ruff check` clean on all touched files; `genizah_app.py` AST parse OK.
- Human sign-off recorded in `109-HUMAN-UAT.md` (Round 4 Verdict — APPROVED).

## Self-Check: PASSED

- `109-HUMAN-UAT.md` frontmatter: `status: complete` + `parity_sign_off: APPROVED`: CONFIRMED
- `genizah_app.py` `_show_vs_dialog` marker contains `REMOVABLE` and no longer `pending parity sign-off`: CONFIRMED
- `_show_vs_dialog` still defined (D-11 retention, not deleted): CONFIRMED (genizah_app.py:4768)
- Automated gate green; ruff clean; AST OK: CONFIRMED
- All six round-4 findings fixed with regression guards: CONFIRMED
