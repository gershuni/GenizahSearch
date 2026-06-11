---
phase: 109-visual-similarity-merge-soft-retire
plan: "07"
subsystem: planning/UAT
tags: [uat, human-verify, gap-closure, superseded]
status: superseded
dependency_graph:
  requires:
    - phase: 109-04
      provides: "G-01 Hebrew label fix + pre-seeded keys"
    - phase: 109-05
      provides: "Toggle redesign (set_source('visual'))"
    - phase: 109-06
      provides: "Pick-mode reroute (G-05) — later REVERSED by 109-12/G-08"
  provides:
    - "Round-2/3 parity UAT scaffold — SUPERSEDED by 109-13 round-4 UAT"
  affects:
    - "109-13: round-4 UAT subsumes this plan's scenarios"
tech_stack:
  added: []
  patterns: []
key_files:
  created: []
  modified: []
decisions:
  - "Plan 07 superseded by Plan 13 per user decision (2026-06-08): consolidate to a single human-UAT round"
metrics:
  duration: "n/a (not executed — superseded)"
  completed: "2026-06-08"
  tasks_completed: 0
  tasks_total: 2
  files_changed: 0
requirements-completed: [JWB-12]
---

# Phase 109 Plan 07: Round-2/3 Parity UAT — SUPERSEDED

**This plan was NOT executed as a separate human-UAT round. It is superseded by Plan 13's
round-4 UAT per the user's explicit decision on 2026-06-08 ("Consolidate — one round").**

## Why superseded

Plan 07 authored a round-2/3 human-verify UAT (Scenarios A–M) covering G-01..G-05 plus three
deferred scenarios (K four-actions, L reused-window re-anchor, M perf ≥80). It was assigned to
Wave 4, but the gap-closure round-3 code (Plans 08–12) lands in the same execution and changes
the very UI several of its scenarios describe. Running it as a separate round would present the
human tester with scenarios that no longer match the live app:

- **Scenario E is stale** — it instructs verifying the `★both` / `⊙VS` badges, but Plan 09 (G-06/G-09)
  replaced that entire badge scheme with a single 👁 eye badge. Plan 13 Scenario A2 re-verifies the
  NEW eye badge.
- **Scenario J is now wrong** — it verifies the G-05 pick-mode return (JoinsDialog → Workbench →
  "Select as partner" → fills Fragment B). Plan 12 (G-08) **reversed** that: the JoinsDialog VS
  button now opens the Workbench *plain* and closes the dialog, with no pick-back. Plan 13
  Scenario A8 re-verifies the NEW (reversed) behavior.

Plan 13's round-4 UAT keeps Scenarios A–M as the base checklist (so the stable scenarios F card
text, H no-VS disabled toggle, I Compare parity are NOT lost) and appends the round-4 scenarios
(A2–A8) for G-06..G-13, plus re-verifies the same deferred K/L/M scenarios. It therefore fully
subsumes Plan 07.

## Disposition

- No code or planning-file changes are attributable to this plan (the UAT-file authorship is owned
  by Plan 13's Task 1, which refreshes `109-HUMAN-UAT.md` for round 4).
- The `_show_vs_dialog` deprecation-marker sign-off gate that Plan 07 owned is carried by Plan 13
  Task 3 (flips the marker live on the consolidated round-4 approval).
- Requirement JWB-12 verification is delegated to Plan 13.

## Self-Check: PASSED (superseded)

- Decision recorded: user chose "Consolidate (one round)" on 2026-06-08.
- The two stale scenarios (E badge scheme, J pick-return) are documented above with their
  superseding round-4 scenarios (A2, A8).
- The automated gate Plan 07 required is GREEN: `python -m pytest tests/test_join_workbench_vs.py
  tests/test_join_workbench_i18n.py tests/test_join_workbench_no_private.py
  tests/test_visual_similarity_dialog.py tests/test_join_workbench_construct.py -q` → 45 passed.
