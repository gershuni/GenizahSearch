---
created: 2026-06-18T00:00:00.000Z
title: Joins Lab — search results should survive navigation away and back
area: web
resolves_phase: 120
files:
  - web/pages/joins_lab.py
  - web/joins_lab_storage.py
---

## Problem

(Phase 118 UAT, reported 2026-06-18.) In the web Joins Lab, navigating away (e.g.
to `/browse`) and back restores the **anchor** but NOT the **search results** /
candidate grid — the grid comes back empty even though the anchor and builder
inputs are intact.

The user flagged this as likely belonging to a later phase, and it does: **Phase
120 (Actions & Persistence)** already scopes "builder/triage/view state survive
refresh (server-side per-session, re-run on restore)". Per the v8.2.0 persistence
decision, the fix is to persist the builder inputs + triage and **re-run the
search on restore** (NOT to store the result snapshot — avoids the
search-history payload-bloat class of bug). Anchor restore already works; this is
the candidate-grid half.

## Acceptance

- After running a search in Joins Lab, navigating to `/browse` and back (same
  session) restores the anchor AND re-runs/repopulates the candidate grid from the
  persisted builder inputs.
- No result snapshots persisted to safe_storage (inputs + triage only; re-run on
  restore), preserving the Phase 87 `safe_user_*` chokepoint invariant.
