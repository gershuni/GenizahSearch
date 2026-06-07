---
phase: 109-visual-similarity-merge-soft-retire
plan: "03"
type: human-uat
status: partial
created: "2026-06-07"
automated_gate: PASSED  # test_load_visual_candidates_parity (D-14a) — green as of Plan 03
parity_sign_off: PENDING  # D-14b — awaiting Hillel's manual parity verification
---

# Phase 109 Plan 03: Parity UAT (D-14b / D-16)

The automated parity invariant (`test_load_visual_candidates_parity`, D-14a) passed before
this scaffold was created. The items below require a human (Hillel) to manually click through
the running desktop GUI.

**Prerequisite confirmed:** `python -m pytest tests/test_join_workbench_vs.py::test_load_visual_candidates_parity -x` — GREEN.

---

## Current Test (automated)

| Test | Status | Details |
|------|--------|---------|
| `test_load_visual_candidates_parity` (D-14a) | PASSED | Workbench Visual sys_id set == service get_suggestions set; all via_vs=True; all shelfmarks non-blank |
| `test_join_workbench_vs.py` (6 tests) | PASSED | Full VS test suite |
| `test_join_workbench_no_private.py` (2 tests) | PASSED | No _vs_* private calls on rerouted paths |
| `test_join_workbench_i18n.py` (4 tests) | PASSED | i18n guard |
| `test_visual_similarity_dialog.py` (6 tests) | PASSED | Pick-mode dialog tests (D-12 preserved) |
| `test_join_workbench_construct.py` (6 tests) | PASSED | Window construction |
| ruff check genizah_app.py desktop/result_dialog.py | PASSED | Clean |

---

## Tests (Manual Parity UAT — PENDING)

The following scenarios must be run on the live desktop application by Hillel.

### Scenario 1: Browse "Visual similarity" reroute (D-10)

Pick 3–5 real anchors that HAVE VS data.

For each anchor:

**1a. Entry point reroute**
- [ ] From Browse, click "Visual similarity" button
- [ ] CONFIRM: Join Workbench opens (not the old orange VS dialog)
- [ ] CONFIRM: Fragment is pinned as anchor in the Workbench
- [ ] CONFIRM: Visual source is auto-selected (not Text)
- [ ] CONFIRM: VS candidates load automatically (paginated 20/page)

**1b. Shelfmark non-blank (review #5)**
- [ ] CONFIRM: Every VS candidate card shows a NON-EMPTY shelfmark
- [ ] CONFIRM: No card shows a raw numeric alma_id or blank shelfmark text

**1c. Parity — same look-alike set**
- [ ] CONFIRM: The sys_id set shown in the Workbench Visual source matches
  `get_vs_service().get_suggestions(sys_id, 200)` output
- [ ] CONFIRM: No text-only candidates leak into the Visual view (no ★both or ⊙VS missing)

---

### Scenario 2: ResultDialog "Search visual similarity" reroute (D-10)

**2a. Entry point reroute**
- [ ] From a search result, open the ResultDialog
- [ ] Click "Search visual similarity" button
- [ ] CONFIRM: Join Workbench opens with Visual source auto-loaded
- [ ] CONFIRM: The ResultDialog closes after launching the Workbench

---

### Scenario 3: Four actions on VS candidate cards (D-16)

On a VS candidate card in the Workbench Visual source:

- [ ] CONFIRM: "Browse" action opens the candidate in Browse
- [ ] CONFIRM: "Puzzle" action adds the candidate to the Fragment Puzzle
- [ ] CONFIRM: "Add to List" action adds the candidate to a list
- [ ] CONFIRM: "Add as Join" action opens the JoinsDialog with the candidate pre-filled

---

### Scenario 4: Reused-window re-anchor (pending-source / review #2)

- [ ] Switch the Workbench from Visual → Text → Combined on a reused window
- [ ] Via Browse "Visual similarity", re-anchor to a DIFFERENT VS-bearing fragment
- [ ] CONFIRM: Visual source reloads for the NEW anchor (not the previous one)
- [ ] CONFIRM: Candidate cards update to the new anchor's VS look-alikes

---

### Scenario 5: No-VS anchor grey-out (D-08)

Pick 1 anchor with NO VS data (approximately 50% of manuscripts have none).

- [ ] CONFIRM: Visual source radio button is GREYED OUT / disabled
- [ ] CONFIRM: Combined source radio button is GREYED OUT / disabled
- [ ] CONFIRM: Pane stays on Text source (not stuck on a disabled source)

---

### Scenario 6: Performance check (D-09 / SC#3)

Open an anchor with ≥80 look-alikes.

- [ ] CONFIRM: First 20-card page renders promptly (no long hang)
- [ ] CONFIRM: Paging (next page) stays responsive
- [ ] CONFIRM: Thumbnails fetch per-page (≤20), not all 200 upfront

---

### Scenario 7: JoinsDialog pick-mode still works (D-12 / SC#2)

- [ ] Open JoinsDialog for a join
- [ ] Open the visual partner-picker ("Visual similarity" in JoinsDialog)
- [ ] Pick a VS candidate as the partner fragment
- [ ] CONFIRM: The join is created with the selected partner (pick-mode works)
- [ ] CONFIRM: The old VS dialog behavior (orange dialog) still appears for the pick-mode path

---

## Summary

| Scenario | Status | Anchors Tested | Notes |
|----------|--------|----------------|-------|
| 1a Browse reroute (Workbench opens, not old dialog) | PENDING | — | — |
| 1b Shelfmarks non-blank | PENDING | — | — |
| 1c Same look-alike set (no stale text) | PENDING | — | — |
| 2a ResultDialog reroute + closes | PENDING | — | — |
| 3 Four actions on VS candidates | PENDING | — | — |
| 4 Reused-window re-anchor reloads VS | PENDING | — | — |
| 5 No-VS anchor grey-out correct | PENDING | — | — |
| 6 Perf: first 20-card page renders promptly | PENDING | — | — |
| 7 Pick-mode still works (D-12 / SC#2) | PENDING | — | — |

---

## Gaps

None known at code-complete state. The automated D-14a gate confirms the VS candidate set matches
the service output for all anchors tested via stub. The manual UAT above is the required human
sign-off (D-14b) before the "pending parity sign-off" deprecation marker on `_show_vs_dialog`
(D-11) is considered live.

**Once Hillel confirms all PENDING scenarios above, update this file:**
- Set `status: complete` in frontmatter
- Set `parity_sign_off: APPROVED` in frontmatter
- Fill in the Summary table with anchors tested + notes
- The deprecation marker on `_show_vs_dialog` is then live (D-11 flips after D-14b)
