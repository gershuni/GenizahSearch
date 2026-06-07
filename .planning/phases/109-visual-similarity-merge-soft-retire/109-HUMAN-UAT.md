---
phase: 109-visual-similarity-merge-soft-retire
plan: "03"
type: human-uat
status: diagnosed
created: "2026-06-07"
updated: "2026-06-07"
automated_gate: PASSED  # test_load_visual_candidates_parity (D-14a) — green as of Plan 03
parity_sign_off: REJECTED  # D-14b — Hillel's manual UAT found 5 gaps (see Gaps section); 2 supersede locked decisions (D-10, D-12)
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
| 1a Browse reroute (Workbench opens, not old dialog) | PASS | (live) | Workbench opens with Visual; reroute works |
| 1b Shelfmarks non-blank | PASS | (live) | Cards show shelfmark |
| 1c Same look-alike set (no stale text) | PASS | (live) | — |
| 2a ResultDialog reroute + closes | PASS | (live) | — |
| 3 Four actions on VS candidates | NOT REACHED | — | Superseded by G-04 redesign |
| 4 Reused-window re-anchor reloads VS | NOT REACHED | — | Superseded by G-04 redesign |
| 5 No-VS anchor grey-out correct | NOT REACHED | — | Superseded by G-04 (radios removed → toggle button) |
| 6 Perf: first 20-card page renders promptly | NOT REACHED | — | — |
| 7 Pick-mode still works (D-12 / SC#2) | CHANGE REQUESTED | — | G-05: reverse D-12 — wire pick-mode into Join Lab + new tooltip |

**Verdict: REJECTED — 5 gaps found (G-01..G-05). G-04 supersedes the D-10 source-model; G-05 reverses D-12. Routing to gap-closure planning.**

---

## Gaps

Hillel's manual parity UAT (2026-06-07) found 5 gaps. The reroute itself works (Browse + ResultDialog
open the Workbench with Visual), but the source UX must be redesigned and two locked decisions reversed.
These feed `/gsd-plan-phase 109 --gaps`.

### G-01 — Hebrew label wrong: חיצוני → חזותי  (severity: low, quick fix)
status: failed
The Hebrew translation for the "Visual" source/label reads **חיצוני** ("external") but must be
**חזותי** ("visual"). Fix in `genizah_translations.py` (the Phase-109 keys added in 109-01). Audit ALL
Phase-109 HE keys for the same mistake.

### G-02 — VS candidate cards must show the transcription text  (severity: medium)
status: failed
VS candidate cards currently show metadata/shelfmark only. They must ALSO display the candidate's
transcription text (like text-source candidate cards do). Affects `CandidateCard` rendering for the
VS / via_vs path in `desktop/join_workbench.py`. Implies the VS adapter (`_normalize_vs_row`, 109-01)
and/or `_load_visual_candidates` (109-02) must carry the candidate `full_text` through, and the card
must render it.

### G-03 — "Search + visual" (Combined) is stuck on "loading" and never renders  (severity: high, bug)
status: failed
Selecting the combined Search+VS view shows a perpetual "loading" state and never displays results.
The Combined assembly path (`_maybe_assemble` combined branch + `_load_visual_candidates` / merge in
109-02) hangs or never resolves. Needs debugging — likely a never-completing fetch, a missing
finished-signal, or an assemble that waits on a text search that was never triggered. (Note: G-04
changes the interaction model, so the fix should land within the new toggle design, not the old
Combined radio.)

### G-04 — Replace the 3-radio source selector with a single "Visual Similarity" toggle button  (severity: high, REDESIGN — supersedes D-10)
status: failed — supersedes locked decision D-10 (Text/Visual/Combined radio source model)
Replace the Text/Visual/Combined radio group with a single **"Visual Similarity" toggle button placed
next to "Find Candidates"**. Required behavior:
- **Toggle ON, search box empty** → show the VS candidates (pure visual).
- **Toggle ON, with a search term** → show only candidates that have BOTH vs AND match the term
  (intersection: search results ∩ VS candidates). "Search will automatically show only those with
  vs+term."
- **Toggle ON after an existing search** → filter the existing results down to the vs∩term intersection
  ("we will see only terms that are in vs candidates").
- **Toggle OFF** → normal text results, but candidates that are also VS look-alikes STILL show the VS
  badge (badge is informational regardless of toggle state).
- **Same behavior in the side-by-side Compare dialog.**
This removes the Combined radio (folds into the toggle ON + search term case) and the no-VS grey-out
(Scenario 5) becomes a disabled/greyed toggle button instead. Re-plan the 109-02 source-selector
internals (`_build_ui` radios, `_on_source_changed`, `apply_source`/`set_source`, `_maybe_assemble`)
around the toggle model. Keep the VS provenance badges.

### G-05 — Wire JoinsDialog pick-mode into the Join Lab + update tooltip  (severity: medium, REVERSES D-12)
status: failed — reverses locked decision D-12 (keep pick-mode on the old standalone dialog)
The JoinsDialog visual partner-picker (pick-mode `_show_vs_dialog` `on_pick` branch) should no longer
open the old standalone orange dialog — reroute it into the new Join Lab / Workbench (in a pick/partner
capacity) and update the button tooltip accordingly. This reverses D-12/SC#2 (which deliberately kept
pick-mode untouched). With pick-mode rerouted, the `_show_vs_dialog` deprecation can extend to the
pick-mode path too (re-evaluate whether the dialog can be fully retired, or what minimal pick surface
the Workbench needs).

### Not-yet-verified (deferred until after gap fixes)
Scenarios 3 (four actions), 4 (reused-window re-anchor), 6 (perf ≥80 look-alikes) were not reached —
re-verify them against the redesigned toggle UX once G-01..G-05 land.

**Next:** `/gsd-plan-phase 109 --gaps` → creates gap-closure plans (G-01..G-05) → `/gsd-execute-phase 109 --gaps-only`.
The `_show_vs_dialog` deprecation marker stays "pending parity sign-off" (NOT live) until a clean re-UAT.
