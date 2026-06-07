---
phase: 109-visual-similarity-merge-soft-retire
plan: "07"
type: human-uat
status: partial
created: "2026-06-07"
updated: "2026-06-07"
automated_gate: PASSED  # 36 tests: test_join_workbench_vs + test_join_workbench_i18n + test_join_workbench_no_private + test_visual_similarity_dialog + test_join_workbench_construct — all green
parity_sign_off: PENDING  # D-14b — Hillel must run Scenarios A-M on the live desktop app; not yet signed off
---

# Phase 109 Plan 07: Parity UAT — Visual Similarity Toggle Design (D-14b re-verification)

This is the SECOND round of the parity UAT. The first round (Plan 03) was **REJECTED** with 5 gaps
(G-01..G-05). Plans 04-06 implemented all gap fixes: G-01 Hebrew label, G-02 VS card text, G-03
no-spinner empty state, G-04 single toggle replacing 3 radios, G-05 pick-mode rerouted to Workbench.

The automated gate (Task 1, including `tests/test_join_workbench_construct.py` per the Codex LOW fix)
is GREEN before this manual UAT proceeds.

**The `_show_vs_dialog` deprecation marker stays "pending parity sign-off; normal AND pick callers
rerouted" until ALL scenarios A-M pass. On Hillel's approval, update this file's frontmatter to
`status: complete` / `parity_sign_off: APPROVED` — the marker then goes live (D-11/D-14b).**

---

## Current Test (automated)

Run before this UAT round. ALL must be green before the human click-through begins.

```
python -m pytest tests/test_join_workbench_vs.py tests/test_join_workbench_i18n.py \
  tests/test_join_workbench_no_private.py tests/test_visual_similarity_dialog.py \
  tests/test_join_workbench_construct.py -q
```

| Test file / suite | Tests | Status | Details |
|-------------------|-------|--------|---------|
| `test_join_workbench_vs.py` | 22 | PASSED | Parity (D-14a), toggle/ensure-vs/set_source-pending, intersection, empty-state, re-anchor, pick-callback, invoke-pick |
| `test_join_workbench_i18n.py` | 4 | PASSED | i18n guard — all new tr() keys present in TRANSLATIONS |
| `test_join_workbench_no_private.py` | 2 | PASSED | No `_vs_*` private calls on rerouted paths (D-18) |
| `test_visual_similarity_dialog.py` | 6 | PASSED | Pick-mode dialog tests (D-12 preserved) |
| `test_join_workbench_construct.py` | 2 | PASSED | Window construction — Qt __init__ ordering (LOW fix from Codex review) |
| **Total** | **36** | **PASSED** | `36 passed in 2.91s` — 2026-06-07 |

---

## Tests (Manual Parity UAT — PENDING sign-off)

The following scenarios cover G-01..G-05 (gap fixes from round 1) plus the three scenarios that
were NOT REACHED in the first round (four actions, reused-window re-anchor, perf ≥80). Hillel
checks each box on the live desktop app (`python genizah_app.py`).

---

### Scenario A — G-01 Hebrew label

**Setup:** Switch the desktop UI to Hebrew (`lang=he`). Open the Joins Workbench.

- [ ] CONFIRM: The Visual Similarity toggle button reads **חזותי** ("visual"), NOT חיצוני ("external")
- [ ] CONFIRM: All VS-related status strings also use **חזותי** wherever the visual concept appears
- [ ] CONFIRM: No occurrence of the word **חיצוני** in the Workbench for the visual-similarity context

---

### Scenario B — Toggle ON, empty search box (pure VS)

**Setup:** Pick 3-5 anchors that HAVE VS data (try Browse → "Visual similarity" entry, or open-by-shelfmark). For each:

- [ ] Toggle the "Visual Similarity" button ON with the search builder box empty
- [ ] CONFIRM: The anchor's VS look-alikes load automatically (no text query needed)
- [ ] CONFIRM: Results are paginated 20 cards per page
- [ ] CONFIRM: Every candidate card shows a NON-EMPTY shelfmark (no raw numeric alma_id, no blank)
- [ ] CONFIRM: The sys_id set displayed matches `get_vs_service().get_suggestions(sys_id, 200)` output (same look-alikes, no text-only leakage)

---

### Scenario C — Toggle ON + a search term (intersection)

**Setup:** With the toggle ON, type a search term in the builder and click Find.

- [ ] CONFIRM: ONLY candidates that are BOTH VS look-alikes AND match the text term appear
- [ ] CONFIRM: Text-only candidates (not VS look-alikes) do NOT appear
- [ ] CONFIRM: VS-only candidates (not matching the text term) do NOT appear
- [ ] CONFIRM: Matching candidates carry a **★both** badge marking them as intersection hits

---

### Scenario D — Toggle ON after an existing search (filter down)

**Setup:** With the toggle OFF, run a text search first. Then toggle ON.

- [ ] CONFIRM: The existing results filter down — only the VS∩term intersection remains visible
- [ ] CONFIRM: Text-only candidates that were showing before are removed from the candidate pane
- [ ] CONFIRM: The filtered set shows **★both** badges on the remaining candidates

---

### Scenario E — Toggle OFF badge behavior (HIGH-1)

**Setup:** With the toggle OFF (or on a fresh Workbench session where the toggle was never turned on), run a text search.

- [ ] CONFIRM: Normal text results appear — no VS-only rows in the pane
- [ ] CONFIRM: Among the text results, any candidate that IS also a VS look-alike for the current anchor shows the **★both** or VS badge — even on a FRESH search where the toggle was never turned on (the current anchor's VS is loaded for badging regardless of toggle state)
- [ ] CONFIRM: Candidates that are NOT VS look-alikes show no VS badge

---

### Scenario F — G-02 VS card transcription text

**Setup:** Toggle ON and load VS candidates (Scenario B condition).

- [ ] CONFIRM: VS candidate cards display the candidate's **transcription text** (not metadata or shelfmark only)
- [ ] CONFIRM: The text appears in the candidate card body (may load lazily per page — wait for the page to settle)
- [ ] CONFIRM: Text renders for the visible page cards; subsequent pages load text on navigation

---

### Scenario G — G-03 empty intersection, no spinner (MEDIUM-1)

**Setup:** Toggle ON + enter a search term that you know matches NO VS look-alike for this anchor (e.g., a very specific rare word not likely in the look-alike set). Click Find.

- [ ] CONFIRM: The result pane does NOT spin indefinitely ("loading…" forever)
- [ ] CONFIRM: The result pane does NOT show a bare "0/0 shown" with no other message
- [ ] CONFIRM: The result pane shows the message **"No look-alikes match this search"** (Hebrew: **"אין דומים חזותית התואמים לחיפוש זה"**)

---

### Scenario H — No-VS anchor disabled toggle (D-08)

**Setup:** Pick an anchor with NO VS data (roughly half of all manuscripts have none).

- [ ] CONFIRM: The "Visual Similarity" toggle button is **greyed out / disabled**
- [ ] CONFIRM: The pane stays on text results (not stuck on a disabled VS view)
- [ ] CONFIRM: No error or crash occurs when clicking the greyed toggle

---

### Scenario I — Compare dialog parity

**Setup:** With the toggle ON and VS (or intersection) candidates visible, open the side-by-side Compare dialog.

- [ ] CONFIRM: The Compare dialog reflects the **toggle-filtered / badged candidate state** (shows the same set of candidates visible in the Workbench pane)
- [ ] CONFIRM: If toggle is OFF (text mode), Compare shows the text candidates with their VS badges

---

### Scenario J — G-05 pick-mode return (HIGH-4)

**Setup:** Open a JoinsDialog (Join Lab) for an existing join. Click the partner-picker button (🔍).

- [ ] CONFIRM: The Join Workbench opens — NOT the old orange VS dialog
- [ ] CONFIRM: The Workbench opens **in pick capacity** (pick mode active)
- [ ] CONFIRM: The **FIRST PAGE** of candidate cards already shows a **"Select as partner"** button — the button is present on the first render, NOT only after paging or scrolling (HIGH-4 ordering: callback set before first render)
- [ ] Pick a candidate by clicking "Select as partner"
- [ ] CONFIRM: **Fragment B** in the JoinsDialog is filled with the picked shelfmark
- [ ] CONFIRM: The picker window **closes** after the pick

---

### Scenario K — Four actions on VS candidate cards (NOT REACHED in round 1, re-verify)

**Setup:** With the toggle ON and VS candidates loaded, focus on one VS candidate card.

- [ ] CONFIRM: **Browse** action opens the candidate in the Browse panel
- [ ] CONFIRM: **Puzzle** action adds the candidate to the Fragment Puzzle
- [ ] CONFIRM: **Add to List** action adds the candidate to a user list
- [ ] CONFIRM: **Add as Join** action opens the JoinsDialog with the candidate pre-filled

---

### Scenario L — Reused-window re-anchor reloads VS (NOT REACHED in round 1 — HIGH-2)

**Setup:** The Workbench is already open on a VS-bearing anchor (call it anchor A). Re-anchor the SAME window to a DIFFERENT VS-bearing fragment (call it anchor B). Use either: the candidate card "⚓" re-anchor button on a candidate of anchor A, OR open Browse "Visual similarity" on a different fragment.

- [ ] CONFIRM: After re-anchoring to B, the VS look-alikes **RELOAD for the NEW anchor B** — the candidate set changes (it is B's look-alike set, NOT A's)
- [ ] CONFIRM: The anchor pin indicator in the Workbench shows anchor B's shelfmark (not A's)
- [ ] CONFIRM: With the toggle OFF on the re-anchored window, any VS badges shown on text candidates reflect **anchor B's** VS set (not anchor A's stale set)

---

### Scenario M — Performance: ≥80 look-alikes (NOT REACHED in round 1)

**Setup:** Find and open an anchor with ≥80 VS look-alikes. Toggle ON.

- [ ] CONFIRM: The **first 20-card page renders promptly** (no multi-second hang before any cards appear)
- [ ] CONFIRM: Navigating to the next page stays **responsive** (no per-card serial network stall)
- [ ] CONFIRM: Thumbnails fetch for the visible page only (≤20 at a time), not all 200 look-alikes upfront

---

## Summary

Record results here as you work through the scenarios.

| Scenario | Status | Notes |
|----------|--------|-------|
| A — Hebrew label (חזותי) | PENDING | |
| B — Toggle ON, pure VS look-alikes | PENDING | |
| C — Toggle ON + term (intersection only) | PENDING | |
| D — Toggle ON after existing search (filter down) | PENDING | |
| E — Toggle OFF badge behavior (HIGH-1) | PENDING | |
| F — VS card transcription text (G-02) | PENDING | |
| G — Empty intersection message, no spinner (G-03/MEDIUM-1) | PENDING | |
| H — No-VS anchor greyed toggle (D-08) | PENDING | |
| I — Compare dialog parity | PENDING | |
| J — Pick-mode return, first-page button (G-05/HIGH-4) | PENDING | |
| K — Four actions on VS cards | PENDING | |
| L — Reused-window re-anchor VS reload (HIGH-2) | PENDING | |
| M — Perf: ≥80 look-alikes, first page prompt | PENDING | |

**Overall verdict:** PENDING — awaiting human sign-off.

---

## Note on Deprecation Marker

The `_show_vs_dialog` deprecation marker in `genizah_app.py` currently reads:
**"pending parity sign-off; normal AND pick callers rerouted"**

This marker stays exactly as-is until ALL Scenarios A-M pass. On Hillel's approval:
1. Update this file's frontmatter: `status: complete` / `parity_sign_off: APPROVED`
2. The `_show_vs_dialog` deprecation marker goes live (D-11/D-14b) — the old method can be
   scheduled for removal in the next cleanup phase.

If any scenario FAILS, describe the failure — it routes to a follow-up gap-closure round (the
marker remains "pending parity sign-off" until a clean re-UAT).

---

## Resume Signal

Type **"approved"** if ALL Scenarios A-M pass — then the executor will update this file's
frontmatter to `status: complete` / `parity_sign_off: APPROVED` and the `_show_vs_dialog`
deprecation marker goes live.

Otherwise, describe the failing scenario(s) by letter — they will route to a follow-up
gap-closure round.
