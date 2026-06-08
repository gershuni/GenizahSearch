---
phase: 109-visual-similarity-merge-soft-retire
plan: "13"
type: human-uat
status: partial
created: "2026-06-07"
updated: "2026-06-08"
automated_gate: PASSED  # 45 tests: test_join_workbench_vs(27) + test_join_workbench_i18n(5) + test_join_workbench_no_private(2) + test_visual_similarity_dialog(6) + test_join_workbench_construct(2) + test_triage_second_click_clears + test_folio_and_triage_share_one_row + test_vs_hint_and_combined_empty_strings_present + test_browse_resultdialog_vs_buttons_removed + test_joinsdialog_opens_plain_and_closes — all green (2026-06-08)
parity_sign_off: PENDING  # D-14b — Round 4 in progress; marker stays "pending parity sign-off" until Hillel signs off
---

# Phase 109 Plan 13: Parity UAT — Visual Similarity Toggle Design (D-14b Round 4)

This is the CONSOLIDATED round-4 re-UAT. It subsumes the superseded Plan 07 round (round 3
was rejected with 8 change requests G-06..G-13). Plans 08-12 implemented all gap fixes: G-06
eye badge (replacing ★both/⊙VS#rank), G-07 VS buttons removed from Browse + ResultDialog,
G-08 JoinsDialog button rerouted plain + closes dialog, G-09 rank label removed, G-10 triage
undo, G-11 merged folio+triage row, G-12 visibly-ON toggle, G-13 hint + combined empty message.

> **Consolidation note (2026-06-08):** Plan 07's round 3 was REJECTED and superseded by this Plan
> 13 per user decision ("Consolidate — one round"). This single consolidated round is the ONLY
> comprehensive human UAT for the phase. The Scenarios A-M base checklist from rounds 2/3 is
> preserved below (scenarios F, H, I that round 4 does not explicitly repeat are retained for
> reference). The round-4 block (Scenarios A2-A8 + re-verified K/L/M) follows the history section.

**The `_show_vs_dialog` deprecation marker stays "pending parity sign-off" until ALL Round-4
scenarios pass. On Hillel's approval, this file's frontmatter flips to `status: complete` /
`parity_sign_off: APPROVED` — the marker then goes live (D-11/D-14b).**

---

## Round 4 Automated Gate (2026-06-08)

Run before this UAT round. ALL must be green before the human click-through begins.

```
python -m pytest tests/test_join_workbench_vs.py tests/test_join_workbench_i18n.py \
  tests/test_join_workbench_no_private.py tests/test_visual_similarity_dialog.py \
  tests/test_join_workbench_construct.py -q
```

| Test file / suite | Tests | Status | Details |
|-------------------|-------|--------|---------|
| `test_join_workbench_vs.py` | 27 | PASSED | Core parity + toggle/intersection + eye badge + triage toggle + folio-merge + VS buttons removed + JoinsDialog reroute |
| `test_join_workbench_i18n.py` | 5 | PASSED | i18n guard — all gap-round-3 keys (visual similarity, hint, combined empty, link tooltip) in TRANSLATIONS |
| `test_join_workbench_no_private.py` | 2 | PASSED | No `_vs_*` private calls on rerouted paths (D-18) |
| `test_visual_similarity_dialog.py` | 6 | PASSED | Pick-mode dialog tests (D-12 preserved) |
| `test_join_workbench_construct.py` | 2 | PASSED | Window construction — Qt __init__ ordering (Codex LOW fix) |
| **Total** | **45** | **PASSED** | `45 passed in 3.00s` — 2026-06-08 |

---

## Round 4 Scenarios (Human click-through on `python genizah_app.py`)

Hillel checks each box on the live desktop app. Switch to `lang=he` for at least Scenarios
A2/A3/A4/A8 to confirm Hebrew strings use **חזותי** (NOT חיצוני).

---

### Scenario A2 — G-06 Single Eye Badge

**Setup:** Toggle ON; load VS look-alikes (pick an anchor with VS data).

- [ ] CONFIRM: Every visual look-alike card shows a SINGLE eye **👁** badge (NO ★both, NO ⊙VS, NO "#rank")
- [ ] CONFIRM (lang=he): Hovering the eye badge shows tooltip **"דמיון חזותי"** (not "דמיון חיצוני")
- [ ] CONFIRM (lang=en): Hovering the eye badge shows tooltip **"visual similarity"**
- [ ] CONFIRM: Text-only candidates (toggle OFF) are **unbadged** — no eye badge on non-VS cards
- [ ] CONFIRM: Intersection hits (toggle ON + term) carry the eye badge (not ★both)
- [ ] CONFIRM: ⚓self / ⇄other-side badges still appear where applicable and take precedence over the eye

---

### Scenario A3 — G-06.3 + G-12 Toggle Appearance

**Setup:** Open the Joins Workbench. Look at the "Visual Similarity" toggle button.

- [ ] CONFIRM: The toggle label reads **👁 Visual Similarity** (eye prefix on the label)
- [ ] CONFIRM (lang=he): The toggle label reads **👁 דמיון חזותי** (eye + correct HE term, not חיצוני)
- [ ] Toggle ON: CONFIRM the ON state is **UNMISTAKABLE** — heavier/darker border (not just a subtle sunken look), visually distinct from OFF across the Windows theme
- [ ] Toggle OFF: CONFIRM it **visibly returns** to the OFF appearance (no ambiguity between ON and OFF states)

---

### Scenario A4 — G-13 Hint + Combined Empty Message

**Setup:** Open an anchor with VS data. Toggle ON with results showing (pure-VS or intersection).

- [ ] CONFIRM: A distinct, subtly-styled, eye-prefixed hint line appears near the grid reading **"Turn off Visual Similarity to see more results"**
- [ ] CONFIRM (lang=he): The hint reads in Hebrew with **חזותי** (not חיצוני)
- [ ] Toggle OFF: CONFIRM the hint **disappears**
- [ ] Toggle ON again to confirm it reappears
- [ ] Now toggle ON + enter a search term that matches NO VS look-alike for this anchor:
  - [ ] CONFIRM: The combined message **"No look-alikes match this search — turn off Visual Similarity to see all results"** appears
  - [ ] CONFIRM: There is NO bare "0/0 shown" with no other message
  - [ ] CONFIRM: There is NO never-resolving "loading…" spinner

---

### Scenario A5 — G-10 Triage Undo

**Setup:** Toggle ON and load VS candidates. Find a candidate card.

- [ ] Click **Y** on a card: CONFIRM it marks the card as "yes" (visually highlighted)
- [ ] Click **Y** again on the same card: CONFIRM it **clears / undoes** the triage (no triage state)
- [ ] Click **Y** then **N** on the same card: CONFIRM it ends on **N** (not a double-clear)
- [ ] Repeat the toggle pattern for **?** (question): click ?, click ? again to clear
- [ ] Repeat the toggle pattern for **N** (no): click N, click N again to clear

---

### Scenario A6 — G-11 Merged Folio+Triage Row

**Setup:** Toggle ON with candidates loaded. Look at the layout of candidate cards.

- [ ] CONFIRM: Each candidate card shows the folio nav (**◀ p.N ▶** or equivalent) and the **Y/?/N** triage buttons on **ONE row** (folio LEFT, triage RIGHT)
- [ ] CONFIRM: This saves a row of vertical space vs the old layout (two separate rows for folio and triage)
- [ ] CONFIRM: The folio prev/next buttons still **flip the card page** (navigate to a different page of that candidate)

---

### Scenario A7 — G-07 VS Buttons Gone

**Setup:** Open the Browse tab on a VS-bearing manuscript.

- [ ] CONFIRM: There is **NO standalone Visual-Similarity button** (🔬 or equivalent) in the Browse tab — only "Find Joins"
- [ ] CONFIRM: The "Find Joins" button **still opens the Workbench** on that fragment

**Setup:** Open a ResultDialog (search result detail).

- [ ] CONFIRM: There is **NO 🔬 VS button** in the ResultDialog — only "Find Joins"
- [ ] CONFIRM: "Find Joins" in the ResultDialog **still opens the Workbench** on that fragment

---

### Scenario A8 — G-08 JoinsDialog Link Button

**Setup:** Open the Join Lab (JoinsDialog) for an existing join.

- [ ] CONFIRM: The partner-picker button shows a **🔗 link icon**
- [ ] CONFIRM (lang=en): The tooltip reads **"find joins in joins lab"**
- [ ] CONFIRM (lang=he): The tooltip reads the correct HE string with **חזותי** (not חיצוני)
- [ ] Click the link button: CONFIRM the **Join Workbench opens** anchored on Fragment A as a NORMAL (toggle-OFF) browse
- [ ] CONFIRM: The JoinsDialog **closes** after the Workbench opens
- [ ] CONFIRM: The Workbench opened via this path shows **NO "Select as partner" button** on the candidate cards (pick-back retired)
- [ ] CONFIRM: The Workbench opened this way is NOT the old orange VS dialog — it is the standard Join Workbench

---

### Scenario K (re-verify) — Four Actions on VS Cards

**Setup:** Toggle ON and load VS candidates.

- [ ] CONFIRM: **Browse** action opens the candidate in the Browse panel
- [ ] CONFIRM: **Puzzle** action adds the candidate to the Fragment Puzzle
- [ ] CONFIRM: **Add to List** action adds the candidate to a user list
- [ ] CONFIRM: **Add as Join** action opens the JoinsDialog with the candidate pre-filled

---

### Scenario L (re-verify) — Reused-Window Re-Anchor VS Reload

**Setup:** The Workbench is open on a VS-bearing anchor A. Re-anchor to a DIFFERENT VS-bearing fragment B.

- [ ] CONFIRM: After re-anchoring to B, the VS look-alikes **reload for anchor B** (the candidate set changes — it is B's look-alike set, NOT A's stale set)
- [ ] CONFIRM: The anchor pin indicator shows **anchor B's shelfmark** (not A's)
- [ ] CONFIRM: With toggle OFF on the re-anchored window, VS badges on text candidates reflect **anchor B's** VS set (not A's stale set)

---

### Scenario M (re-verify) — Performance: ≥80 Look-alikes

**Setup:** Find and open an anchor with ≥80 VS look-alikes. Toggle ON.

- [ ] CONFIRM: The **first 20-card page renders promptly** (no multi-second hang before any cards appear)
- [ ] CONFIRM: Navigating to the next page stays **responsive** (no per-card serial network stall)
- [ ] CONFIRM: Thumbnails fetch for the visible page only (≤20 at a time), not all 200 look-alikes upfront

---

## Round 4 Summary Table

Record results as you work through the scenarios.

| Scenario | Status | Notes |
|----------|--------|-------|
| A2 — Eye badge (single 👁, no ★both/⊙VS/rank) | | |
| A3 — Toggle eye label + unmistakable ON state | | |
| A4 — Hint line + combined empty message | | |
| A5 — Triage undo (second-click clears) | | |
| A6 — Merged folio+triage row | | |
| A7 — VS buttons gone (Browse + ResultDialog) | | |
| A8 — JoinsDialog link button (🔗, plain open, closes, no pick-back) | | |
| K — Four actions on VS cards | | |
| L — Re-anchor VS reload | | |
| M — Perf: ≥80 look-alikes, first page prompt | | |

---

## Resume Signal

Type **"approved"** if ALL Round-4 scenarios pass — then Task 3 (executor) will:
1. Update this file's frontmatter to `status: complete` / `parity_sign_off: APPROVED`
2. Flip the `_show_vs_dialog` deprecation marker from "pending parity sign-off" to **REMOVABLE
   (signed off)** in `genizah_app.py` — the old method and its orphaned helpers become
   schedulable for physical deletion in the next cleanup phase (retained one more cycle per D-11).

Otherwise, describe the failing scenario(s) by label (e.g., "A4 fails: hint shows when toggle
OFF") — they route to a follow-up gap-closure round and the marker stays "pending parity sign-off".

---

## History — Previous Rounds (audit trail)

### Round 1 (Plan 03 — REJECTED 2026-06-07)

The first parity UAT after initial VS integration. Rejected with 5 gaps (G-01..G-05):
- G-01: Hebrew label חיצוני → חזותי
- G-02: VS candidate cards missing transcription text
- G-03: Combined "Search + visual" perpetually loading (never renders)
- G-04: Radio group replaced by single toggle (D-10 source model superseded)
- G-05: JoinsDialog pick-mode rerouted to Workbench pick-capacity (D-12 reversed)

### Round 2/3 Base Scenarios (Plans 04-06 implemented G-01..G-05 — then REJECTED again 2026-06-07)

Plans 04-06 implemented G-01..G-05. The second round UAT (Plan 07) ran on 2026-06-07 and was
**REJECTED** with 8 further change requests (G-06..G-13). The toggle/intersection mechanics
worked; the rejections were display, layout, entry-point, and affordance refinements.

The Scenarios A-M from round 2/3 are preserved below for reference. Scenarios F, H, and I were
NOT changed by G-06..G-13 and remain verifiable; they are included in the round-4 consolidated
pass implicitly (the new A2-A8 scenarios cover their gap-specific successors).

#### Scenario A — G-01 Hebrew label (round 2/3 baseline)

- [ ] Toggle reads **חזותי** (not חיצוני) in Hebrew mode
- [ ] No occurrence of חיצוני in the VS context

#### Scenario B — Toggle ON, pure VS look-alikes (round 2/3 baseline)

- [ ] VS look-alikes load automatically; paginated 20/page; shelfmarks non-empty

#### Scenario C — Toggle ON + search term (intersection only) (round 2/3 baseline)

- [ ] Only VS∩term candidates appear; text-only excluded; VS-only excluded

#### Scenario D — Toggle ON after existing search (filter down) (round 2/3 baseline)

- [ ] Existing results filter to VS∩term; text-only candidates removed

#### Scenario E — Toggle OFF badge behavior (round 2/3 baseline)

- [ ] Text-only candidates appear; VS look-alikes among text results carry a badge

#### Scenario F — G-02 VS card transcription text (round 2/3 baseline — NOT changed by G-06..G-13)

- [ ] VS candidate cards display the candidate's transcription text
- [ ] Text appears in the card body (may load lazily per page)

#### Scenario G — G-03 empty intersection message (round 2/3 baseline — superseded by A4)

- [ ] No perpetual spinner; no bare "0/0 shown"; correct empty message shown

#### Scenario H — No-VS anchor disabled toggle (D-08) (round 2/3 baseline — NOT changed by G-06..G-13)

- [ ] Toggle is greyed out / disabled for an anchor with no VS data
- [ ] No error or crash when clicking the greyed toggle

#### Scenario I — Compare dialog parity (round 2/3 baseline — NOT changed by G-06..G-13)

- [ ] Compare dialog reflects the toggle-filtered / badged candidate state

#### Scenario J — G-05 pick-mode return (round 2/3 baseline — superseded by A8 in round 4)

> **SUPERSEDED by Scenario A8.** The G-05 behavior (pick-mode Workbench with "Select as
> partner") was **reversed by G-08** (Plans 12). The JoinsDialog button now opens the
> Workbench PLAIN (no pick-back). A8 is the correct round-4 verification of this entry point.

Round 2/3 overall verdict: REJECTED 2026-06-07 — 8 change requests (G-06..G-13). The
toggle/intersection mechanics worked; rejections were display, layout, entry-point refinements.
The `_show_vs_dialog` deprecation marker stayed "pending parity sign-off; normal AND pick callers
rerouted".

**Gap requests from round 3 (all implemented in Plans 08-12):**
- G-06: Single eye badge replaces ★both + ⊙VS#rank; eye on toggle label
- G-07: Remove VS buttons from Browse + ResultDialog
- G-08: JoinsDialog button → 🔗 link, plain open, closes dialog (REVERSES G-05 pick-back)
- G-09: Remove VS rank label (#N)
- G-10: Triage second-click undo
- G-11: Merge folio nav onto triage row
- G-12: Make toggle ON state visually unmistakable
- G-13: "Turn off Visual Similarity" hint + combined empty message

---

## Note on Deprecation Marker

The `_show_vs_dialog` deprecation marker in `genizah_app.py` currently reads:
**"no live caller; both normal-mode (G-07) and pick-mode (G-08) callers gone; pending parity sign-off"**

This marker stays as-is until ALL Round-4 scenarios pass. On Hillel's approval:
1. Update this file's frontmatter: `status: complete` / `parity_sign_off: APPROVED`
2. The `_show_vs_dialog` deprecation marker goes LIVE (D-11/D-14b) — the whole dead cluster
   (`_show_vs_dialog`, `_on_vs_fetch_complete`, `_enrich_vs_suggestions`) becomes removable
   together in the next cleanup phase. The code is **retained** (NOT deleted) in Phase 109.

If any scenario FAILS, describe the failure — it routes to a follow-up gap-closure round (the
marker remains "pending parity sign-off" until a clean re-UAT).
