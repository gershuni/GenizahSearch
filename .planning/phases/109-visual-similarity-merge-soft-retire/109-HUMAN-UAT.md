---
phase: 109-visual-similarity-merge-soft-retire
plan: "13"
type: human-uat
status: complete
created: "2026-06-07"
updated: "2026-06-08"
automated_gate: PASSED  # final gate 59 tests green (incl. round-4 crash/eye/zoom/text/glyph/nav/session regression tests); see Round-4 Findings Log
parity_sign_off: APPROVED  # D-14b — Hillel approved round 4 on 2026-06-08 (F-R4-1..F-R4-6 all fixed + re-verified); _show_vs_dialog marker now live-removable
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

## Round 4 Findings Log

### F-R4-1 — CRASH on toggle Visual Similarity OFF after a search — FIXED, re-test required

- **Reported (2026-06-08, Hillel):** "Clicking off VS after search caused crash. Process finished
  with exit code -1073740791 (0xC0000409)." Relates to Scenario E (toggle-OFF path) / A4.
- **Root cause:** `JoinCandidatePane._start_enrich` tore down the previous `_EnrichWorker` (a
  `QThread`) with `self._enrich_worker = None` immediately after `cancel()`. `cancel()` only sets a
  flag — `run()`'s in-flight measurement SQL batch keeps executing. Dropping the only Python
  reference let CPython refcounting destroy the C++ QThread mid-run → Qt
  *"QThread: Destroyed while thread is still running"* → `abort()` → Windows `0xC0000409`
  (MSVC fastfail). Toggling OFF right after a search hits the window where the search's enrich
  worker is still running.
- **Fix (commit `bf0a6353`):** extracted `_retire_enrich_worker()` — cancel + disconnect the stale
  `enriched` signal, then **retain** a still-running worker in `self._retired_workers` and reap it on
  its `finished()` signal (delivered on the UI thread) instead of dropping it; an already-finished
  worker is released immediately. `_start_enrich` now calls `_retire_enrich_worker()`.
- **Regression guard:** 5 headless tests in `tests/test_join_workbench_vs.py` (running-worker
  retained, finished-worker released, None no-op, reaper releases, static `_start_enrich` guard).
- **Status:** automated gate **50 passed** (was 45), ruff clean.

### F-R4-2 — Eye badge + triage missing from Compare and Table views — FIXED, re-test required

- **Reported (2026-06-08, Hillel), 3 items:** (1) the 👁 eye badge should also appear in the
  Compare window; (2) the Y/?/N triage toggle should also work in the Compare window; (3) the 👁
  eye badge should also appear in Table mode. The round-3 work (G-06 eye, G-10 triage) had only
  been applied to the grid **card** view.
- **Root cause:** the eye badge and triage state were card-only. Compare's `_fill_candidate` set a
  bare shelfmark; Table's `_render_table` set a bare shelfmark; Compare's Y/?/N buttons existed but
  `_mark()` re-coloured the border with the clicked value *after* `paint()`, so a G-10 second-click
  toggle-OFF left the border stuck (looked like the toggle didn't work).
- **Fix (commit `26a57088`):**
  - New shared pure helper `_candidate_shelf_badge(c)` (👁 + "visual similarity" tooltip; ⚓self /
    ⇄other-side precedence). **Compare** `_fill_candidate` and **Table** `_render_table` now badge
    the shelfmark through it (eye + tooltip in both). The card keeps its inline copy (pinned test).
  - Compare `_mark()` no longer overrides `paint()` — `paint()` re-reads the actual (possibly
    toggled-off) triage and restyles the border, so Y/?/N now toggle correctly in Compare.
- **Regression guard:** 4 headless tests (helper eye + precedence + text-only-unbadged; table &
  compare use the helper; compare `_mark` toggle-off honored).
- **Status:** automated gate **54 passed** (was 50), ruff clean.

> **Re-test focus for round 4:** the marker stays **PENDING** until Hillel re-verifies on the
> rebuilt app: (a) toggle VS OFF after a search — no crash (F-R4-1); (b) 👁 badge shows in the
> Compare candidate pane AND in Table mode; (c) Y/?/N in the Compare window mark AND clear on a
> second click (border + position label follow); plus the rest of Round 4 (A2–A8, K/L/M).

### F-R4-3 — Compare window: zoom, anchor/candidate text, glyphs, nav width — FIXED, re-test required

- **Reported (2026-06-08, Hillel), 5 items in the Compare window:** (1) zoom in/out does nothing;
  (2) no transcription below the anchor; (3) the text below the candidate ("other side") is the
  WHOLE manuscript, not the matched page/image; (4) make the Y/?/N triage glyphs ✓/?/✗;
  (5) the prev/next buttons are too narrow for their labels.
- **Fixes (commit `453bbcf1`):**
  1. **Zoom** — `_pane_zoom` re-fetched a larger image but `_pump_images` downscaled it back to the
     label, so zoom was invisible. Reworked to **client-side** scaling of a cached full pixmap
     (mirrors the main anchor `_apply_zoom`): the pane image now sits in a `_PannableScrollArea`
     (drag to pan); a new opt-in `on_pixmap` hook delivers the full pixmap so it fits-to-view on
     load and scales by zoom on each click (no network).
  2. **Anchor text** — `_fill_anchor` now fetches the matched-page transcription via a shared
     `_load_pane_page_text` (background worker), like folio nav.
  3. **Candidate text** — `_fill_candidate` no longer dumps `c.full_text` (whole MS); it loads the
     matched-PAGE text via the same helper.
  4. **Glyphs** — ✓ / ? / ✗ on the card + compare triage buttons and the table glyph map.
  5. **Nav width** — Compare prev/next widened (fixed 34px → `setMinimumWidth(84)`).
- **Regression guard:** 4 headless tests (client-side zoom math + label resize; page-scoped text in
  both panes; ✓/✗ glyphs; wider nav). `_pane_folio_step` shares the same helpers.
- **Status:** automated gate **58 passed** (was 54), 200 passed across join-workbench/compare/
  candidate suites, ruff clean.

> **Re-test focus (round 4, cumulative):** marker stays **PENDING** until Hillel confirms on the
> rebuilt app — in the **Compare** window: zoom +/- visibly enlarges/shrinks the image (drag to pan
> when zoomed); the anchor pane shows transcription; the candidate pane shows only the matched
> page's text (not the whole MS); triage reads ✓/?/✗ and toggles on second click; prev/next labels
> fit — plus the earlier F-R4-1/F-R4-2 items and the rest of Round 4 (A2–A8, K/L/M).

### F-R4-4 — Compare nav arrows must point outward (bidi mirroring) — FIXED, re-test required

- **Reported (2026-06-08, Hillel, with screenshot):** both Compare nav arrows rendered as `>`
  (Qt bidi-mirrors `<`/`>` inside the RTL buttons). The arrows should point to the OUTER edges —
  prev (right button) `>` on the right, next (left button) `<` on the left — with `הקודם`/`הבא`
  inner.
- **Fix (commit `9e1113e1`):** the toolbar stays RTL (prev right / next left) but each nav button
  is forced to LTR internal layout (`setLayoutDirection(LeftToRight)`) so the brackets render
  literally; visual-order HE strings `הקודם>` (arrow trails, outer-right) and `<הבא` (arrow leads,
  outer-left). English keys unchanged. Gate: 59 passed.

### F-R4-5 — Join Lab state not remembered on close/reopen or across restart — FIXED, re-test required

- **Reported (2026-06-08, Hillel):** the Join Lab state (anchor/builders/triage) is not remembered
  upon closing the Join Lab window, nor across restore sessions. "It was before." (Also: the
  Compare next-button arrow should sit after the word — `הבא>`; prev stays `<הקודם`.)
- **Root cause (session persistence):** `open_join_workbench` / `open_joins_workbench` recreated
  the window whenever it was merely HIDDEN (`or not self._join_workbench.isVisible()`), discarding
  the in-memory instance. Closing then reopening built a fresh EMPTY window, and `_save_session`
  then serialized that empty window at app exit — so state was lost both on reopen and across
  restart, defeating the Phase-108 Feature-7 persistence. (Pre-existing since Phase 107, surfaced
  by heavy close/reopen during UAT.)
- **Fix (commit `e94f6540`):** honor the documented D-02 "single reusable instance" — create the
  window only when the instance is `None`; a hidden window is re-shown with its state intact. Disk
  `restore_state` is gated to a freshly-created window so a reused instance is not clobbered. Plus
  the Compare next arrow → `הבא>`.
- **Regression guard (static):** the recreate-on-hidden anti-pattern is pinned out and the
  fresh-only restore asserted. ruff clean; gate 54 passed; 6 Qt construct tests pass offscreen.
- **Re-test:** open Join Lab, set an anchor + some triage, CLOSE the window, reopen (corner button)
  → state is there; also confirm it survives an app restart (open Join Lab again after relaunch).
- **Within-session: CONFIRMED by Hillel.** Restart still failed → see F-R4-6.

### F-R4-6 — Join Lab state didn't survive app restart (jw-None save wiped it) — FIXED, re-test required

- **Reported (2026-06-08, Hillel):** after the F-R4-5 fix, state is remembered within a session but
  still does not survive an app restart.
- **Diagnosis:** the on-disk `session.json` DID contain the correct `join_lab` anchor after exit
  (verified against the live file: `T-S 8.242`, `open:false`), and `restore_state` rebuilds from it
  correctly (probe confirmed `_anchor_sid` restored). The break: a background `_save_session` firing
  AFTER startup-restore completes — while the Join Lab window is not yet instantiated (`jw is None`)
  — rebuilt the session dict WITHOUT a `join_lab` key, silently wiping the remembered anchor before
  the user reopened the Lab. The `_restoring_session` guard only covers the restore window, not the
  post-restore/pre-open gap.
- **Fix (commit `6c52a3b9`):** in `_save_session`, when `jw is None`, carry forward the
  previously-persisted `join_lab` from disk instead of dropping the key. Manual reopen
  (`open_join_workbench`) then restores it; if the window was open at exit, `_restore_join_lab`
  auto-reopens it. Static regression guard added; ruff clean; gate 55 passed.
- **Re-test:** set an anchor in Join Lab, fully quit + relaunch the app, open the Join Lab → the
  last anchor/builder/triage is restored.

---

## Round 4 Verdict — APPROVED (2026-06-08)

Hillel approved the consolidated round-4 UAT. All round-4 findings fixed and re-verified:
F-R4-1 (toggle-off crash), F-R4-2 (eye badge in Compare + Table), F-R4-3 (Compare zoom / page-text /
✓?✗ glyphs / nav width), F-R4-4 (nav arrows), F-R4-5 (Join Lab state within-session), F-R4-6 (Join
Lab state across restart). `_show_vs_dialog` deprecation marker is now **live-removable** (D-11/D-14b)
— the method + its orphaned helpers (`_on_vs_fetch_complete`, `_enrich_vs_suggestions`) are retained
one cycle and scheduled for physical deletion in a future cleanup phase. `status: complete` /
`parity_sign_off: APPROVED`.

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
