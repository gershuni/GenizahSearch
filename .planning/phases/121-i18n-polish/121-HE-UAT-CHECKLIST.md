# Phase 121 — Joins Lab HE-Mode RTL UAT Checklist

**Purpose:** Load-bearing SC#2 acceptance gate. Headless render-smoke tests structurally cannot
verify computed-height collapse, clipping/overlap, or visual mirroring correctness (per project
memory `feedback_nicegui_render_smoke_gap`; Phases 119 and 120 both shipped CI-green then
accrued real RTL fixes only during live HE-mode UAT). This checklist is the required human pass.

**Defect triage rule:** Any defect found here that is small (a CSS class, a string key, a
`flex-row-reverse` missing on one element) → fix inline this phase and re-check. Anything
larger (a new layout component needed, a new Supabase schema) → log to `docs/OPEN_ISSUES.md`
or seed per project memory `feedback_seed_midphase_fixes_to_cloud`. SC#2 is met only when
every surface row in the Sign-off block is PASS.

---

## How to Enter HE Mode

1. Start the web app:
   ```
   python -m web.main
   ```
   Opens on port 8080 (or 8081 if 8080 is in use).

2. Open the app in your browser (http://localhost:8080 or :8081).

3. Click the **EN/HE language toggle** in the top bar. The page reloads in Hebrew.
   - Web default language is already `he`; if the UI already shows Hebrew, you are set.
   - If you see English, click the toggle once.

4. Verify the top bar itself shows Hebrew labels (Home = 'בית', Lists = 'רשימות', etc.)
   before proceeding to Joins Lab surfaces.

---

## Surface 1 — Anchor Pane (`anchor_viewer.py`)

**How to reach:** Open `/joins-lab` (or navigate via /search or /browse "Find joins" button
with a fragment). Load an anchor fragment (enter a shelfmark, e.g. `T-S 12.123`, and click
"Load" / "טען").

| # | Check | Result | Notes |
|---|-------|--------|-------|
| A1 | Anchor pane heading ("Anchor" / "עוגן") is in Hebrew | `[ ] PASS / [ ] FAIL` | |
| A2 | Numbered transcription lines are **right-aligned** (text starts from the right side of the pane) | `[ ] PASS / [ ] FAIL` | |
| A3 | Line numbers appear on the **right side** of each transcription line (RTL position) | `[ ] PASS / [ ] FAIL` | |
| A4 | Anchor image and transcription text do **not overlap or clip** each other | `[ ] PASS / [ ] FAIL` | |
| A5 | Anchor pane height is not collapsed (pane has visible height; lines are not hidden under a zero-height container) | `[ ] PASS / [ ] FAIL` | Height-collapse is the #1 headless-invisible defect class |
| A6 | "Load" / "טען" button and shelfmark input are in Hebrew | `[ ] PASS / [ ] FAIL` | |
| A7 | "No anchor loaded" toast / placeholder text shows in Hebrew ('לא נטען עוגן' or similar) if shown | `[ ] PASS / [ ] FAIL` | Shows before an anchor is loaded |

---

## Surface 2 — Query Builder (`joins_builder.py`)

**How to reach:** With an anchor loaded, the query builder rows appear below the anchor pane.
Add a line with the "+" button.

| # | Check | Result | Notes |
|---|-------|--------|-------|
| B1 | Builder rows read **right-to-left** (word boxes flow RTL, first word box is on the right) | `[ ] PASS / [ ] FAIL` | |
| B2 | Per-row **gear icon (⚙)** modifier dialog labels are in Hebrew and laid out RTL | `[ ] PASS / [ ] FAIL` | Click ⚙ on any row to open |
| B3 | Global toggle labels (Variants / Judeo-Arabic / Flexible Spacing / Bidirectional) show in Hebrew | `[ ] PASS / [ ] FAIL` | Toggles at the bottom or top of the builder |
| B4 | Global toggles are **correctly placed** (not pushed off-screen or clipped to one side) | `[ ] PASS / [ ] FAIL` | |
| B5 | Gap field (number-of-words gap between lines) is placed on the correct side (RTL layout) | `[ ] PASS / [ ] FAIL` | |
| B6 | "Search" / "חפש" button and "Clear" / "נקה" button are in Hebrew | `[ ] PASS / [ ] FAIL` | |
| B7 | Syntax legend (operator examples like `#מילה`, `מילה#`, `%מילה`) is visible and not clipped | `[ ] PASS / [ ] FAIL` | These examples are intentionally bilingual-safe |

---

## Surface 3 — Candidate Grid (`candidate_grid.py`)

**How to reach:** Run a search from the builder. Candidate cards should appear.

| # | Check | Result | Notes |
|---|-------|--------|-------|
| C1 | Card shelfmark and library name are in Hebrew where applicable (Hebrew library names) | `[ ] PASS / [ ] FAIL` | |
| C2 | Card title/description is in Hebrew where a Hebrew title exists | `[ ] PASS / [ ] FAIL` | |
| C3 | Triage glyph buttons (Yes ✓ / Maybe ? / No ✗) and browse/compare icons appear in the **correct visual order** for RTL (triage on the left side of the card, reading left-to-right in HE reversed = right-side in LTR) | `[ ] PASS / [ ] FAIL` | |
| C4 | Badge tooltip on hover shows in Hebrew: 'קטע עוגן' (Anchor fragment), 'נמצא דרך הצד השני' (Found via other side), 'דומה חזותית' (Visually similar) — for relevant candidates | `[ ] PASS / [ ] FAIL` | Hover over the badge icon on a candidate card |
| C5 | **Pagination row**: Previous/Next arrows and page count are in the correct left/right position for RTL (`flex-row-reverse` applied — left arrow on right, right arrow on left) | `[ ] PASS / [ ] FAIL` | This is the flex-row-reverse regression guard |
| C6 | Pagination row is **not clipped** (arrows visible, not hidden behind another element) | `[ ] PASS / [ ] FAIL` | |
| C7 | "No candidates" / "אין מועמדים" empty-state message shows in Hebrew if no results | `[ ] PASS / [ ] FAIL` | |

---

## Surface 4 — Candidate Table (`candidate_grid.py`)

**How to reach:** Switch to table view (the table icon / list icon in the candidate grid toolbar).

| # | Check | Result | Notes |
|---|-------|--------|-------|
| D1 | Column **headers** are in Hebrew (Shelfmark / ספר תורה / Library / ספריה / Title / etc.) | `[ ] PASS / [ ] FAIL` | |
| D2 | Column headers are **right-aligned** (RTL table alignment) | `[ ] PASS / [ ] FAIL` | |
| D3 | Table **cells** with text content are right-aligned | `[ ] PASS / [ ] FAIL` | |
| D4 | Sort affordance (sort arrows) is on the **correct side** (right side for RTL column headers) | `[ ] PASS / [ ] FAIL` | |
| D5 | Multi-select **checkboxes** appear on the correct side (left in LTR = right in RTL) | `[ ] PASS / [ ] FAIL` | |
| D6 | Filter row inputs / dropdowns at the top of the table are in Hebrew and RTL | `[ ] PASS / [ ] FAIL` | "Filter by shelfmark…" = placeholder text |
| D7 | Bulk-action bar (appears when rows are selected) shows "Mark N selected as:" in Hebrew | `[ ] PASS / [ ] FAIL` | Select some rows and the bulk bar appears |

---

## Surface 5 — Compare Modal (`compare_modal.py`)

**How to reach:** Click the "Compare" button on any candidate card.

| # | Check | Result | Notes |
|---|-------|--------|-------|
| E1 | Two panes are **mirrored for RTL** — anchor on the expected side, candidate on the other (the modal layout is horizontally appropriate for a Hebrew right-to-left reader) | `[ ] PASS / [ ] FAIL` | Visual mirroring cannot be checked headlessly |
| E2 | **CRITICAL — LTR counter**: The prev/next counter (e.g., '5 / 118') is displayed as '5 / 118' and is **NOT bidi-flipped** to '118 / 5' | `[ ] PASS / [ ] FAIL` | This is an explicit regression guard; '5 / 118' must stay in LTR order inside an RTL context |
| E3 | **Verdict/triage glyph nav bar** (Yes/Maybe/No buttons at the bottom of the modal) is mirrored (`flex-row-reverse` applied — triage buttons on the correct side for RTL) | `[ ] PASS / [ ] FAIL` | flex-row-reverse regression guard |
| E4 | No element in the Compare modal is **clipped** (pane headers, transcription text, buttons all fully visible) | `[ ] PASS / [ ] FAIL` | |
| E5 | **Outer layout scrolls as one** — the modal body scrolls as a single scroll container, NOT two separate inner scrolling boxes side-by-side | `[ ] PASS / [ ] FAIL` | Phase 120 regression guard — inner-scroll-box trap was fixed; must stay fixed in HE mode |
| E6 | "Size mismatch" badge / 'חוסר התאמת גודל' shows in Hebrew on applicable candidates | `[ ] PASS / [ ] FAIL` | |
| E7 | Transcription text in both panes is **right-aligned** | `[ ] PASS / [ ] FAIL` | |
| E8 | Pane height is not collapsed (both panes visible; content not hidden under a zero-height container) | `[ ] PASS / [ ] FAIL` | |

---

## Surface 6 — Known-Joins Group (`known_joins_group.py`)

**How to reach:** With an anchor that has known joins, the known-joins section appears below or alongside the candidate list. Try a well-known joined fragment (e.g., check the anchor's known joins panel if visible).

| # | Check | Result | Notes |
|---|-------|--------|-------|
| F1 | Source badges (PGP / FJMS / user / community) show Hebrew labels where labelled | `[ ] PASS / [ ] FAIL` | |
| F2 | Join chips (fragment labels) are **not clipped** (full shelfmark visible) | `[ ] PASS / [ ] FAIL` | |
| F3 | Known-joins section heading ("Known Joins" / "צירופים ידועים") is in Hebrew | `[ ] PASS / [ ] FAIL` | |
| F4 | RTL layout — chips flow right-to-left | `[ ] PASS / [ ] FAIL` | |

---

## Surface 7 — Dialogs and Toasts

**How to reach:** Trigger each dialog/toast as noted.

| # | Check | Trigger | Result | Notes |
|---|-------|---------|--------|-------|
| G1 | Add-to-List dialog is in Hebrew and RTL | Click "Add to List" on a candidate (requires login) | `[ ] PASS / [ ] FAIL` | |
| G2 | Login dialog (prompted when attempting a logged-in action while anonymous) is in Hebrew and RTL | Attempt an action requiring login while not logged in | `[ ] PASS / [ ] FAIL` | |
| G3 | "No anchor loaded" toast / "לא נטען עוגן" is in Hebrew | Click a bulk-action button before loading an anchor | `[ ] PASS / [ ] FAIL` | |
| G4 | Bulk-puzzle cap notice ("Only the first 20 selected candidates will be added to the Puzzle" / first 20) is in Hebrew | Attempt to add >20 candidates to puzzle | `[ ] PASS / [ ] FAIL` | |
| G5 | "Could not load your lists" error toast is in Hebrew when triggered | (Requires a lists-load failure condition; check if reproducible) | `[ ] PASS / [ ] FAIL` | Skip with note if not easily reproducible |
| G6 | All dialog text is **not clipped** — dialog width accommodates Hebrew RTL text without overflow | Check any dialog above | `[ ] PASS / [ ] FAIL` | |

---

## Surface 8 — Entry Points

**How to reach:** Navigate to each page and check the Joins Lab entry point.

| # | Check | Page | Result | Notes |
|---|-------|------|--------|-------|
| H1 | "Find joins" card on `/search` results shows in Hebrew | `/search` — search for any fragment, check the card's "Find joins" button | `[ ] PASS / [ ] FAIL` | |
| H2 | "Find joins" Quick View panel entry on `/search` is in Hebrew | `/search` — open Quick View for a fragment, check for joins button | `[ ] PASS / [ ] FAIL` | |
| H3 | "Find joins" entry on `/browse` is in Hebrew | `/browse` — open any fragment browse page, look for the joins button | `[ ] PASS / [ ] FAIL` | |
| H4 | **CRITICAL — Drift-fix check**: On `/lists`, the "Open in Joins Lab" button tooltip shows **'פתח במעבדת הצירופים'** (with `הצירופים`, NOT the old `ההצטרפות`) | `/lists` — open any list, hover the Joins Lab button | `[ ] PASS / [ ] FAIL` | This is the glossary drift fix from Plan 01; verify the corrected Hebrew appears |
| H5 | Entry point buttons and labels are **not clipped** on any of the above pages | All three pages above | `[ ] PASS / [ ] FAIL` | |

---

## Sign-off

Fill in after completing all surface checks above. Mark each surface PASS only when all its
line items are PASS (or any FAIL items were fixed inline and re-verified).

| Surface | Result | Notes / Inline Fixes Made |
|---------|--------|--------------------------|
| 1. Anchor Pane | PASS | |
| 2. Query Builder | PASS | |
| 3. Candidate Grid | PASS | String fixes applied: "Add as Join" הוסף כחיבור→הוסף כצירוף (bfc658fa); "View in Browse" צפה בדפדוף→עיין בכתב יד (bfc658fa); gear icon tune→settings to match ⚙ tooltip (1a8c9aca) |
| 4. Candidate Table | PASS | |
| 5. Compare Modal | PASS | Known-join confirm dialog body reworded to a question form (bfc658fa) |
| 6. Known-Joins Group | PASS | |
| 7. Dialogs and Toasts | PASS | |
| 8. Entry Points | PASS | Image-resolution + zoom defect found but is language-independent; deferred → SEED-010 (ea4140f9) |

**Overall sign-off:**

- [x] PASS — All surfaces verified, any inline fixes committed, SC#2 is met.
- [ ] FAIL — One or more surfaces not yet resolved; list open items below.

**Date:** 2026-06-21

**Signed by:** Hillel

**Conditional note:** Overall PASS. The i18n/RTL surfaces are accepted and SC#2 is met. Inline
string and icon fixes were applied during the pass (bfc658fa, 1a8c9aca). An image-resolution +
zoom defect was found (CUDL/Oxford images not resolving consistently; zoom dead when image fails
to load) but is language-independent and was untestable during an NLI outage — explicitly deferred
as SEED-010 (logged ea4140f9; docs/OPEN_ISSUES.md P2 row) and out of i18n scope.

**Open items (if any FAIL):**
- SEED-010: Joins Lab image-resolution + zoom bug (CUDL/Oxford images / zoom on failed load) —
  language-independent defect; deferred to dedicated cloud-branch debug/fix. See docs/OPEN_ISSUES.md.
