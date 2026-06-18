---
status: passed
phase: 117-vertical-spine
source: [117-VERIFICATION.md]
started: 2026-06-17T19:35:00Z
updated: 2026-06-18T03:40:00Z
---

## Current Test

[complete — user approved after interactive UAT]

## Tests

### 1. Anchor image renders with zoom/pan + folio navigation (ANC-01)
expected: Open `/joins-lab?sys_id=<known id>` in a browser without logging in. The fragment image is visible with working zoom/pan controls and functional previous/next folio buttons; images load via the per-provider proxy (never a direct IIIF URL).
result: pass (after fixes). Initial UAT found: (a) ImportError on `service` default resolver, (b) dead zoom (dynamic `add_head_html` script never executed → moved to build-time `inject_viewer_assets()`), (c) RTL-reversed nav arrows, (d) **broken folio navigation** — the stateless core was being called with `p_num=None` every nav so it always restarted from index 0 (advance-once-then-stuck-then-"no more folios"). Fixed by navigating relative to the current folio's `p_num` (mirrors `/browse`), boundary-disable via `current_idx`, LTR control bar. Re-tested by user: `>`/`<` walk all folios and disable at the ends.

### 2. RTL numbered transcription alongside the image (ANC-03)
expected: On the same loaded anchor, the transcription is displayed as right-aligned (RTL) numbered lines next to the image.
result: pass (after fixes). Initial UAT: white-on-cream illegible text (dark-theme inheritance) → forced dark text on the cream transcription panel; also added a shelfmark + library + title info header (was missing — user had no context for which fragment they viewed), localized via `get_language()`.

### 3. End-to-end search → deduped candidate grid (BLD-05, CND-01, CND-02)
expected: On a loaded anchor, type 2–3 Hebrew manuscript lines into the Search-lines textarea and click Run Search. A deduped one-per-image candidate grid renders below the builder within a few seconds, each card showing thumbnail + shelfmark + library chip + title. Rapidly clicking Run Search twice shows only the latest result (latest-wins).
result: pass. Search → compose → execute → dedup → grid works end-to-end. UAT found a render-side crash: a 782-hit common-term search ("פזורה") completed, then rendering all 782 image cards at once dropped the websocket ("Connection Lost" + session reset). Mitigated with a 200-card render cap + truncation notice (`cap_candidates`, commit `cad43f8e`); full pagination/lazy-loading deferred to Phase 119 (JL-UAT6).

### 4. Two anonymous sessions — no cross-session anchor bleed (FND-06 / SC#5)
expected: Open `/joins-lab` in two separate private/incognito windows and load a different anchor in each. Each window independently keeps its own anchor; session A's anchor never appears in session B (and vice versa). Bonus: on a narrow (<640px) viewport the layout stacks to a single column (D-03).
result: pass. RTL good, no bleed, narrow viewport stacks to one column (user-confirmed).

## Summary

total: 4
passed: 4
issues: 0
pending: 0
skipped: 0
blocked: 0

## Gaps

None blocking. Enhancement follow-ups tracked in STATE.md deferred items
(JL-UAT1..6): bigger candidate image + expandable snippet (P119), search
progress bar (P119/120), search-compute Connection-Lost via async-job
pattern (future), stop+partial results (P120), pane-width tuning (P119/121),
candidate pagination/lazy-loading above the interim 200-cap (P119).
