---
status: passed
phase: 118-joins-entry-full-builders
source: [118-VERIFICATION.md]
started: "2026-06-18T08:10:00Z"
updated: "2026-06-19T04:10:00Z"
approved_by: human
approved_date: "2026-06-19"
---

## Current Test

[complete — human approved 2026-06-19]

## Approval

Human gave blanket phase approval ("approved") on 2026-06-19 after an
iterative live-UAT session. The Browse / Quick-View / search-card entry
points (tests 2–4) and the structured + single-line builders (tests 1, 5–7)
were exercised directly; several follow-up fixes were landed and re-tested
during the session (see commits 2c2d68de, 4892a700, 7545f6b1, ca0f46a4,
7392663f, 2d7184e6, 509f517c):
  - tooltip → "Find Joins in the Joins Lab"
  - per-card / bootstrap timer parent_slot storm → asyncio.call_later
  - joins dialog no-joins branch now carries a Lab entry point
  - Browse Phase-B path now passes find_joins_url (entry point restored)
  - Browse→Lab teardown: client-liveness guards (no "Client deleted" spam)
  - joins dialog/count fetches moved off the event loop (run.io_bound)
  - safe_user_get off-context reads downgraded to debug
  - single-line boxes reuse the main /search placeholder

## Tests

### 1. Known-joins group: source badge colors + re-anchor (ANC-04, D-16)
Load `/joins-lab?sys_id=<known-fragment>` with PGP + community joins. Confirm the Known Joins expansion shows source badges with correct colors (PGP blue, FJMS purple, user emerald, community grey). Click the push_pin on a member — confirm the anchor pane reloads to that fragment. Confirm builder rows are preserved after re-anchor.
expected: Source badges render with four distinct colors; re-anchor loads the new fragment in the sticky pane without clearing typed builder rows.
result: pass (human approved 2026-06-19)

### 2. "Find joins" from /search card WITH joins → dialog → new tab (FND-04)
From a `/search` result card for a fragment WITH known joins: click the 'link' icon. Confirm a joins dialog opens with a 'Find more joins' button. Click 'Find more joins' — confirm a new browser tab opens to `/joins-lab` pre-loaded on that fragment.
expected: Dialog appears (not a new tab directly); 'Find more joins' button inside dialog navigates to `/joins-lab` in a NEW tab. Original search page is preserved.
result: pass (human approved 2026-06-19)

### 3. "Find joins" from /search card WITHOUT joins → straight to new tab (FND-04)
From a `/search` result card for a fragment WITHOUT known joins: click the 'link' icon. Confirm no empty dialog appears — clicking goes straight to `/joins-lab` in a NEW tab.
expected: New tab opens directly to `/joins-lab` with sys_id in URL. No near-empty dialog.
result: pass (human approved 2026-06-19)

### 4. "Find joins" on /browse: button tint + new-tab behavior (FND-05)
Open `/browse` on a fragment with known joins. Confirm the joins button color (green when joins, neutral when none). Click it — joins-exist path should show dialog with 'Find more joins'; no-joins path should open `/joins-lab` in a NEW tab.
expected: Button tint correct (var(--primary-600) when joins, neutral otherwise). New-tab behavior works. Original `/browse` context preserved.
result: pass (human approved 2026-06-19)

### 5. Builder auto-collapse → bilingual summary bar → Edit re-expand (BLD-03, WR-04)
Build ≥2 lines in the Joins Lab builder, run search. Confirm the builder collapses to a summary bar showing mode and line count. In the Hebrew UI the summary bar must be in Hebrew (not English). Click 'Edit' — confirm the builder re-expands showing the typed rows.
expected: Summary bar is bilingual (tr() applied). Auto-collapse works. Edit re-expands without losing typed content.
result: pass (human approved 2026-06-19)

### 6. Global toggle wiring end-to-end: flexible spacing → responsa_options (BLD-04)
Toggle 'Flexible spacing' ON in the Advanced options (now shown inline, #1). Run a search. Check server logs or the network inspector to confirm the executed responsa_options contain `flex_spacing=True`.
expected: `flex_spacing` in the search options is True when the toggle is checked.
result: pass (human approved 2026-06-19)

### 7. Word-level builder redesign (#5, Phase 118-06)
Build a query using the new word boxes: type a word, set a modifier on it (e.g. prefix) and confirm the responsa symbol (e.g. `#_`) appears beneath the box with a tooltip. Click '+ Add word' to add a second word with a gap box; click '+ Add line' for a second line; set line-start ⊢ / line-end ⊣ on a line. Run the search.
expected: per-word modifiers render as symbols with tooltips; word gaps and line gaps both take effect; line ⊢/⊣ anchors apply to the line's first/last word; the executed query matches the built structure; no crash on any modifier/gap/add/remove action.
result: pass (human approved 2026-06-19)

## Summary

total: 7
passed: 7
issues: 0
pending: 0
skipped: 0
blocked: 0

## Gaps
