---
phase: 96
plan: "09"
iteration: 6
subsystem: desktop-local-browse
tags: [line-numbering, view-all, large-file, gutter, qt]
key-files:
  modified:
    - genizah_app.py
    - genizah_translations.py
    - docs/OPEN_ISSUES.md
decisions:
  - "Fix per-page restart by using <p> tags (one per page) instead of <br>-only HTML — ensures each page is a separate QTextBlock matchable by _mark_blocks_for_pages"
  - "Option A (hard cap + warning dialog) chosen for large-file fix over threading/progressive approaches — simpler, ships now, documented as known limitation"
---

# Phase 96 Plan 09: Iteration 6 Summary

**One-liner:** Fixed per-page line restart (root cause: single QTextBlock from `<br>`-only HTML) and added 200-page cap with warning dialog to prevent View All freeze on large files.

## What Was Fixed

### Fix 1 — Per-page line restart NOT working (root cause investigation)

**Root cause found:** The iteration 5 approach rendered all pages as:
```html
<div dir='rtl'>page1_text<br><br>— page 2 —<br><br>page2_text</div>
```
Qt renders this entire `<div>` as a **single QTextBlock**. `_mark_blocks_for_pages` tried to match individual page texts against that one block (which contains the entire document). Since the one block's text never equals any single page's text, every block got `userState=-1` (separator), and **no lines were numbered at all**.

**Fix applied:** Build HTML with one `<p>…</p>` per page, using `<br>` only for within-page line breaks:
```html
<div dir='rtl'>
  <p>page1 line1<br>page1 line2<br>page1 line3</p>
  <p>— page 2 —</p>
  <p>page2 lineA<br>page2 lineB</p>
</div>
```
Qt renders each `<p>` as a separate QTextBlock. `_mark_blocks_for_pages` now matches page N's block against `norm_pages[N]` (the normalized page text). Separator blocks get `userState=-1` → no number. Line numbers restart at 1 per page correctly.

**Verification:** `_normalize_block_text` converts U+2028 (Qt's `<br>` representation inside a block) to `\n`, which matches the stored page texts (`\n`-separated). Confirmed via Python trace that a 3-page file produces 5 blocks (3 content + 2 separators), all matching correctly.

### Fix 2 — View All freezes window on large files

**Problem:** Aggregating all pages of a 1000-page PDF into one QTextEdit on the main thread can freeze the UI for seconds.

**Fix (Option A — hard cap with warning):**
- `_VIEW_ALL_PAGE_CAP = 200`
- If `len(_raw_pages) > 200`: show `QMessageBox.Warning` with:
  - Primary button (default): "Show first 200 pages" — truncates `_raw_pages` and proceeds
  - Cancel button: flips back to per-page mode, renders page 1, returns early
- Hebrew translations added for all 3 new dialog strings

**Rationale for Option A:** Simplest to ship; no threading complexity; documented in OPEN_ISSUES.md as D-F7 with recommended follow-up (background QThread approach to eliminate the cap).

## Files Changed

| File | Change |
|------|--------|
| `genizah_app.py` | Replaced view-all HTML build in `_open_local_browse`: `<br>`-only → `<p>` per page; added `_VIEW_ALL_PAGE_CAP` guard with `QMessageBox` |
| `genizah_translations.py` | 3 new keys: "Large file", "This file has {n} pages...", "Show first {cap} pages" |
| `docs/OPEN_ISSUES.md` | D-F7 entry documenting 200-page cap as known limitation with follow-up path |

## Tests

- `tests/test_local_nav_codex_fix4.py` — 22 tests, all pass
- `tests/test_local_browse_panel.py` — pass
- `tests/test_local_nav_page_chunk.py` — pass
- Full suite: **2569 passed, 23 skipped, 4 xfailed** — green

## Commits

| Hash | Description |
|------|-------------|
| `c6ee39db` | fix(96-09): iter-6 — per-page line restart + large-file View All guard |

## Deviations from Plan

None — both fixes are exactly as specified in the iteration 6 objective. Option A chosen for Fix 2 (explicitly recommended in objective).

## Self-Check: PASSED

- [x] `genizah_app.py` modified — confirmed
- [x] `genizah_translations.py` modified — confirmed
- [x] `docs/OPEN_ISSUES.md` updated — D-F7 entry confirmed
- [x] Commit `c6ee39db` exists — confirmed
- [x] No accidental file deletions in commit
- [x] Full test suite green (2569 passed)
- [x] Ruff clean on all modified files
