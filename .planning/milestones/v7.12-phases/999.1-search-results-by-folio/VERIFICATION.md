---
phase: 999.1-search-results-by-folio
verified: 2026-05-18T20:45:00Z
status: passed
score: 10/10 must-haves verified
re_verification:
  previous_status: none
  previous_score: n/a
  gaps_closed: []
  gaps_remaining: []
  regressions: []
---

# Phase 999.1: Search results by folio — Verification Report

**Phase Goal:** Surface `result['display']['img']` (page/image number) on every web `/search` result card after the shelfmark, with desktop COL_IMG parity. Single render-site insertion, no plumbing.
**Verified:** 2026-05-18 (initial)
**Status:** PASSED

## Goal-Backward Truths

| #  | Truth | Status | Evidence |
|----|-------|--------|----------|
| 1  | Chip rendered when truthy (`display.get('img')` + `if _img_num:` + `ui.label(str(...))` + chip classes + bg/color style + tooltip) | PASS | `web/pages/search_results.py:477-481` — `_img_num = display.get('img')` (477) / `if _img_num:` (478) / `ui.label(str(_img_num)).classes('text-xs px-2 py-0.5 rounded shrink-0').style('background: var(--bg-tertiary); color: var(--text-muted);').tooltip(tr('Image number'))` (479-481) |
| 2  | Placement immediately after shelfmark label at :468, inside same `ui.row()` opened at :385 | PASS | Insertion at L469-481 directly below `ui.label(shelfmark)...` at L468; matching indentation (20-space leading whitespace identical to the shelfmark line); same enclosing `with ui.row().classes('items-center gap-2 flex-wrap'):` opened at L385 |
| 3  | Left-cluster pills (:386-428) byte-identical to pre-phase | PASS | `git diff 75cd8a45..HEAD -- web/pages/search_results.py` shows ONLY one hunk @ L466+13 (the new block); no other lines modified anywhere in the file |
| 4  | Quick View `display.get('img', ...)` sites preserved, shifted by +12-13 lines | PASS | Now at `:896` (`page_num_str = display.get('img', '1')`) and `:1120` (`page_num = display.get('img', '')`) — previously :883 / :1107; shift = +13 / +13 = matches block size. The other pre-existing uses also shifted: :634 → :647 |
| 5  | Translation entry `'Image number': 'מספר תמונה'` in TRANSLATIONS dict | PASS | `genizah_translations.py:3186-3187` — header comment `# Phase 999.1 (FOLIO-01): tooltip for the per-card image-number chip after the shelfmark` followed by the Hebrew entry. Also present in the upper case-style dict at L2144 (`"Image number": "מספר תמונה"`) — both lookup paths return Hebrew |
| 6  | No new imports added | PASS | `git diff 75cd8a45..HEAD -- web/pages/search_results.py \| grep -E '^[+-](from \|import )'` returns empty. `tr` was already imported at L19 (`from web.translations import tr, is_rtl, get_language`) |
| 7  | FOLIO-01 marked Complete in REQUIREMENTS.md (checkbox + traceability) | PASS | `.planning/REQUIREMENTS.md:90` — `- [x] **FOLIO-01**: Surface result['display']['img'] ... Descriptive tooltip tr('Image number') / "מספר תמונה" added post-smoke-check per D-05 revision 2026-05-18.` AND `:148` — `\| FOLIO-01 \| Phase 999.1 (backlog) \| Complete \|` traceability row |
| 8  | ROADMAP.md Phase 999.1 entry flipped to SHIPPED with commit hashes | PASS | `.planning/ROADMAP.md:376` — `### Phase 999.1: Search results by folio (BACKLOG — SHIPPED 2026-05-18)` + `:383` — `- [x] 999.1-01-PLAN.md — ... — commits \`8368a962\` + \`9db7b18e\`` |
| 9  | STATE.md counters incremented (8→9 phases, 25→26 plans) | PASS | `.planning/STATE.md:11-13` — `completed_phases: 9` / `total_plans: 33` / `completed_plans: 26`; `last_activity: 2026-05-18 -- Phase 999.1 (backlog) complete -- FOLIO-01 shipped (commits 8368a962 + 9db7b18e)` |
| 10 | Desktop app (`genizah_app.py`) untouched | PASS | `git diff 75cd8a45..HEAD -- genizah_app.py` returns empty. COL_IMG render at :5582 / :5591 / :16111 unchanged |

**Score:** 10/10 truths verified

## Diff Summary

`git diff 75cd8a45..HEAD -- web/pages/search_results.py`:
- **Single hunk** at `@@ -466,6 +466,19 @@`: +13 / -0 lines.
- The insertion is exactly the 7 doc-comment lines + 4-line `if _img_num:` block + 1-line tooltip-chained `ui.label` continuation that the PLAN prescribed.
- Phase commits: `8368a962` (Task 1 chip insertion without tooltip) → `9db7b18e` (Task 2 revision: `.tooltip(tr('Image number'))` chained + Hebrew translation entry) → `f6c8c73a` (docs closeout).

## Follow-up Notes

- **Desktop parity NOT formally smoke-checked side-by-side.** Per the PLAN's Task 2 step 5, the desktop parity spot-check was OPTIONAL ("optional but recommended"); Hillel skipped it during the 8-step smoke run. This is acceptable: web reads `result['display']['img']` from the same `genizah_core` search pipeline that populates desktop's `meta['img']` at `genizah_app.py:16111`, so the values are structurally identical by data-source identity. The chip displays exactly what the desktop "Img" column shows.
- **D-05 revised in place after smoke check.** Original PLAN forbade tooltips (`AC-7: no .on(, on_click=, ui.link(`); the post-approval revision added `.tooltip(tr('Image number'))` per Hillel's explicit request. `.tooltip()` is not a click handler and does not violate AC-7's surface ban. SUMMARY documents this clearly at lines 56-67.
- **No new pytest added.** Justified in the PLAN `<verification>` section: this is a 1-block NiceGUI render addition with `display` already in scope; standing up a playwright/selenium harness for this scope is a cost/benefit failure. Visual smoke check (Task 2) was the right verification tier.
- **All AC-1..AC-10 from PLAN already recorded as PASS** in `999.1-01-SUMMARY.md`. This goal-backward verification re-confirmed them against the live file state, not the SUMMARY's claims.

---

*Verified: 2026-05-18*
*Verifier: Claude (gsd-verifier)*
