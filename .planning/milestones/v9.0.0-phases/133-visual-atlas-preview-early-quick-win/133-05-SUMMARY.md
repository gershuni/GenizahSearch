---
phase: 133-visual-atlas-preview-early-quick-win
plan: 05
subsystem: web
tags: [atlas, feature-flag, nicegui, i18n, cls, homepage]

# Dependency graph
requires:
  - phase: 133-01
    provides: "scripts/check_atlas_masking.py (D-07 masking scan)"
  - phase: 133-03
    provides: "web/atlas_assets.atlas_preview_available() (the single flag+readiness predicate) + the pre-registered homepage-teaser translation keys in genizah_translations.py"
provides:
  - "Predicate-gated claim-free Connections Atlas teaser card in the homepage Main Action Cards Grid (web/pages/home.py), linking to /atlas"
  - "tests/render_smoke/test_home_teaser_render_smoke.py — the fourth-surface half of the MEDIUM-6 predicate coverage (page/nav/data routes covered by 133-03's tests/test_atlas_flag_gating.py)"
affects: [133-06]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Fourth-surface single-predicate gating: the homepage teaser imports and gates on the SAME atlas_preview_available() the /atlas page route, its nav link, and both data routes use — a flag-ON/asset-missing window hides cleanly everywhere, including now the homepage"
    - "Static CLS-safe teaser card forked from the existing Community Card shape (gradient header + body + badge row) — no async fetch, no layout shift"
    - "Element .mark('atlas-teaser-card') as a stable test locator for render-smoke assertions (matches existing repo convention: compare-candidate-pane, compare-flip-counter, stop_search_btn, anchor-viewer-image-pane)"

key-files:
  created:
    - tests/render_smoke/test_home_teaser_render_smoke.py
  modified:
    - web/pages/home.py

key-decisions:
  - "Gated on atlas_preview_available() (imported directly from web.atlas_assets), not the bare ATLAS_PREVIEW_ENABLED flag — required by MEDIUM-6 so a flag-ON/asset-missing window never advertises a broken /atlas link from a fourth surface"
  - "Reused the two 133-03-pre-registered teaser keys verbatim ('Explore the Connections Atlas' as the CTA subtitle, the claim-free description one-liner) plus the existing 'Connections Atlas' and 'Beta' keys already used by the nav/page chrome — no new translation keys were needed, so genizah_translations.py was never touched by this plan"
  - "Render-smoke test drives the REAL atlas_preview_available() predicate via monkeypatching web.atlas_assets module state (ATLAS_PREVIEW_ENABLED + _state), not a return_value mock of the predicate itself — proves the teaser reads the same live predicate the other three surfaces use"
  - "HTML content from ui.html-based elements (e.g. the h3 SemanticHeading wrapper) is tag-stripped before the claim-free/digit scan in the test, so a digit inside a tag name (the '3' in '<h3>') doesn't produce a false positive"
  - "HE forbidden-substring list narrowed to 'תגליות' (discoveries) only — the approved 133-03 Hebew description itself reads '...ללא טענות זיהוי' ('...without identification claims'), so 'זיהוי' legitimately appears there as part of the honesty NEGATION, not as a claim; the test instead positively asserts the negation phrase 'ללא טענות' is present"

requirements-completed: [ATLAS-01]

# Metrics
duration: 45min
completed: 2026-07-21
---

# Phase 133 Plan 05: Homepage Teaser Summary

**Claim-free, predicate-gated Connections Atlas teaser card added to the homepage Main Action Cards Grid, forked from the existing static Community Card shape and gated on the same `atlas_preview_available()` predicate the `/atlas` page, nav link, and data routes already share.**

## Performance

- **Duration:** ~45 min
- **Completed:** 2026-07-21
- **Tasks:** 2/2
- **Files modified:** 2 (1 modified: web/pages/home.py; 1 created: tests/render_smoke/test_home_teaser_render_smoke.py)

## Accomplishments

- **Homepage teaser card (`web/pages/home.py`).** Imported `atlas_preview_available` from `web.atlas_assets` and added a small static card to the Main Action Cards Grid (after the Community Card), wrapped in `if atlas_preview_available():` — the SAME predicate as the `/atlas` page route, its nav link, and both data routes (MEDIUM-6), so a flag-ON/asset-missing window can never advertise a broken link from this fourth surface either. The card navigates to `/atlas` on click, keydown.enter, and keydown.space (matching every other homepage card's pattern), carries a teal/cyan gradient header (icon `hub` + `tr('Connections Atlas')` title + `tr('Explore the Connections Atlas')` subtitle), a claim-free body description, and a `tr('Beta')` badge. It is a fully static card — no async data fetch, so it is CLS-safe by construction (same as the Community/Search/Parallels cards it was forked from). Carries a `.mark('atlas-teaser-card')` test locator.
- **No new translation keys needed.** All strings the card uses (`Connections Atlas`, `Beta`, `Explore the Connections Atlas`, and the claim-free description) were already pre-registered with real Hebrew values by plan 133-03 — `genizah_translations.py` was never staged or touched by this plan.
- **Render-smoke test (`tests/render_smoke/test_home_teaser_render_smoke.py`, 4 tests).** Drives the live NiceGUI `/` render path via a `User` over `httpx.ASGITransport`, monkeypatching the real `web.atlas_assets` module state (not a hand-wired stand-in) so the test proves the SAME predicate is read:
  - **available (EN):** the marked card renders, is claim-free (no digit anywhere in its text; none of `identification` / `discoveries found` / `discovery`), carries the `Beta` badge, and clicking it calls `ui.navigate.to('/atlas')`.
  - **available (HE):** the card renders with REAL Hebrew values (`בטא`, `אטלס החיבורים`) — not leaked English tr() keys — is still claim-free (no digits, no `תגליות`/"discoveries"), and carries the honesty-negation phrase `ללא טענות` ("without claims").
  - **unavailable — flag OFF:** the card is absent; the rest of the homepage (e.g. the Text Search card) still renders normally.
  - **unavailable — flag ON but asset not loaded:** the card is absent too (this is the MEDIUM-6 case the plan calls out by name) — confirming the gate is `atlas_preview_available()` (flag AND loaded asset), not the bare flag.
- **Full render-smoke suite regression-clean.** Ran the entire `tests/render_smoke/` package together (41 passed, 1 skipped) to rule out cross-test NiceGUI global-state pollution from the new file; also reran `tests/test_atlas_flag_gating.py` (24 passed) and the existing `tests/render_smoke/test_atlas_render_smoke.py` (2 passed) to confirm the `web/pages/home.py` import change didn't regress the 133-03/133-04 atlas surfaces.

## Task Commits

Each task committed atomically with explicit-path staging (never `git add -A`):

1. **Task 1: Predicate-gated claim-free atlas teaser card in the homepage grid** — `dc46143d` (feat)
2. **Task 2: Homepage teaser render-smoke test** — `0bb7d63d` (test)

**Plan metadata:** (this SUMMARY + STATE/ROADMAP update) — see final docs commit.

## Files Created/Modified

- `web/pages/home.py` — import `atlas_preview_available`; new predicate-gated teaser card in the Main Action Cards Grid
- `tests/render_smoke/test_home_teaser_render_smoke.py` (created) — 4 render-smoke tests (available EN, available HE, absent-flag-OFF, absent-flag-ON-asset-not-loaded)

## Decisions Made

See `key-decisions` in the frontmatter (predicate import choice, key reuse, monkeypatch-the-real-state test strategy, HTML tag-stripping for the claim-free scan, and the HE forbidden-word narrowing). All are implementation-detail choices within the plan's RESOLVED scope — no architectural changes.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Tag-stripped HTML content before the render-smoke claim-free/digit scan**
- **Found during:** Task 2 (first test run)
- **Issue:** The test's `_descendant_texts()` helper collected both the clean `.text` property AND the raw `.content` HTML string from `ui.html`-based elements (the `h3` `SemanticHeading` wrapper in `web/components/typography.py` renders as `<h3 class="...">...</h3>`). The literal tag name `h3` contains the digit `3`, so the naive "no digit anywhere in the card's text" assertion false-failed on markup, not actual displayed content.
- **Fix:** Added a `_TAG_RE = re.compile(r'<[^>]+>')` strip step before appending `.content` strings in `_descendant_texts()`, so only the rendered inner text is scanned.
- **Files modified:** tests/render_smoke/test_home_teaser_render_smoke.py
- **Verification:** All 4 tests pass; the fix is test-only (no production code change).
- **Committed in:** `0bb7d63d` (Task 2 commit — the file was authored with the fix already applied before its first commit)

**2. [Rule 1 - Bug] Narrowed the HE forbidden-claim-word list to avoid a false positive on the approved negation phrasing**
- **Found during:** Task 2 (first test run, HE case)
- **Issue:** The initial HE forbidden-substring list included the bare word `זיהוי` ("identification"). The 133-03-approved Hebrew teaser description itself reads `...סקירה אלגוריתמית, ללא טענות זיהוי` ("...an algorithmic overview, WITHOUT identification claims") — `זיהוי` legitimately appears there as part of the honesty NEGATION (exactly the claim-free framing the plan requires), not as an actual claim. The naive substring check couldn't distinguish negation from assertion.
- **Fix:** Narrowed `_FORBIDDEN_SUBSTRINGS_HE` to `('תגליות',)` (discoveries — the actual Pitfall #8 name-collision concern) and added a positive assertion that the negation phrase `ללא טענות` ("without claims") is present, which better captures the plan's actual claim-free intent for Hebrew than a naive keyword blocklist.
- **Files modified:** tests/render_smoke/test_home_teaser_render_smoke.py
- **Verification:** All 4 tests pass; `genizah_translations.py` (the actual approved translation) was not modified.
- **Committed in:** `0bb7d63d` (Task 2 commit)

---

**Total deviations:** 2 auto-fixed (both Rule 1, test-only bugs found while authoring the new test — no production code changes, no plan scope creep).
**Impact on plan:** None on the shipped feature. Both fixes are internal to the new test file's assertion logic; the teaser card in `web/pages/home.py` matches the plan exactly.

## Issues Encountered

None beyond the two auto-fixed test-assertion bugs documented above.

## Masking / Staging Discipline (binding phase rule, honored)

- Ran `python scripts/check_atlas_masking.py --scan-repo` (with `MASKING_SCAN_PATTERNS_FILE` exported) before **each** of the two task commits — both exited 0 ("no matches — clean").
- `genizah_translations.py`, `web/main.py`, and `web/pages/browse.py` (all pre-existing dirty files unrelated to this plan) were NEVER staged or touched. `git diff --cached` was inspected before each commit and contained ONLY this plan's own file (Task 1: `web/pages/home.py`; Task 2: the new test file).
- No `git add -A` / `git add .` / `git commit -a` used at any point — explicit-path staging only.

## Known Stubs

None. The teaser card is fully wired: real predicate gate, real navigation target, real (pre-registered) translated strings.

## User Setup Required

None. The teaser is gated by the existing `ATLAS_PREVIEW_ENABLED` flag (default OFF) plus the same asset-load state as the `/atlas` page — no new environment variables or manual configuration.

## Next Phase Readiness

- **133-06** (deploy checkpoint) can now include the homepage teaser in its live 4-surface smoke: with the flag ON and the real baked asset present, `/` should show the teaser linking to `/atlas`, and the PHASE-EXIT masking scan (133-06's own step) should cover the rendered `/` HTML alongside `/atlas`.
- No blockers. All four surfaces (page route, nav link, both data routes from 133-03, plus this homepage teaser) now share the single `atlas_preview_available()` predicate, closing MEDIUM-6 end-to-end.

## Self-Check: PASSED

- FOUND: `web/pages/home.py` (modified, contains `atlas_preview_available(` and `navigate.to('/atlas')`)
- FOUND: `tests/render_smoke/test_home_teaser_render_smoke.py`
- FOUND: `.planning/phases/133-visual-atlas-preview-early-quick-win/133-05-SUMMARY.md`
- FOUND: commit `dc46143d` (Task 1)
- FOUND: commit `0bb7d63d` (Task 2)

---
*Phase: 133-visual-atlas-preview-early-quick-win*
*Completed: 2026-07-21*
