---
phase: 135-precision-certificate-confidence-bands
plan: 02
subsystem: ui
tags: [nicegui, discovery, help-page, seo, noindex, bilingual, rtl, band-precision, feature-flag]

# Dependency graph
requires:
  - phase: 135-01
    provides: "shared/discovery_band_labels.py (band_label / format_precision_copy / band_measurement_status / NUMERATOR_LABEL / DENOMINATOR_LABEL / DRAW_SIZE_LABEL) + shared/discovery_service.py band-precision + claim-count readers (get_band_precision / get_band_precision_collection / get_band_claim_counts + _async)"
  - phase: 134
    provides: "web/discovery_assets.py::discovery_available() single-authoritative gate + the web/discovery.py async pass-through pattern"
provides:
  - "BAND-05 bilingual EN/HE 'Confidence Bands & Methods' section inside /help (flag-gated; TOC entry + body card each independently gated — Codex #11)"
  - "Per-band deep-link anchors help-confidence-<band> (D-10) — the Phase-136 tooltip deep-link targets, from an explicit registry"
  - "web/discovery.py supported async wrappers get_band_precision / get_band_precision_collection / get_band_claim_counts / get_all_band_precision (all fail-open) + the discovery_methods_noindex() pre-release SEO predicate (Codex #18)"
  - "web/feature_flags.py DISCOVERY_PUBLIC_RELEASED flag (Phase-139 REL-01 gate, default OFF)"
  - "async /help route reading rows+counts through the wrappers and noindexed only pre-release (flips to indexed at REL-01)"
affects: [136-read-surfaces, 139-launch]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Independent gating of a Help TOC entry vs its body card (the render loop emits only TOC links; the body card is a separate ui.card() carrying its own discovery_available() guard — Codex #11)"
    - "Dedicated pre-release SEO predicate (discovery_available AND NOT DISCOVERY_PUBLIC_RELEASED) — a bounded pre-release noindex window that flips at a named launch gate, never a forever de-index"
    - "Fail-open web wrapper aggregate (get_all_band_precision) probing every stored band key (v1 expert_verified + v2 high_confidence_algorithmic) for version-agnostic reads"

key-files:
  created:
    - "tests/render_smoke/test_help_methods_render_smoke.py"
  modified:
    - "web/pages/help.py"
    - "web/discovery.py"
    - "web/feature_flags.py"
    - "web/main.py"

key-decisions:
  - "Body card gated independently of the TOC entry in BOTH the EN and HE builders (Codex #11)"
  - "population = the RUNTIME display-deduplicated SHIPPED-CLAIM count via get_band_claim_counts (never band_precision.denominator, never raw evidence rows, never the Wave-4 frame doc); 0.926 collection-scope only; 4 registry fields placeholder-safe, never fabricated"
  - "noindex from discovery_methods_noindex() (flips at REL-01) imported at module level in web/main.py so it is patchable in the render-smoke"
  - "D-10 per-band anchor names as an explicit _CONFIDENCE_BAND_ANCHORS registry (single greppable source of truth for Phase-136 tooltip deep-links)"

patterns-established:
  - "Confidence-section marker class (discovery-methods-section) so a render-smoke word-gate can be scoped to the section (מאושר legitimately appears elsewhere in Help)"

requirements-completed: []  # BAND-05 deferred — see Deviations (its 'band tooltips link to it' clause is satisfied by Phase-136 tooltip surfaces)

# Metrics
duration: 40min
completed: 2026-07-24
---

# Phase 135 Plan 02: Bilingual Methods/Confidence Help Section Summary

**A flag-gated, bilingual EN/HE "Confidence Bands & Methods" section inside /help — per-band deep-link anchors, population from the runtime display-deduplicated shipped-claim count, 0.926 at collection scope only, placeholder-safe registry fields — served by an async route noindexed only in the pre-release window (flips to indexed at the Phase-139 REL-01 gate).**

## Performance

- **Duration:** ~40 min
- **Started:** 2026-07-24T09:56:00Z
- **Completed:** 2026-07-24T10:36:00Z
- **Tasks:** 2
- **Files modified:** 5 (4 modified + 1 test created)

## Accomplishments
- BAND-05 methods section renders inside the existing Help page (never a new route) in EN + HE, documenting each of the 7 bands with the full field set: population, unit `(page, work)`, sample size (draw / determinate / successes, distinctly labelled), strata, weighted estimate + CI, measurement date, grader, audit status, and an immutable report identifier.
- BOTH the section's TOC entry AND its body card are independently gated on `discovery_available()` (Codex #11 — the render loop emits only TOC links, so the body card carries its own guard, in both builders).
- Per-band `help-confidence-<band>` anchors (D-10) via an explicit registry — the deep-link targets Phase-136 tooltips will link to.
- New supported fail-open async wrappers + the `discovery_methods_noindex()` pre-release SEO predicate in `web/discovery.py`; the `DISCOVERY_PUBLIC_RELEASED` flag in `web/feature_flags.py`; `/help` is now an async route reading rows+counts through the wrappers and noindexed only pre-release.
- Render-smoke (6 tests) proves EN+HE render, all 7 anchors, the runtime display-deduplicated population, placeholder-safe registry fields, the 0.926 at collection scope only, the three-state noindex transition + the predicate truth-table, the no-"certified"/HE-equivalents gate, and HE RTL.

## Task Commits

1. **Task 1: Render the bilingual section (body card independently gated)** — `ec3b6ff5` (feat)
2. **Task 2: wrappers + noindex predicate + async route** — `5e801521` (feat)
3. **Task 2: render-smoke test** — `e6203b26` (test)

**Plan metadata:** committed with this SUMMARY + STATE.md + ROADMAP.md (docs).

## Files Created/Modified
- `web/pages/help.py` — the bilingual "Confidence Bands and Methods" section: module-level bilingual copy + the `_CONFIDENCE_BAND_ANCHORS` registry + `_render_confidence_section`/`_render_one_band` helpers; TOC entry + body card each gated on `discovery_available()` in both `_create_english_content` / `_create_hebrew_content`; `create_help_page(precision=None, band_counts=None)` threads the data through.
- `web/discovery.py` — `get_band_precision` / `get_band_precision_collection` / `get_band_claim_counts` / `get_all_band_precision` async wrappers (fail-open) + `discovery_methods_noindex()`.
- `web/feature_flags.py` — `DISCOVERY_PUBLIC_RELEASED` (default OFF; the Phase-139 REL-01 gate).
- `web/main.py` — `help_page_route` is now `async`; `noindex=discovery_methods_noindex()`; awaits `get_all_band_precision()` + `get_band_claim_counts()` and threads both into `create_help_page(...)`; the three wrappers imported at module level (patchable as `web.main.*`).
- `tests/render_smoke/test_help_methods_render_smoke.py` — the NiceGUI User render-smoke (6 tests).

## Decisions Made
- **Body card gated independently of the TOC entry (Codex #11).** The Help render loop emits only `ui.link` TOC entries; the section body is its own `ui.card()` after the loop, so it carries its own `if discovery_available():` in BOTH the EN and HE builders (mirrors the `WEB_PUZZLE_ENABLED` `_puzzle_card.set_visibility` body-gating precedent).
- **Field sourcing (Codex #9/#B1).** population from `get_band_claim_counts()` (the runtime display-deduplicated shipped-claim count, each claim once via `display_evidence_id`) — never `band_precision.denominator`, never raw evidence rows, never `discovery-frames-v2.md` (a Wave-4 doc). The propagated **0.926** renders ONLY in a collection-scope paragraph via `get_band_precision_collection`, never on the corroborated/weak per-band rows. The four CERT-01 registry fields (`measurement_date`/`grader`/`audit_status`/`report_id`) render via `.get()` → placeholder ("not yet measured" / "independent audit pending"), never fabricated.
- **Dedicated pre-release SEO predicate (Codex #18).** `discovery_methods_noindex() = discovery_available() AND NOT DISCOVERY_PUBLIC_RELEASED` — noindex only in the pre-release window; flips to indexed at REL-01; imported at module level so it is patchable as `web.main.discovery_methods_noindex`.

## Deviations from Plan

### In-spirit adaptations (no scope change)

**1. [Design] D-10 anchor names as an explicit registry (not a bare f-string)**
- **Task:** 1
- **What:** The 7 `help-confidence-<band>` anchor names live in a `_CONFIDENCE_BAND_ANCHORS` dict (single greppable source of truth for the Phase-136 tooltip deep-links). A DRY f-string would have rendered the same 7 anchors at runtime but produced only 1 literal `help-confidence-` occurrence in source, failing the plan's static `>= 7` acceptance heuristic. The registry satisfies the heuristic (9 occurrences) AND is better design.
- **Verification:** `re.findall('help-confidence-', help.py) == 9`; render-smoke asserts all 7 anchors present in both EN and HE.

**2. [Test] Real-predicate truth-table added alongside the route-wiring noindex test**
- **Task:** 2
- **What:** The three-state noindex render-smoke patches `web.main.discovery_methods_noindex` (per plan). An additional `test_discovery_methods_noindex_predicate_truth_table` exercises the REAL predicate under patched `web.discovery.discovery_available` / `DISCOVERY_PUBLIC_RELEASED`, proving the REL-01 flip LOGIC itself (not just the route wiring).
- **Verification:** both green.

**3. [Masking] Per-file `--scan-asset` instead of the malformed `--scan-repo --strict`**
- **Task:** both
- **What:** The plan's `--scan-repo --strict` is malformed for a no-asset wave (`--strict` requires `--scan-asset PATH`, exits 2). Masking was verified with `MASKING_SCAN_PATTERNS_FILE=… check_atlas_masking.py --scan-asset <file>` (pattern-based, tolerates the section's legitimate Hebrew UI copy) for all 5 touched files — all clean; `grep -ciE 'maagar' web/pages/help.py` == 0. The orchestrator runs the authoritative full `--scan-repo` afterward.

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Fail-open guard around format_precision_copy in the Help render**
- **Found during:** Task 1
- **Issue:** `shared.discovery_band_labels.format_precision_copy` FAILS CLOSED (raises ValueError) on a malformed one-sided confidence interval; an uncaught raise would crash the whole /help page over one bad stored row.
- **Fix:** `_precision_copy_safe()` wraps it and renders the not-yet-measured placeholder on ValueError (T-135-02-03 availability). A well-formed row is unaffected.
- **Files modified:** web/pages/help.py
- **Verification:** ruff clean; render-smoke green (well-formed fixtures unaffected).
- **Committed in:** ec3b6ff5 (Task 1 commit)

---

**Total deviations:** 3 in-spirit adaptations + 1 auto-fix (Rule 2 availability guard). **Impact:** no scope creep; all serve correctness/availability or the plan's own acceptance intent.

## Issues Encountered
- The whole-page no-"certified" word gate could not run against the full rendered Help page: the HE Joins-Lab help legitimately contains `מאושר` ("confirmed join"). Resolved by tagging the confidence card with a marker class (`discovery-methods-section`) and scoping the word gate to that card's descendants — the section itself is clean of `certified` / `מאומת` / `מאושר` / `מוסמך`.

## Verification Results
- `pytest tests/render_smoke/test_help_methods_render_smoke.py -q` → **6 passed**.
- `ruff check` on all 5 touched files → **clean**.
- Acceptance one-liners: `help-confidence-` count **9** (≥7); `web/discovery.py` has all 3 defs; `web/main.py` has `noindex=discovery_methods_noindex` + `async def help_page_route`; body-card own-gate present in both builders (2).
- Masking: per-file `--scan-asset` **clean** for all 5 files; `grep -ciE 'maagar' web/pages/help.py` **0**.
- No regression: render_smoke package **59 passed / 1 skipped**; discovery suites (band_labels/flag/loader/composition) **48 passed**; discovery back-edge guard **7 passed**.
- Manual sanity: with discovery unavailable, `/help` renders as today (no confidence section, no noindex) — asserted by `test_help_flag_off_section_absent_and_indexed`.

## User Setup Required
None — no external service configuration required. (`DISCOVERY_PUBLIC_RELEASED` stays unset/OFF until the Phase-139 REL-01 gate.)

## Next Phase Readiness
- The methods page + per-band `help-confidence-<band>` anchors are the deep-link targets Phase 136's browse-panel/`/work/{id}` band tooltips will link to.
- BAND-05 requirement left Pending: the methods PAGE landed, but its "band tooltips link to it" clause completes when Phase 136 ships the consuming tooltip surfaces (mirrors this phase's deferral precedent).

## Self-Check: PASSED

- Files: web/pages/help.py, web/discovery.py, web/feature_flags.py, web/main.py, tests/render_smoke/test_help_methods_render_smoke.py, 135-02-SUMMARY.md — all FOUND.
- Commits: ec3b6ff5 (feat, Task 1), 5e801521 (feat, Task 2 code), e6203b26 (test, Task 2) — all FOUND.

---
*Phase: 135-precision-certificate-confidence-bands*
*Completed: 2026-07-24*
