---
phase: 109-visual-similarity-merge-soft-retire
plan: "09"
subsystem: ui
tags: [join-workbench, visual-similarity, badges, i18n, desktop, tdd, gap-closure]

# Dependency graph
requires:
  - phase: 109-visual-similarity-merge-soft-retire
    plan: "08"
    provides: "visual similarity" tr() key pre-seeded in TRANSLATIONS
provides:
  - Single eye 👁 badge on all via_vs CandidateCards (replaces ★both + ⊙VS#rank)
  - Eye-prefixed toggle label (👁 Visual Similarity)
  - Explicit QPushButton:checked stylesheet on the VS toggle
  - TDD: test_eye_badge_replaces_star_and_vs, test_eye_badge_precedence_after_self_otherside,
         test_toggle_eye_and_checked_style
affects:
  - JWB-12

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Single unified badge vocabulary: eye glyph as literal + tr() tooltip (not a tr()-wrapped badge text)"
    - "QPushButton:checked stylesheet with border + faint shade for unambiguous ON state"
    - "TDD static-source-scan tests (headless, no Qt __init__) for badge and style assertions"

key-files:
  created: []
  modified:
    - desktop/join_workbench.py
    - tests/test_join_workbench_vs.py

key-decisions:
  - "Eye badge is the literal glyph '  👁' + setToolTip(tr('visual similarity')); not a tr()-wrapped badge text"
  - "branch order: is_anchor_self → via_other_side → via_vs (eye); self/other-side take precedence"
  - "QPushButton:checked uses heavier border #475569 + faint shade #e2e8f0 + bold — NO full accent fill (Hillel's explicit choice)"
  - "Precedence test uses badge-block-anchored search window (600 chars from '# 2. Shelfmark + provenance badge') to avoid false matches from other field usages"

requirements-completed: [JWB-12]

# Metrics
duration: ~3min
completed: 2026-06-08
---

# Phase 109 Plan 09: Eye Badge + Toggle Polish Summary

**Single eye 👁 badge replaces ★both/⊙VS#rank on all visual look-alike CandidateCards; VS toggle eye-prefixed and ON state given an explicit border/shade stylesheet**

## Performance

- **Duration:** ~3 min
- **Started:** 2026-06-08T01:59:14Z
- **Completed:** 2026-06-08T02:01:58Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments

- Task 1 (G-06/G-09): Replaced the two-branch `★ both` / `⊙ VS#rank` badge logic in `CandidateCard.__init__` with a single `elif c.via_vs:` branch that appends the literal `"  👁"` glyph and sets `setToolTip(tr("visual similarity"))`. The `f"#{c.vs_rank}"` rank append is gone. The ⚓self and ⇄other-side branches are unchanged and precede the eye branch (G-06.4 precedence). Text-only candidates remain unbadged. Stale `★both`/`⊙VS` references in the `_maybe_assemble` docstring and inline comment were updated to the eye vocabulary.
- Task 2 (G-06.3/G-12.1): Changed `btn_vs_toggle` label to `"👁 " + tr("Visual Similarity")` and added an explicit `QPushButton:checked` stylesheet (2px solid border `#475569`, faint background `#e2e8f0`, bold font). The `tr("Visual Similarity")` call is preserved so the `test_no_source_radios_in_build_ui` i18n guard and the full-scan guard stay green.
- 4 new TDD tests added (2 per task): `test_eye_badge_replaces_star_and_vs`, `test_eye_badge_precedence_after_self_otherside`, `test_toggle_eye_and_checked_style`; all headless static source scans.
- Overall gate: 40 tests pass (36 pre-existing + 4 new); ruff clean.

## Task Commits

1. **Task 1: Replace ★both/⊙VS#rank badges with single eye 👁 badge** - `f37b8223` (feat)
2. **Task 2: Eye-prefix toggle label + explicit :checked stylesheet** - `883db227` (feat)

**Plan metadata:** (docs commit follows)

## Files Created/Modified

- `desktop/join_workbench.py` — badge block (lines ~1726-1743): single `elif c.via_vs:` eye branch + tooltip; toggle construction (lines ~2203-2218): eye prefix + :checked stylesheet; `_maybe_assemble` docstring + comment updated to eye vocabulary
- `tests/test_join_workbench_vs.py` — 4 new tests appended (Plan 09 sections for Task 1 and Task 2)

## Decisions Made

- Eye glyph is a literal `"  👁"` in the badge text; human-readable label routes through `tr("visual similarity")` as a tooltip (pre-seeded by Plan 08). This satisfies the AST i18n guard without a new tr() key.
- Precedence test (`test_eye_badge_precedence_after_self_otherside`) uses a 600-char window anchored at `"# 2. Shelfmark + provenance badge"` to avoid false matches from the many other uses of `c.via_other_side` elsewhere in the file.
- `:checked` rule uses `#475569` border (heavier/darker) + `#e2e8f0` background (faint shade only, not a full accent like `#2563eb`) per Hillel's explicit G-12 direction.

## Deviations from Plan

None — plan executed exactly as written. The precedence test required a slightly more targeted search strategy (badge-block anchor) than the plan's raw `src.find()` suggestion, due to `c.via_other_side` appearing earlier in the file at line 270 in a dict literal. This is a correct refinement, not a behavioral change.

## Known Stubs

None.

## Threat Flags

None — desktop UI refinement only; no new I/O, network, auth, or input-parsing surface.

---

## Self-Check

**Created files:**
- `tests/test_join_workbench_vs.py` — exists (pre-existing, modified)
- `desktop/join_workbench.py` — exists (pre-existing, modified)
- `.planning/phases/109-visual-similarity-merge-soft-retire/109-09-SUMMARY.md` — this file

**Commits exist:**
- `f37b8223` — feat(109-09): replace ★both/⊙VS#rank badges with single eye 👁 badge (G-06/G-09)
- `883db227` — feat(109-09): eye-prefix toggle label + explicit :checked stylesheet (G-06.3/G-12.1)

## Self-Check: PASSED

*Phase: 109-visual-similarity-merge-soft-retire*
*Completed: 2026-06-08*
