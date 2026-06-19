---
phase: 119-candidates-compare-visual-similarity
plan: "09"
subsystem: i18n + shared-constants
tags: [gap-closure, i18n, r2-1, r2-4, triage-icons, translations]
dependency_graph:
  requires: []
  provides: [TRIAGE_ICONS, HE-compare-translations]
  affects: [119-10-candidate-grid, 119-11-compare-modal]
tech_stack:
  added: []
  patterns: [language-gated-tr, module-level-constant, desktop-parity]
key_files:
  created: []
  modified:
    - genizah_translations.py
    - shared/joins_lab.py
decisions:
  - "RTL-correct nav labels: Next › → ‹ הבא (chevron leads in RTL direction); ‹ Prev → הקודם › (chevron trails)"
  - "TRIAGE_ICONS placed as sibling of badge_and_tooltip in shared/joins_lab.py — no web/* dependency introduced"
  - "Page N of M translation preserves literal N/M placeholders for caller .replace() chain"
metrics:
  duration: "8min"
  completed: "2026-06-19T15:25:56Z"
  tasks: 2
  files: 2
---

# Phase 119 Plan 09: R2-1 Hebrew Translations + R2-4 TRIAGE_ICONS Substrate Summary

Hebrew translations for 7 missing Compare modal strings + shared triage-icon mapping for grid/Compare parity.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Add missing Hebrew Compare translations (R2-1) | `1db87608` | genizah_translations.py |
| 2 | Add shared TRIAGE_ICONS mapping for R2-4 icon parity | `a8e044a6` | shared/joins_lab.py |

## What Was Built

**Task 1 — R2-1 Hebrew translations:** Added 7 English→Hebrew key/value pairs to `TRANSLATIONS` in a new `# Phase 119-09` cohesive block at the end of `genizah_translations.py`:

- `"Candidate"` → `"מועמד"` (compare_modal.py:502)
- `"Anchor"` → `"עוגן"` (compare_modal.py:482)
- `"Maybe"` → `"אולי"` (verdict bar, candidate_grid.py:700)
- `"Next ›"` → `"‹ הבא"` (RTL-correct: chevron leads leftward)
- `"‹ Prev"` → `"הקודם ›"` (RTL-correct: chevron trails rightward)
- `"Compare fragment"` → `"השווה קטע"` (candidate_grid.py:764)
- `"Page N of M"` → `"עמוד N מתוך M"` (N/M placeholders preserved)

The tr() function is language-gated (English passthrough) — adding HE values cannot reverse-leak into the English UI.

**Task 2 — R2-4 TRIAGE_ICONS substrate:** Added `TRIAGE_ICONS` module-level constant to `shared/joins_lab.py` as a sibling of `badge_and_tooltip()`:

```python
TRIAGE_ICONS = {
    "yes":   {"glyph": "✓", "tooltip": "Mark yes",   "color": "#15803d"},
    "maybe": {"glyph": "?", "tooltip": "Mark maybe",  "color": "#a16207"},
    "no":    {"glyph": "✗", "tooltip": "Mark no",    "color": "#b91c1c"},
}
```

Glyphs match desktop `join_workbench._TRIAGE_GLYPH`; colors are identical to `candidate_grid._TRIAGE_COLORS` (plans 119-10/11 can import without color drift).

## Verification

- `python -c "from genizah_translations import TRANSLATIONS as T; assert all(k in T for k in ['Candidate','Anchor','Maybe','Next ›','‹ Prev','Compare fragment','Page N of M']); assert 'N' in T['Page N of M'] and 'M' in T['Page N of M']; print('OK')"` → OK
- `python -c "from shared.joins_lab import TRIAGE_ICONS; assert TRIAGE_ICONS['yes']['glyph']=='✓'; print('OK')"` → OK
- `python -m pytest tests/test_joins_lab.py -q` → 71 passed

## Deviations from Plan

None — plan executed exactly as written.

## Known Stubs

None. Both artifacts are complete and immediately consumable by Plans 119-10 and 119-11.

## Threat Flags

No new security surface introduced. Changes are pure string data (translations) and a static constant (icon mapping). No network endpoints, auth paths, or schema changes.

## Self-Check: PASSED

- [x] `genizah_translations.py` modified with 7 new keys
- [x] `shared/joins_lab.py` modified with `TRIAGE_ICONS` constant
- [x] Commit `1db87608` exists: `git log --oneline | grep 1db87608`
- [x] Commit `a8e044a6` exists: `git log --oneline | grep a8e044a6`
- [x] 71 test_joins_lab tests pass
