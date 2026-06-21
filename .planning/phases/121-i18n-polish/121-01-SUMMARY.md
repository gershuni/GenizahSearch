---
phase: 121-i18n-polish
plan: "01"
subsystem: i18n
tags: [i18n, translations, joins-lab, bilingual]
dependency_graph:
  requires: []
  provides: [FND-07-sc1-keys]
  affects: [genizah_translations.py, web/pages/joins_lab.py]
tech_stack:
  added: []
  patterns: [TRANSLATIONS.update block, tr()-wrap]
key_files:
  created: []
  modified:
    - genizah_translations.py
    - web/pages/joins_lab.py
decisions:
  - "17 new HE keys appended in a single Phase 121 TRANSLATIONS.update block at end of file"
  - "Three glossary drifts corrected in-place (Open in Joins Lab, Clear Joins Lab, Clear all Joins Lab state…) — all fixed to הצירופים form"
  - "Filter by shelfmark… HE = סנן לפי מספר מדף… (established glossary, not סימן מדף — REVIEWS #2)"
  - "ws.title = tr('Candidates') — single-line change, tr already imported"
metrics:
  duration: "5min"
  completed: "2026-06-21"
  tasks: 2
  files: 2
---

# Phase 121 Plan 01: Joins Lab i18n Gap Closure Summary

**One-liner:** 17 missing HE translation keys added + 3 glossary drifts (מעבדת החיבורים/ההצטרפות → הצירופים) fixed + XLSX sheet name wrapped in tr().

## Tasks Completed

| # | Task | Commit | Files |
|---|------|--------|-------|
| 1 | Add 17 missing HE keys + fix 3 glossary drifts | `85330c9e` | genizah_translations.py |
| 2 | tr()-wrap XLSX Candidates sheet name | `047dab5f` | web/pages/joins_lab.py |

## What Was Built

### Task 1 — genizah_translations.py

Appended a new `# === Phase 121: Joins Lab i18n gap closure (FND-07) ===` block with 17 HE entries:

**14 AST-catchable literal keys:**
- `'No anchor loaded'` → `'לא נטען עוגן'`
- `'No candidates selected'` → `'לא נבחרו מועמדים'`
- `'Loading visual similarity…'` → `'טוען דמיון חזותי…'` (U+2026 ellipsis)
- `'No candidates match both text and visual similarity. Try clearing the builder for VS-only browse.'` → full HE
- `'Could not load your lists. Please try again.'` → `'לא ניתן לטעון את הרשימות שלכם. נסו שוב.'`
- `'Has dimensions data'` → `'קיימים נתוני מידות'`
- `'Exclude size mismatch'` → `'הסתר אי-התאמת גודל'`
- `'Select for bulk actions'` → `'בחר לפעולות קבוצתיות'`
- `'Triage state'` → `'מצב מיון'`
- `'Filter by shelfmark…'` → `'סנן לפי מספר מדף…'` (established glossary — REVIEWS #2)
- `'Mark N selected as:'` → `'סמן N שנבחרו כ:'` (literal N preserved for runtime substitution)
- `'Select exactly one candidate to add as a join'` → `'בחרו מועמד אחד בלבד כדי להוסיף כצירוף'`
- `'Add anchor + this candidate to the Fragment Puzzle'` → `'הוסף את העוגן ואת המועמד הזה לפאזל הקטעים'`
- `'Size mismatch'` → `'אי-התאמת גודל'`

**3 badge_and_tooltip() strings (runtime variable, not AST-catchable):**
- `'Anchor fragment'` → `'קטע עוגן'`
- `'Found via other side'` → `'נמצא דרך הצד השני'`
- `'Visually similar'` → `'דומה חזותית'`

**3 glossary drift fixes (in-place edits):**
- Line 2538: `'Open in Joins Lab'` value fixed from `'פתח במעבדת ההצטרפות'` → `'פתח במעבדת הצירופים'` (D-06 / REVIEWS #1)
- Line 4259: `'Clear Joins Lab'` value fixed from `'נקה מעבדת החיבורים'` → `'נקה את מעבדת הצירופים'` (REVIEWS #1 HIGH)
- Line 4257: `'Clear all Joins Lab state: anchor, builder, triage, filters'` value fixed from `…מעבדת החיבורים…` → `…מעבדת הצירופים…` (REVIEWS #1 HIGH)

### Task 2 — web/pages/joins_lab.py

Changed `ws.title = 'Candidates'` → `ws.title = tr('Candidates')` at line 2252 (XLSX export block). The `tr` import and `'Candidates'` key (`'מועמדים'`) were already present.

## Verifications Passed

- Task 1: `python -c "...verify command..." → "OK 17 keys hebrew-valued + 3 drift fixed"`
- Task 2: `python -c "...verify command..." → "OK sheet name wrapped"`
- Final: `import genizah_translations; import web.translations → "imports OK"`

## Deviations from Plan

None — plan executed exactly as written.

## Known Stubs

None. All 17 keys are fully translated Hebrew values (not placeholders).

## Threat Flags

None — this plan adds translation dict entries and wraps one string in tr(). No new attack surface.

## Self-Check: PASSED

- genizah_translations.py modified: confirmed (56 insertions, 3 deletions)
- web/pages/joins_lab.py modified: confirmed (1 insertion, 1 deletion)
- Commit 85330c9e exists: confirmed
- Commit 047dab5f exists: confirmed
