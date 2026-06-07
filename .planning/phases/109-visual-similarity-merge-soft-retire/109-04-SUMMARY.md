---
phase: 109-visual-similarity-merge-soft-retire
plan: "04"
subsystem: i18n / genizah_translations
tags: [i18n, hebrew, translations, visual-similarity, gap-closure]
dependency_graph:
  requires: []
  provides: [corrected-vs-hebrew-keys, gap-round-keys-pre-seeded]
  affects: [desktop/join_workbench.py, desktop/corrections_ui.py]
tech_stack:
  added: []
  patterns: [surgical-edit-tool-per-value, additive-translations-block]
key_files:
  created: []
  modified:
    - genizah_translations.py
decisions:
  - "Used Edit tool per-value (not blanket sed) to avoid corrupting legitimate חיצוני 'external' strings"
  - "Pre-seeded gap-round keys in this plan (sole owner of genizah_translations.py) so Plans 05/06 stay i18n-green without touching this file"
metrics:
  duration: "~3 minutes"
  completed: "2026-06-07T17:45:00Z"
  tasks_completed: 2
  tasks_total: 2
  files_changed: 1
---

# Phase 109 Plan 04: Hebrew VS Key Fix + Gap-Round Pre-seed Summary

**One-liner:** Surgical חיצוני→חזותי fix on 6 VS-meaning keys + 4 new gap-round translation keys pre-seeded so Plans 05/06 stay i18n-green.

## Tasks Completed

| # | Task | Commit | Files |
|---|------|--------|-------|
| 1 | Correct VS-meaning Hebrew keys חיצוני → חזותי | 441cb01f | genizah_translations.py |
| 2 | Pre-seed gap-round tr() keys (toggle, empty-state, pick) | 6907797e | genizah_translations.py |

## What Was Done

### Task 1 — Surgical VS Hebrew fix

Six Phase-108/109 VS-meaning keys were changed from חיצוני ("external") to חזותי ("visual"):

**Phase-108 source-selector block (~line 3832):**
- `"Visual similarities"`: `"דמיון חיצוני"` → `"דמיון חזותי"`
- `"Search + visual"`: `"חיפוש + חיצוני"` → `"חיפוש + חזותי"`
- `"Visual similarity candidates — arrives in Phase 109"`: value updated to `"מועמדים בדמיון חזותי — יגיע בשלב 109"`
- `"Visual source (coming soon)"`: value updated to `"מקור חזותי (בקרוב)"`

**Phase-109 VS block (~line 4005):**
- `"Visual look-alikes loaded"`: `"דמיון חיצוני נטען"` → `"דמיון חזותי נטען"`
- `"No visual similarity data for this manuscript"`: `"אין נתוני דמיון חיצוני עבור כתב יד זה"` → `"אין נתוני דמיון חזותי עבור כתב יד זה"`

All 7 legitimate "external" keys were verified untouched after the edit (grep audit confirmed only valid חיצוני occurrences remain: External Website, External Metadata, External Viewer, External link, External, All external services, Open in external library website).

### Task 2 — Gap-round keys pre-seeded

New block appended after the Phase-109 block:

```python
# === Phase 109 gap-closure (G-04 toggle + G-05 pick) ===
TRANSLATIONS.update({
    "Show only visual look-alikes; with a search term, only look-alikes that also match":
        "הצג רק דומים חזותית; עם מונח חיפוש, רק דומים חזותית שגם מתאימים",
    "No look-alikes match this search": "אין דומים חזותית התואמים לחיפוש זה",
    "Select as partner": "בחר כשותף",
    "Pick a partner in the Join Lab": "בחר שותף במעבדת הצירופים",
})
```

No `"  ✎ text"` badge key added (CONTEXT ✎text RESOLVED — text-only candidates render UNBADGED).

## Verification

- `python -m pytest tests/test_join_workbench_i18n.py -q` → 4 passed (both tasks)
- `python -m ruff check genizah_translations.py` → All checks passed (both tasks)
- Grep audit: all remaining `חיצוני` occurrences are legitimate "external" strings
- Key `"דמיון חזותי נטען"` present; `"דמיון חיצוני נטען"` absent

## Deviations from Plan

None — plan executed exactly as written.

## Known Stubs

None.

## Threat Flags

None — pure in-process translation-table edit; no new network, auth, or data surface.

## Self-Check: PASSED

- genizah_translations.py modified and committed: FOUND (441cb01f, 6907797e)
- "דמיון חזותי נטען" present in file: CONFIRMED
- "אין נתוני דמיון חזותי עבור כתב יד זה" present: CONFIRMED
- "דמיון חיצוני נטען" absent: CONFIRMED
- "External Website": "אתר חיצוני" still present: CONFIRMED
- "External": "חיצוני" still present: CONFIRMED
- "Select as partner": "בחר כשותף" present: CONFIRMED
- "Pick a partner in the Join Lab": "בחר שותף במעבדת הצירופים" present: CONFIRMED
- i18n guard: 4 passed; ruff: clean
