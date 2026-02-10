---
status: diagnosed
trigger: "Investigate the desktop tabular query builder's layout direction. The user reports it should be RTL (right-to-left) even in English, matching the web version."
created: 2026-02-10T00:00:00Z
updated: 2026-02-10T00:00:00Z
---

## Current Focus

hypothesis: CONFIRMED - Desktop QDialog only sets RTL when CURRENT_LANG=='he', web sets RTL unconditionally
test: Compared desktop (genizah_app.py:4367-4368) with web (search.py:1424)
expecting: Root cause confirmed - need to remove language condition
next_action: Return diagnosis to user

## Symptoms

expected: Tabular query builder should use RTL layout (right-to-left) even in English, matching web version
actual: Desktop tabular query builder uses LTR layout when language is English
errors: None
reproduction: Open tabular query builder in desktop app with English UI, observe left-to-right layout
started: Unknown - cosmetic issue, likely since tabular builder was first implemented

## Eliminated

## Evidence

- timestamp: 2026-02-10T00:00:00Z
  checked: genizah_app.py lines 4362-4368 (TabularQueryBuilderDialog.__init__)
  found: RTL layout only set conditionally - `if CURRENT_LANG == 'he': self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)`
  implication: Dialog defaults to LTR when UI language is English

- timestamp: 2026-02-10T00:00:00Z
  checked: web/pages/search.py line 1424 (tabular query builder dialog)
  found: Web version sets `direction: rtl;` unconditionally in dialog card style
  implication: Web version is always RTL regardless of UI language

- timestamp: 2026-02-10T00:00:00Z
  checked: genizah_app.py lines 4503-4504
  found: Preview label ALSO has conditional RTL - `if CURRENT_LANG == 'he': self._preview_label.setLayoutDirection(Qt.LayoutDirection.RightToLeft)`
  implication: Two places need fixing - main dialog AND preview label

## Resolution

root_cause: Desktop TabularQueryBuilderDialog only sets RTL layout when CURRENT_LANG=='he' (lines 4367-4368 and 4503-4504), but web version sets RTL unconditionally (line 1424). The language check should be removed to match web behavior.

fix: Remove the `if CURRENT_LANG == 'he':` condition from both setLayoutDirection calls (dialog at line 4367 and preview label at line 4503). Both should be unconditional like the web version.

verification: Open tabular query builder with English UI language, verify dialog and preview label both display RTL

files_changed:
  - genizah_app.py: Remove language condition from lines 4367-4368 and 4503-4504
