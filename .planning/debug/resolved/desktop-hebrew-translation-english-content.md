---
status: resolved
trigger: "Desktop app Hebrew translation shows English content"
created: 2026-02-11T00:00:00Z
updated: 2026-02-11T00:30:00Z
symptoms_prefilled: true
goal: find_root_cause_only
---

## Current Focus

hypothesis: CONFIRMED - Desktop doesn't sort translations by language, web app does
test: Compared desktop _populate_pgp_combo with web version_selector translation grouping
expecting: Desktop shows translations in sequence_order, web groups Hebrew-first
next_action: Return diagnosis to caller

## Symptoms

expected: Hebrew translation tab shows Hebrew content, English translation tab shows English content (like web app)
actual: Hebrew translation tab shows English content instead of Hebrew content
errors: None reported
reproduction: Open desktop app, view PGP document with both Hebrew and English translations, check Hebrew translation tab
started: After Phase 21 added source_language/source_direction columns

## Eliminated

## Evidence

- timestamp: 2026-02-11T00:10:00Z
  checked: document_sources schema (migrations/add_sections_column.sql, migrations/create_document_sources_table.sql)
  found: Two columns exist - 'language' (original, values: "Hebrew", "English") and 'source_language' (Phase 21, values: 'he', 'en', 'jrb', 'ar')
  implication: Phase 21 added source_language but didn't populate the old language column

- timestamp: 2026-02-11T00:15:00Z
  checked: Desktop app genizah_app.py:6104-6115 _populate_pgp_combo
  found: Line 6104-6115 iterates translations in order received, no language sorting
  implication: Desktop shows translations in database sequence_order

- timestamp: 2026-02-11T00:20:00Z
  checked: Web app web/components/version_selector.py:256-264
  found: Lines 256-264 group translations by language - Hebrew first (line 261), English second (line 262)
  implication: Web app explicitly sorts translations Hebrew-first, English-second

- timestamp: 2026-02-11T00:25:00Z
  checked: shared/document_service.py:392-396 get_all_sources_for_fragment sorting
  found: Sorts by (Edition=0/Translation=1, sequence_order) - no language consideration
  implication: Sources arrive at desktop in sequence_order, which may not be Hebrew-first

## Resolution

root_cause: Desktop app displays translations in database sequence_order (not sorted by language), while web app groups and displays Hebrew translations before English. When database has English (sequence_order=1) and Hebrew (sequence_order=2), desktop shows English first but web shows Hebrew first. Labels follow content order, so "Hebrew - Scholar" label gets attached to English content in desktop.
fix: Add language-based grouping/sorting to desktop _populate_pgp_combo (lines 6070-6115) to match web app behavior - Hebrew translations before English, sorted by language then sequence_order
verification: Test with PGP document having both Hebrew and English translations, verify Hebrew translation shows Hebrew content
files_changed: [genizah_app.py]

root_cause:
fix:
verification:
files_changed: []
