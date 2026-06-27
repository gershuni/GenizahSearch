---
status: diagnosed
trigger: "RunningTitle translation shows in desktop but not web catalog dialog; Type/numLines note not translated"
created: 2026-03-13T00:00:00Z
updated: 2026-03-13T00:00:00Z
---

## Current Focus

hypothesis: Web catalog dialog uses wrong translation lookup method for RunningTitle
test: Compare desktop vs web translation fetch approach
expecting: Desktop uses signature_id-based lookup, web uses alma_id batch (wrong)
next_action: Report findings

## Symptoms

expected: RunningTitle translations appear in web catalog dialog (like desktop)
actual: RunningTitle translations appear only in desktop, not in web
errors: None (silent failure -- wrong lookup returns no matching data)
reproduction: Open any FJMS catalog dialog in web app with RunningTitle that has translation
started: Since web catalog dialog was created (never worked for RunningTitle)

## Eliminated

(none needed -- root cause found on first hypothesis)

## Evidence

- timestamp: 2026-03-13
  checked: Desktop RunningTitle translation code (genizah_app.py:6766-6777)
  found: Uses get_fjms_translations_by_signature_ids('RunningTitle', rec_ids) keyed by UnitCatalogRecId
  implication: Correct approach -- matches how translations are stored in DB

- timestamp: 2026-03-13
  checked: Web RunningTitle translation code (catalog_dialog.py:48-57, 277-301)
  found: Uses get_fjms_translations_batch([sys_id]) which returns {field_name: (text, dir)} keyed by alma_id
  implication: Wrong lookup method -- returns at most ONE translation per field_name (last wins), and doesn't key by UnitCatalogRecId

- timestamp: 2026-03-13
  checked: TranslationService.get_fjms_translations_batch (translation_service.py:360-390)
  found: Groups by alma_id then field_name -- dict overwrites mean only last row per field_name survives
  implication: Even if it returns data, only one RunningTitle translation per alma_id (wrong for multi-record manuscripts)

- timestamp: 2026-03-13
  checked: Sub-issue 2 -- "Type: Document numLines" note line
  found: This text is part of catalog_free_desc.FreeDesc content (not a separate field). 8,998 from "Instatution" source, 959 from CUL. 3,686 DO have translations, ~6,271 do not.
  implication: This is a data coverage gap, not a code bug. The web FreeDesc translation pipeline works correctly for entries that have translations.

## Resolution

root_cause: |
  **Sub-issue 1 (RunningTitle not translated in web):**
  Web catalog_dialog.py uses `get_fjms_translations_batch([sys_id])` (line 53) which is an
  alma_id-level batch lookup returning {field_name: (text, direction)}. This method:
  1. Returns only ONE translation per field_name per alma_id (dict key overwrite on line 386 of translation_service.py)
  2. Does NOT key by UnitCatalogRecId, so the translation cannot be matched to the correct running title record

  The desktop (genizah_app.py:6774-6777) correctly uses `get_fjms_translations_by_signature_ids('RunningTitle', rec_ids)`
  which looks up by signature_id = UnitCatalogRecId, returning per-record translations.

  **Sub-issue 2 (Type/numLines note not translated):**
  This is NOT a code bug. The "Type: Document numLines: X lines (recto; verso is blank)" text is part of
  free description content in catalog_free_desc. The translation pipeline for FreeDesc IS wired in both
  web and desktop. However, ~6,271 of these specific descriptions (mostly from "Instatution" source)
  simply don't have translations in the fjms_translations table. 3,686 similar entries DO have translations.
  This is a batch translation data coverage gap.

fix: (not applied -- diagnosis only)
verification: (not applied)
files_changed: []
