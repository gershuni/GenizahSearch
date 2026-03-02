---
status: diagnosed
trigger: "Desktop search status text needs Hebrew translations -- searching... and Showing 500 of X results appear in English"
created: 2026-03-01T00:00:00Z
updated: 2026-03-01T00:00:00Z
---

## Current Focus

hypothesis: Translation keys used in code do not match keys defined in genizah_translations.py
test: Compare exact tr() key strings against TRANSLATIONS dict entries
expecting: Key mismatch (case, punctuation, or entirely missing)
next_action: Report diagnosis

## Symptoms

expected: Status text during/after search should display in Hebrew when UI language is Hebrew
actual: "Searching... 0:05" and "Showing 500 of 1234 results" appear in English
errors: None (no crash -- just English fallback from tr())
reproduction: Switch to Hebrew UI, run any search, observe status_label text
started: These strings were likely never translated (missing keys)

## Eliminated

(none needed -- root cause found on first pass)

## Evidence

- timestamp: 2026-03-01
  checked: tr() function in genizah_core.py:1927
  found: Simple dict lookup -- TRANSLATIONS.get(text, text). Falls back to English if key not found. Case-sensitive.
  implication: Any key mismatch means silent English fallback

- timestamp: 2026-03-01
  checked: genizah_app.py:17081 -- progress timer during search
  found: Code uses tr('Searching') (capital S, no ellipsis), then appends "... {elapsed}"
  implication: Key "Searching" does NOT exist in translations. Only "Searching..." and "searching" exist.

- timestamp: 2026-03-01
  checked: genizah_app.py:17237, 16706, 17288 -- results count status
  found: Code uses tr("Showing {} of {} results") with positional {} placeholders
  implication: Key "Showing {} of {} results" does NOT exist in translations.

- timestamp: 2026-03-01
  checked: genizah_app.py:17233 -- Responsa expanded terms status
  found: Code uses tr("Showing {} of {} results (searching {} expanded terms)")
  implication: Key "Showing {} of {} results (searching {} expanded terms)" does NOT exist in translations.

- timestamp: 2026-03-01
  checked: genizah_app.py:16702 -- domain filtering status
  found: Code uses tr("Showing {} of {} results (filtering {} domains)")
  implication: Key "Showing {} of {} results (filtering {} domains)" does NOT exist in translations.

- timestamp: 2026-03-01
  checked: genizah_translations.py for similar keys
  found: Existing keys that are close but don't match:
    - "Searching..." (line 619, 1523, 2203) -- has ellipsis, code uses no ellipsis
    - "searching" (line 2474) -- lowercase, code uses uppercase
    - "Showing top {} of {} results. (Export for full list)" (line 534) -- different pattern
    - "Showing {start}-{end} of {total} manuscripts" (line 2433) -- named placeholders, browse context
  implication: Keys are simply missing from the translations dictionary

## Resolution

root_cause: Four translation keys used in genizah_app.py search status updates are NOT present in genizah_translations.py TRANSLATIONS dict. The tr() function silently falls back to English when a key is missing. The keys are:
  1. "Searching" (line 17081) -- exists as "Searching..." and "searching" but not bare "Searching"
  2. "Showing {} of {} results" (lines 16706, 17237, 17288) -- completely missing
  3. "Showing {} of {} results (searching {} expanded terms)" (line 17233) -- completely missing
  4. "Showing {} of {} results (filtering {} domains)" (line 16702) -- completely missing

fix: Add these 4 translation entries to genizah_translations.py
verification: (pending)
files_changed: []
