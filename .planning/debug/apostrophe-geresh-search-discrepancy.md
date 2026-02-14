---
status: diagnosed
trigger: "Investigate why searching הקב'ה returns 503 results while הקבה returns 11006 results in the GenizahSearch app."
created: 2026-02-11T00:00:00Z
updated: 2026-02-11T00:05:00Z
---

## Current Focus

hypothesis: CONFIRMED - ASCII apostrophe (U+0027 ') is NOT stripped by strip_search_diacritics
test: Verified pattern only handles Hebrew punctuation
expecting: Root cause confirmed, ready to document fix
next_action: Document complete root cause and recommended fix

## Symptoms

expected: הקב'ה (with apostrophe) should return similar results to הקבה (without), as Phase 19 added geresh/gershayim stripping
actual: הקב'ה returns only 503 results while הקבה returns 11006 results (21x difference)
errors: None reported, but massive result count discrepancy
reproduction: Search for הקב'ה vs הקבה in web or desktop app
started: Unclear when this started, but Phase 19 (Search Normalization) implemented diacritic stripping

## Eliminated

## Evidence

- timestamp: 2026-02-11T00:00:00Z
  checked: genizah_core.py:4449 COMBINING_DIACRITICALS_PATTERN definition
  found: Pattern is r'[\u0300-\u036F\u05F3\u05F4]' - includes Hebrew geresh (U+05F3) and gershayim (U+05F4), but NOT ASCII apostrophe (U+0027)
  implication: strip_search_diacritics only removes Hebrew punctuation marks, not ASCII apostrophe

- timestamp: 2026-02-11T00:00:01Z
  checked: Python test of strip_search_diacritics with different apostrophe types
  found: Hebrew geresh הקב׳ה strips to הקבה (equal to base form), but ASCII apostrophe הקב'ה does NOT strip (stays הקב'ה)
  implication: Confirms pattern does not handle ASCII apostrophe

- timestamp: 2026-02-11T00:00:02Z
  checked: genizah_core.py:3810 - Main Genizah index schema definition
  found: Index uses tokenizer_name="whitespace" for content field
  implication: Whitespace tokenizer keeps punctuation attached to words, so הקב'ה and הקבה are DIFFERENT tokens in the index

- timestamp: 2026-02-11T00:00:03Z
  checked: genizah_core.py:3858, 3887 - Index building code
  found: Content is indexed as-is with NO normalization applied during index building
  implication: Index contains raw tokens with apostrophes/geresh/gershayim preserved exactly as they appear in source documents

- timestamp: 2026-02-11T00:00:04Z
  checked: genizah_core.py:1018, 1157-1159 - Query stripping in execute_search
  found: strip_search_diacritics is called on query_str before Tantivy query building
  implication: Query normalization happens at search time, but only removes characters in the pattern

- timestamp: 2026-02-11T00:00:05Z
  checked: corpus_mapper/symbol_discovery.py:50 - Symbol patterns
  found: Pattern for geresh already includes apostrophe: r'[\u05F3\u05F4\']'
  implication: Other parts of codebase already recognize apostrophe should be grouped with geresh/gershayim

- timestamp: 2026-02-11T00:00:06Z
  checked: corpus_mapper/text_cleaner.py:91 - Text cleaning rules
  found: Converts geresh/gershayim to apostrophe: (re.compile(r'[\u05F3\u05F4]'), "'")
  implication: There's inconsistency - text_cleaner converts TO apostrophe, but search normalization doesn't strip it

## Resolution

root_cause: |
  The strip_search_diacritics function (genizah_core.py:4449) only strips Hebrew geresh (U+05F3 ׳) and gershayim (U+05F4 ״), but NOT the ASCII apostrophe (U+0027 ').

  ROOT CAUSE BREAKDOWN:

  1. PATTERN DEFINITION (genizah_core.py:4449):
     COMBINING_DIACRITICALS_PATTERN = re.compile(r'[\u0300-\u036F\u05F3\u05F4]')
     This pattern includes:
     - U+0300-U+036F: Combining diacritical marks
     - U+05F3: Hebrew geresh ׳
     - U+05F4: Hebrew gershayim ״
     BUT MISSING: U+0027 (ASCII apostrophe ')

  2. TOKENIZATION IMPACT:
     The main Genizah index (genizah_core.py:3810) uses tokenizer_name="whitespace", which splits only on whitespace.
     This means punctuation stays attached to words:
     - הקב'ה (with ASCII apostrophe) → token: "הקב'ה"
     - הקב׳ה (with Hebrew geresh) → after strip → token: "הקבה"
     - הקבה (no mark) → token: "הקבה"

  3. SEARCH BEHAVIOR:
     When user types הקב'ה:
     - If typed with ASCII apostrophe (U+0027): NOT stripped → Tantivy searches for "הקב'ה" → 503 results (only docs with apostrophe)
     - If typed with Hebrew geresh (U+05F3): stripped → Tantivy searches for "הקבה" → 11006 results (all docs without marks)

  4. USER IMPACT:
     Most users type ASCII apostrophe (') from keyboard, not Hebrew geresh (׳). This causes them to get dramatically fewer results because:
     - Source documents contain a mix of ASCII apostrophes, Hebrew geresh, and no marks
     - Query normalization only strips Hebrew marks, not ASCII apostrophe
     - Only documents with the exact same apostrophe type match

fix: |
  Add ASCII apostrophe (U+0027 ') and related apostrophe-like characters to COMBINING_DIACRITICALS_PATTERN.

  RECOMMENDED FIX (genizah_core.py:4449):

  OLD:
  COMBINING_DIACRITICALS_PATTERN = re.compile(r'[\u0300-\u036F\u05F3\u05F4]')

  NEW:
  COMBINING_DIACRITICALS_PATTERN = re.compile(r'[\u0300-\u036F\u0027\u05F3\u05F4\u2018\u2019]')

  Additional characters to strip:
  - U+0027: ASCII apostrophe ' (most common keyboard input)
  - U+2018: Left single quotation mark ' (curly apostrophe)
  - U+2019: Right single quotation mark ' (curly apostrophe)

  RATIONALE:
  - Users predominantly type ASCII apostrophe from keyboard
  - Documents may contain any combination of these marks
  - Stripping all variants normalizes to base form, maximizing recall
  - Existing Phase 19 architecture already handles mark-tolerant regex patterns via make_mark_tolerant_pattern

  ALTERNATIVE APPROACH (if needed):
  If we want to allow searching WITH apostrophes/geresh when explicitly typed, we could:
  1. Keep stripping in queries (current behavior)
  2. Add apostrophe/geresh stripping during INDEX BUILDING (genizah_core.py:3858, 3887)
  3. This would normalize the index itself, not just queries
  But this requires full reindex and changes the index structure.

verification: |
  After applying the fix:

  1. Test query normalization:
     - strip_search_diacritics("הקב'ה") should return "הקבה"
     - strip_search_diacritics("הקב׳ה") should return "הקבה"
     - strip_search_diacritics("הקב'ה") should return "הקבה" (curly apostrophe)

  2. Test search behavior:
     - Search הקב'ה (ASCII apostrophe) should return ~11000 results (same as הקבה)
     - Search הקב׳ה (Hebrew geresh) should return ~11000 results
     - Search הקבה (no mark) should return ~11000 results
     - All three should return identical result sets

  3. Regression testing:
     - Verify combining diacritical marks still stripped (U+0300-U+036F)
     - Verify gershayim still stripped (U+05F4)
     - Verify Hebrew nikud preserved (NOT stripped)
     - Test with various Responsa queries that may contain apostrophes

files_changed: []
