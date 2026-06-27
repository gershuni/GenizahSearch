---
status: diagnosed
trigger: "Investigate why suffix wildcard search (e.g., שלום*) in Responsa mode doesn't match words like שלומו because of sofit letter mismatch"
created: 2026-02-10T00:00:00Z
updated: 2026-02-10T00:01:00Z
---

## Current Focus

hypothesis: CONFIRMED — Two independent issues in wildcard handling
test: Traced full pipeline from parse_responsa_query through expansion to build_regex_pattern and build_tantivy_query
expecting: N/A — root cause confirmed
next_action: Return diagnosis

## Symptoms

expected: שלום* should match שלומו (and other words starting with שלומ-)
actual: Regex (שלום\S*) has final ם (mem sofit) but שלומו contains regular מ (mem), so no match. Tantivy query is just ("שלום"^5) — only finds exact שלום, misses documents that only contain שלומו.
errors: No error — silent failure to match
reproduction: Search שלום* in Responsa mode
started: Since wildcard was implemented (Phase 14-02)

## Eliminated

## Evidence

- timestamp: 2026-02-10T00:00:30Z
  checked: _parse_single_token() at line 4495-4521
  found: שלום* is parsed as ResponsaComponent(words=['שלום'], wildcard='suffix') — sofit mem preserved in words
  implication: The word enters the pipeline with its final-form letter intact

- timestamp: 2026-02-10T00:00:35Z
  checked: Expansion pipeline at lines 5495-5552
  found: For a simple שלום* query, none of the expansion flags are set (plene_defective=False, grammatical_prefixes=False, grammatical_suffixes=False). expanded_words stays as ['שלום']. regex_terms = ['שלום'] with sofit mem.
  implication: No expansion step converts the sofit letter for wildcard components

- timestamp: 2026-02-10T00:00:40Z
  checked: _build_wildcard_regex() at lines 4861-4868
  found: For wildcard=='suffix', builds re.escape(term) + r'\S*' directly from regex_terms. No sofit-to-normal conversion anywhere in this function.
  implication: PRIMARY BUG — regex pattern שלום\S* cannot match שלומו because ם != מ

- timestamp: 2026-02-10T00:00:45Z
  checked: build_tantivy_query() at lines 4983-5028
  found: No special handling for wildcard components. Tantivy query for שלום* is just ("שלום"^5). No wildcard query syntax used.
  implication: SECONDARY ISSUE — Tantivy only recalls documents containing exact word שלום, misses documents that only have derived forms like שלומו

- timestamp: 2026-02-10T00:00:50Z
  checked: _SOFIT_TO_NORMAL at line 4590-4596 and expand_grammatical_suffixes() at line 4599-4624
  found: The sofit-to-normal mapping exists and is correctly used in suffix expansion (line 4616-4618). But _build_wildcard_regex() does NOT use it.
  implication: The conversion logic exists but is only wired into grammatical suffix expansion (#), not wildcard suffix expansion (*)

- timestamp: 2026-02-10T00:00:55Z
  checked: Existing tests for suffix wildcard
  found: test_responsa_integration.py:147 tests suffix wildcard with English 'shalom' (no sofit letters). test_responsa_core.py:119 only checks parsing, not regex output. No test uses Hebrew words with sofit letters in wildcard context.
  implication: Test gap — no test catches this because tests don't use Hebrew words with final-form letters

- timestamp: 2026-02-10T00:01:00Z
  checked: Planning docs (14-RESEARCH.md:181-184)
  found: Tantivy wildcard limitation was a KNOWN design decision: "For suffix wildcards, send the stem to Tantivy (best effort recall) and rely on regex for precision." This means the Tantivy side was intentionally limited, but the regex side (which was supposed to be the precision filter) is broken due to the sofit issue.
  implication: The intended design relies on regex being correct, which it is not

## Resolution

root_cause: Two issues in suffix wildcard handling in genizah_core.py:

**PRIMARY (Bug): _build_wildcard_regex() does not convert sofit letters to normal form before building the wildcard regex pattern.**
- Location: genizah_core.py, function _build_wildcard_regex(), lines 4861-4868
- When building the suffix wildcard regex, it takes regex_terms directly (e.g., ['שלום'] with sofit mem) and creates re.escape('שלום') + r'\S*'
- This regex cannot match שלומו because the text has regular מ (mem), not ם (mem sofit)
- The _SOFIT_TO_NORMAL mapping (line 4590) and conversion logic exist but are only used in expand_grammatical_suffixes() (line 4616), not in _build_wildcard_regex()
- Fix direction: Before building the wildcard regex, convert any trailing sofit letter in each regex_term to its normal form using _SOFIT_TO_NORMAL. For suffix wildcards, the pattern should also include both forms (sofit and normal) since the sofit form could appear at the end of a word in text.
- Actually: The correct approach for suffix wildcard is to replace the trailing sofit with a character class [םמ] so it matches BOTH the standalone word שלום (ends with sofit) AND the stem שלומ- (with normal mem before continuation). For prefix wildcards, sofit at end of the base term should stay as-is (it's a word ending).

**SECONDARY (Known limitation, but improvable): build_tantivy_query() sends only exact terms for wildcard components.**
- Location: genizah_core.py, function build_tantivy_query(), lines 4988-5028
- No check for comp.get('wildcard') — wildcard components are treated identically to regular components
- Tantivy query for שלום* is ("שלום"^5) — only finds documents with exact word שלום
- Documents containing ONLY שלומו (without standalone שלום) are missed entirely at the Tantivy phase, so regex never gets a chance to match them
- This was a known design decision per 14-RESEARCH.md, but could be improved by also sending the sofit-converted stem to Tantivy

fix:
verification:
files_changed: []
