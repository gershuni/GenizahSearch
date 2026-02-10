---
created: 2026-02-10T20:41:05Z
title: JA diacritic dots normalization in search
area: search
files:
  - genizah_core.py:4415 (expand_judeo_arabic)
  - Transcriptions.txt (needs investigation)
---

## Problem

Judeo-Arabic transcription files sometimes include diacritic dots on Hebrew letters (e.g., צ׳, ט׳, ץ׳ and possibly others) but usage is inconsistent — the same word may appear with or without dots across different transcriptions. This means a JA search for a word may miss results where the transcriber used (or didn't use) diacritics.

Key questions to investigate:
1. Which letters carry diacritic dots in the transcription corpus? (צ, ץ, ט, and possibly more — ג׳, ז׳, כ׳?)
2. How do the dots appear in the actual files — as Unicode combining marks, as geresh (׳) after the letter, or as apostrophe (')?
3. Should the Tantivy index strip/normalize diacritics at index time so searches match regardless?
4. Or should the JA expansion button generate both dotted and undotted variants at query time?

## Solution

TBD — needs investigation first:

1. **Survey the corpus**: Search Transcriptions.txt for occurrences of diacritic dots/geresh/apostrophe after Hebrew letters to catalog how they appear
2. **Decide approach**: Index-time normalization (strip diacritics) vs query-time expansion (JA button generates both forms)
   - Index normalization is simpler and more complete but requires re-indexing
   - Query-time expansion keeps index intact but increases term explosion
3. **Implement**: Either add a Tantivy tokenizer filter or extend `expand_judeo_arabic()` to generate dotted/undotted variants
