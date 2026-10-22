## [Unreleased] -- Responsa search: `*` at the start of a word; page-break matches from the seams (web + desktop, branch `claude/gifted-brahmagupta-yy43jt-leading-stars`)

Phase 3, second part, of `docs/plans/RESPONSA_TRUE_WILDCARDS_PLAN.md` (§7, "Phase 3, settled
before building", decision 10), on top of the first part (`*` at the end of a word or inside it).

- **A `*` at the start of a word stands for any letters too**, as it does everywhere else in a
  word: `*נים *לים` finds `כהנים גדולים`, `*כהנ*` finds every word that holds `כהנ`, and
  `*כ*ה*נים` finds `הכוהנים`. On a rebuilt index it is looked up from the end of each word,
  quickly; on an older index it reads the index's whole word list, several seconds a word.
- **Matches across a page break come from the seams on a rebuilt index**, for every search of
  several words (Exact, Variants, Fuzzy and Responsa, up to 30 words on a side): only pages
  that follow each other in a manuscript are joined, and every such match is found, not only a
  manuscript's first. A longer phrase reads the whole manuscripts, as before.
- Counts change: `*word` now finds every word that ends with the letters (the word with its 24
  grammatical prefixes is `#word`), `*word*` is searched as typed rather than as `#word#`, and on
  a rebuilt index a page-break match is read only where the pages follow each other.
- Desktop: on an index built by an earlier version, a notice after startup offers a rebuild once
  ("Rebuild now" opens the usual rebuild and its confirmation; "Later" is remembered for that
  index). Nothing rebuilds by itself.
- `scripts/verify_responsa_wildcards.py --phase 3` holds the leading-star queries and compares
  finite words' crossings in full on a rebuilt index; `scripts/measure_responsa_wildcards.py`
  gains `missing-pages --via-engine` and a `crossing-diff` that checks the engine against the
  seams' simulation.
