## [Unreleased] -- Responsa search: whole words, every grammatical form, and bracketed words found (web + desktop, branch `claude/responsa-wildcards-phase1-36d505`)

Phase 1 of `docs/plans/RESPONSA_TRUE_WILDCARDS_PLAN.md`: one meaning of a Responsa query on every
search path -- the main search, the line search (`|`), My Library, both together, and the complete
pass that search-within relies on.

- **Every word is a whole word**, the first and the last included, by the same rule as Exact:
  `כהנים גדול` finds `כהנים גדול` and no longer `כהנים גדולים`; a lone `ספר` finds the word ספר,
  not `מספרים`. The rule holds inside each `AND` word, in both directions of a bidirectional
  search, and at each line's edges; with flexible spacing it bounds the joined words as one unit.
  Result counts of existing searches change accordingly (owner decision 8, 2026-10-21).
- **`#word#` searches every combination of prefixes and suffixes** (600 forms per word): the
  limit that dropped the suffixes now counts the terms a search really looks up, measured on the
  real index, and is 5,000 (`Config.MAX_EXPANDED_TERMS`). When a search has to turn an option off,
  the note stays beside the result count (it was a five-second message).
- **A word with a bracket inside it is found**: `שלום` finds `של[ו]ם`, on the website, the desktop
  and in My Library, and the bracket stays in the highlight.
- **Excluded words read the page the match is on**, in every mode (a Responsa `-word` and the
  "Exclude Words" box of Exact, Variants and Fuzzy): a result is no longer hidden because the word
  appears on another page of the same manuscript (owner decision 7). In My Library, excluded words
  now apply to every search.
- A `*` inside a word, a word made only of `*`, and other shapes with several `*` are not searched
  yet: the website, the desktop, Joins Lab (both sides) and the API say so (API: 400
  `responsa_unsupported_star`), instead of showing no results. `*word`, `word*` and `*word*` work as
  before.
- Website: the expanded result text highlights words written with an ASCII quote (`ר"ת`): it is
  now matched before it is escaped.
- Line search results mark the matched words, not their whole lines.
- `shared/responsa_match.py`: the word compiler. Retrieval per index (each word's scored terms,
  ORed with a bracket- and Hebrew-mark-tolerant pattern lookup of the same forms); the check
  pattern; the highlight pattern. Its gate, `scripts/verify_responsa_wildcards.py`, compares the
  engine with an independent reading of `Transcriptions.txt` on the real index: all nine Phase 1
  queries agree, page and span (2026-10-22).
