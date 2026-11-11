## [Unreleased] -- "Only long passages" in letter-level Composition Search and Parallels (web + desktop, branch `claude/search-long-segments-option-e7a9d6`)

A third choice under **Passage length**, beside *Normal passages (default)* and *Also short
passages*: **Only long passages** keeps only manuscripts whose match covers at least 120 letters
of the text (half the text, when it is shorter). Asked for by a desktop user, for searches at
*Very wide* and *Maximal*, where the list had grown past reading — the desktop has shown every
letter-level result, with no 200-manuscript limit, since 9.7.0.

- `shared/passage_policy.py`: the `long` profile (`LENGTH_PROFILES`, `LENGTH_RECORD_FLOORS`), a new
  policy field `min_record_letters` (in `policy_id` only when non-zero, so every existing policy
  keeps its id) and `PassagePolicy.record_floor()`.
- `shared/passage_search.py::search_passage` drops a record scoring under the floor after merging,
  and counts it in `QueryReport.rejected_below_floor`. "Filter texts" conditions search without the
  floor (`PassageSearcher.search_condition`), so a "must not appear" text is still found in a
  short passage.
- Desktop Composition Search and the website's Parallels page offer it, with the tooltip and Help
  in English and Hebrew. The public API and MCP do not take a passage length and are unchanged.
- Measured on the production index (spec section 8.2): on 398 FGP queries it removes 45-49% of the
  rows at Very wide, Maximal and Maximal + Deep, losing 1 of ~375 known source manuscripts and 6-8
  of ~152 owner-confirmed matches.
