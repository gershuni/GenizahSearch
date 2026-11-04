## [Unreleased] -- Lane's Arabic-English Lexicon under Blau's cards on /browse (web, branch `claude/arabic-lane-cards`)

Phase B of `docs/plans/ARABIC_DICTIONARY_LAYER_PLAN.md` (§7.2). Not deployed: the server needs
`arabic_data/arabic.db`, and the section's wording is a draft for the owner.

- In Blau's dictionary panel, after Blau's cards and credit: the clicked or typed word's Arabic
  readings as chips, the commonest in an OpenITI word count first (a click looks another reading up
  in the box), and the first reading's root card with Lane's entries, those whose headword is the
  reading first, cut short and linked to the entry on semitic.lang.emeiri.org. On every Lane
  section, the Perseus credit line verbatim, then the entries' source and the ranking's. A word
  typed in Arabic letters is its own reading.
- `shared/arabic_service.py`: the read-only, fail-closed reader of `arabic_data/arabic.db`
  (schema and normalizer versions, tables, lookup indexes, row counts, the credit line,
  `quick_check`); kill switch `LANE_DICTIONARY_ENABLED` (default on, read live).
- `shared/arabic_method.py`: the method v1-v4, moved verbatim out of
  `scripts/measure_arabic_readings.py` so the gate measures what is served (its numbers on the built
  file are unchanged).
- `web/components/lane_panel.py` draws the section; `web/components/blau_panel.py` and
  `web/pages/browse.py` ask both dictionaries in one trip off the event loop.
