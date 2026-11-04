## [Unreleased] -- Lane's Arabic-English Lexicon in its own tab on /browse (web, branch `claude/arabic-lane-cards`)

Phase B of `docs/plans/ARABIC_DICTIONARY_LAYER_PLAN.md` (§7.2). Not deployed: the server needs
`arabic_data/arabic.db`, and the wording is a draft for the owner.

- **One tab per dictionary** (owner, D12): the dictionary panel has a tab for Blau and one for
  Lane, between its head and its body, so a long entry in one never hides the other. It opens on
  the tab the reader last chose while that dictionary has something for the word, else on the
  first that has; a tab with nothing is greyed. The panel is there when either dictionary is
  served (one alone: no tab row, its name in the head). Its title, hint and switch name no one
  dictionary ("Dictionaries", "Click a word to look it up in the dictionaries").
- In Lane's tab: the clicked or typed word's Arabic
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
- `web/components/lane_panel.py` draws Lane's tab; `web/components/blau_panel.py` draws the tabs
  (`pick_tab` decides which opens), and `web/pages/browse.py` asks both dictionaries in one trip
  off the event loop.
