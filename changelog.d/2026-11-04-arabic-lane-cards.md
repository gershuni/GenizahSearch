## [Unreleased] -- Lane's Arabic-English Lexicon in its own tab on /browse (web, branch `claude/arabic-lane-cards`)

Phase B of `docs/plans/ARABIC_DICTIONARY_LAYER_PLAN.md` (§7.2). The owner passed the grading and
approved the wording on 2026-11-05; the server needs `arabic_data/arabic.db` before Lane shows.

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
- **Roots fixed after the grading (plan §5.4).** Two AI judges graded the 200-word sample, and
  the fix follows what they found. On the same sample, the root is right 75.0% → 92.2%.
  - The closed-class table grows from 141 to 181 rows: frequent forms filed under the wrong root,
    such as مولاي (not مول 'wealth'), ستين (not تين 'fig'), بن (not 'coffee'), قلت and كنت.
  - A card on a root Lane splits in two (بوع / بيع) is titled by the heading of its matched entry,
    or else by the larger heading.
- `shared/arabic_service.py`: the read-only, fail-closed reader of `arabic_data/arabic.db`
  (schema and normalizer versions, tables, lookup indexes, row counts, the credit line,
  `quick_check`); kill switch `LANE_DICTIONARY_ENABLED` (default on, read live).
- `shared/arabic_method.py`: the method v1-v4, moved verbatim out of
  `scripts/measure_arabic_readings.py` so the gate measures what is served (its numbers on the built
  file are unchanged).
- Deploy: `scripts/deploy_arabic_sidecar.ps1 -Entries 47890` (owner), as Blau's -- the gate on the
  local file, upload under a temporary name, the digest and the gate on the server, keep the served
  file, swap, restart, the gate on the served file; the code must be on the server first.
- `web/components/lane_panel.py` draws Lane's tab; `web/components/blau_panel.py` draws the tabs
  (`pick_tab` decides which opens), and `web/pages/browse.py` asks both dictionaries in one trip
  off the event loop.
