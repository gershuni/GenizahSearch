## [Unreleased] -- Blau's dictionary on /browse: click a word for its entry (web, branch `claude/blau-browse`)

Phase 2 of `docs/plans/BLAU_DICTIONARY_PLAN.md`, website only (owner ruling (b), 2026-11-03).

- On `/browse`, a click on a word of the transcription opens Joshua Blau's *Dictionary of
  Mediaeval Judaeo-Arabic Texts* (2006, blau-dict.netlify.app) beside the word (a sheet at the
  foot of a phone): one card per entry the word reaches -- the entry name, the Arabic spelling,
  the printed page, the senses with their Hebrew and English glosses, binyan and flags, the
  matched form first -- in the order the word's candidate stems were tried (the word, then one
  proclitic peeled, then one pronoun suffix), with a link to the entry on the site; a word found
  only inside the dictionary's example phrases is listed apart. The site's credit line is on
  every panel (ruling (a)). `web/components/blau_panel.py`, `web/static/js/blau_lookup.js`.
- `shared/blau_service.py` reads `blau_data/blau.db` read-only and fail-closed: a file that is
  absent, of another schema version, keyed by another normalizer, without the credit line,
  without the lookup index, with a row count that disagrees with `meta`, or that fails
  `quick_check` is not served, and the page is then what it was before (no script, no panel).
  Kill switch `BLAU_DICTIONARY_ENABLED=0`. The sidecar is checked once at startup.
- Deploy: `scripts/deploy_blau_sidecar.ps1` (owner) -- the gate on the local file, upload under a
  temporary name, the gate on the server copy, swap, restart, the gate on the served file.
