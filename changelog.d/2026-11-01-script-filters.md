## [Unreleased] -- Search by script style and script place (web + desktop, branch `script-filter`)

A user asked whether the site can find manuscripts by their script, as the Friedberg site
could. The catalogue's script fields (FJMS `catalog_fields` `TypeOfScriptStyle` and
`TypeOfScriptPlace`) were shown in each manuscript's catalog details but nothing could
search by them.

- **"Search only in..."** on /search, /parallels and the desktop dialog has **Script Style**
  (Square, Semi-Cursive, Cursive, Rabbinical, Naskhi, Karaitic...) and **Script Place**
  (Oriental, Spanish, Syrian, Yemenite, Ashkenazi, North African, Italian, Byzantine,
  Persian...), each choice with its manuscript count, Hebrew labels in the Hebrew UI. Any
  chosen value in a list matches; the two lists AND with each other and with the other
  filters; the Include/Exclude toggle does not turn them around. A manuscript matches when
  any of its catalogue records gives the value. Manuscripts whose records describe no script
  are not included, and the panel says so. Coverage in today's sidecar: 45,125 manuscripts
  have a script style, 36,937 a script place, of 226,555 catalogued.
- Chips, the history summary, saved searches, Back to a run and Clear All carry the two
  lists; the desktop main window shows them as chips.
- **API / MCP:** `filters.script_styles` and `filters.script_places` on `/api/search` and
  `/api/parallels` (and the MCP `filters`); unknown values -> 400 `unresolvable_filter_value`,
  no vocabulary -> 503 `filter_vocabulary_unavailable`.
- `shared/fjms_service.py`: `get_filter_sys_ids(script_styles=, script_places=)`,
  `FjmsService.get_script_values`, `script_filter_options`, `script_value_label`,
  `SCRIPT_FILTER_CATEGORIES`. Tests: `tests/test_script_filters.py`,
  `tests/test_script_filters_desktop.py`.
