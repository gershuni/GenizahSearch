## P3 web: The Toggle Filters panel drops the other result filters
Where: `web/pages/search.py::apply_filters`, `clear_filters`

The panel's **Apply Filters** and **Clear Filters** start from every result row
(`search_state.results`). **Apply Filters** now keeps the printed, PGP, scholarly transcription
and library filters (the result filter chips re-render through it while a panel field is set), but
it still drops the domain exclusions, the manuscript exclusions and the word-search exclusions,
and its count line ("N / total Results") does not name the filters it kept. **Clear Filters**
keeps only the scholarly transcription filter, so the rows hidden by the printed, PGP and library
filters and by the exclusions come back while those chips still show as active. Found 2026-11-16
in a review of the scholarly transcription filter; the rest predate it.

Fix sketch: run the panel's rows through the same cascade the chips use (`_apply_manuscript_exclusions`
→ word-search → domain → printed/PGP/scholarly/library → measurements), with the panel's fields as
one more step, in both Apply and Clear, and give the count line the same parts.
