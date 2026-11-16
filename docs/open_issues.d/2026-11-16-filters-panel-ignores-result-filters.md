## P3 web: The Toggle Filters panel drops the other result filters
Where: `web/pages/search.py::apply_filters`, `clear_filters`

The panel's **Apply Filters** and **Clear Filters** start from every result row
(`search_state.results`) and apply only their own shelfmark/title/text fields and the measurement
filters, so the rows hidden by **Filter Printed**, **Filter PGP**, the library filter, the domain
exclusions, the manuscript exclusions and the word-search exclusions come back while those
buttons still show as active, and the count line ("N / total Results") does not name them. Found
2026-11-16 in a review of the scholarly transcription filter, which the panel now keeps (its own
fix); the others predate it.

Fix sketch: run the panel's rows through the same cascade the buttons use (`_apply_manuscript_exclusions`
→ word-search → domain → printed/PGP/scholarly/library → measurements), with the panel's fields as
one more step, and give the count line the same parts.
