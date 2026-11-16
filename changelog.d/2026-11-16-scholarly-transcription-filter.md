## [Unreleased] -- Filter results by scholarly transcription (web, branch `claude/scholarly-transcription-filter`)

After a search, **Filter scholarly transcriptions** cycles through all results, only the results
whose page has a scholarly transcription, and only those whose page has none (pages no one has
edited yet). It sits beside **Filter PGP**, which is unchanged: that one asks whether the manuscript
has any PGP record, this one whether the result's page has an FGP or PGP edition.

A page has one when the version chooser offers an edition with text on it: the chooser's own page
filter decides (`shared/fgp_service.py::filter_sources_for_page`), on the page, page count and text
Quick View opens the result with, so the filter, Quick View and /browse agree. Translations do not
count; a result found in a scholarly transcription has one by definition. The check
(`shared/transcription_service.py::rows_with_scholarly_transcription`) reads only manuscripts with
an edition at all and runs with the rest of the results' enrichment, visible page first. It reads
the FGP and PGP sidecars, so it works whether or not the editions search is switched on.

Kept with the run and across reloads like the PGP filter (`search_scholarly_filter`), reset by New
Search. Since the result filters stay on for the next search, a **Reset filters** button beside the
count shows while any of them is on (printed, PGP, scholarly transcription, library, domains, the
Toggle Filters panel) and clears them all; the manuscript and word-search exclusions keep their own
chips. Help describes both in both languages. Web only; no sidecar or API change.
