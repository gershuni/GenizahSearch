## P3 browse: The page's "V0.8" source badge shows on a page that has no automatic transcription
Where: `web/pages/browse.py` (the source badge beside "Page N": `'V0.7' if 'V0.7' in page.full_header else 'V0.8'`)
Opened: 2026-10-29

Browse labels every page's automatic text "V0.8" (or "V0.7") beside the page number. On a page without any automatic
transcription -- since 2026-10-27 it opens on its FGP or PGP edition, and its `full_header` is empty when the index
holds no pages of the manuscript -- the badge still says "V0.8" while an edition is shown and no V0.8 text exists
(seen on a local server, 2026-10-29: T-S NS 243.145 with its FGP edition; the version chooser beside the text says
"FGP"). Quick View's info bar has the same chip from the search result's `display['source']`, which is right for the
result's own page but not for another page reached by paging. Fix sketch: no badge when the page has no automatic
text (`not page.text`); the version chooser already names the text shown.
