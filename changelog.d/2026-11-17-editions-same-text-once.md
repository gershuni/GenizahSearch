## [Unreleased] -- The same text shown once in a search with the scholarly transcriptions (web + desktop, branch `fix/editions-same-text-once`)

A search with the scholarly transcriptions showed one text again and again: a PGP edition of a
document on several fragments is indexed once per fragment, and an edition whose page could not be
identified was a result of its own beside its manuscript's other results.
`shared/editions_search.py::fold_same_text` now folds a result found in scholarly transcriptions
alone into an earlier result holding the same hit (the same edition, the same match), or, when its
page was not identified, into an earlier result of the same manuscript. A result whose automatic
text matched is never folded, and a second match of an edition on another page stays a result.
The result keeps the folded editions among its sources and names the other fragments of the document: web
"Other parts of this document" under the card (links), desktop the Shelfmark and Src tooltips, both apps' exports in
the Source cell, the API `also_in`. Completing a step (`ids_only`) still returns every match.
Desktop: the results table's tooltip handler answered every request itself and served only cut-off
text and the PGP cell, so a cell's own tooltip never showed (v10.0.0's Src "Matched in" and Img
"Page not identified" among them); it now serves it, and the Title and Library cells' full names
show on hover too. Measured on the local editions index: 37 -> 17, 9 -> 2 and 4 -> 1 results for three phrases of
joined PGP documents.
