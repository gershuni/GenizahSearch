## [Unreleased] -- One text, one result, in whatever order its parts rank (web + desktop, branch `fix/fold-same-text-order`)

A search in the scholarly transcriptions could still show one text twice after 10.0.1: a
document on several fragments with several editions (PGP 1800, T-S 24.18 + T-S 12.634 +
T-S 16.223, in Gil's and Udovitch's editions and an FGP edition) was one result or two
depending on the order its rows ranked in. `shared/editions_search.py::fold_same_text` placed
each row once, in rank order, and never joined two results it had started; a row linking two
of them went into one. It now joins, after that pass, any two results that hold one text
(`_join_linked`: by manuscript first, then by a hit they share) under the same rules: one page
at most whose own text matched, and it leads; two identified pages only through a hit they
share; no edition at two places joined by manuscript alone. Code only.
