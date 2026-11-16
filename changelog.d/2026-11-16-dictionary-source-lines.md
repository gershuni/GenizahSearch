## [Unreleased] -- Dictionaries name their source at the top, and show no internal names (web, branch `claude/dictionary-friedman-notes-c8621c`)

Each dictionary tab (Blau, Friedman, Lane) now opens on its author and title, linked to the
dictionary's site, always in view at the top (`blau_panel.SOURCES`, `_render_source`); the full
credit line stays at the foot. Under tabs, Friedman's and Lane's separate section titles are gone,
since the source line names them.

No internal name or lookup key reaches the reader any more: "Possible spelling", Friedman's "found by
its base form" line and Blau's "Looked for" list show the word with its final letters (ממלך, not the
key ממלכ; `shared/ja_roots.with_final_letters`), and the match-details tooltip (ⓘ), which printed the
internal step names (`mater → source_suffix`), is gone: the lookup does not explain how it reached
a match.

Both lookups drop a match that needs both a spelling the word does not have (a yod or waw added or
dropped, צ/ט) and a form the entry does not print (`shared/blau_normalize.stacked_variant`): ממלוך no
longer reaches ממלכה̈ ("fiancée") as ממלך with the feminine ending. Of 4,000 corpus words, 30 had such
matches in Friedman, almost all wrong (יהודה -> באליהודיה, לעולם -> לעלמה); Blau had none.
No sidecar or setting changes.
