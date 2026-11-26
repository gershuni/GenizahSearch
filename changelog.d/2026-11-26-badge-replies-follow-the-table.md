## [Unreleased] -- A late PGP badge reply no longer blanks a PGP-tag search's column (desktop, branch `claude/gs-11-badge-generation`)

GS-11. After a text search the desktop starts three workers that fill the PGP,
scholarly-transcription, printed and domain information. Their replies went
straight to the table, so a reply that arrived after a PGP-tag search had
replaced the results overwrote the tag search's PGP set and blanked its PGP
column and links.

- The table now carries `_results_table_generation`, advanced whenever its
  result set is replaced: a text search starting or finishing (and a restored
  one), a PGP-tag search, an error, New.
- `_launch_enrichment_workers` takes the generation when it is called, before
  the 500 ms deferral of a session restore: a deferred start for a table that
  was replaced starts nothing, and a reply for one is dropped.
- It is kept apart from `_search_new_generation`, which a tag search must not
  advance: a text search still running would lose its results and leave the
  tab "searching".
- First PR of the desktop search-run plan (private repository). Tests:
  `tests/test_badge_replies_follow_the_table.py`.
