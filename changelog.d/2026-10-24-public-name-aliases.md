## [Unreleased] -- Everyone has a public name: aliases and a one-time question (web, branch `master-main`)

- After the 2026-10-21 hardening took the email out of the username, 164 of 193 accounts had no
  public name, and every page that credits a contribution -- corrections, comments, joins, the
  citation of a corrected text -- showed them as "Unknown" (38 of the 51 visible corrections).
- `migrations/profiles_public_aliases.sql` gives each of them an alias such as "Curious Scribe" or
  "Patient Copyist" (40 adjectives x 40 Genizah-world trades and callings, then "of <city>" on a
  clash), and new sign-ups without a username of their own get one at sign-up. Every surface,
  installed desktop versions included, already falls back to the username, so all of them show
  it. Undo statement at the end of the file; dry run as written.
- The website asks each person with an alias once, on any page after sign-in: "How should your
  name appear?", prefilled with the name their Google sign-in carries (never published unless
  they save it). Save sets the full name; "Keep this name" keeps the alias. Both are remembered
  in the new `profiles.public_name_confirmed`, so the question is asked once per account, on
  any device (`web/public_name.py`). Nothing is asked before the migration runs.
- Author lookups that ignored the username now use it: published joins on browse
  (`web/components/joins_panel.py`, `shared/puzzle_publish_service.py::resolve_author_profiles`,
  which also returned None for a profile without a full name) and the pending list on
  /corrections.
- Pinned by `tests/test_public_name.py` (the real dialog, off-loop write with the visitor's
  client) and `tests/test_profiles_public_aliases_sql.py`; verification on the live project:
  `scripts/verify_profiles_public_aliases.sql`.
