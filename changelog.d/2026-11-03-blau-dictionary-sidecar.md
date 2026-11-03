## [Unreleased] -- Blau dictionary sidecar: the data layer for a Judeo-Arabic companion (ci, branch `claude/blau-sidecar`)

Phase 1 of `docs/plans/BLAU_DICTIONARY_PLAN.md`: no UI yet, nothing served, nothing bundled.

- `scripts/build_blau_sidecar.py` builds `blau_data/blau.db` from the JSON published at
  blau-dict.netlify.app (Joshua Blau, *A Dictionary of Mediaeval Judaeo-Arabic Texts*, 2006;
  digital edition release 1.1): 3,047 roots, 3,665 sub-entries, 8,295 stems, 12,753 forms
  (Judeo-Arabic text, Arabic spelling, Hebrew and English glosses, flags, printed page, the
  segments for rendering), 41,970 citations with their bibliography ids, 337 sources, a `meta`
  table (schema and source versions, digests, row counts, build time, the site's credit line)
  and `forms_norm`, 41,932 lookup keys. The build fails on a missing, empty, truncated or
  non-v6 download and refuses to replace a sidecar that holds more rows unless `--force`.
- `shared/blau_normalize.py`: `normalize_ja`, the one key a dictionary form and a corpus token
  share (combining marks, the corpus's apostrophe/geresh and brackets dropped, finals folded),
  `normalize_arabic`, and `candidate_stems` (one proclitic layer and one pronoun suffix peeled;
  not a lemmatizer).
- `scripts/verify_blau_sidecar.py --expect-roots 3047 --expect-forms 12753` is the gate:
  integrity, schema, counts against `meta`, every lookup key recomputed with the running
  normalizer, dangling targets and sources; fails rather than skips.
- `blau_data/` is gitignored; one row in `docs/architecture/DATA_LIFECYCLE.md`.
