## [Unreleased] -- Arabic dictionary sidecar, phase A: Lane, a word count and a closed-class table (ci, branch `claude/arabic-lane-sidecar`)

Phase A of `docs/plans/ARABIC_DICTIONARY_LAYER_PLAN.md`, on the owner's rulings of 2026-11-04: no UI
yet, nothing served, nothing bundled.

- `scripts/build_arabic_sidecar.py` fetches its sources into an empty folder (every file pinned by
  sha256) and builds `arabic_data/arabic.db`: Lane's *Arabic-English Lexicon* (47,855 articles
  under 4,621 roots, from Ephraim Meiri's parse of the Perseus TEI; 35 entries the source gives no
  root are counted and left out), Lane's 42,351 one-word headwords as a form-to-root table, an
  OpenITI word count (808,441 rows from 247 million tokens of 2,816 texts dated 250-650 AH) and a
  141-row closed-class table (`scripts/arabic_closed_class.json`). The Perseus credit line and
  availability statement are checked against the source and kept in `meta`. It fails on a file
  that differs from its pin, a count the source contradicts or a closed-class row Lane cannot
  answer, and refuses to replace a sidecar that holds more rows unless `--force`.
- `shared/arabic_normalize.py`: a Judeo-Arabic word's Arabic readings (`readings`), the frequency
  key (`skeleton`) and the root key (`root_key`), moved out of `scripts/measure_arabic_readings.py`.
- `scripts/verify_arabic_sidecar.py --expect-entries 47890` is the gate: integrity, schema, counts
  against `meta`, every key recomputed with the running normalizer, the credit line; fails rather
  than skips.
- `scripts/measure_arabic_readings.py --sidecar` measures the plan's gate on the built file.
- `arabic_data/` is gitignored; one row in `docs/architecture/DATA_LIFECYCLE.md`.
