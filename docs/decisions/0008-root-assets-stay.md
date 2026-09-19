# 0008 -- Root data assets stay where `Config` and the spec resolve them

- **Status:** binding until the root-finder consolidation (Round 2 backlog)
- **Date:** 2026-09-18
- **Source:** [shared/config.py](../../shared/config.py) (`BASE_DIR` is two levels above the module
  and, un-frozen, `INTERNAL_DIR = BASE_DIR`; `Transcriptions.txt` and `AllGenizah_OLD.txt` are
  expected next to the executable) and the `datas` list in
  [GenizahSearchPro.spec](../../GenizahSearchPro.spec), which bundles by literal path.

## Decision

The root data assets stay at the repository root. `libraries.csv`, `oxford_full_db.json`,
`Help.html` and the transcription inputs (`Transcriptions.txt`, `AllGenizah_OLD.txt`) are resolved
by `shared/config.py`; `icon.ico`, `ie_volume_map.json`, `bodleian_master_index.csv` and
`libraries_translations.db` are bundled by literal path in the spec (and resolved at runtime by
`browse_map_utils.py`, `settings_dialogs.py`/`genizah_app.py` and `translation_service.py`
respectively); `cambridge_genizah.json`, `char_merges_report.xlsx` and the `fist_gap_*` files are
resolved by the `scripts/` and tests that read them, relative to the repo root. Eight
`shared/*_service.py` modules also walk up to five parents until they find `libraries.csv` and
treat that directory as the project root, so the file doubles as a sentinel (seven in a helper
named `_find_project_root`, one inlined in `puzzle_image_service.py`).

## Why

Three independent mechanisms resolve these paths (`Config`, the spec, the per-module root finders).
Moving an asset means changing all three in one commit and re-verifying the frozen build. That is a
consolidation project, not a tidy-up.

## Consequences

- A new data asset goes under a named data directory with a `Config` attribute, not at the root.
- Round 2 consolidates the eight root finders and the `"GenizahSearchPro"` literals onto
  `shared/config.py`; until then, moving any listed asset is out of scope.
- `.gitignore` rules still match several of these tracked assets (`bodleian_master_index.csv`,
  `char_merges_report.xlsx`, the `fist_gap_*` files, `GenizahSearchPro.spec`); they stay in the
  repository because they are already tracked; since 2026-09-18 `.gitignore` ends with explicit
  negations for them, so a deleted-and-recreated copy, or a clean clone, can re-add them
  (`git check-ignore --no-index -q <path>` exits 1).

## Supersedes

Nothing.

## Enforced by

[shared/config.py](../../shared/config.py) and [GenizahSearchPro.spec](../../GenizahSearchPro.spec)
by construction; the packaging smoke test once it runs against a fresh build.
