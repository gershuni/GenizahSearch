## [Unreleased] -- Search index format 2: page-break documents and a word-ending lookup (web + desktop, branch `claude/responsa-wildcards-phase2-b10c68`)

Phase 2 of `docs/plans/RESPONSA_TRUE_WILDCARDS_PLAN.md` (design: §6.1). A rebuilt index carries
two additions the coming wildcard search (`*` as any letters, Phase 3) reads; nothing searches
through them yet, so results are unchanged.

- **Seam documents**: one small document for each place where one page of a manuscript runs on
  into the next in reading order (the same transcription, the same volume, the next page
  number), holding the 30 words on each side, with where each slice stands in its page. Oxford
  parts also join the last page of one folio to the first of the next. 1,541,186 seams over the
  V0.8 and V0.7 files; they live in their own index inside `tantivy_db/seams/`, so nothing that
  reads the main index meets them.
- **A reversed copy of every word** (`content_rev`) on every document, so a word's ending is a
  fast lookup.
- **A format marker**, `tantivy_db/index_format.json`, written last. An index without it (every
  index built before this) is searched exactly as today; the desktop's Settings says which
  format the index has, and rebuilding stays the user's click.
- `build_index.py main --index-dir <folder>` builds into a new folder beside the live one (with
  `--corpus-v8`, `--corpus-v7`, `--caches-from`, `--no-seams`, `--no-reversed-field`);
  `docs/guides/DEPLOYMENT_TECHNICAL.md` has the server's rebuild, swap and rollback.
- New checks in `scripts/measure_responsa_wildcards.py`: `seam-plan`, `index-sample`,
  `seam-topology`, `reversed-check`, `crossing-diff`.
