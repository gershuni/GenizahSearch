## [Unreleased] -- Responsa wildcards, Phase 0: the measurements (ci, branch `claude/responsa-wildcards-phase0-8f917f`)

- `docs/plans/RESPONSA_TRUE_WILDCARDS_PLAN.md` gains §3.6, "Phase 0 results": a number, the command
  that produced it, the machine and the wall time for each of the seven items of §7 Phase 0 -- the
  reversed word list beside the index, bracket- and mark-tolerant pattern lookups and the pattern
  size limit, scored term queries with 600 and 1,200 forms, the pages a brute-force scan finds for
  `*נים *לים` that the index path misses, the junctions inside whole-manuscript and part documents
  that are not consecutive pages, today's desktop timings for the §8 query set on both regex
  engines, and the Hebrew-block punctuation kept inside index tokens. The status line now says which
  owner decisions (§9) these numbers inform. No product code: nothing under `shared/`, `web/` or
  `desktop/` changes.
- `scripts/measure_responsa_wildcards.py`: the measurement script, one subcommand per item, so the
  owner can rerun it on a Linux worker (`--index-dir`, `--corpus-file`). Read-only on the index and
  the corpus file; Hebrew goes to UTF-8 files, never to the console; the `*נים *לים` oracle is written
  by hand (Exact's word predicate, a letters-only star) and imports neither `build_regex_pattern` nor
  the compiler, as §8 gate 1 requires.
