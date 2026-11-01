## P2 data: The April "Instatution" fix is missing from the FJMS sidecar, in production too
Where: `fist_data/fjms_enrichment.db` (catalog, catalog_free_desc); `scripts/fix_instatution_sources.py`; Phase 85-03 export (`a9807a6a`, 2026-05-08)
Status: ❌ Partly fixed (web 2026-11-01; desktop with the next installer)

On 2026-04-21 `scripts/fix_instatution_sources.py` rewrote 267,104 `catalog` rows and 47,800 `catalog_free_desc`
rows whose `SourceName` was the misspelled 'Instatution' into the named teams (GRU - Cambridge, the Fleischer Piyut
Project, Uri Ehrlich, ...), so that about 30,625 manuscripts showed their catalogue dialogs and scholarly-source
counts (`.planning/quick/260421-handoff.md`). The local sidecar (saved 2026-05-11, after the Phase 85-03 re-export
of 2026-05-08) again has 267,481 'Instatution' rows (read-only count, 2026-11-01): the fix is a post-export step and
the re-export did not re-run it. **Production too** (owner's read-only check, 2026-11-01): the server copy (saved
2026-05-12) is identical in size (1,589,698,560 bytes), meta (5.0.0, created 2026-03-11), catalog rows (731,354) and
'Instatution' rows (267,481 + 47,809), so production has shown those manuscripts without their team names since
2026-05-12. Fix sketch: re-run the script (idempotent; needs the
local `fist_data/FIST.db`) as part of the 5.1.0 build of `docs/plans/CATALOGUE_CRITERIA_PLAN.md` (D19), and make the
export pipeline run it after every re-export.

**Hotfix built 2026-11-01, awaiting upload (owner):** the April script re-run on a copy,
`fist_data/fjms_enrichment.hotfix.db` (1,618,018,304 bytes, sha256 `031c7bb6…851079e`): 267,481 + 47,809 rows
resolved (100%; GRU – Cambridge 161,593, Schocken – Zulay 51,258, Fleischer 30,375, Yad Harav Herzog 15,751, Uri
Ehrlich 8,114, …); every other table's row count unchanged; `PRAGMA quick_check` ok. Through `FjmsService`: of the
140,328 manuscripts with such rows, 109,700 showed a named source before and 140,328 after (30,628 regained theirs).
The desktop installer bundles the local file, so the next installer must be built from the fixed file too.

**Web fixed 2026-11-01:** the owner uploaded the hotfix and swapped it in on the server; a read-only count there
gives 0 'Instatution' rows in `catalog` and `catalog_free_desc`. The local `fist_data/fjms_enrichment.db` is the same
file (sha256 `031c7bb6…851079e`); the old one is kept as `fist_data/fjms_enrichment.pre_instatution_20261101.db`.
**Still open:** desktop users have the old file until the next installer, built from the fixed local file. Close
this issue when that installer ships.
