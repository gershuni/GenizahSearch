# GenizahSearch -- Data Lifecycle

> Last updated: 2026-09-18
>
> One row per data artifact: what produces it, who reads it, which class it belongs to, whether it
> is tracked, bundled into the desktop installer or provisioned by hand, **where its location is
> decided**, how to rebuild it, what validates it, and how it reaches production. Companion to
> [OVERVIEW.md](OVERVIEW.md). Every row was verified against the code on 2026-09-18 (path:line
> references are to that state; grep before trusting a line number).

## Classes

| Class | Meaning | Deleting it |
|---|---|---|
| **Input (owner-supplied)** | arrives from outside the repo; nothing in-repo can regenerate it | you need the source again |
| **Read-only reference** | tracked or provisioned data the apps read but never write | restore from git or re-provision |
| **Corpus index (rebuilt by build)** | derived from the inputs by a documented build; content is a function of the corpus | rebuild (minutes to an hour) |
| **Mutable personal state** | written by the desktop app for one user | that user loses settings, history or their My Library index |
| **Mutable shared state** | written by the web server or Supabase for all users | users lose data; no rebuild |
| **Secrets / session** | credentials, tokens, salts, the masking vocabulary | rotate; re-authenticate |
| **Disposable / generated** | caches, stubs, evidence dumps; safe to delete | regenerated on demand or not needed |

## Where locations are decided (the three mechanisms)

1. **`Config`** ([shared/config.py](../../shared/config.py)) decides the desktop's roots: `BASE_DIR`
   (repo root from source; the executable's directory when frozen), `INTERNAL_DIR` (`_internal/`
   beside the EXE, or `_MEIPASS`), and `INDEX_DIR`, chosen at import in this order: a portable
   `Genizah_Index/` under `BASE_DIR` if it exists, else the legacy `~/Genizah_Tantivy_Index` if
   the AppData path does not exist yet, else `%LOCALAPPDATA%\GenizahSearchPro\Index`. Every
   derived path (`CONFIG_FILE`, `SESSION_FILE`, `LAB_INDEX_DIR`, `LOCAL_INDEX_DIR`, ...) hangs off
   `INDEX_DIR`. Only three reference assets are `Config` attributes: `LIBRARIES_CSV`, `OXFORD_DB`,
   `HELP_FILE`. The one derived path that does not hang off `INDEX_DIR` is `REPORTS_DIR`, which
   prefers `Documents\GenizahSearchPro\Reports` and falls back to `INDEX_DIR/Reports`.
   `build_index.py` creates the portable `Genizah_Index/` before importing anything, which is why
   the web server uses the repo-root copy rather than AppData.
2. **Module-private root finders.** Eight `shared/*_service.py` modules walk up to five parents
   looking for `libraries.csv` and treat that directory as the project root. Six of them
   (`document_service`, `fgp_service`, `fjms_service`, `nli_crossref_service`,
   `translation_service`, `visual_similarity_service`) first check a
   `%LOCALAPPDATA%\GenizahSearchPro\data\<dir>\<file>` override; the two puzzle modules use
   AppData only as a fallback when no root is found or the root is read-only
   (`puzzle_service.py` falls back to `%LOCALAPPDATA%\GenizahSearchPro\joins_data\joins.db`).
   Seven share the name `_find_project_root`
   ([shared/document_service.py](../../shared/document_service.py),
   [shared/fgp_service.py](../../shared/fgp_service.py), [shared/fjms_service.py](../../shared/fjms_service.py),
   [shared/nli_crossref_service.py](../../shared/nli_crossref_service.py),
   [shared/puzzle_service.py](../../shared/puzzle_service.py),
   [shared/translation_service.py](../../shared/translation_service.py),
   [shared/visual_similarity_service.py](../../shared/visual_similarity_service.py)); the eighth is
   inlined in [shared/puzzle_image_service.py](../../shared/puzzle_image_service.py). The web
   loaders do the same with two `dirname()` calls: [web/discovery_assets.py](../../web/discovery_assets.py),
   [web/atlas_assets.py](../../web/atlas_assets.py), [web/passage_assets.py](../../web/passage_assets.py).
   Consolidating these onto `Config` is a Round 2 item; until then a data asset moves only if all
   its finders move with it.
3. **Environment variables**, read once at import: `GENIZAH_DISCOVERY_DATA_DIR`,
   `GENIZAH_PASSAGE_DATA_DIR` (dev/CI overrides for the two web loaders), `MASKING_SCAN_PATTERNS_FILE`,
   and the secrets in `.env`. Reference: [docs/guides/ENV_VARS.md](../guides/ENV_VARS.md).

**Distribution has two channels and they differ.** The desktop installer ships everything in the
`datas` list of [GenizahSearchPro.spec](../../GenizahSearchPro.spec) plus whatever the import graph
from `genizah_app.py` pulls in; Inno Setup copies the whole `dist/` tree. The web server gets
**only tracked files**: [deploy.sh](../../deploy.sh) is `git fetch`, `git reset --hard`,
`pip install`, `systemctl restart genizah-web` -- no data step. Everything gitignored on the server
(corpus, indexes, sidecars, `.env`, `discovery_data/`, `atlas_data/`) is placed by hand or by `scp`
and survives a deploy only because it is ignored ([docs/guides/DEPLOYMENT_TECHNICAL.md](../guides/DEPLOYMENT_TECHNICAL.md)).

---

## 1. Inputs (owner-supplied)

| Artifact | What | Read by | Location decided at | Tracked / shipped | Notes |
|---|---|---|---|---|---|
| `Transcriptions.txt` (~1.4 GB) | the V0.8 HTR corpus every index is built from | `shared/indexer.py`, `shared/lab_engine.py`, `shared/browse_map_utils.py`, `scripts/build_passage_index.py` | `Config.FILE_V8` = `BASE_DIR/Transcriptions.txt` | gitignored; next to the EXE (desktop) or at the server checkout root; not bundled | source: Zenodo, linked from the installer's post-install task and `DEPLOYMENT_TECHNICAL.md`; `scripts/rebuild_index.sh` refuses to build unless its SHA-256 matches; repairs are applied by `scripts/apply_v08_repair.py` from the tracked `data/v08_repair_manifest.json` |
| `AllGenizah_OLD.txt` (~1.6 GB) | the optional legacy V0.7 corpus; indexed as a second source when present | same indexers; `shared/metadata_manager.py` (file-map cache); `build_index.py` treats it as optional | `Config.FILE_V7` | gitignored; optional | every consumer guards with `os.path.exists`; absence is a supported state. `DEPLOYMENT_TECHNICAL.md` calls it `Genizah_OLD.txt`, the code says `AllGenizah_OLD.txt` |
| `cambridge_genizah.json` (19 MB) | CUDL IIIF manifest dump; a **build-time** input for `nli_crossref.db` | `scripts/import_nli_crossref.py` only; no runtime reader | script-private (`project_dir / "cambridge_genizah.json"`) | tracked at the root, so it also lands on the server although only the build needs it | listed in `.gitignore`'s "intentional root assets" comment |
| `nli_crossreference.csv` | NLI cross-reference export, the other input of `nli_crossref.db` | `scripts/import_nli_crossref.py` | script-private | untracked, owner-held | |
| `fist_data/FIST.db` (3.2 GB) and `fist_data/FIST_Computed_Measurements.xlsx` (275 MB) | the FIST backup, source of the FJMS enrichment and visual-similarity sidecars, and its measurements workbook | `scripts/export_fist_enrichment.py`, `scripts/import_visual_similarity.py` | CLI arguments (`--fist-db`) | gitignored (`fist_data/*`); the repo-root `FIST.db` is a **0-byte stub** | owner-held; not on the server |
| FGP source tree (`fgp_data/transcriptions/`, `metadata/`, `manifest*.{csv,json}`) | PDF/XML transcriptions copied from an external drive | the `fgp_data/fgp_*.py` extractor scripts | hard-coded absolute paths inside those scripts | the whole `fgp_data/` directory is gitignored, **including the producer scripts and the schema README** | single-machine dependency: the code that builds this sidecar is not in version control |
| `reports/synthetic_parent_shelfmarks.csv` | 175 parent shelfmarks synthesized as containers although real children exist (Phase 86 audit input) | `scripts/generate_synthetic_rows.py`, `scripts/scan_cudl_coverage_phase86.py` | script-private `ROOT / reports` | tracked (data-only commit; nothing in the tree writes it) | readers degrade to an empty set if missing |
| `char_merges_report.xlsx` (34 MB) | V0.7-vs-V0.8 character-merge statistics; the input of the generated `unified_variants.py` | `scripts/generate_unified_variants.py` | CLI argument | tracked at the root although `.gitignore` also matches it (inert for a tracked path) | produced by `scripts/analyze_char_merges.py` from the two corpora, so strictly it is derived -- but the corpora are not in the repo |

## 2. Read-only reference (tracked, or provisioned sidecars)

### 2a. Root assets

| Artifact | What | Read by | Location decided at | Bundled / server | Rebuild | Validation |
|---|---|---|---|---|---|---|
| `libraries.csv` (49 MB) | master catalogue, ~255K rows; also the **sentinel** the eight root finders look for | `shared/metadata_manager.py` (`_load_csv_bank`), `shared/browse_map_utils.py`, build scripts | `Config.LIBRARIES_CSV` (`INTERNAL_DIR`) + the eight finders | spec `datas`; server via git | no full regeneration; mutated in place by `scripts/generate_fist_gap_csv.py`, `scripts/generate_synthetic_rows.py`, `scripts/fix_nli_oxford_mislabel.py` | `tests/test_nli_oxford_attribution.py` reads the real file; `scripts/check_sys_id_prefixes.py` |
| `oxford_full_db.json` (8.8 MB) | Oxford/Bodleian part-and-folio structure | `shared/codicological.py`; `scripts/translate_oxford_metadata.py` (build-time reader) | `Config.OXFORD_DB` | spec; server via git | no producer in the repo | none (tests substitute a fixture) |
| `ie_volume_map.json` (1.5 MB) | IE id to NLI IIIF volume map | `shared/browse_map_utils.py` (`_load_ie_volume_map`) | inline `Config.INTERNAL_DIR` join in `browse_map_utils.py` | spec; server via git | `python scripts/build_ie_volume_map.py --local` (or `--server`) | `scripts/validate_ie_volume_map.py` (network, sampled) |
| `Help.html` (121 KB) | bilingual desktop help; also read by the privacy-disclosure tests | `genizah_app.py`, `desktop/settings_dialogs.py`, `tests/test_privacy_disclosure_strings.py` | `Config.HELP_FILE`, then **reassigned** in `genizah_core.py` to `Config.resource_path("Help.html")` (same value) | spec; web app does not use it | hand-edited | `tests/test_privacy_disclosure_strings.py` |
| `icon.ico` | EXE resource icon, installer icon, dialog icon | `GenizahSearchPro.spec`, `CompileScriptGenizah.iss` (absolute `C:\GenizahSearch\icon.ico`), `desktop/settings_dialogs.py`, `genizah_app.py` | **two inconsistent rules**: `Config.BASE_DIR` in `settings_dialogs.py`, `resource_path()` (`INTERNAL_DIR`) in `genizah_app.py` | spec | hand-authored | none |
| `bodleian_master_index.csv` (89 KB) | Bodleian shelfmark index | **no code reader anywhere** | nowhere | still in spec `datas`, so it ships in every installer | none | none. `.gitignore` already files it under "legacy, unreferenced"; dropping it from the spec is an owner call (Round 2 backlog) |
| `unified_variants.py` (1 MB, generated) | 25,802 character-variant pairs behind the variant slider | `shared/variants.py`; `genizah_core.py` facade (falls back to an empty list on ImportError) | imported as a module | frozen as code; server via git | `python scripts/generate_unified_variants.py char_merges_report.xlsx --output unified_variants.py` | none |
| `pgp_tag_translations.py` | PGP tag translations and categories (hand-edited data module) | `genizah_app.py`, `web/pages/search.py` (both lazy) | imported as a module; **also** a spec `datas` entry by filename | spec + frozen; server via git | hand-edited | none |
| `shared_export_utils.py`, `shared/export_utils.py`, the whole `shared/` tree | source code the spec ships **as data** on top of the frozen import graph (the export-utils shim and its target twice over, `shared/` in full) | the frozen app's import graph already contains them | spec `datas` entries by literal path | spec; server via git | n/a | none; why `shared/` ships as data at all is a Round 2 question |
| `genizah_translations.py` | the Hebrew/English UI string table | `genizah_core.py` facade and several `shared/` modules | imported as a module | frozen; server via git | hand-edited | i18n tests |
| `fist_gap_rows.csv`, `fist_gap_manifest.txt`, `fist_gap_ambiguous_titles.txt` | Phase-53 FIST gap-fill residue; their content already lives in `libraries.csv` | the first two by `tests/test_fist_gap_fill.py` only; `fist_gap_ambiguous_titles.txt` has no reader at all | CWD-relative names in `scripts/generate_fist_gap_csv.py` (run from the root) | tracked; not bundled | `python scripts/generate_fist_gap_csv.py` | `tests/test_fist_gap_fill.py` |
| `version.py`, `version_info.txt` | the version source of truth; the EXE's Windows version resource | both apps, telemetry; `GenizahSearchPro.spec` (`version=`) | rewritten by `scripts/bump_version.py` | frozen / build-time | `python scripts/bump_version.py X.Y.Z` | `tests/test_release_artifacts.py` pins `version.py`; nothing compares the two files |
| `data/v08_repair_manifest.json` | hash-guarded record of the 8 repairs applied to `Transcriptions.txt` | `scripts/apply_v08_repair.py` | script-private `ROOT/data` | tracked (one of three negations under `data/*`, with the few-shot files and their notes); not bundled | `python scripts/build_v08_repair_manifest.py` | per-entry SHA-256 guard |
| `data/few_shot_*.json`, `data/FEW_SHOT_NOTES.md` | few-shot prompts for Dicta translation calls | `gui_threads.py` (desktop field translation), `shared/dicta_client.py`, translation scripts | `gui_threads.py` anchors `data/` on **its own `__file__`**, not on `Config` | tracked; **not bundled** -- in a frozen build the path resolves inside `_internal/` where `data/` does not exist and the failure is swallowed with a warning | hand-authored | `scripts/compare_few_shot.py` (A/B harness) |
| `eval/antiochus/` | the adjudicated Megillat Antiochus recall benchmark (deck, aliases, query, graded runs) | `scripts/score_antiochus_deck.py`, `scripts/shelfmark_join.py`, `tests/test_antiochus_deck.py` | script-private | tracked; eval-only | never rebuilt by design (a rebuilt deck is not comparable) | `tests/test_antiochus_deck.py` reproduces the README's numbers |
| `integrations/chatgpt/openapi.json`, `privacy.html` | the ChatGPT Action's schema and privacy page, **served live** at `/api/chatgpt/openapi.json` and `/api/chatgpt/privacy` | `web/chatgpt_api.py` | `Path(__file__).parents[1] / integrations / chatgpt` in `web/chatgpt_api.py`; producer writes beside itself | tracked; server via git | `python integrations/chatgpt/build_schema.py` | `integrations/chatgpt/validate.py`; `tests/test_chatgpt_api.py` |
| `migrations/*.sql`, `supabase_setup.sql` | Supabase DDL, applied by hand in the SQL editor | no code; cited in script docstrings and by `tests/test_joins_anc05_rls.py` | n/a | tracked | hand-written | behavioural tests of the resulting objects only. `supabase_setup.sql` defines 13 tables; `document_sources` is defined only in `migrations/create_document_sources_table.sql`, and `published_joins`, `published_join_fragments` and `discovery_responses`, all written by code, have no DDL anywhere in the repo |
| `reports/*` (tracked evidence) | CUDL coverage audits, orphan scans, synthetic-row residue and coverage narratives | none in production, except `reports/leading_zero_collisions.csv` (see §3); `scripts/build_cudl_fixture.py` reads `reports/cudl_orphans_all.csv`, which is **untracked** (ignored, present only where the scan was run) | script-private `ROOT/reports` | tracked although `/reports/*` is ignored; the Phase-85 and Phase-86 subsets carry `!` negations, the older tracked reports predate the rule and have none | the `scripts/scan_cudl_*.py`, `scripts/generate_synthetic_rows.py`, `scripts/audit_leading_zero_collisions.py` producers | none. `cudl_orphans_post_phase84.csv` is a byte-identical duplicate of `cudl_orphans_all_post_phase84.csv` with no producer; the `_post_phase84` set is archived in Round 1 |
| `pgp_data/full_import_report.txt`, `import_report.csv` | human-read verification output of the PGP import | none | script-private | tracked on purpose (`pgp_data/*.db` is the ignore rule, not the directory) | `scripts/import_pgp_full.py`, `scripts/import_pgp_documents.py` | they are the validation output |

### 2b. Sidecar databases (gitignored, provisioned)

None of the sidecar databases is a `Config` attribute; each owning service resolves its path via
its root finder, preceded by the `%LOCALAPPDATA%\GenizahSearchPro\data\<dir>\<file>` override.
Desktop: the five databases plus `vs_manifest.txt` are bundled by the spec (the tracked
`synthetic_manifest.json` is not) and three of them are refreshed post-install by the sidecar
updater in `gui_threads.py` (service map covers `pgp.db`, `fjms_enrichment.db`,
`nli_crossref.db`). Web: `scp` by hand (`DEPLOYMENT_TECHNICAL.md` lists `fjms_enrichment.db`,
`nli_crossref.db`, `pgp.db`, `libraries_translations.db`; `synthetic_manifest.json` is tracked and
arrives with the code; the delivery path of `visual_similarity.db` + `vs_manifest.txt` and of
`fgp_transcriptions.db` to the server is undocumented).

| Artifact | What | Owner module | Producer / rebuild | Validation | Notes |
|---|---|---|---|---|---|
| `pgp_data/pgp.db` (156 MB) | Princeton Geniza Project reference corpus | [shared/document_service.py](../../shared/document_service.py) | `python scripts/export_pgp_sidecar.py` (from Supabase) | `tests/test_document_service.py`, `tests/test_offline_verification.py`; the export script validates row counts | the repo-root `pgp.db` is a 0-byte stub |
| `fist_data/fjms_enrichment.db` (1.6 GB) | FJMS/FIST enrichment: domains, joins, catalogue, bibliography, persons, titles | [shared/fjms_service.py](../../shared/fjms_service.py) | `python scripts/export_fist_enrichment.py` (from `FIST.db`) | `tests/test_fjms_service.py`, `tests/test_export_fist_synthetic.py`; `scripts/checkpoint_sidecars.py` (WAL guard before a build) | `DEPLOYMENT_TECHNICAL.md` records v5.0.0 / ~941 MB; the file on disk is larger. A gitignored `fjms_enrichment.db.browse_cache.json` sits beside it, written and read by `shared/fjms_service.py`'s browse cache |
| `fist_data/visual_similarity.db` (1.3 GB) + `fist_data/vs_manifest.txt` | visual-similarity suggestions; the manifest stamps `has_vs` without opening the DB | [shared/visual_similarity_service.py](../../shared/visual_similarity_service.py); manifest read by `shared/metadata_manager.py` relative to `Config.LIBRARIES_CSV` | `python scripts/import_visual_similarity.py`; manifest producer not found | WAL guard only; no dedicated service test | also downloadable from the server at `/api/visual_similarity_db` (`desktop/vs_cache.py`) |
| `fist_data/synthetic_manifest.json` (tracked) | authoritative list of synthetic FIST rows the export must materialize | `scripts/export_fist_enrichment.py` | `python scripts/generate_synthetic_rows.py` | `tests/test_export_fist_synthetic.py` | tracked via `!fist_data/synthetic_manifest.json`; `.gitignore` uses the glob `fist_data/*` precisely so the negation can work |
| `nli_data/nli_crossref.db` (273 MB) | NLI images, Cambridge manifests, Manchester LUNA, JTS DPUL | [shared/nli_crossref_service.py](../../shared/nli_crossref_service.py) | `python scripts/import_nli_crossref.py` (+ `import_jts_dpul.py`, `import_manchester_luna.py`) | `tests/test_nli_crossref_service.py`; `scripts/check_sys_id_prefixes.py` | whole directory ignored; the repo-root `nli_crossref.db` and `fist_data/nli_crossref.db` are 0-byte stubs |
| `fgp_data/fgp_transcriptions.db` (405 MB) | FGP transcriptions extracted from PDFs | [shared/fgp_service.py](../../shared/fgp_service.py) | the untracked `fgp_data/fgp_*.py` scripts | `tests/test_fgp_service.py` and the `test_fgp_*` family | **bundled by the spec already**, while the tracker still lists shipping as pending and the flag is off; not in the sidecar-updater map; four same-size `.bak-*` copies sit beside it locally |
| `libraries_translations.db` (77 MB) | Dicta title translations | [shared/translation_service.py](../../shared/translation_service.py) (`_find_titles_db`: AppData first, then project root) | `scripts/extract_libraries_english.py`, then `translate_libraries_titles.py` / `translate_library_titles_en2he.py` | none | the one root asset that is gitignored **and** in spec `datas`: the PyInstaller build fails without a local copy |
| 0-byte stubs: `pgp.db`, `nli_crossref.db`, `fjms_enrichment.db`, `FIST.db` at the root; `fist_data/pgp.db`, `fist_data/nli_crossref.db` | leftovers of an earlier layout that shadow the real sidecars | nothing; `scripts/build_discovery_sidecar.py` explicitly refuses a candidate of size 0 | n/a | | a bare `sqlite3.connect` on a sidecar path creates one of these; probe with `mode=ro` |

## 3. Corpus indexes (rebuilt by build)

| Artifact | What | Producer / rebuild | Read by | Location decided at | Distribution | Validation |
|---|---|---|---|---|---|---|
| `<INDEX_DIR>/tantivy_db/` (3.3 GB) | the main Tantivy index behind ordinary search | `python build_index.py main` (`shared/indexer.py::Indexer.create_index`); server: `bash scripts/rebuild_index.sh` (verifies the corpus hash, keeps `tantivy_db.bak`, does **not** restart the service) | `shared/search_engine.py`; `genizah_app.py` (offers to build when missing) | the literal `"tantivy_db"` joined to `Config.INDEX_DIR` at four sites; not a `Config` attribute | built in place on the server and on each user's machine; never bundled | the rebuild script prints old/new `num_docs` for a human to compare |
| `<INDEX_DIR>/lab_index/` (3.0 GB) | the parallels/Lab index | `python build_index.py lab` (`shared/lab_engine.py::rebuild_lab_index`) | `shared/lab_engine.py` | `Config.LAB_INDEX_DIR` | same; `rebuild_index.sh` covers only the main index | none |
| `<INDEX_DIR>/browse_map.pkl` | sys_id to ordered page records, for browsing and adjacent-fragment navigation | tail of the index build (`shared/indexer.py`); replay without a rebuild: `python scripts/rebuild_browse_map.py --install` | `shared/search_engine.py` (which also **rewrites** a deduplicated copy atomically), `web/stats_service.py` | `Config.BROWSE_MAP` | same | `tests/test_browse_map_atomic_write.py` |
| `passage_index/current/` at the **repo root** (3.5 GB) | the web app's letter-level passage index | `python scripts/build_passage_index.py` (wraps `desktop/passage_lifecycle.py`: build, validate, atomic swap, rollback); built **on the server**, never uploaded | `web/passage_assets.py` (fail-closed `open_index`) | module-private default in `web/passage_assets.py`; `GENIZAH_PASSAGE_DATA_DIR` override read once at import | server only | `shared/passage_index.py::open_index` is the validator; `scripts/verify_passage_index.py` exists but no test or CI job runs it |
| `<INDEX_DIR>/passage_index/` | the **desktop's** passage index -- a different location from the web one | in-app: Settings, "Build passage index" (`desktop/passage_lifecycle.py`); no CLI | `desktop/passage_lifecycle.py`, `genizah_app.py` | `Config.PASSAGE_INDEX_DIR` | built on the user's machine from their own corpus | `tests/test_config_passage_index_dir.py`, the `test_passage_*` family |
| `discovery_data/` (`manifest.json` + `discovery-v1-<hash>.db`) | the discovery sidecar behind `/computed-identifications` | `python scripts/build_discovery_sidecar.py --out discovery_data/...` (runbook: [docs/specs/discovery-deploy.md](../specs/discovery-deploy.md)); CI uses a synthetic twin from `scripts/ci_materialize_discovery_fixture.py` | `web/discovery_assets.py` (resolves only the basename the manifest names) | module-private in `web/discovery_assets.py`; `GENIZAH_DISCOVERY_DATA_DIR` override | `scp` to the server, out of band; **not** bundled (the `.gitignore` comment claiming the installer bundles it is wrong) | `scripts/verify_discovery_sidecar.py`, `scripts/smoke_discovery_readiness.py`, `scripts/verify_review_artifact.py` (nightly, fails rather than skips) |
| `atlas_data/` (`manifest.json` + `atlas-v1-<hash>.bin[.br]`) | the baked Connections Atlas asset, deliberately outside `web/static/` | `python scripts/build_atlas_asset.py <research-db>` (needs `requirements-atlas-bake.txt`) | `web/atlas_assets.py`; served at `/atlas-data/...` | module-private in `web/atlas_assets.py` | `scp`, asset first, then code | `tests/atlas_bake/` (CI job `atlas-bake-tests`) |
| `reports/leading_zero_collisions.csv` | the D-06 gate: normalized CUDL keys excluded from the alias index | `python scripts/audit_leading_zero_collisions.py` (writes the transparency dump `cudl_full_normalization_collisions.csv` too) | `shared/shelfmark_bridge.py::load_collision_keys` -- at **runtime** as well as build time | `Path(__file__).parent.parent / reports` in `shelfmark_bridge.py` | tracked, so via git | none |
| `<INDEX_DIR>/metadata_cache.pkl`, `nli_cache.pkl` | parsed corpus metadata; per-fragment NLI metadata and image lists | rebuilt silently (metadata: re-parse; NLI: one fragment at a time from live lookups) | `shared/metadata_manager.py` and every image surface | `Config.CACHE_META`, `Config.CACHE_NLI` | never leaves the machine | `tests/test_nli_cache_*.py`, `tests/test_research_review_guards.py`; the NLI cache is a bounded LRU in memory and pickles as a plain dict, so entries beyond the bound are dropped on save |

## 4. Mutable personal state (desktop)

All under `Config.INDEX_DIR` unless stated. None is bundled; only `lists.pkl` ever leaves the machine.

| Artifact | What | Written by | Read by | Location decided at | If deleted | Validation |
|---|---|---|---|---|---|---|
| `config.pkl` | one pickled dict of every preference and flag, **including** telemetry consent and install id, update-banner and What's New state, and `last_save_folder` | `genizah_core.save_app_config` (read-modify-write, **not atomic**; load swallows every exception) | `genizah_core.load_app_config`, settings dialogs, `desktop/telemetry.py`, `genizah_app.py` | `Config.CONFIG_FILE` | all preferences reset silently; telemetry consent resets to off and the anonymous install id is lost | `tests/test_telemetry_consent_*.py` |
| `session.json` | last search session (queries, results, browse position, join-lab anchor) | `shared/session_persistence.py` (atomic `os.replace`), debounced from `genizah_app.py` | `genizah_app.py` restore; version-gated | `Config.SESSION_FILE`, resolved per call | next launch starts empty | `tests/test_session_restore_*.py` and siblings |
| `search_history.json` | recent-searches menu (regular and composition) | `shared/session_persistence.py` (atomic) | `genizah_app.py` | **module-level constant** bound at import in `session_persistence.py`, so patching `Config` afterwards does not move it | history menu empties | `tests/test_history_no_result_snapshots.py` |
| `lang.pkl` | UI language | `genizah_core.save_language` | read once at `genizah_core` import into `CURRENT_LANG` | `Config.LANGUAGE_FILE` | UI reverts to English | none |
| `lists.pkl` (+ `.bak1..3`) | saved research lists, projects, items, recent items | `shared/lists_manager.py::ListsManager.save` (three rotating backups); also after each cloud sync in `lists_sync.py` | `ListsManager.load` (desktop and, wrapped per user, web) | **class attribute** `LISTS_FILE` in `lists_manager.py`, bound at import -- not a `Config` attribute | recoverable from `.bak1` or from Supabase via `sync_from_cloud` | `tests/test_user_lists_*.py`, `tests/test_local_namespace_no_lists_leak.py` |
| `lab/lab_config.json` | every Lab/parallels tuning knob | `shared/lab_settings.py::LabSettings.save` -- from the desktop **and** from `web/pages/settings.py` (one shared file on a machine running both) | `LabSettings.load` (inline defaults for every field) | `Config.LAB_CONFIG_FILE` | defaults restored silently | none |
| `lab/lab_weights.json` | output of the HTR character-confusion analysis (not a settings file) | `genizah_core.py` at the end of the analysis | `shared/lab_engine.py` when dynamic weights are on | `Config.LAB_WEIGHTS_FILE` | Lab scoring falls back to static weights | none |
| `LocalIndex/`, `LocalLabIndex/` | the My Library side-indexes over the user's own documents, plus their SQLite bookkeeping | `desktop/my_library_tab.py` (creates `LocalIndex`), `shared/local_indexer.py` (creates `LocalLabIndex`) | `shared/search_engine.py`, `shared/lab_engine.py`, `shared/research_worker.py` | `Config.LOCAL_INDEX_DIR`, `Config.LOCAL_LAB_INDEX_DIR` (co-located for portable mode) | user re-indexes ("Re-index All"); content is private and never synced | the `test_local_*` family; `tests/conftest.py` isolates both |
| `images_cache/` | JPEG cache of page images (NLI by FL id at 2000px, others by URL hash) | `desktop/image_loader.py` | same | `Config.IMAGE_CACHE_DIR` | re-downloaded on demand | `tests/test_desktop_image_loader_breaker.py`. **No eviction, no size cap, no clear action**; the `_v2` suffix orphaned the older files |
| `faulthandler_dump.txt`, `genizah.log`, `lab/lab_genizah.log` | native-crash dump (read on the next launch, then truncated); rotating app logs | `desktop/telemetry.py`; the logging setup in `genizah_core.py` | telemetry (a derived crash label is sent once consent is on) | literal join in `telemetry.py`; `Config.LOG_FILE`, `Config.LAB_LOG_FILE` | nothing lost | `tests/test_native_crash.py` |
| `Documents\GenizahSearchPro\Reports\` | exported reports and sheets | export code in `genizah_app.py` | the user | `Config.REPORTS_DIR` (probes writability at import; falls back to `INDEX_DIR/Reports`) | user files | |
| `~/.genizah_corrections/supabase_credentials.json`, `community_cache.json` | the desktop Supabase session (access + refresh token, cached user) and a cache of community payloads | `supabase_corrections_client.py` | same | **module-private**: `Path.home() / ".genizah_corrections"`, never `Config` | user is logged out; cache refills | none. Tokens are stored as plaintext JSON with default file permissions; the password goes to the OS keyring instead |
| OS keyring, service `GenizahSearch` | the saved password for "Remember me" | `supabase_corrections_client.py` via `keyring` (optional dependency; silently skipped when missing) | same | n/a | user re-enters the password | none |
| `~/.genizah_search/sefaria_cache/` | Sefaria API responses | `sefaria_utils.py` | `filter_text_dialog.py`, `web/pages/parallels.py` | module-private in `sefaria_utils.py` | re-fetched | none |

## 5. Mutable shared state (web server)

| Artifact | What | Written by | Read by | Location decided at | Notes |
|---|---|---|---|---|---|
| `.nicegui/storage-user-<uuid>.json` | NiceGUI's per-browser-session store: auth session, `_session_uuid`, current page, page-state snapshots | NiceGUI itself; app code writes only through [web/safe_storage.py](../../web/safe_storage.py) | `safe_user_get` (the chokepoint), `web/export_state.py` (compaction and retention at startup), `web/storage_diagnostics.py` | NiceGUI's default, relative to the process CWD (`/home/ubuntu/GenizahSearch` in production) | `app.storage.general` is not used anywhere; `app.storage.browser` holds exactly the two "Remember me" keys. Rules: [docs/guides/MULTITENANT.md](../guides/MULTITENANT.md); guard: `tests/test_no_raw_storage_access.py` |
| `joins_data/joins.db` | saved puzzle/join documents and their fragment index (WAL); shared by all web users | created on first use by [shared/puzzle_service.py](../../shared/puzzle_service.py) | desktop and web puzzle pages, `web/api.py`, the publish service | `puzzle_service.py` root finder, with an AppData fallback | no export/import; not bundled, not in the scp list; gitignored so it survives deploys |
| Supabase (hosted Postgres) | all community data: profiles, projects, lists and items, recent items, corrections and votes, comments, discoveries and responses, fragment joins, published joins, PGP `documents`/`document_sources`/`document_fragments` | `web/supabase_client.py`; `supabase_corrections_client.py` (desktop); scripts for the PGP tables; identification reviews only through `SECURITY DEFINER` RPCs (`web/identification_reviews.py`) | both apps | `SUPABASE_URL` + anon key from `.env` (`shared/supabase_provider.py`) | schema: `supabase_setup.sql` + `migrations/` (incomplete, see §2a); guide: [docs/guides/SUPABASE_GUIDE.md](../guides/SUPABASE_GUIDE.md) |
| `reports/cudl_alias_collisions.csv` | diagnostic dump of CUDL alias keys excluded for ambiguity | `shared/shelfmark_bridge.py` as a **side effect of `build_alias_index()` at app start-up** (swallows `OSError` on read-only installs) | nothing | `Path(__file__).resolve().parent.parent / "reports"` in `shelfmark_bridge.py` (a fixed join, not one of the `libraries.csv` root finders) | untracked (correctly ignored); `tests/test_shelfmark_bridge_ambiguity.py` asserts the tests leave it byte-identical |
| `web/_secrets/posthog_ip_salt` | 32-byte salt for hashing client IPs before PostHog | `web/api_hardening.py` on first run when `POSTHOG_IP_SALT` is unset (atomic, `chmod 600` best effort) | `hash_ip` | inside `web/` (`web/_secrets/`, derived from `api_hardening.py`'s own `__file__`), module-private | the directory self-ignores (`*` then `!.gitignore`); regenerating it makes earlier IP hashes uncorrelatable |
| journald unit `genizah-web` | the web app's logs (stdout/stderr; no file handler anywhere under `web/` or `shared/`) | systemd | `journalctl -u genizah-web` | the unit file (`DEPLOYMENT_TECHNICAL.md`) | nginx keeps its own logs |

## 6. Secrets and session material

| Artifact | What | Read by | On disk | Notes |
|---|---|---|---|---|
| `.env` | Supabase credentials, PostHog key, feature flags, Search-API and discovery knobs, optional HMAC secrets | `load_dotenv()` in `web/main.py`; `shared/supabase_provider.py`; scripts | repo root (gitignored); pinned on the server by the systemd `EnvironmentFile=` | template: `.env.production.example` (tracked, placeholders only). `tests/test_env_vars_doc_matches_code.py` keeps `ENV_VARS.md` honest about defaults |
| `.masking_patterns` via `MASKING_SCAN_PATTERNS_FILE` | the restricted-vocabulary pattern file | [scripts/check_atlas_masking.py](../../scripts/check_atlas_masking.py) and the emit/attach/package scripts | repo root by convention (gitignored); CI materializes it from the `MASKING_SCAN_PATTERNS` secret for the length of the job | unset means the scan **fails**; see [decision 0009](../decisions/0009-masked-corpus-rule.md) |
| `.masking_attest_key` via `MASKING_ATTESTATION_KEY` | HMAC key for the non-disclosing attestation of which pattern set ran | `check_atlas_masking.py` (the env var; no code reads the file) | repo root (gitignored) | rotating it makes every recorded attestation unverifiable |
| `PUZZLE_UPLOAD_SECRET` | signs the 5-minute upload tokens for the puzzle image cache | `web/puzzle_tokens.py` (read once at import) | `.env` only | when unset a per-process random key is used **silently**, so tokens do not survive a restart; no test covers the token functions |
| PostHog ingestion keys | the web key from `.env`; the desktop key as a literal in `desktop/telemetry.py` | `web/main.py` (inlined into the page), `shared/posthog_server.py`, `desktop/telemetry.py` | publishable by design (`phc_` only; personal keys rejected) | not secrets in the strict sense; listed here because they are credentials |
| Desktop Supabase session and password | see §4 | | `~/.genizah_corrections/`, OS keyring | |

## 7. Disposable / generated

| Artifact | What | Notes |
|---|---|---|
| `discovery_builds/` (multi-GB, gitignored) | the discovery pipeline's build depot (sources, crosswalks, match DBs, candidate sidecars, runbooks) | consumers only; no in-repo producer writes to it; its **output** `discovery_data/` is what ships. `git clean -xfd` would delete it |
| the six 0-byte sidecar stubs | see §2b | |
| `.server.pid` | PID file of the local dev server CLI `server.py` | **not gitignored** (`.gitignore` covers `app.pid` only); does not exist in the working tree today |
| `_ie_volume_map_build.log` and similar build logs at the root | untracked debris | |
| `reports/*_post_phase84.*`, `reports/synthetic_ambiguity_residue*.csv`, `reports/synthetic_coverage.md`, `pgp_data/*_report.*` | evidence outputs of one-off pipeline runs | the `_post_phase84` set moves to `docs/archive/reports/phase84/` in Round 1; `scripts/build_residue_patterns_artifact.py` reads the untracked `..._dryrun.csv` sibling, so that consumer has no committed input |
| `char_merges_all.xlsx`, `char_merges_filtered.xlsx` | intermediate outputs of `scripts/analyze_char_merges.py` | archived in Round 1; the report stays |
| `<INDEX_DIR>/metadata_cache.pkl`, `nli_cache.pkl`, `images_cache/` | see §3 and §4 | |

## What this table found (facts for the tracker)

Recorded here so the rows above stay descriptive; each becomes a tracker row or a Round 2 item.

- `bodleian_master_index.csv` has no code reader but ships in every installer via the spec.
- `data/` is not bundled, yet `gui_threads.py` anchors `data/few_shot_*.json` on its own `__file__`; in the frozen app the read fails and is swallowed (field translation silently loses its few-shot prompts).
- `fgp_data/fgp_transcriptions.db` is already in the spec while the tracker lists shipping as pending; the scripts that build it are gitignored with the rest of `fgp_data/`.
- Three tables the code writes (`published_joins`, `published_join_fragments`, `discovery_responses`) have no DDL anywhere in the repo; a fourth, `document_sources`, is defined only in `migrations/`, not in `supabase_setup.sql`.
- `config.pkl` is the one personal-state file written without an atomic replace, and its loader swallows every exception.
- `images_cache/` grows without bound.
- The desktop Supabase refresh token is stored as plaintext JSON under the home directory.
- `.server.pid` is not gitignored; `.gitignore`'s comment near its `Genizah_Index` rule cites the wrong line.
- `DEPLOYMENT_TECHNICAL.md` is stale on sizes and counts (`libraries.csv`, `fjms_enrichment.db`) and names the V0.7 corpus differently from the code; `ENV_VARS.md`, `ci.yml` and `scripts/ci_materialize_discovery_fixture.py` all credit `tests/test_cert01_grading_validator.py` with resolving the real artifact through `discovery_data/manifest.json`, which it does not do (`scripts/cert01_freeze.py` does).
- `icon.ico` is resolved by two different rules; `version.py` and `version_info.txt` have no consistency test; `scripts/verify_passage_index.py` is run by nothing.
- `migrations/` has no ledger of which files have been applied to production, no runner and no ordering; `supabase_setup.sql` is the bootstrap and the migrations are increments on top.
- CI installs `requirements-lock.txt` (plus `requirements-atlas-bake.txt` in the `atlas-bake-tests` job), the server installs `requirements.txt` only; the release skill's hand-run diff is the sole check that the lockfile and `requirements.txt` agree.
- `DEPLOYMENT_TECHNICAL.md` documents an `extension/store/` directory that neither exists on disk nor is tracked.
- `scripts/build_cudl_fixture.py` reads `reports/cudl_orphans_all.csv`, which is untracked; like the residue-patterns script, that consumer has no committed input.
- The desktop build needs seven gitignored files present locally (the five sidecar databases the spec bundles, `libraries_translations.db` and `fist_data/vs_manifest.txt`); nothing checks the `datas` list against the code that reads those files.

## Adding a row

Pick the class first; it decides the columns that matter. Name where the location is decided (a
`Config` attribute, a module-private finder with `path:line`, or an environment variable), and
state tracked / bundled / server delivery explicitly -- "it is in the repo" is not a distribution
answer for the web server if the file is gitignored, and "it is in the spec" is not one for the
web. Link tracked files; put untracked and generated paths in inline code.
