# Phase 95: My Library — Codex Critique

**Reviewer:** OpenAI Codex (gpt-5.5, xhigh reasoning)
**Date:** 2026-05-21
**Inputs:** `95-SPEC.md` (10 requirements) + `95-CONTEXT.md` (33 decisions) + `95-DISCUSSION-LOG.md` + spot-checked code anchors
**Status:** Two P0 warnings verified by Claude post-critique against `genizah_core.py:7916-7921` and `lists_sync.py:736-756`. Other warnings not independently verified — planner to confirm.

---

## 1. RECOMMENDATIONS (changes worth considering)

- **D-08**: Do not rely on raw BM25 comparability across two independent Tantivy indexes. Use reciprocal-rank fusion or per-index score normalization, then keep "Genizah first on tie." Cost now: small result-merger change. Cost after planning: higher, because tests and UX expectations will encode unstable ordering.

- **D-15**: Store the folder list next to the side-index, or mirror `QSettings` into `local_index.sqlite3`. D-14 makes LOCAL data portable with `Config.INDEX_DIR`, but `QSettings` stays in HKCU, so portable installs keep the index but lose source-folder state. Cost now: low. Cost later: migration and stale-index cleanup.

- **D-18 / D-19**: Add deterministic collision handling for the 8-digit `machine_id` / `content_hash` components. An 8-digit hash space has real birthday collision risk at the 5,000-file ceiling, and the "hex chars converted to decimal and zero-padded" wording can exceed 8 digits unless reduced explicitly. Cost now: low. Cost later: sys_id migration.

- **D-21**: Prefer an explicit batch transaction protocol: mark rows `pending`, commit Tantivy, then atomically mark SQLite rows `committed`, with startup recovery for pending batches. The current "commit every 25 files + SQLite update" decision does not define crash ordering. Cost now: moderate. Cost later: hard, because inconsistent caches become user data.

- **D-24**: Check cancellation between PDF pages and DOCX chunks, not only between files. A single 1,000-page PDF can make "Cancel" feel broken while still preserving per-file commit atomicity. Cost now: low. Cost later: threading/UI retest.

- **D-02**: Keep the dead-code RTL helpers, but add at least one real PyMuPDF Hebrew fixture. Testing helpers that are never invoked does not prove v1 runtime extraction quality. Cost now: low.

- **D-33**: Add a one-line Help disclosure that the local index is stored as cleartext on disk while never uploaded. This does not require encryption or extra UX, but it makes the privacy claim precise. Cost now: trivial. Cost later: trust/support issue.

## 2. WARNINGS (real risks the decisions create)

- **D-08 / D-11 — P0**: Existing `_deduplicate()` keeps only `V0.8` and `V0.7`; LOCAL rows merged before that path can be dropped silently at `genizah_core.py:7916-7921`. **Verified by Claude:** `final = list(v8.values()); for r in results: if r['display']['source'] == "V0.7" and r['uid'] not in v8: final.append(r)` — anything that isn't V0.8/V0.7 is discarded. Mitigation: merge LOCAL after main dedupe **or** make dedupe preserve non-Genizah sources.

- **D-13 / D-18 — P0**: Core parsers still match `99\d...`, not `97...`, in `parse_header_smart`, `parse_full_id_components`, and serializer batch extraction. LOCAL display/library/sys_id fields will disappear unless these are generalized. Mitigation: centralize sys_id extraction through `is_local_sys_id`-aware helpers.

- **D-30 / REQ-9 — P0**: `lists_sync.sync_item_to_cloud()` gets the cloud client at line 742 and may call `sync_list_to_cloud()` at line 753 before reading `item_data.sys_id` at line 762. **Verified by Claude:** the gate inserted at the natural-looking spot (after `item_data = ...lookup`) leaves the cloud connection + parent-list sync already happening. Mitigation: inspect `item_id` → `sys_id` lookup **before** `_get_client()` and before any `sync_list_to_cloud()` call. SPEC REQ-9 acceptance test (`test_local_namespace_no_lists_leak.py`) must mock `_get_client` and assert it's not called at all.

- **D-20 / D-21 — P1**: Deleting modified/removed files needs exact page-UID tracking; the SPEC's schema match gives no `filepath` field on the Tantivy doc, and `full_header` text-field queries are not a safe delete key. Mitigation: add a `local_pages(sys_id, uid, page_num)` table in SQLite and delete by exact `unique_id`.

- **D-21 — P1**: Crash between Tantivy commit and SQLite update can make unchanged files look indexed when their docs are missing, or vice versa. Mitigation: pending/committed batch state plus recovery re-extract on startup.

- **D-09 — P1**: `fingerprint_dyn` depends on current LAB dynamic weights; local lab index can become stale after LAB settings or main LAB rebuilds. Mitigation: store a weights/version hash and rebuild local lab when it changes.

- **D-20 / D-24 / D-25 — P1**: Auto-rescan, manual Refresh, and Remove Folder can race into the same Tantivy/SQLite side-index. Mitigation: one indexer mutex, disable mutating controls during a run, and queue refresh requests.

- **D-10 — P1**: Persisted `Only Local` plus "hide button when no LOCAL hits exist" can create invisible filtering that shows zero rows. Mitigation: LOCAL filter is no-op when the current result set has no LOCAL hits, while preserving state for future mixed sets.

- **D-16 / D-17 — P1**: Overlap rejection is fragile on Windows without `normcase`, `realpath`, UNC normalization, and junction awareness. Mitigation: normalize with `Path.resolve(strict=False)` / `os.path.normcase` and use `commonpath`.

- **D-26 — P2**: Pre-scan count ignores the 2 GB text ceiling and can choke on OneDrive/network-drive permission errors or junction loops. Mitigation: count supported files and byte size, `followlinks=False`, catch per-directory errors.

- **D-01 — P2**: PyMuPDF requires packaging work, not just `requirements.txt`: PyInstaller hidden import / collect handling for `fitz` / `pymupdf` should be pinned. Mitigation: update `GenizahSearchPro.spec` (Inno Setup compile script + PyInstaller spec) and smoke-test the packaged EXE.

## 3. INSIGHTS (non-obvious connections or implications)

- **D-14 / D-15**: The storage decisions split "portable local data" from "non-portable folder configuration." Not fatal, but Help should describe the actual resolved path and source-folder persistence honestly.

- **D-11**: Reusing `COL_SRC` is workable, but it changes `source` from "transcription version" into "result provenance" for LOCAL. Export/report paths that write `display.source` will now emit `LOCAL`; that should be intentional and tested.

- **D-09**: The local LAB side-index is the real enabler for Composition/Parallels. Main-search merging can tolerate imperfect BM25 ordering, but LAB merging must preserve the custom scoring path (fingerprint-based), not reuse raw Tantivy scores.

- **D-27 / D-28**: The Browse text-only view and `Open file` button imply a richer local metadata store than `processed_files(filepath, mtime, size, sys_id)`. That store becomes the natural foundation for future local-only Lists.

- **D-30 / D-31**: Adding `"LOCAL"` to shared `LIBRARY_CODES` is clean, but every web dropdown/list consumer must opt out. A static test scanning web library-option builders would prevent later regressions.

## 4. GAPS (decisions the discussion missed)

- **D-03 / D-04 / D-18**: What is the canonical LOCAL `unique_id` and `full_header` format per page/chunk? It must be parseable by existing result, browse, serializer, and export code without pretending to be a `99...` Genizah ID.

- **D-27 / D-28**: Where are filepath, display title, original filename, page count, extraction status, and page UID mappings stored? The Tantivy schema intentionally cannot hold all of this — needs a sidecar `local_files` table beyond `processed_files`.

- **D-20 / D-21**: What is the exact update algorithm for modified files: delete old pages first, add new pages, commit, then update cache, or build a replacement index and swap? This matters for cancellation and crash recovery.

- **D-08**: What happens when the side-index is missing, locked, or corrupt during search? The spec says main index must survive, but the fallback behavior and user-visible status are not decided.

- **D-09**: What triggers local LAB rebuild besides MyLibraryTab refresh: LAB settings changes, main LAB rebuild, app upgrade, schema/version bump? This needs an invalidation contract.

- **D-10**: Are LOCAL filter states persisted separately for Search, Composition Search, and Parallels, and what are the session keys? The cascade order is decided; state ownership is not.

- **D-16 / D-25**: What is the behavior for unavailable folders at startup: external drive unplugged, OneDrive offline, permissions denied? Delete nothing by default; mark folder unavailable until explicitly removed.

- **D-26**: Is the 2 GB ceiling based on source file size, extracted text bytes, or Tantivy stored content size? The warning threshold needs one measurable definition.

---

*Critique generated by Codex (gpt-5.5) — 2026-05-21*
*Two P0 warnings (D-08/D-11 dedup; D-30/REQ-9 lists_sync ordering) verified against codebase by Claude before saving.*
*Other warnings/recommendations not independently verified — planner to triage during `/gsd-plan-phase 95`.*
