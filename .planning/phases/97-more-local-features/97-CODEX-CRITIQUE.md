# Codex Critique of Phase 97 CONTEXT

> **Source:** `codex exec --sandbox read-only` (gpt-5.5, session 019e5d5f-2932-74e3-bbdd-66b8b941ced2)
> **Brief:** `97-CODEX-BRIEF.md`
> **Date:** 2026-05-25
> **Raw transcript:** `97-CODEX-CRITIQUE-raw.txt`

## P0 issues (must fix before planning)

- **C-02 is technically underspecified / partly wrong for tantivy-py.** Current code uses `index.writer(heap_size=...)`; there is no existing repo pattern for `with_index_writer_heap_size`. More importantly, a heap limit is not a durability boundary. You still need explicit commit checkpoints and a way to decide when to call `commit()`. Use heap size **plus** explicit max files / source bytes / elapsed seconds per batch, not "commit when heap fills."

- **R-01 can expose inconsistent pending docs unless LOCAL search is gated during recovery.** Current search opens the LOCAL Tantivy searcher independently of SQLite status. If the app crashed after Tantivy commit but before SQLite `status='committed'`, those docs may already be searchable. If Phase 97 adds "Skip," either disable LOCAL search until Resume/Restart is resolved, or make pending docs unqueryable.

- **R-02 depends on R-03. Ship text cache before lifting the ceiling.** Without cached per-page/chunk text, corrupt Tantivy recovery falls back to full source re-extraction, which fails for unavailable/network folders and is exactly the slow path Phase 97 is trying to avoid.

- **R-02 must rebuild atomically, not in place.** Build a fresh main LOCAL index in a temp dir, validate `searcher()`, close live readers/writers, then swap directories. Same for `LOCAL_LAB_INDEX_DIR`. In-place repair risks leaving both SQLite and Tantivy unusable after a second crash.

- **R-03 needs a real SQLite migration plan.** Existing `local_pages` stores only `sys_id`, `uid`, `page_num`; `CREATE TABLE IF NOT EXISTS` will not add `cached_text` to existing user DBs. Add `PRAGMA user_version`, migration tests, backfill behavior, and handling for rows whose cache is absent.

- **F-06 as written risks corrupting RTL text.** `_fix_rtl_line` / `_fix_rtl_page` were intentionally dead code for PDF fallback. Applying them to HTML / XLSX / CSV logical text can reverse already-correct Hebrew. Honor `dir="rtl"` / XLSX RTL as display or metadata hints, not automatic text reversal.

- **U-02 needs run-level transaction semantics.** "Discard everything indexed so far" requires a `scan_run_id` or manifest of sys_ids touched in this run, including already-committed batches. "Keep partial" must explicitly commit pending writer state. Current-style cancellation plus later `close()` can otherwise commit work the user thought was canceled.

## P1 issues (strong recommendation)

- **F-02 one-doc-per-XLSX-sheet is too coarse.** A single sheet can be enormous and memory-heavy. Use `openpyxl.load_workbook(read_only=True, data_only=True)` and chunk large sheets by row windows / max chars, similar to CSV.

- **C-05 needs zip-container limits.** `.docx` / `.xlsx` can be under 100 MB but decompress into huge XML. Add max uncompressed bytes, max cells / rows, max chars per chunk, and zip-bomb checks.

- **C-03 and U-03 must not mutate Qt widgets from worker threads.** Folder walking can move to QThread, but item creation / update must stay on the GUI thread via batched signals. For 100K files, prefer lazy / paged detail views over materializing 100K `QTreeWidgetItem`s.

- **R-04 overclaims durability.** WAL + `synchronous=NORMAL` is usually corruption-safe, but can lose last transactions on OS crash / power loss. For critical pending → committed transitions, consider `FULL` or an explicit "durable checkpoint" option.

- **LAB parity is not automatic at scale.** Current LAB rebuild reads stored content from the main LOCAL Tantivy index and can run synchronously. At 100K files it needs its own recovery path from cached text, atomic rebuild, progress, cancellation, and heap/batch settings.

- **C-06 disk planning must account for merge scratch space.** Tantivy segment merges can temporarily need substantial extra disk. Warn based on projected post-index free space **plus merge headroom**, not only "index size / free space."

- **Incremental audit should move from float `mtime` to `st_mtime_ns`.** Current mtime + size logic is mostly fine, but at scale / network drives the float tolerance can miss rapid edits. Store `mtime_ns`, size, and optionally a cheap hash for suspicious same-size changes.

- **F-01 / F-05 need encoding decisions beyond CSV.** HTML needs charset sniffing / meta handling and cp1255 fallback. CSV should consider delimiter sniffing, semicolon Excel exports, UTF-16 exports, and strict decode-failure behavior.

## P2 / improvements

- **U-01 ETA should be phase-aware.** Separate "walking," "extracting," "committing," and "rebuilding LAB." Bytes/sec alone will mislead across PDF, DOCX, XLSX, and cached skips.

- **U-04 should not intentionally freeze the main thread for 30 seconds.** If raising View All to 500 pages, use cached text plus incremental rendering or background assembly.

- **C-04 folder aggregation should include persisted counters.** Don't recompute folder counts by scanning all file rows on every UI refresh.

- **C-05 "100 MB hard skip" should be surfaced as configurable advanced policy eventually.** Some power users may have legitimate large PDFs, even if default skipping is sensible.

- **Requirements / build packaging need updates.** `zstandard` is in `requirements-lock.txt` but not `requirements.txt`; if R-03 uses zstd, pin it and include PyInstaller collection if needed. Same for any HTML parser beyond stdlib.

## Missing decisions (gaps)

- Define SQLite schema additions: `cached_text`, codec / version, uncompressed length, text hash, extraction format version, and migration / backfill behavior.
- Define recovery choices precisely: Resume, Restart, Skip, "LOCAL disabled until resolved," and what happens if SQLite itself fails `integrity_check`.
- Define network-drive semantics: unavailable folder vs transient timeout, retry / backoff, per-folder skip, and whether startup auto-rescan should touch slow shares.
- Define file-change-during-index handling: stat before and after extraction; if size / mtime changes mid-read, retry or mark `changed_during_index`.
- Define supported-file scope. Current indexer walks all files and records unsupported ones; at 100K+ trees this can bloat SQLite / status UI unless Phase 97 limits rows to supported / oversized / error files.
- Define format-specific chunk locators for Browse: HTML heading / chunk labels, XLSX sheet + row range, CSV row range. Page number alone will be ambiguous.
- Define privacy / docs updates. Cached SQLite text is another local cleartext copy of user content, even if compressed.
- Keep tests pinning Phase 95/96 invariants: cloud-write gates, web LOCAL filtering, `is_local_sys_id`, and LOCAL RRF merge after `_deduplicate()`.

## Overall assessment

The scope is directionally right, but the sequencing should change: implement schema migration + cached text + atomic recovery first, then lift the hard ceiling. Otherwise the first users who try 13K / 43 GB are also the first users exposed to expensive or impossible recovery.

The biggest technical changes are not the new formats; they are making indexing a resumable run with durable checkpoints, cached page text, atomic main / LAB index rebuilds, and scalable UI models. Once those are planned, C-01 becomes reasonable. Without them, C-01 mainly removes the guardrail before the crash-recovery story is strong enough.
