# Phase 97 Codex Review Brief

## Context

GenizahSearch is a dual desktop+web application for searching the Cairo Genizah manuscript corpus. Phase 95 (v7.14.0, shipped 2026-05-24) added a desktop-only "My Library" feature that indexes user-owned `.docx`/`.pdf`/`.txt` files into a separate Tantivy side-index and merges hits into search/Composition Search/Parallels via RRF fusion. Phase 96 fixed bugs and added per-file opt-out. Phase 97 extends LOCAL.

**Trigger for Phase 97:** the email from Yehuda Seewald (the external prototype author whose work was productized as v7.14.0's My Library). Seewald himself runs 13,000 files / 43 GB on his prototype because v7.14.0 hard-caps at 5,000 files / 2 GB. v7.14.0 lacks `--update` (incremental) and `--recover` (crash recovery) parity with his prototype. Phase 97's main goal: close the capacity gap so power users can actually adopt v7.14.

## Phase 97 Scope (locked during discuss-phase)

**In scope:**
- Lift 5K-files / 2GB hard ceiling
- Crash recovery (`--recover` equivalent)
- Incremental update audit at scale
- 3 new file formats: `.html`/`.htm`, `.xlsx`, `.csv`

**Out of scope / deferred to v7.15+:**
- D-F2 PDF OCR (Tesseract for image-only PDFs)
- D-F3 Side-by-side PDF rendering
- `.md`, `.epub`, `.rtf` formats
- D-F8 View All page-block matching refactor
- D-F10 View All renderer path consolidation
- Web parity for LOCAL (desktop-only stays the rule)

## Decisions Captured

### Capacity Track

- **C-01 — Ceiling:** No hard cap. Soft warning at 50K files OR 50 GB, non-blocking, "Proceed / Cancel". Replaces Phase 95's 5K/2GB hard-stop.
- **C-02 — Memory budget:** Replace fixed 25-file commit batch with Tantivy heap-bounded commits (`with_index_writer_heap_size=256 MB`). Commit when heap fills, regardless of file count.
- **C-03 — Pre-scan dialog:** Walk folder tree first (metadata only), compute file count + total bytes + ETA. Show non-blocking dialog only when above 50K/50GB. Otherwise begin indexing immediately.
- **C-04 — Status panel UI:** Aggregate-by-folder view default (Indexed / Errors / Pending counts per folder). Click a folder row → drill-down detail with per-file rows. Scales to 100K+ files. Replaces today's QTableWidget-with-one-row-per-file.
- **C-05 — Per-file size cap:** 100 MB hard skip with `status='oversized'` in per-file panel + log warning. No Tantivy doc emitted. User can manually split + reindex.
- **C-06 — Disk-usage surface:** Live indicator in MyLibraryTab ("Index size: 4.2 GB / 1.1 TB free"). Inline warning when >80% of free space utilized OR estimated growth would exceed free space.

### Crash Recovery Track

- **R-01 — Recovery UX:** On next launch, if SQLite cache has pending-status rows, modal prompt at MyLibraryTab open: "Previous indexing interrupted — Resume / Restart / Skip?"
- **R-02 — Tantivy corruption recovery:** On startup, attempt `index.searcher()` on LOCAL Tantivy index. If it raises, walk SQLite `processed_files` WHERE `status='committed'` and rebuild Tantivy from cached text (no PyMuPDF re-extract).
- **R-03 — Text cache:** Add `cached_text` column to SQLite per-page records, zstd-compressed. Enables R-02 fast rebuild. Estimated ~400 MB compressed for 13K files. Also speeds up View Page rendering.
- **R-04 — SQLite durability:** Enable WAL mode with `synchronous=NORMAL`. Better crash-safety than rollback journal, ~3x faster small commits.

### Format Extraction Track

- **F-01 — HTML chunking:** Split at semantic boundaries (h1/h2 elements). Fallback to 20-paragraph chunks if h1/h2 sparse (TBD: define "sparse" — proposal: <3 such headings in document).
- **F-02 — XLSX chunking:** One Tantivy doc per sheet.
- **F-03 — CSV chunking:** Per-200-rows windows.
- **F-04 — Header-row handling:** No header-row assumption. Every row extracted uniformly as `cell1 | cell2 | cell3 | ...` joined text. Survives synopses, headerless data, pivoted tables, multi-row headers.
- **F-05 — CSV encoding:** utf-8-sig (BOM-tolerant) first; cp1255 fallback on UnicodeDecodeError. Mirrors Phase 95 D-07 TXT policy but with cp1255 fallback because CSVs from Excel commonly use it.
- **F-06 — RTL:** Honor format-level RTL hints (HTML `dir="rtl"`, XLSX cell rtl alignment). Apply Phase 95 `_fix_rtl_line`/`_fix_rtl_page` as dead-code safety net per format.

### Indexing UX at Scale

- **U-01 — ETA:** Hybrid display — bytes-based ETA + file count both shown. "X of Y files (12 min remaining — about 2.3 GB to go)". Throughput computed from running bytes/sec with 30-sec smoothing.
- **U-02 — Cancel semantics:** Cancel triggers confirmation prompt: "Discard everything indexed so far, or keep partial library + stop?" Avoids the assumption that Cancel == "abort everything".
- **U-03 — Folder walk threading:** `_UnifiedFileTreeWidget.populate_for_folder` moves to QThread. Status panel updates throttled to once per 100 files OR 0.5 sec. Closes D-F9 in scope.
- **U-04 — View All cap:** Raise 200-page cap to 500 pages, keep on main thread. Lighter than D-F7's full background-thread refactor. Acknowledges that ~500-page PDFs may freeze for ~30 sec but doesn't impose QThread complexity.

## Phase 95/96 Invariants to Preserve

These MUST NOT regress:
- RRF k=60 fusion POST-`_deduplicate()` (Codex D-08 P0)
- Cloud-write gates at TOP of `shared/search_serializer.py`, `corrections_client.py`, `lists_sync.{sync_item_to_cloud, sync_list_to_cloud}` (Codex D-30 P0)
- Web `LIBRARY_CODES` allowlist `[]`, multitenant `[]`
- PyMuPDF only, `get_text("dict")` block-level join (Phase 96 D-F4 fix)
- `is_local_sys_id()` 18-digit `97`-prefixed sys_ids
- SQLite mtime cache + sys_id derivation `% 10**8` (Phase 95 D-19)
- Per-file status panel + cancellation
- `_deduplicate()` whitelist behavior unchanged

## Questions for Codex

1. **Technical soundness:** Are any of the 18 decisions (C-01..C-06, R-01..R-04, F-01..F-06, U-01..U-04) technically wrong, naive, or set up for a known failure mode at 13K-100K files / 50 GB scale?

2. **Missing decisions / gaps:** What decisions did we fail to make that planning will require? Particularly:
   - Crash-recovery edge cases (laptop sleep, OS update mid-index, network drive disconnect)
   - Multi-folder semantics when one folder is on a slow drive / network share
   - LAB side-index (`Config.LOCAL_LAB_INDEX_DIR`) parity with main LOCAL index for all decisions above
   - Settings panel / advanced config surface

3. **P0/P1 risks:** Any decision that would silently corrupt user data, lose work, or compromise the Phase 95/96 invariants (especially the three cloud-write gates and the RRF POST-`_deduplicate()` ordering)?

4. **Sequencing:** Should any decision change to reduce risk? E.g., should the text cache (R-03) ship before the ceiling lift (C-01) so recovery exists before users push to 13K files?

5. **Anything else** you'd flag for a planning agent — undefined terms, ambiguities, dependencies on Tantivy/PyMuPDF/SQLite versions not pinned in the requirements.

## Codex Output Format

Please respond as a structured critique:

```markdown
# Codex Critique of Phase 97 CONTEXT

## P0 issues (must fix before planning)
- ...

## P1 issues (strong recommendation)
- ...

## P2 / improvements
- ...

## Missing decisions (gaps)
- ...

## Overall assessment
[1-2 paragraphs]
```

Be specific. Cite decision IDs (C-01, R-02, F-04, U-03). If you think a decision is correct, you don't need to comment on it — focus on what should change or be added.
