# Phase 97: More LOCAL features - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-05-25
**Phase:** 97-more-local-features
**Areas discussed:** Scope, Capacity ceiling, Crash recovery, HTML/XLSX/CSV extraction, Indexing UX at scale, Codex critique fold-in

---

## Initial Scope Selection

| Option | Description | Selected |
|--------|-------------|----------|
| PDF OCR (D-F2) | Tesseract; deferred to v7.15+ by user | (initial pick, later deferred) |
| Side-by-side PDF (D-F3) | Image alongside extracted text | (initial pick, later deferred) |
| Perf refactors (D-F7/F9) | Background-thread View All + folder walk | |
| More file formats | .md, .epub, .html, .rtf | ✓ (later narrowed to HTML/XLSX/CSV) |

**User's choice:** PDF OCR + Side-by-side PDF + More file formats — plus added Seewald email context flagging capacity/recovery as critical blockers.

**Pivot:** Email from Yehuda Seewald comparing v7.14.0 (5K/2GB hard-cap) to his prototype (13K files / 43GB). Phase 97 reframed around capacity gap.

---

## Scope Refinement (Capacity vs Reading Features)

| Option | Description | Selected |
|--------|-------------|----------|
| Yes — include all 3 (lift ceiling + crash recovery + scale audit) | Full Seewald-driven track | |
| Yes — ceiling only | Defer recovery to v7.16+ | |
| No — keep Phase 97 as OCR + side-by-side + formats only | Polish-only phase | |
| Capacity-only phase | Defer OCR + side-by-side + formats; pure capacity | ✓ |

**User's choice:** Capacity-only phase — but added selected file formats below (HTML/XLSX/CSV as light textual additions that ride along).

**Notes:** OCR + side-by-side deferred to v7.15+ per user direction. Phase 97 now scoped as Capacity + Recovery + 3 light formats.

---

## File-Format Narrowing

| Option | Description | Selected |
|--------|-------------|----------|
| Markdown (.md) | Lightest add | |
| EPUB (.epub) | Heaviest add | |
| HTML (.html / .htm) | Medium | ✓ |
| RTF (.rtf) | Medium | |

**User's choice:** HTML + xlsx + csv (added via free-text). User noted: "several textual synopses are in those formats."

**Notes:** XLSX and CSV were not in the original menu — user added them. Reflects scholarly synopsis-table distribution patterns.

---

## Gray Area Selection

All 4 areas selected: Capacity ceiling design, Crash recovery model, HTML/XLSX/CSV extraction, Indexing UX at scale.

---

## Area 1: Capacity Ceiling Design

### Q1 — New target ceiling

| Option | Description | Selected |
|--------|-------------|----------|
| No hard cap, soft warning at 50K / 50GB | Remove hard-stop, ETA-based dialog only at high thresholds | ✓ |
| No hard cap, no warning at all | Trust user fully | |
| Higher hard cap (50K / 20GB) | Keep hard-stop, raise numbers | |
| Tiered (warning at 10K, hard cap at 100K) | Two-stage gate | |

### Q2 — Memory budget during indexing

| Option | Description | Selected |
|--------|-------------|----------|
| Bounded streaming — 256 MB writer heap | Heap-bounded commits | ✓ |
| Fixed larger batch (100 files) | Simpler, less adaptive | |
| Memory-aware adaptive batch | Sample-based tuning | |
| Keep 25-file batch | Phase 95 default unchanged | |

**Codex revision (P0):** Heap alone is not a durability boundary. Combined with explicit max-files / max-bytes / max-elapsed-seconds.

### Q3 — Pre-scan dialog behavior

| Option | Description | Selected |
|--------|-------------|----------|
| ETA + estimated size, Proceed / Cancel | Non-blocking; only above 50K/50GB | ✓ |
| Skip pre-scan entirely | Faster start, no upfront ETA | |
| Two-mode (small skip, large show) | Hybrid | |
| Always-show summary dialog | Current Phase 95 pattern | |

### Q4 — Status-panel design at 13K+ files

| Option | Description | Selected |
|--------|-------------|----------|
| Aggregate by folder, drill-down on click | Scales to 100K+ | ✓ |
| Virtual-scrolling per-file (QAbstractItemModel) | Keep per-file UX | |
| Filtered per-file (errors + last 100) | Hybrid | |
| Keep current design, accept slowdown | No work | |

### Q5 — Per-file size cap

| Option | Description | Selected |
|--------|-------------|----------|
| 100 MB hard skip + log warning | Prevent pathological PDFs | ✓ |
| 500 MB hard skip | Higher tolerance | |
| No cap — trust user | No safety | |
| Configurable in settings | Advanced surface | |

**Codex revision (P1):** Add zip-container limits for docx/xlsx (500 MB uncompressed, 100K cells, 1 MB per chunk).

### Q6 — Disk-usage surface

| Option | Description | Selected |
|--------|-------------|----------|
| Live indicator + warn at 80% free | Cheap to compute | ✓ |
| Just show index size | No warning | |
| Pre-commit free-space check (block if <1 GB) | Hard safety | |
| Don't surface anything | Trust user | |

**Codex revision (P1):** Account for Tantivy merge headroom (2× current index size) when computing warning threshold.

---

## Area 2: Crash Recovery Model

### Q1 — Behavior on next launch after crash

| Option | Description | Selected |
|--------|-------------|----------|
| Auto-resume silently | Most Seewald-prototype-like | |
| Prompt: Resume / Restart / Skip | Explicit | ✓ |
| Auto-resume + non-modal banner | Hybrid | |
| Manual only (user clicks Refresh) | Simplest | |

**Codex revision (P0):** Gate LOCAL search during recovery to prevent searching inconsistent pending docs.

### Q2 — Tantivy index corruption

| Option | Description | Selected |
|--------|-------------|----------|
| Verify on open; rebuild from SQLite cache | Fast rebuild via cached text | ✓ |
| Delete + force full reindex | Simple, slow | |
| Commit-hash verification | Detect early | |
| Trust Tantivy's own guarantees | No defensive code | |

**Codex revision (P0):** Atomic rebuild via temp-dir swap (not in-place).

### Q3 — Cached text column

| Option | Description | Selected |
|--------|-------------|----------|
| Cache per page in SQLite, zstd-compressed | ~400 MB at 13K files; enables fast rebuild | ✓ |
| File-level metadata only, re-extract on rebuild | Slower rebuild | |
| Cache as sidecar files | Easier inspection, more inodes | |
| Skip cache; document rebuild as "go for coffee" | Save disk | |

**Codex revision (P0):** Needs full SQLite migration plan (PRAGMA user_version=2, test fixtures, backfill behavior). Added as D-NEW-1.

### Q4 — SQLite WAL mode

| Option | Description | Selected |
|--------|-------------|----------|
| WAL + synchronous=NORMAL | Standard cache pattern | ✓ |
| WAL + synchronous=FULL | Max durability | |
| Keep rollback journal | Default mode | |
| Defer to planner benchmark | Evidence-backed | |

**Codex revision (P1):** WAL+NORMAL for routine; explicit FULL or `wal_checkpoint(TRUNCATE)` on the critical pending→committed transition.

---

## Area 3: HTML / XLSX / CSV Extraction

### Q1 — Page-break model per format

| Option | Description | Selected |
|--------|-------------|----------|
| HTML: per-20-para; XLSX: per-sheet; CSV: per-200-rows | Uniform | |
| HTML: semantic h1/h2; XLSX: per-sheet; CSV: per-200-rows | Semantic-aware | ✓ |
| All three: single Tantivy doc per file | Simplest | |
| HTML: 20-para; XLSX: per-sheet + per-200-rows; CSV: per-200-rows | Tightest relevance | |

**User notes:** "Make the HTML fallback to 20-paragraph chunks if h1/h2 are sparse."

**Codex revision (P1):** XLSX should also chunk per-row-window inside sheets (F-02), not one-doc-per-sheet.

### Q2 — XLSX/CSV header-row handling

| Option | Description | Selected |
|--------|-------------|----------|
| First row searchable + tagged as header | Renders rows as "Header: value" | |
| First row treated as plain text — no header logic | Uniform extraction | |
| First row stripped | Skip as schema | |
| Auto-detect heuristic | Detect by content | |

**User's choice (free-text):** "xlsx/csv may be in many different variations. Probably we should not assume what is their structure ahead." Captured as F-04 uniform extraction.

### Q3 — CSV encoding policy

| Option | Description | Selected |
|--------|-------------|----------|
| utf-8-sig + cp1255 fallback | Excel-friendly | ✓ |
| utf-8-sig only (strict) | Phase 95 mirror | |
| utf-8-sig + chardet | Phase 95 rejected chardet | |
| utf-8-sig + cp1255 + utf-16 | Wider chain | |

**Codex revision (P1):** Widen to include utf-16-le (Excel non-ASCII default) + delimiter sniffing via `csv.Sniffer`.

### Q4 — Hebrew RTL handling

| Option | Description | Selected |
|--------|-------------|----------|
| Honor format hints + Phase 95 helpers as dead-code safety net | Initial pick | (later reversed) |
| Ignore RTL hints — trust parsers | Cleanest | |
| Reverse logical-order strings | Wrong direction | |
| Per-format test fixtures, planner decides | Defer | |

**Codex revision (P0):** REVERSE the choice — applying `_fix_rtl_line`/`_fix_rtl_page` to HTML/XLSX/CSV will corrupt already-correct Hebrew. Honor format hints as **metadata only** (no text reversal). Captured as F-06 reversal.

---

## Area 4: Indexing UX at Scale

### Q1 — ETA strategy

| Option | Description | Selected |
|--------|-------------|----------|
| Bytes-based ETA | Closer to reality for mixed sizes | |
| Files-based ETA (Phase 95 default) | Simple | |
| Hybrid: bytes + file count | Both displayed | ✓ |
| No ETA, just throughput | Honest | |

**Codex revision (P2):** Phase-aware ETA — separate sub-progress for walking / extracting / committing / rebuilding LAB.

### Q2 — Cancellation semantics

| Option | Description | Selected |
|--------|-------------|----------|
| Commit current batch + stop | Phase 95 pattern extended | |
| Stop immediately, discard in-flight | Loses up to 1 batch | |
| Stop immediately, keep partial | Risky | |
| Prompt confirmation at >5K | Pause option | |

**User's choice (free-text):** "When user cancels, I assume he doesn't want the batch at all. But it can be otherwise so it can prompt: cancel and discard library or keep what has been done so far?" Captured as U-02 with two-option confirmation.

**Codex revision (P0):** Needs `scan_run_id` + per-run manifest for reliable discard across multiple committed batches.

### Q3 — Main-thread responsiveness

| Option | Description | Selected |
|--------|-------------|----------|
| Folder walk to QThread + status throttling | Closes D-F9 | ✓ |
| Folder walk only, no throttling | Lift worst freeze | |
| Throttling only, walk stays on UI | Cheap | |
| Defer all UI-thread work | Smaller phase | |

**Codex revision (P1):** Widget mutation MUST stay on UI thread via batched signals — worker only does filesystem work.

### Q4 — View All cap

| Option | Description | Selected |
|--------|-------------|----------|
| Lift via background-thread aggregation | Closes D-F7 fully | |
| Raise cap to 500, keep main-thread | Lower effort | ✓ |
| Lift cap + external viewer for >500 | Hybrid | |
| Defer D-F7 | No work | |

**Codex revision (P2):** Avoid intentional 30-sec main-thread freeze — use cached_text (R-03) + incremental rendering (`QTimer.singleShot(0, append_next_batch)`).

---

## Codex Critique Fold-In

| Option | Description | Selected |
|--------|-------------|----------|
| Adopt all P0 + most P1 | Recommended | ✓ |
| Adopt P0 only, defer P1/P2 | Lighter | |
| Adopt P0 + reverse sequencing | Multi-wave plan | (effectively also adopted — see Wave structure) |
| Discuss specific items | Interactive | |

**User's choice:** Adopt all P0 + most P1 — folded inline into CONTEXT.md as `[Codex revision]` annotations on every affected decision, plus 8 new D-NEW-N decisions closing missing-decision gaps, plus a 6-wave Sequencing section that addresses Codex's overall-assessment recommendation (recovery foundation before ceiling lift).

---

## Claude's Discretion

- F-01 "sparse" threshold heuristic — planner tunes based on smoke tests
- Wave-internal ordering of parallelizable items
- Specific zstd compression level for `cached_text`
- Worker thread count for parallel extraction
- Exact split between rebuild-from-cache vs rebuild-from-source

---

## Deferred Ideas (noted during discussion)

- D-F2 PDF OCR — explicitly deferred to v7.15+
- D-F3 Side-by-side PDF rendering — explicitly deferred
- `.md`, `.epub`, `.rtf` formats — not requested this phase
- Full D-F7 background-thread View All refactor — partially addressed via U-04
- D-F8 substring page-block matching refactor — Phase 96 known limit
- D-F10 View All renderer path consolidation
- Configurable per-file size cap as advanced setting (Codex P2)
- Per-folder telemetry (Codex P2)
- Format-extraction telemetry (Codex P2)
