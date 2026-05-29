# Phase 102: LOCAL PDF Text-Layer Extraction Rewrite (RTL-gated reorder + letter-spacing de-collapse) - Context

**Gathered:** 2026-05-29
**Status:** Ready for planning

<domain>
## Phase Boundary

Rewrite the desktop **My Library** PDF text-layer extractor in `shared/local_indexer.py`
onto a `page.get_text("rawdict")` (per-glyph bbox) foundation that produces clean,
searchable plain text for the Tantivy indexer. Fixes the Spike 001 failure-mode catalog
(F-A..F-G). Closes **D-F13**; reframes **D-F14** (adopt Meiri's reorder *core*, RTL-gated —
NOT wholesale, NOT the DOCX pipeline); detects **D-F16** (corrupt encoding).

**In scope:** RTL-gated segment reorder (adapted Meiri core), adaptive letter-spacing
de-collapse, punctuation normalization (F-B), reversed-bracket fix (F-C), running-header
reversal (F-F), corrupt-encoding detection (F-G/D-F16). Desktop-only, text-layer PDFs.

**Out of scope:** OCR for image-only scans (D-F2 → deferred optional extension `SEED-003`);
multi-column layout reconstruction (detect-and-flag only — see D-09); P3 View-All renderer
cleanups (D-F8/D-F10/D-F7).

**Hard constraint — NO LTR regression.** Meiri's reorder HURTS Latin/LTR (spike: NW Semitic
Dictionary was better in the current extractor). RTL transforms must be gated to Hebrew/RTL
content; clean/modern/Latin PDFs that work today must not degrade.

</domain>

<decisions>
## Implementation Decisions

### Architecture (path & gating)
- **D-01:** `rawdict` is the **single primary extraction path** for every PDF page (a full
  rewrite, NOT a triggered fallback like today's single-word-per-line detector). Rationale:
  the letter-spacing bug is whole-book, not pathological-page — a letter-spaced book has many
  tokens per line so it never trips a single-*word*-per-*line* detector, and a trigger-based
  design would miss it. De-space + reorder apply **only to RTL/Hebrew content**; LTR content
  passes through rawdict's natural text untouched.
- **D-02:** RTL gate granularity = **per-line**. Lines are rebuilt from glyph y-bands
  (cf. Meiri `_regroup_lines`), each classified RTL vs LTR by Hebrew ratio, gated per line.
  Per Codex MED-5: grouping must be **baseline/font-size based, NOT a fixed `y_tol=2.5`**;
  compute the baseline from **base glyphs only** (ignore nikud combining marks and
  superscript footnote refs) so vocalized Hebrew isn't split and superscripts don't merge
  into the wrong row.
- **D-03:** Safety net = **LTR-damage guard** (strengthened per Codex HIGH-1, supersedes the
  weaker empty-only idea). For low-Hebrew/LTR pages, compare rawdict output against the old
  `blocks` output (token-count / Jaccard) and **fall back to `blocks` for that page** if
  rawdict loses or scrambles too much — not only when rawdict is empty. Keep the old `blocks`
  extraction as this thin net. Protects clean/modern/Latin PDFs that already work today.

### Letter-spacing de-collapse (F-D / F-E — the dominant bug)
- **D-04:** Adaptive **1.8× median inter-glyph gap** threshold (spike-proven) + the PDF's
  embedded space-glyph positions as **soft hints**. Per Codex MED-6, use **hysteresis / two
  thresholds**: a hard break for very large gaps; a mid-gap break only when corroborated by an
  explicit space cluster, punctuation boundary, font/span boundary, or abnormal-long-token
  guard. Otsu/bimodal split **deferred** unless regression fixtures show it's needed.
- **D-05:** De-space runs **BEFORE** reorder (so de-spaced words can be reordered — F-E).
  Per Codex HIGH-3, implement via **word-unit inference (bbox unions)** — do **NOT** mutate
  the `chars` list with synthetic zero-bbox space glyphs before Meiri reorder (zero-bbox
  spaces create bogus x-direction jumps in `_normalize_span_dir`). Pipeline:
  original glyphs → drop nikud for metrics → infer word units via bbox unions →
  reorder units/segments (RTL-gated) → emit plain string.

### Nikud (vocalized text) — REVISED per Codex HIGH-2
- **D-06:** **Keep original nikud-bearing text** in `local_pages.cached_text` (drives result
  display, page browsing, index rebuild) and **index a nikud-STRIPPED copy** in the Tantivy
  `content` field (so un-vocalized queries still match). This diverges `content` from
  `cached_text` in `_write_page_doc` (today they're the same `text`) and requires an
  `extraction_format_version` bump. Reuse `genizah_core.strip_nikud`. *(Reverses the earlier
  strip-at-extraction pick, which was irreversible and would have made all display
  consonantal-only.)* Nikud combining marks are also excluded from the D-04 gap math (so a
  mark between two consonants can't be misread as a word boundary).

### Corrupt-encoding detection (F-G / D-F16)
- **D-07:** Detect via a **codepoint-garbage ratio** on the extracted text — NOT ToUnicode
  cmap introspection. Per Codex MED-7, be **conservative**: count U+FFFD, PUA, noncharacters,
  illegal controls, and low wordlike ratio; **ALLOW** Arabic, Greek, common math, Hebrew
  presentation forms, bidi marks, broad punctuation. Flag file-level only with strong evidence
  (e.g. text length ≥ 100 AND FFFD/PUA/control garbage > 5–10%, OR wordlike chars < ~40% with
  high unknowns).
- **D-08:** New `extraction_status = 'corrupt_encoding'`, surfaced in the My Library tree as a
  future-OCR (`SEED-003`) candidate. Per Codex HIGH-4, wire it into **all** status surfaces:
  `_ERROR_STATUSES_KEPT` (`shared/local_indexer.py:125`), scan success/error classification
  (`:1951`), folder counter aggregation (`:2868`), and tree label/color mapping
  (`desktop/my_library_tab.py:333` `_build_leaf_item_status`). Otherwise it counts/displays wrong.

### Scope boundaries
- **D-09:** Multi-column layouts (Talmud, dictionaries) are **deferred** — out of scope for
  reconstruction. But add a **cheap "multi-column suspected" detector + log/status marker**
  (Codex LOW-10) so UAT footnote/dictionary/Talmud-page failures are explainable rather than
  silent. Seed a follow-up item. Phase 102 targets single-column text-layer PDFs.
- **D-10:** **Migration of existing indexed libraries = NO new auto-flip mechanism.** Users
  re-run the existing manual **"Re-index All" / "אנדקס מחדש הכל"** button. Honors the Phase 101
  D-04 rollback and the hard rule: **no mass re-indexing from `__init__` / the UI thread** —
  bulk row-state changes defer to the `LocalIndexerWorker` background thread.

### Performance
- **D-11:** **Disable image data in rawdict** via the appropriate `TEXTFLAGS` (Codex LOW-8).
  PyMuPDF rawdict is ~4.5× baseline with default image behavior vs ~1.68× with images
  excluded — material for a 12K-PDF re-index.

### Claude's Discretion
- F-B punctuation spacing (no space before punctuation) normalization details.
- F-C reversed-parens fix — adopt/adapt Meiri's `_fix_visual_brackets` (`pdf_to_docx.py:653`).
- F-F running-header word reversal — falls out of the RTL reorder core.
- `extraction_format_version` bump bookkeeping + rebuild-path handling for the new format.
- Deliberate Unicode normalization of Hebrew presentation forms / Latin ligatures
  (Codex LOW-9) — keep display vs index text separate.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Spike evidence (read first)
- `.planning/spikes/001-meiri-glyph-reorder-vs-current/README.md` — failure-mode catalog
  F-A..F-G, verdict PARTIAL, the directional findings, and the final decisions that reshaped
  this phase.
- `.planning/spikes/001-meiri-glyph-reorder-vs-current/compare_extractors.py` — the Meiri
  reorder wrapper that was benchmarked (`meiri_extract_page_text`, `_metrics`).
  ⚠ **Note:** the 1.8×-median de-space prototype is described in the README (Finding 3) with
  before/after examples but is **NOT committed** in the spike `.py` files — researcher/planner
  must re-derive or re-prototype it from the README description.

### Cross-AI critique (locked refinements live here)
- `.planning/phases/102-.../102-CODEX-CRITIQUE.md` — 11 findings; HIGH-1/2 changed D-03/D-06,
  HIGH-3/4 + all MED/LOW are baked into the decisions above.

### Current extractor + integration points (`shared/local_indexer.py`)
- `extract_pdf_pages` (`:794`) — the function being rewritten; current blocks→sort=True path.
- `_fix_sort_true_rtl_line` / `_fix_sort_true_rtl_page` (`:371` / `:431`) — Phase 101 S-1
  per-line directional-run reversal (the RTL-gating idea to carry forward / supersede).
- `_collapse_intra_block_newlines` (`:458`), `_detect_single_word_per_line` (`:483`).
- `_write_page_doc` (`:2420`) — **the content-vs-cached_text split site for D-06**; writes
  Tantivy `content` (`:2466`) and `cached_text` (`:2484`).
- `compress_cached_text` (`:635`), `_ERROR_STATUSES_KEPT` (`:125`), scan classification
  (`:1951`), folder counter aggregation (`:2868`) — D-08 wiring.
- `shared/local_indexer_migrations.py` — `extraction_format_version` column / bump site.

### Meiri reorder core (`ephraim_meiri_pdf_converter/pdf_to_docx.py`)
- `_normalize_span_dir` (`:691`) — the RTL segment-reorder core to adapt (x-direction
  segmentation, RTL sort, embedded-digit-run handling, `MAX_BACKWARD_JUMP`).
- `_span_text` (`:897`), `_fix_visual_brackets` (`:653`, F-C), `_attach_nikud_page` (`:791`,
  nikud-attachment reference), `_regroup_lines` (`:854`, line-grouping reference for D-02).

### Search normalization (reuse, don't reinvent)
- `genizah_core.py`: `strip_nikud` (`:199`), `strip_search_diacritics` (`:6302`) — used
  query-side and in the regex filter phase; reuse for the D-06 index-side stripping.

### UI status surface
- `desktop/my_library_tab.py`: `_build_leaf_item_status` (`:333`) — D-08 `corrupt_encoding`
  label + color.

### Tracker entries
- `docs/OPEN_ISSUES.md` — D-F13 (`:517`), D-F14 (`:518`), D-F16 (`:519`), D-F2 (`:506`).
- `.planning/ROADMAP.md` — Phase 102 (`:307`).

### Existing test fixtures (`tests/fixtures/local_indexer/`)
- `single_word_per_line.pdf` (existing guard — must still pass), `clean_sample.pdf`,
  `hebrew_sample.pdf` + `.expected.txt`, `hebrew_rtl_fixture.pdf`, `corrupt_sample.pdf`,
  `encrypted_sample.pdf`, `multipage_sample.pdf`.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **Meiri `_normalize_span_dir` + helpers** — the reorder core to adapt (RTL-gated), plus
  `_regroup_lines` (line grouping) and `_attach_nikud_page` (nikud handling reference).
- **`genizah_core.strip_nikud` / `strip_search_diacritics`** — already strip nikud at search
  time; reuse for the D-06 index-side normalized copy (don't write a new stripper).
- **`compress_cached_text` / zstd `cached_text` plumbing** — D-06 keeps original text here.
- **Existing `extraction_status` machinery** — D-08 adds one value across the same surfaces.

### Established Patterns
- `_write_page_doc` currently writes the **same** `text` to Tantivy `content` AND
  `cached_text`; D-06 deliberately diverges them. This is the load-bearing change.
- Status values flow through 4 surfaces (kept-set, scan classification, folder counters, tree
  label/color) — adding `corrupt_encoding` means touching all 4 (Codex HIGH-4).
- Bulk re-extraction is background-worker-only (`LocalIndexerWorker`); NEVER `__init__`/UI
  thread (Phase 101 D-04 rollback).

### Integration Points
- `extract_pdf_pages` is the single function the rewrite lands in; callers
  (`_extract_and_write_pdf` `:2495`) iterate its `(page_num, text, title)` yield unchanged.
- `extraction_format_version` bump propagates through `local_pages` + the index-rebuild path
  (`rebuild_main_index_atomic`, reads `cached_text`).

</code_context>

<specifics>
## Specific Ideas

- **"De-space before reorder, via bbox-union word units, never synthetic spaces"** is the
  precise sequencing the planner must honor (Codex HIGH-3) — it's the subtle correctness trap.
- **Separate display text (with nikud) from index text (stripped)** is the architectural
  spine of D-06 — the rest of nikud handling follows from it.
- Verification must include a **letter-spaced fixture (אוצר הגאונים-style)**, a
  **letter-spaced + order-reversed line**, an **RTL running header**, a **corrupt-encoding
  file (Israeli_Vilna_shabbat-style)**, AND an **LTR/Latin no-regression case**. Add **both
  under-split and over-merge** de-space fixtures (Codex MED-6). Where a real PDF can't be
  committed, use **rawdict JSON / glyph-trace fixtures** — text-only `.expected.txt` won't pin
  a bbox-dependent bug (Codex LOW-11).

</specifics>

<deferred>
## Deferred Ideas

- **Multi-column reconstruction** — detect-and-flag only in Phase 102 (D-09); full
  column-aware extraction is a future phase. Common in Talmud/dictionary/Geniza books.
- **OCR for image-only scans (D-F2)** — optional opt-in extension `SEED-003`; a large share
  of the real library is image-only and currently unsearchable, but OCR is heavy and most
  users won't need it. `corrupt_encoding` (D-08) files are future OCR consumers.
- **Otsu/bimodal gap split** — only if D-04 hysteresis proves insufficient on fixtures.

### Reviewed Todos (not folded)
The 6 todos `todo.match-phase` surfaced (corrections-service migration, Reading-Desk UX,
server-side search, NLI MARC crawl, unified metadata search, fill-missing-manuscripts) all
matched on generic keywords (desktop/shared/search) and are **unrelated to PDF text
extraction** — none folded.

</deferred>

---

*Phase: 102-pdf-extraction-reorder-adopt-meiri-glyph-level-parser-d-f13*
*Context gathered: 2026-05-29*
