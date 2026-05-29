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

### Nikud (vocalized text) — FINAL 2026-05-29 (strip everywhere; nikud display DEFERRED)
- **D-06 (FINAL — supersedes all earlier nikud framings in this file):** **Strip nikud once for
  ALL LOCAL formats, for BOTH index and display.** Decision history: (1) round-1 picked
  content/cached_text *divergence*; (2) user said "no need to display nikkud" → strip-once; (3)
  round-2 M4 narrowed the strip to PDF-only; (4) investigation then PROVED that un-vocalized search
  REQUIRES a stripped index for **every** format (see "Why strip is mandatory" below), so PDF-only
  would leave vocalized DOCX/TXT/HTML **unfindable**; (5) user weighed full divergence (searchable +
  nikud display) vs. simplicity and chose: **start simple — strip everywhere now, no nikud display,
  log the nikud-display enhancement as DEFERRED.**
- **Implementation:** apply `genizah_core.strip_nikud` once at the **single shared write site
  `_write_page_doc`** (which serves PDF, DOCX, TXT, HTML, XLSX, CSV) as the value written to BOTH
  the Tantivy `content` field AND `local_pages.cached_text`. They stay EQUAL — exactly as today —
  just stripped. Bump `extraction_format_version` 1→2. **Lazy-import** `strip_nikud` inside
  `_write_page_doc` (keep `shared/local_indexer.py` free of a module-top `genizah_core` import —
  round-2 L1). NO divergence, NO second Tantivy field, NO `genizah_core` read-path change, NO
  cached_text-by-uid lookup. PDF text still carries nikud THROUGH de-space/reorder (D-04 gap math);
  the strip is simply the value `_write_page_doc` persists, applied to all formats uniformly.
  → **REVERTS round-2 M4** (no PDF-path strip in `extract_pdf_pages`; `extract_pdf_pages` yields
  nikud-bearing reconstructed text and `_write_page_doc` does the strip for all formats).
- **Why strip is mandatory for search (the decisive finding, verified in live code):** the LOCAL
  Tantivy `content` field uses the **`whitespace` tokenizer** (`shared/local_indexer.py:535`) — no
  diacritic folding; `שלוֹם`-style tokens keep their vowel marks. The query is NOT nikud-stripped,
  and `strip_search_diacritics` (`genizah_core.py:6302`) deliberately PRESERVES nikud. So an
  un-vocalized query token (`אמר`) can never retrieve a vocalized indexed token (`אָמַר`) — Tantivy
  returns zero candidates and the regex phase (whose mark-tolerance excludes nikud `0x05B0-0x05C7`)
  never runs. The ONLY way `אמר` finds `אָמַר` is a **nikud-stripped index**. (The main Genizah
  corpus avoids this because its text is already un-vocalized.)
- **Rebuild path:** `rebuild_main_index_atomic` (`shared/local_indexer.py:3052`/`:3072`) re-indexes
  `cached_text` into `content`. Because the new extractor writes a STRIPPED `cached_text`, the
  rebuilt `content` stays stripped automatically — no change strictly required. Optionally add a
  defensive `strip_nikud` there to also normalize legacy pre-Phase-102 rows on rebuild (cheap,
  idempotent, all-format; non-load-bearing).
- **Display consequence (accepted):** all LOCAL display becomes consonantal. For PDF this is
  invisible to the user (the page **image** rendered alongside still shows the vowels, v7.15). For
  DOCX/TXT/HTML the extracted-text panel shows consonantal text — accepted for now (see DEFERRED).

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
  NOTE (Codex round-3): the pre-existing `encoding_error` status STAYS in the indexed bucket at
  `:1951` (it is a legacy status, not a CONTEXT decision); only the NEW `corrupt_encoding` routes to
  the error bucket.

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
- F-B punctuation spacing (no space before punctuation) normalization details — include Hebrew
  sof-pasuq (U+05C3) / maqaf (U+05BE) coverage, not ASCII-only (Codex round-3 LOW).
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
- `_write_page_doc` (`:2420`) — **the single shared write site for D-06 (strips ONCE for all
  formats)**; writes Tantivy `content` (`:2466`) and `cached_text` (`:2484`).
- `_index_one_file` (`:2177`) — pre-inserts `processed_files` (`:2225`) + `local_files` (`:2243`)
  BEFORE extraction; `_rollback_partial` (`:2653`) deletes both for a sys_id; `_commit_batch`
  (`:2809`) flips pending → committed (the buffer-phase-cancel rollback dependency, Codex round-3).
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
  query-side and in the regex filter phase; reuse `strip_nikud` for the D-06 index-side stripping
  (applied to BOTH content and cached_text at `_write_page_doc`).

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
  time; reuse `strip_nikud` for the D-06 index-side normalized copy (don't write a new stripper).
- **`compress_cached_text` / zstd `cached_text` plumbing** — under D-06 FINAL, `cached_text`
  stores the STRIPPED text (equal to `content`), not the original nikud-bearing text. Keep the
  zstd plumbing as-is; just pass the stripped value.
- **Existing `extraction_status` machinery** — D-08 adds one value across the same surfaces.

### Established Patterns
- `_write_page_doc` currently writes the **same** `text` to Tantivy `content` AND `cached_text`;
  under D-06 FINAL it KEEPS them the SAME — both receive the STRIPPED value (content == cached_text
  == stripped, NO divergence). The load-bearing change is the single strip-once at this site for all
  formats + the `extraction_format_version` 1→2 bump. (An earlier draft proposed diverging the two
  fields; D-06 FINAL reversed that — see D-06 above.)
- Status values flow through 4 surfaces (kept-set, scan classification, folder counters, tree
  label/color) — adding `corrupt_encoding` means touching all 4 (Codex HIGH-4). The pre-existing
  `encoding_error` stays indexed (Codex round-3).
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
- **Strip nikud once at the single write site (`_write_page_doc`), as the LAST transform** —
  D-06 (FINAL) keeps `content` and `cached_text` EQUAL and stripped (no divergence, no display
  field, no `genizah_core` read-path change). Nikud is retained only through the de-space/reorder
  glyph math and dropped from the final persisted string. (User 2026-05-29: nikud need not be displayed.)
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
- **Nikud DISPLAY for non-PDF LOCAL formats (`SEED-004`, deferred 2026-05-29):** Phase 102 strips
  nikud from the LOCAL index AND display for all formats so un-vocalized search works (D-06 FINAL).
  PDF hides this (page image shows vowels); DOCX/TXT/HTML show consonantal extracted text. If users
  find consonantal non-PDF display unhelpful, restore nikud display via a stored-only Tantivy
  `content_display` field (nikud) alongside the stripped `content` (search) — read it at the two
  display sites (`genizah_core.py:7166`, `:9631`). Cost: larger LOCAL index. (The cached_text-by-uid
  variant is heavier — `genizah_core`'s search/browse hold no SQLite connection.) Start simple now;
  revisit only if shown useful. Logged in `docs/OPEN_ISSUES.md`.

### Reviewed Todos (not folded)
The 6 todos `todo.match-phase` surfaced (corrections-service migration, Reading-Desk UX,
server-side search, NLI MARC crawl, unified metadata search, fill-missing-manuscripts) all
matched on generic keywords (desktop/shared/search) and are **unrelated to PDF text
extraction** — none folded.

</deferred>

---

*Phase: 102-pdf-extraction-reorder-adopt-meiri-glyph-level-parser-d-f13*
*Context gathered: 2026-05-29*
