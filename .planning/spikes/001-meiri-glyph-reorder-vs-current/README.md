---
spike: 001
name: meiri-glyph-reorder-vs-current
validates: "Given Hillel's real problem PDFs, when extracted with Meiri's glyph-level rawdict reorder core vs. the current blocks+S-1 stack, then Meiri produces measurably better Hebrew reading order (intact words, no reversed letter-spaced emphasis, no bidi fragmentation)"
verdict: PARTIAL
related: []
tags: [pdf, rtl, hebrew, extraction, phase-102, d-f13, d-f14]
---

# Spike 001: Meiri Glyph-Level Reorder vs. Current Extractor

## What This Validates

**Given** 3–5 of Hillel's real problem PDFs (≥1 with Hebrew letter-spaced emphasis
like `מ ש נ ה  ת ו ר ה`, ≥1 bidi-fragmented Hebrew paragraph, optionally ≥1 OCR'd
image-only PDF),
**when** each is extracted by (a) the current production path
`shared.local_indexer.extract_pdf_pages` and (b) a minimal plain-text wrapper around
Meiri's real reorder functions (`_normalize_span_dir` + `_span_text` +
`_attach_nikud_page`) over `get_text("rawdict")`,
**then** we can judge whether Meiri produces **better** Hebrew reading order —
intact words, no reversed letter-order on emphasized words, fewer single-letter
fragments — and whether `rawdict` per-glyph bbox is usable on OCR'd PDFs.

## How to Run

From the project root `C:\Genizahsearch`:

```bash
python .planning/spikes/001-meiri-glyph-reorder-vs-current/compare_extractors.py \
    "C:\path\to\problem1.pdf" "C:\path\to\problem2.pdf" "C:\path\to\problem3.pdf"
```

Outputs:
- A per-page quantitative table to stdout (chars, Hebrew tokens, single-Hebrew-letter
  tokens, mean Hebrew word length) for CURRENT vs MEIRI.
- Full side-by-side text per PDF written to `out/<pdfname>.txt` for human reading.

## What to Expect

- **CURRENT** on a letter-spaced-emphasis PDF should show the bug: emphasized words
  with reversed letter order and/or many single-Hebrew-letter tokens.
- **MEIRI** should reconstruct those words (lower `single_heb_letter_tokens`, higher
  `mean_heb_word_len`) and read in correct RTL order.
- On a clean Hebrew PDF both should look similar (sanity check — no regression).
- On an OCR'd PDF, MEIRI may degrade (rawdict gives per-line/word bbox, not per-glyph) —
  this is the signal for whether OCR (D-F2) can ever share this path.

## Decision This Drives

- **(A) Meiri wins cleanly** → adopt the reorder core wholesale in Phase 102;
  D-F13 likely resolves for free.
- **(B) Wins on text PDFs but rawdict unusable on OCR** → keep clean-PDF path only;
  OCR (D-F2) stays out of scope.
- **(C) No clear win** → abandon Phase 102; patch D-F13 narrowly in
  `_fix_sort_true_rtl_line` instead.

## CORRECTION (after Hillel's expert review of the diff batch)

The metric-only comparison (Finding 1 below) was **reorder-blind** — char/token counts
cannot see word reordering. On a reorder-visible text diff + Hillel's reading, the true
picture is:

- **Neither extractor is universally better.** Meiri wins in קדמוניות/Yosipon
  (mixed-direction, digits, parens, footnote order). CURRENT wins in places too —
  e.g. ירחי משוח מלחמה: `אופנים . האחד – הוא דן באותו פסוק שקודמו דן בו ,120 "ויצא
  איש הביניים ממחנות פלשתים גלית"` is correct in CURRENT, Meiri makes it worse.
- **Reading-order reversal IS a real bug** (Shilat headers + lines) — my original
  "reading order already correct" was wrong; it was only true for the Yosipon pages I
  happened to read.

### Shared failure modes both extractors exhibit (Phase 102 target list)

- **F-A — reference/footnote number misplacement:** the note/ref number lands at the
  start or middle of a line instead of its correct position (digit-run reordering both
  get wrong in different ways).
- **F-B — punctuation spacing:** a space is inserted before punctuation
  (`אופנים .` should be `אופנים.`).
- **F-C — reversed parentheses:** opening/closing parens come out mirrored. (NOTE: my
  Meiri port omitted `_fix_visual_brackets`; re-test with it wired in before judging
  Meiri on this axis.)
- **F-D — letter-spacing fragmentation:** `מ ל ח מ ת` → single-letter tokens (neither
  fixes; rawdict adaptive de-spacing prototyped, works).
- **F-E — letter-spaced AND order-reversed lines:** `היה ב ל ש ו ן ... מ ה נ ה` should
  be `מהנה היה בלשון ערבי` (neither fixes; needs de-space BEFORE reorder).
- **F-F — running-header reversal:** `תיכון אבן` should be `אבן תיכון` (in scope).

### Open work for the spike

Hillel: "we should inspect more sorts of PDFs to find other rendering issues." Current
corpus is 5 books (mostly clean-text typeset Hebrew). Still unprobed: OCR/image-only
scans (D-F2), vocalized/nikud text, multi-column journals, Judeo-Arabic, manuscript
transcriptions. Spike 002 / continued 001 should broaden the corpus and complete the
failure-mode catalog before the phase plan locks.

---

## FINAL SYNTHESIS (after corpus profiling + visual review)

### Corpus reality (profile_corpus.py — 1 PDF/folder across all 30 folders of ~18K PDFs)

- **A large share of the real library is IMAGE-ONLY scans** (no text layer): תפילה (siddur),
  מדרש (Albeck), ערוך, פילון, מילון בן יהודה, אנציקלופדיות, בית שני, תרגומים, ספריה, and
  more. The current My Library indexer indexes **nothing** for these.
- **Letter-spacing fragmentation is rampant** in text-layer Hebrew scholarship:
  ראשונים/אוצר-הגאונים 0.46, רמבם 0.21, ספרי-מחקר 0.15 (single-Heb-letter-token ratio).
- **Nikud** (vocalized) and **2-column** layouts (Talmud, dictionaries, Geniza books) common.

### Full failure-mode catalog (F-A … F-G)

| ID | Failure | Fixed by current? | Fixed by Meiri? | Phase 102? |
|----|---------|-------------------|-----------------|------------|
| F-A | ref/footnote number misplaced (line start/mid) | no | partial | yes |
| F-B | space before punctuation (`אופנים .`) | no | no | yes |
| F-C | reversed parentheses | no | partial (`_fix_visual_brackets`) | yes |
| F-D | letter-spacing → single-letter tokens | no | no | yes (adaptive de-space) |
| F-E | letter-spaced AND order-reversed line | no | no | yes (de-space BEFORE reorder) |
| F-F | running-header word reversal | no | **yes** | yes |
| F-G | **corrupt text-layer encoding** (e.g. Israeli_Vilna_shabbat_part_2.pdf — bad/missing ToUnicode cmap; bytes are garbage) | no | no (unfixable by reorder) | detect + flag/skip; OCR is the only real fix |

### Key directional findings

- **Meiri's reorder is Hebrew/RTL-specific.** On Latin/LTR PDFs (NW Semitic Dictionary)
  CURRENT is better — Meiri's reordering HURTS LTR. Phase 102 must **gate reorder to RTL
  content per line/block and not regress LTR PDFs.**
- **"Most times Meiri is better"** (Hillel) on Hebrew → adopt the RTL reorder core.
- Neither tool fixes letter-spacing (F-D/F-E) — the adaptive per-line gap de-collapse
  prototyped here (1.8× median gap, ignore embedded space glyphs) is the missing piece.

### DECISIONS (Hillel, this spike)

1. **Phase 102 = RTL-gated text-layer extraction rewrite** on rawdict: Meiri-style segment
   reorder (RTL-gated) + adaptive letter-spacing de-collapse + bracket/punctuation
   normalization + header reversal handling + corrupt-encoding (F-G) detection. Closes
   D-F13, reframes D-F14 (adopt reorder *core*, not wholesale). **No LTR regression.**
2. **OCR (D-F2) = deferred OPTIONAL extension**, seeded (not in Phase 102): opt-in,
   on-demand, separate install, common users unaffected; off-the-shelf pre-OCR is the
   power-user escape hatch. Build only on demand. F-G corrupt-encoding files are a future
   OCR consumer.

---

## Results — VERDICT: PARTIAL (reframes Phase 102) — see FINAL SYNTHESIS + CORRECTION above, supersedes Findings 1-2

Ran on 5 real PDFs (Yosipon/Kadmoniyot 1944 Bialik, Igrot ha-Rambam–Shilat, Yarhei
Mashuach Milchama, Dead Sea Scrolls Reader 4, Shemot Rabbah–Shanan), 6 sampled pages each.

### Finding 1 — Meiri's reorder core does NOT beat the current extractor

CURRENT vs MEIRI metrics were **identical** on every page (char counts within ~0.5%,
single-letter-token counts and mean Hebrew word length effectively equal). The literal
D-F14 bet — "adopt Meiri's `_normalize_span_dir` wholesale and the PDFs get better" — is
**INVALIDATED**. The reorder core changes nothing on these files.

### Finding 2 — The real bug is letter-spacing, not reading-order reversal

Reading order is **already correct** in the current output. Example (Yosipon p.73):
`מ ל ח מ ת כ ו ש ש ל משה` reads correctly RTL — it's just that every letter of the
justified/emphasized words is space-separated, so each letter becomes its own token.
On the worst pages **>88% of Hebrew tokens are single letters** (p.73: 1341/1520;
Shemot Rabbah p.161: 593/905). This is the D-F13 bug at full-book scale, in old
justified Hebrew typesetting (1944 Bialik, Shanan). Modern PDFs (Shilat) are clean —
the bug is edition-specific, not universal.

### Finding 3 — A rawdict glyph-gap re-collapse fixes it; needs a per-line adaptive threshold

Inter-glyph center gaps on letter-spaced lines are **bimodal**: intra-word ~1.7–4.0,
word boundaries ~6.9–8.2. A **global** threshold fails because justification varies the
spacing per line. A **per-line adaptive** threshold (word-break = gap > 1.8× the line's
median gap, ignoring embedded space glyphs and re-deriving spacing from bboxes)
reconstructs real words:
- `מ ל ח מ ת כ ו ש ש ל משה` → `מלחמת כוש של משה`
- `בידו א ת ה ע י ד ב ת נ א י ש ת י נ ש א לו` → `בידו את העיד בתנאי שתינשא לו`

Residual over-merging (`ישנו אצל יוסף` → `ישנואצליוסף`) where a word-gap dips under the
threshold — tunable via bimodal/Otsu split or using the PDF's embedded space-glyph
positions as hints. Even unrefined, it converts ~1300 junk single-letter tokens into
mostly searchable words.

**This fix REQUIRES `rawdict` glyph bboxes — the current `get_text("blocks")` path
structurally cannot do it.** So moving to a rawdict-based extractor is the right
architectural direction — just with a *letter-spacing re-collapse* algorithm, NOT
Meiri's segment-reorder function.

### Finding 4 — OCR/D-F2 probe inconclusive

The Dead Sea Scrolls Reader has a text layer (mostly English; 0 Hebrew tokens on most
sampled pages), so it is NOT an image-only PDF and did not exercise the OCR path.
rawdict-on-OCR remains unprobed; D-F2 stays out of scope and unvalidated.

### Decision

Neither (A) adopt-Meiri-wholesale nor (C) abandon. The spike surfaced a **third path**:
re-scope Phase 102 around a **rawdict-based adaptive letter-spacing re-collapse**
extractor (fixes D-F13 *and* the pervasive whole-book fragmentation in one algorithm;
reading-order reversal is a non-issue on these PDFs). The ROADMAP Phase 102 goal —
currently centered on adopting `_normalize_span_dir` — needs rewriting accordingly.
