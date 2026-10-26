# FGP Word-file structure: census and draft rules

Drafted 2026-10-26 for the owner's ruling. Gate 4 of
[`FGP_SOURCE_FOLDERS_ASSESSMENT.md`](FGP_SOURCE_FOLDERS_ASSESSMENT.md) holds a new DOCX row out of the candidate
sidecar when gate 3's structure census found any of five Word features in the file
([`FGP_CHOOSER_VALIDATION_RULES.md`](FGP_CHOOSER_VALIDATION_RULES.md) R5, hold `docx_structure`): 17,124 rows,
the largest held group by far. This document says what each feature actually holds, how the text pipeline
already renders it, what evidence today's rows give, and the rule proposed for each — so the rows can be
released feature by feature through the same two gates. Census script `scripts/fgp_docx_structure_census.py`
(reads `gate3.db`, `gate4.db`, `inventory.db`, `FIST.db` and the FIST file store; writes
`fgp_data/inventory/docx_structure_census.json`).

## 1. The census

17,124 held rows from 13,355 FIST units (a unit fans out to one row per manuscript). Rows and units per
feature, with the teams (FIST `dbo_CodeSource.EngDesc`):

| feature | rows | units | teams (rows) |
|---|---|---|---|
| hidden runs | 15,646 | 12,094 | Nuscha 15,632 · Judeo-Persian 6 · 850 5 · Linguistics 2 · Books 1 |
| auto-numbering | 1,262 | 1,056 | Glick 622 · Aggadic Midrashim 434 · Books 142 · Linguistics 21 · Judeo-Persian 16 · Philosophy 8 · Ladino 7 · 850 6 |
| tables | 639 | 540 | Glick 620 · Linguistics 9 · 850 4 · Books 3 · Goitein 3 |
| footnotes | 267 | 256 | Philosophy 61 · Linguistics 59 · Judeo-Arabic Halakha 54 · Glick 35 · Judeo-Persian 29 · Responsa 26 · Firkovitch 1 · Books 1 |
| tracked changes | 9 | 5 | Judeo-Persian 8 · Aggadic Midrashim 1 |

Combinations (a row is held for all the features its file has): hidden runs only **15,638** (12,086 units) ·
numbering only 614 · numbering + tables 581 · footnotes only 192 · footnotes + numbering + tables 40 · footnotes +
numbering 25 · tables only 14 · tracked changes only 8 · footnotes + hidden 6 · three more combinations 3 rows.
So one rule (hidden runs) frees 91 % of the held rows, and numbering with single-column tables most of the rest.

The yardstick exists: 12,685 of the 12,690 held files have FGP's own PDF of the same file beside the DOCX in the
store (125 of them scans).

## 2. What each feature holds, and the proposed rule

### 2.1 Hidden runs — the Nuscha project's apparatus (rule: drop, never a hold)

All but 14 of the 12,094 units are the Nuscha (Talmud text) team's. Over every hidden run of those files
(741,290 runs): line numbers 287,830 · blank separators 290,926 · Talmud locations per line 66,195
(`1 מסכת ראש השנה (10,2,29)`, and spans such as `פסחים מ לב,א,33 עד לב,ב,43`) · 12-digit image/line codes 60,483
(`020160301300`) · record ids and shelfmarks 24,276 (`491647`, `CambridgeL-G Talm.I.108`) · short Hebrew 8,938
(library names: `קמבריג'` 3,397, `נ"י ביהמ"ל`, `פריז מוצרי`, `ירושלים ביהס"ל`, `אוקספורד`; truncated tractate
names) · longer Hebrew 2,642 in 2,371 units, every sampled one a Talmud span or a library name
(`מנצסטר ריילנדס`). No transcription text is hidden: the hidden layer is the project's bookkeeping, which FGP's
own PDF does not print either.

Gate 3's reader already drops hidden runs (`read_docx`, `_collect`: a run with `w:vanish` or a hidden style
contributes nothing), so the text of these units is already right; the hold was only ever "we have not decided".
Evidence from today's rows with the same feature (the same team): 5,035 rows — 2,679 XML rows verified by
their own DOCX, 1,587 accepted (replaced by the DOCX text), 361 identical, 408 held — and the 408 holds are the
PDF side's own classes (spacing, regrouped lines, brackets, typed sigla), none about hidden text.

**Rule H.** A hidden run is not text: dropped, as today, and no longer a hold reason. Worth keeping for later
(not now): the per-line Talmud location and the 12-digit code are exactly the metadata a "where in the Talmud is
this line" feature or the positions work would want; the census records them, nothing reads them yet.

### 2.2 Auto-numbering — line numbers (rule: render as Word does, never a hold)

1,056 units; Glick (responsa) numbers every line `1.`, `2.` inside a one-column table (§2.3); Aggadic Midrashim
and Books number lines without the table. Every numbering definition in the held files renders (0 unrendered
paragraphs); the level-0 formats are decimal `%1` 5,674 definitions, `%1.` 4,882, `%1)` 274, no visible number
940, bullets 16, Hebrew letters 8, five rarer punctuations 17. The reader renders the number the way the
definition says (`_render_numbering`: `1. text`, `1 text`, `א. text`), which is what FGP's PDF prints (today's
PDF text shows the same numbers, often mirrored as `.1` — gate 3's `numbering` class).

Evidence: 2,263 of today's rows carry numbering — 1,807 accepted, 3 identical, 453 held by the PDF side's
classes (spacing, brackets, bidi, diacritics).

**Rule N.** Numbering is rendered as the definition says and is no longer a hold reason. One small fix first: a
bullet definition (16 units) must render `• ` rather than the Symbol-font private-use glyph.

### 2.3 Tables — line blocks, and a few real tables (rule: one column flows, several columns hold)

540 units, 512 of them single-column: the Glick team types each page as a one-column table with one line per
row (table rows = numbered paragraphs in every sample), a layout device. The reader already reads a cell as a
line (`inside_table`, cells one per line), so these render as the plain line lists they are. Evidence: 725 of
today's rows have tables — 409 accepted, 316 held by the Glick PDFs' usual classes.

28 units have real tables with 2–46 columns: the Books team's synoptic tables of liturgical customs (five
columns `מנהג אשכנז · מנהג איטאל' · סדר רב עמרם …`) and a 46-column vocalisation grid, the Linguistics team's
`מקור | תרגום` source-and-translation pairs (8), eleven two-column Glick files (Mosseri, T-S G1.84, Or.1080),
three Goitein and three team-850 files. Read cell by cell these become interleaved noise; read row by row they
become one line of several texts.

**Rule T.** A table whose widest row has one cell flows as lines (as today) and is no longer a hold reason. A
table with two or more columns holds the row under a new reason `docx_multicolumn_table` (28 units, about 30
rows) until a rendering exists — the Linguistics pairs could become an edition row and a translation row, the
synoptic tables probably never belong in a line-oriented chooser.

### 2.4 Footnotes — the apparatus (rule: notes apart, marker made searchable)

256 units, the critical teams: `צ"ל: כתֹאר.`, `התוספת מעל השורה`, biblical references (`יחזקאל כח:22`),
translator's notes. The reader keeps the body and returns the notes apart, numbered in reference order; the
text is body, a blank line, then `n note` one per line (`docx_text`, `FOOTNOTE_SEPARATOR`). Today's PDF-derived
text has the notes wherever the PDF printed them (page bottoms, inside the lines) — gate 3's `footnote_apart`
class — so the DOCX rendering is cleaner than what the site shows for these rows today. Evidence: 1,856 of
today's rows have footnotes — 619 accepted, 1,237 held, and 1,220 of those holds carry `footnote_apart`.

One defect to fix before release: the reader writes the reference as a plain digit glued to the word
(`כְתוֹאַר1`). The search tokenizer keeps digits inside a word, so that word is unsearchable — measured on the
real analyzer: `כְתוֹאַר1` → one token `כְתוֹאַר1`; `כתואר¹` (superscript) → `כתואר`, the marker invisible to
search; `כתואר (1)` → `כתואר`, `1`. A superscript marker reads as a footnote mark and costs search nothing.
Gate 3's comparison must then treat `1` and `¹` as the same marker (`line_key` normalisation), or every footnoted
line diffs on the marker.

**Rule F.** Body first, a blank line, then the notes; the reference is a superscript digit on the word. Not a
hold reason once the marker change is in.

### 2.5 Tracked changes — five files (rule: Word's final view, never a hold)

Five units: three Judeo-Persian files of T-S Ar.30.301 with nine empty insertions and one deleted space, one
file whose only insertion is `!DRAFT`, one Aggadic Midrashim file with one change. The reader already takes the
final view (insertions kept, deletions dropped). Evidence: today's 5 rows — 4 accepted, 1 held by other classes.

**Rule C.** The final view, as today; no longer a hold reason. `!DRAFT` is the drafts question (the owner's
ruling: drafts ship as today), not a structure one.

## 3. Validation before each tranche

1. **Today's rows are the first evidence** (§2): the same reader, the same teams, 7,000+ rows compared against
   the PDF text, and the holds are the PDF side's classes, never the feature's rendering.
2. **The PDF yardstick for the new rows.** Every held file but five has FGP's PDF. Gate 3 can diff the DOCX
   rendering of a new unit against its PDF text exactly as it does today's rows — the PDF text read by the old
   pipeline's extractor (`scripts/fgp_rebuild_text_and_credit.py`, PyMuPDF; gate 3 has the `pdf_today` route as a
   marker only). The PDF side carries today's known defects (lost line starts, final letters, bidi), all declared
   classes; a block outside the classes holds the row `unclassified`, as for today's rows. This is a day of
   wiring and makes every released row a measured one.
3. **A declared by-eye sample**: 10 units per feature, drawn with seed 20261026, read by the owner or me against
   the Word file before the tranche is accepted.

## 4. Release plan

Each tranche: the reader change if any, gate 4's hold keys shrink (`STRUCTURE_HOLD_KEYS` → finally
`multicolumn_tables` only), expectations re-declared from two runs, gates 3 and 4 rerun, record here and in the
gate-4 run record.

| tranche | rule | rows released | prerequisite |
|---|---|---|---|
| 1 | H hidden runs | 15,638 | none — the rendering is today's |
| 2 | N numbering + T single-column tables | about 1,180 (614 + 581 + 14, less the multi-column units' rows) | bullet glyph; `docx_multicolumn_table` hold |
| 3 | F footnotes | 267 | superscript marker; marker normalisation in gate 3's comparison |
| 4 | C tracked changes | 9 | none |

About 17,090 rows enter; the candidate grows from 50,833 to about 67,900 rows. The chooser's checks (C2–C5)
then run over the larger candidate as before, with the reference draw reseeded by the new row set.

## 5. Decisions for the owner

1. The footnote marker: a superscript digit on the word (recommended), or ` (1)` after it.
2. Multi-column tables: hold the 28 units (recommended), or flatten each row with ` | ` between cells.
3. The validation bar for the new rows: the PDF diff for all of them (recommended, a day of wiring), or the
   by-eye sample alone.
4. Order: the four tranches in sequence (recommended: tranche 1 alone is 91 % and needs no code), or everything
   in one run once the PDF diff exists.
5. The Nuscha hidden layer (Talmud location and image code per line): record the intent to keep it for a later
   feature, or ignore it.
