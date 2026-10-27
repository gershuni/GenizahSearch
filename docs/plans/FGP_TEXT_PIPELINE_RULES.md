# FGP unified text pipeline (gate 3)

**Status:** readers, classes and checks applied and measured 2026-10-25 (run record in §7). Nothing in
production changes: the pipeline writes to its own database and says what a rebuilt sidecar may replace.
**Scope:** gate 3 of [`FGP_SOURCE_FOLDERS_ASSESSMENT.md`](FGP_SOURCE_FOLDERS_ASSESSMENT.md) §8 — one text
pipeline with several source readers, run over the gate-2 outcomes
([`FGP_IDENTITY_CREDIT_VISIBILITY_RULES.md`](FGP_IDENTITY_CREDIT_VISIBILITY_RULES.md)), keyed on the FIST
record, diffed line by line against today's text, replacing a text only when every changed line is an
artefact of the PDF route. Script `scripts/fgp_gate3_text_pipeline.py`; output `fgp_data/inventory/gate3.db`
(`text_rows`, `unit_texts`, `line_diffs`, `checks`, `counts`); declared counts
`scripts/fgp_gate3_text_pipeline.expect.json`; wrapper `scripts/run_fgp_gate3_text_pipeline.ps1` (two runs);
tests `tests/test_fgp_gate3_text_pipeline.py` over the text world of `tests/fgp_fixture_world.py`.

## 1. Readers, in order — the record decides, never the image or the 14-digit id

| Reader | Source | When | Source link recorded |
|---|---|---|---|
| `b1_xml` | folder B, `<dir>/<n>.xml`, through `fgp_rebuild_text_and_credit.parse_fgp_xml` (the parser today's XML rows came through, so a same-folder row is byte-identical) | the unit's `ImageTranscriptionFolder` exists and gate 1 typed it B1 | `fist_itf` |
| `b2_rows` | folder B, `<dir>/data.js` → `ac[].rows[n].txt` in row order, entities decoded, columns separated by a blank line | the folder exists and is B2 | `fist_itf` |
| `docx` | `<nopos>/<unit FolderPath>/word/<FileName>.docx`, read structure-aware (§1.1) | no usable B folder, DOCX present | `fist_filename_docx` |
| `pdf_today` | today's text unchanged; the delivered PDF is hashed against today's copy | no B folder, no DOCX | `pdf_hash_identical` / `pdf_hash_differs` / `pdf_today_missing` |
| `none` | — | the file is absent from the delivery (19 rows) or the row has no FIST unit | status `no_source` |

A row is one of today's 45,034 (`row_key = today:<id>`, carrying its gate-2 durable key) or a **new row**
(`new:<unit_table>:<unit_id>|<sys_id>`, one per AlmaId of a candidate file; §4). Texts are read once per
unit (`unit_texts`) and diffed once per (unit, today's text).

### 1.1 The DOCX reader

Paragraphs one per line and a table row one line (its cells in document order; the PDF printed a
`number | text` row as `01 text`, which the bidi class absorbs), manual breaks as lines, tabs as spaces; tracked deletions,
hidden runs and hidden styles dropped (the Genuzos files keep line ids as hidden text); auto-numbering
rendered from `numbering.xml` and the paragraph style (`%1.` → `1.`, decimal / letter / roman / Hebrew
formats; unknown formats counted; a bullet definition renders `•`); footnote references numbered in order of
appearance **as superscript digits on their word** (`כְתוֹאַר¹` — the owner's ruling of 2026-10-26: the `hebword`
tokenizer drops a superscript, so the word stays searchable, where a plain digit is kept in the token) and the
footnotes returned apart and appended after a blank line as `<n> <text>`; header and footer text returned apart
(`PAGE` fields removed); the column labels `טור_א` … become the blank line that separates columns; one
rendering of a text box (`mc:Choice`). The structure census per unit (`structure`): paragraphs, tables,
footnotes, superscript runs, bold runs, breaks, tabs, numbered paragraphs, hidden runs, tracked changes,
images, column labels, header parts, numbering the reader could not render, and `table_max_columns`, the
widest table row (one cell: a line block, the Glick layout; two or more: a real table — the owner's ruling holds it).

## 2. The diff and its classes

Today's lines and the new body lines are normalised (`ws`: entities, NBSP, invisible marks, whitespace),
the DOCX footnotes and header lines the PDF printed inline are matched and set apart, then the two lists
are aligned by a **line key** — the line with the cheap artefacts normalised away (catchword form,
line-number order, spaces, backslashes, bracket direction, marks, control characters, final letters) — so
that only lines the artefacts do not explain fall into changed blocks. Every aligned pair that still
differs and every changed block gets a class set and a disposition: `accepted` (all classes in
`ACCEPTED_CLASSES`), `verify_only` (§3), `held` (an `unclassified` difference). Blocks with as many lines
on both sides are classified line by line, then as a whole when a word crossed a line boundary.

| Class | Test (today → source) | Example (fixture) | Replaces? |
|---|---|---|---|
| `whitespace` | equal after `ws` | — | yes |
| `spacing` | equal with spaces removed | `. . . .` → `....`, `ישר' הולד` → `יש?ר?' הו?לד?` | yes |
| `escape` | equal with backslashes removed | `{ה\\/\\/ב}` → `{ה\/\/ב}` | yes |
| `bracket` | equal with `[]()<>{}` direction-insensitive | `]אב[ ג` → `[אב] ג`; `>החג<` → `<החג>` | yes |
| `numbering` | today's `.1` / `-1` line number re-ordered to `1.` / `1-`; a footnote marker the PDF printed on a line of its own | `.1 אב` → `1. אב` | yes |
| `diacritics` | equal without Hebrew, Latin and Arabic combining marks, after canonical decomposition (the PDF emits precomposed letters with dagesh or a vowel, U+FB1D–FB4E) | `א רֶֶֿץ` → `אֶֿרֶץ`; `בּ` (U+FB31) → `ב` + dagesh | yes |
| `vowel_glyphs` | an ASCII glyph of today's stands where the source has a vowel point, or a vocalised letter the font rendered as one symbol or dropped | `ש0ב` → `שֵב`; `מֵ<ה` → `מֵאָה`; `כּ חִים` → `כְּאַחִים` | yes |
| `control_chars` | a C0 control character stands for a Hebrew or Arabic letter, or is dropped | `שלו\x0e ולכ\x04` → `שלום ולכן` | yes |
| `final_letters` | a symbol stands for a final letter, or a final letter the PDF dropped is restored | `ואיל"` → `ואילך`; `דנו` → `דנום` | yes |
| `arabic_alef` | an Arabic alef the PDF dropped is restored | `العقل` → `العقلا` | yes |
| `arabic_glyphs` | an Arabic word today's text holds only as ASCII junk and control characters | `!"` → `تاييده` | yes |
| `restored_prefix` | today's line is the end of the source line after a gershayim | `ג שנים` → `ועד י"ג שנים` | yes |
| `catchword` | a `%…%` line the PDF dropped, or rendered as `<…>` | `<<הן>>` → `%(הן)%` | yes |
| `column_label` | a `טור_א` line today's text has and the reader turned into a column break | — | yes |
| `regrouped` | the same text over a different number of lines | `אב גד` + `הו` → `אב גד הו` | yes |
| `bidi_reorder` | a mixed-direction line (ASCII present, or symbols only) with the same characters in another order | `12.528: TS ראה` → `ראה TS 12.528:`; `@]......[` → `[......]@` | yes |
| `footnote_apart` | today's lines equal a DOCX footnote (number + text, wrapped), set apart | `1 זו הערת שוליים` | yes |
| `header_apart` | today's line equals DOCX header/footer text (page numbers ignored) | `T-S 1.5 Cambridge` | yes |
| `footnote_marker` | the PDF's plain footnote digit against the DOCX rendering's superscript (one marker; the alignment key folds them too) | `הערה1` ↔ `הערה¹` | yes |
| `sigla_rendering` | equal as letters and digits only: the typed sigla differ (`?..?`, `[[..]]`, `~`, `²`/`\`) | `שורה` ↔ `?ש?ורה` | **verify only** |
| `extra_line` | lines the source has and today's text lacks (a transcriber's note, a line the PDF dropped) | `1.(שורה 8) יש הערת גליון…` | **verify only** |
| `unclassified` | anything else — a reading difference, scrambled Arabic, a stray character | `אבג` ↔ `אבד` | **no** |

Each class has a test in `tests/test_fgp_gate3_text_pipeline.py::test_classes_of_the_pdf_artefacts`; the
report (`GATE3_REPORT.md`) prints one accepted example per class from the real run and the first 25 held
blocks for review. A new class is added to `ACCEPTED_CLASSES` with its test, and the expectations move.

## 3. Statuses, replacement and verification

| Status | Meaning | `text_replaced` | `text_accepted` |
|---|---|---|---|
| `identical` | the source text equals today's | 0 | 1 |
| `accepted` | every changed block is an accepted artefact | **1** | 1 |
| `verified` | an XML row whose own record's DOCX agrees (accepted or verify-only classes only) | 0 | 1 |
| `approved` | a held row the owner approved (§5) | 1 | 1 |
| `held` | an unclassified or verify-only difference — listed with its blocks | 0 | 0 |
| `checked` | a new DOCX row whose rendering is clean against FGP's PDF of the file (§4) | 0 | by policy |
| `pdf_held` | a new DOCX row with a block outside the classes against its PDF (§4) | 0 | 0 |
| `no_comparator` | a new row without a PDF text (folder B, a scan, no PDF) (§4) | 0 | by policy |
| `no_source` / `retired` | nothing to read / retired by gate 2 | 0 | 0 |

**XML never regresses, and the DOCX never replaces an XML text.** Today's XML text is FGP's decoded
rendering of the edition; the DOCX is the typed original, whose sigla are characters and styles
(`?..?` uncertainty, doubled brackets, the `~` filler, `\` for the `²` mark, notes in the `rn` style) that
the XML carried as class codes or dropped. So for a row whose text came from the XML, the DOCX is a
*verifier*: when the two agree up to the classes (including the verify-only ones) the row is `verified`,
its gate-2 hold `xml_unverified` / `xml_region` / `xml_other_edition` is lifted (`xml_verified = 1`), and
the XML text stays. A B1 row read from its own folder is `identical`; a B2 row whose today's text is
another edition's XML is `held` (`xml_text_unverified`) — correctly, since the texts differ in readings.

`publishable_text` = text accepted **and** gate 2 publishable (or the only gate-2 holds were the XML
holds now lifted). Content loss is measured per block (`lost_today_lines`: letters of today's side the
new side lacks, the font's ASCII junk excluded where a glyph class attests to it); check `T06` requires
0 on every replaced XML row.

Two observations the comparison made that are not artefacts of the PDF route: FGP's **XML export drops
the `?..?` uncertainty marks** (it carries them as class codes), so a PDF-route row with `?` marks is
not replaced by its own B1 folder's text (6 rows, `needs_reading:sigla_rendering`) and today's 6,381
XML rows presumably lack marks their DOCX has; and the **Genuzos DOCX files carry typed sigla and
transcriber's notes** (`?..?`, `##`, `\`, `1.(שורה 8) יש הערת גליון…`) that neither the PDF nor the XML
shows — the owner decides whether the DOCX revision or FGP's rendering is the edition to publish.

## 4. New rows (files FIST holds that today's sidecar does not)

Every transcription / translation unit of a `candidate`, `candidate_multi_alma`, `hold_visibility` or
`unresolved_alma` file gets a row per AlmaId (none → one row with `sys_id` NULL) with its text, source
link and DOCX structure census. **A new DOCX unit is checked against FGP's own PDF of the same file** (the
owner's ruling of 2026-10-26, [`FGP_DOCX_STRUCTURE_RULES.md`](FGP_DOCX_STRUCTURE_RULES.md) §3): the PDF's text is
read the way today's sidecar got it (`shared.local_indexer.extract_pdf_pages`, pages joined, blank lines collapsed;
cached by file hash in `--pdf-cache`, `fgp_data/inventory/pdf_text_cache.db`, shared by the wrapper's two runs) and
diffed against the rendering through §2's classes exactly as today's rows are: no block outside the classes → status
`checked` (a verify-only agreement counts: a new row has no text to replace, so letters-and-digits equality
confirms the rendering as it verifies an XML row); a block outside the classes → `pdf_held`; no PDF text (a scan, no PDF, folder-B text) → `no_comparator`.
The blocks are recorded in `line_diffs` under the new row's key; `yardstick` / `yardstick_hash` name the comparator.
Under `--new-rows-policy hold` (the default, and the record run's) none is accepted (`hold_reason =
needs_acceptance`; the status is still computed). Under `plain`, folder-B text is accepted by rule
(`rule:folder_b_text`); a DOCX is accepted when its tables are line blocks (`table_max_columns` < 2, else held
`docx_multicolumn_table`), its rendering is `checked` (`rule:docx_checked_against_pdf`) or has no PDF text
(`rule:docx_no_pdf_text`), and its text is not blank (`blank_text`); a `pdf_held` rendering holds the row
(`pdf_held`). Hidden runs, auto-numbering, footnotes, tracked changes and one-column tables are rendered, never a
hold (the rulings replaced the earlier `plain`/`docx_structure` rule, under which any of them held the row). Gate 3
never marks a new row publishable: credit and visibility for new rows are the import's job (assessment §6.2).
Descriptions are never read (check `T07`).

## 5. Approvals

`scripts/fgp_gate3_approvals.json` (absent = none):

```json
{"approvals": [{"durable_key": "dbo_UnitTranscription:4|990001|", "today_hash": "<sha256 of today's content>",
                "new_hash": "<sha256 of the source text>", "by": "owner", "approved_on": "2026-10-25", "note": "…"}]}
```

An entry names a gate-2 durable key (or `"unit": "<unit_table>:<unit_id>"` for every row of a unit) and
**both hashes**, so an approval dies with either text. A held row with a matching approval becomes
`approved` (an XML row: `verified`); its blocks are dispositioned `approved`. Approvals never override a
visibility or association hold from gate 2.

The first real approvals (2026-10-26, the owner, by phone): the 10 rows whose PDF yielded no text at all (today's
hash is the empty text's) while their own DOCX holds it — 8 translations and 2 editions, 1 to 59 lines each. Gate 3
had held them as `extra_line` because an empty text has nothing to align; the owner approved the DOCX texts
wholesale. Three more text-less rows (975, 1010, 16978) have an empty DOCX too and stay without text.

## 6. Checks

`T01` two runs identical (the digest over every row's reader, text hash, status, classes and flags equals
the previous run's — the wrapper runs twice; `--allow-single-run` is for development and tests) · `T02`
every replaced row has a FIST source link · `T03` every diff block has a disposition and an accepted row
has only accepted blocks · `T04` every block of a replaced row is in `ACCEPTED_CLASSES` or approved · `T05`
no new row accepted without a basis · `T06` no replaced XML row loses a line · `T07` no row from a
FreeDescription unit or a `_D` file · `T08` every row is a transcription or translation by FIST · `T09` an
accepted new DOCX row with a PDF yardstick has only accepted blocks against it · `T10` the final-letter repair is
right on at least 99.5% of at least 1,000 codes judged on the rows that exist both ways (§8) · `E_*` the declared expectations. The tests prove the gate fails when a lossy difference is accepted (`T06`), when a
replaced row loses its source link (`T02`), when two runs differ (`T01`), and on a missed expectation.

## 7. Run record

2026-10-26 — `scripts/run_fgp_gate3_text_pipeline.ps1`: two runs (21 and 19 minutes) with the identical digest
`d6eef6ffcfc34f83`, **PASS 8 checks**; 47 counts from them were then declared in
`scripts/fgp_gate3_text_pipeline.expect.json` and reproduced by a third run (**PASS 55 / 55**, 10 minutes).

| Reader | Rows | identical | accepted (replaced) | verified | held |
|---|---|---|---|---|---|
| `b1_xml` | 3,283 | 3,274 | 0 | 0 | 9 |
| `b2_rows` | 15,907 | 6,347 | 9,514 | 0 | 46 |
| `docx` | 25,775 | 1,161 | 18,190 | 2,679 | 3,745 |
| `none` | 69 | — | — | — | 19 absent from the delivery, 50 retired |

- **Replaced: 27,704** rows (9,514 from folder B's line text, 18,190 from the DOCX); **verified: 2,679** XML rows,
  lifting 2,731 of the 3,130 gate-2 XML holds; **publishable with text: 41,143** (rerun 2026-10-26 after gate 2's
  `fist_comment_unchecked` hold: 40,980, gate 2 41,642, digest `ba1e47e9db555190`, PASS 55/55; no other count
  changed; second rerun the same day with the 10 approvals of §5: 10 rows `approved`, replaced 27,714, held
  3,790, publishable with text 40,990, digest `496575a202e34af2`, PASS 56/56; third rerun the same day after gate 2
  narrowed its comment hold to the explicit do-not-publish comments: publishable with text 41,137, gate 2 41,790,
  digest `3ed1cfcbcdc1415c`, PASS 56/56, about 10 minutes per run; no other count changed; fourth rerun the same
  day with the Word-file rulings — superscript markers, `table_max_columns`, every new DOCX row checked against its
  PDF: today's rows unchanged in every status, `footnote_marker` on 1,872 rows, new rows 21,049 `checked` / 1,261
  `pdf_held` / 2,969 without a PDF text, digest `0088ba547d3dfc83`, PASS 64/64 (9 checks + 55), about 15 minutes
  per run with the PDF cache; record in `FGP_DOCX_STRUCTURE_RULES.md` §7) (gate 2: 41,807 — 3,393 of those are
  held here, 2,731 come back through the lifted holds).
- **Held: 3,800** — 2,545 with an unclassified difference (a reading, a stray character, Arabic the font garbled
  beyond the junk rule, a line the DOCX lacks), 771 whose only differences are typed sigla (`sigla_rendering`), 85
  with lines today's text lacks (`extra_line`), 399 XML rows whose own record disagrees (`xml_text_unverified`: 391
  DOCX, 5 B2 editions, 3 B1 regions). Held rows concentrate in a few teams' DOCX files: 101 (718 of 935 rows — Arabic
  grammar with vowel points), 132 (539 of 3,953 — Goitein, with tables and Arabic), 200 (405 of 5,359 — Genuzos
  typed sigla), 600 (371 of 3,520), 105 (264 of 648), 103 (249 of 341); 701 of the 3,342 translations are held.
- **Classes** (rows carrying each, over 484,884 blocks): spacing 28,331 · bracket 20,914 · regrouped 11,246 · bidi
  7,761 · control characters 6,714 · final letters 5,513 · diacritics 5,300 · escape 2,869 · numbering 2,797 ·
  vowel glyphs 2,114 · footnotes apart 1,839 · restored prefix 592 · catchword 533 · header apart 318 · column label
  227 · Arabic junk 98 · Arabic alef 73 — verify-only: sigla 4,289, extra line 187 — unclassified 2,944.
- **New rows: 25,342** (22,511 DOCX, 2,018 B1, 750 B2, 63 without a text source) — none accepted under `hold`;
  structure: 645 with tables, 342 with footnotes, 621 with superscripts, 1,281 with auto-numbering, 15,652 with
  hidden runs (line ids), 71 with headers.
- Content loss: no replaced row loses a letter of today's text (`replaced_with_lost_lines` 41 before the line-number
  exclusion, all footnote markers on their own line; not declared).

Open for the owner: (1) the 771 + 85 verify-only rows and the Genuzos DOCX question (§3); (2) whether B2's 22 extra
short lines (`לנו@`, `מצרימה:`) may be accepted as restored lines; (3) the `plain` policy for new rows; (4) the held
reading differences, by team, in `line_diffs`.

## 8. Final letters on the rows that keep today's PDF text (GS-276; declared 2026-10-27, before the run)

The owner ruled on 2026-10-27 that the rows keeping today's PDF text have their final letters restored before the
release. FGP's PDF font left `ך ם ן ף ץ` as control characters U+0001-U+001F; in the source-folders candidate 1,810
rows still carry them (the rows with no Word or line text: 1,709 `pdf_rtl`, 101 `original`), and today's sidecar
7,240. The code-to-letter mapping differs from PDF to PDF (U+0018 is `ן` 2,203 times and `ם` 1,453 times over the
rows that exist both ways), but within one row a code is one letter (99.92% of 156,315 aligned occurrences; 8,914
of 8,981 row-and-code pairs fully consistent). So gate 3 reads each row's codes off the row's own words
(`scripts/fgp_final_letters.py`):

- **Vocabulary**: the words ending in a final letter of every text gate 3 holds without a control code (the source
  texts it read and today's clean texts), each distinct text counted once. The corpus HTR was tried as extra
  vocabulary and lowered accuracy at every weight; it is not used.
- **Votes**: a code is word-final when at least as many of its occurrences end a word as stand inside one; each
  word ending in it (stem of two letters or more, no other code) casts one vote, split between the final letters
  that make it a vocabulary word by how often each does. Codes inside words are vowel points and dots in those
  PDFs (U+05C4, U+0323, U+0307, hiriq, patah) and are left as typed.
- **Assignment**: the row's codes are assigned together, the most votes less 1 for each code beyond the first on
  one letter (96% of rows use a distinct code per letter).
- **Applied** only when the letter has at least 2 votes and at least 70% of the code's votes; the code is then
  replaced everywhere in the row's text and in its PDF page split (`sections`). Otherwise it stays as typed: a box is
  honest, a wrong final letter is not.

Measured before the run on the rows that exist both ways (today's PDF text against the candidate's clean text of the
same row; each row's own clean words taken out of the vocabulary): **99.95% of 6,454 applied codes right** (3 wrong);
at 60% it was 99.74% (18 wrong of 6,823), and the codes below the bar are right 71-95% of the time. On the 1,810 rows
it applies 1,922 codes and leaves 700 word-final codes and the inside-word codes as typed.

Gate 3 writes the decisions to `final_letters` (one row per kept today row with a code: the mapping, the votes,
the repaired text and its hash), counts them under `final_letters.*`, folds them into the digest, and repeats the
measurement on every run: **`T10`** fails when an applied code exists and fewer than 1,000 codes were judged or
fewer than 99.5% of them are right. Gate 4 applies a row's mapping to the content and sections it carries from
today (`text_source` `<today's>+final_letters`, `text_provenance` `gate3:final_letters`).

**Run record (2026-10-27, `scripts/run_fgp_gate3_text_pipeline.ps1`):** two runs (28 and 11 minutes) with the identical
digest `3f166188e2ebcc56`, **PASS 65 / 65** (10 checks + 55 expectations; no earlier count changed). T10: **6,421 of
6,426** applied codes right on the 5,392 rows that exist both ways (99.92%; every assigned code, applied or not,
8,350 / 8,546). Of 7,206 today rows with a control code, 1,814 keep today's text: 1,967 codes applied (119,522
occurrences) in 1,176 rows, 643 of them now without a code; left as typed 664 word-final codes below the bar and
1,487 inside-word codes. Vocabulary 110,422 words.

## 9. Re-reading the PDF text through the fonts' glyph names (GS-276; declared 2026-10-27, before the measured run)

Plan: `docs/plans/FGP_PDF_GLYPH_REREAD_PLAN.md` (revision 3, approved 2026-10-27 with decisions 1-4 as recommended).
Code: `scripts/fgp_glyph_reread.py` (census, certification, repaired copies, traced extraction, checks) and
`scripts/fgp_glyph_registry.py` (names). Working files in `fgp_data/inventory/glyph_reread/` (gitignored). Nothing in
this section touches the candidate; step 6 (gate integration) comes after the owner's review.

**Census (step 1).** Every PDF of the candidate rows that ship PDF text and of the C3 benchmark is read with pdfminer
from its content streams: each font by its PDF object, each drawn glyph by its byte code (38,570 paths, 33,959
distinct PDFs). 463,122 drawn glyphs carry a `/Differences` name the standard decoding gets wrong (172 names, all in
TrueType fonts).

**Registry (step 1).** A name becomes the character(s) it draws by an explicit table or a stated rule (Hebrew letter
plus marks, Arabic letter or ligature plus position by its Unicode name, the Adobe Glyph List), never by similarity.
Version `2026-10-27.1`: 203 of the 314 problem names resolve; the rest (mostly Microsoft `afii627xx` Arabic names)
are decoded only if their shape certifies them.

**Certification (step 3).** A glyph's outline (TrueType `glyf`, composites decomposed, units scaled to 2,048 per em,
contour start and order normalised, hinting ignored) is matched exactly against every cmap-mapped glyph of 25
installed reference fonts (David, Narkisim, Frank Ruehl, Miriam, Rod, Levenim, Gisha, Times New Roman, Arial,
Tahoma, Courier New; files pinned by SHA-256, `reference_version` in every output). The font's typeface is the
family most of its glyphs match; a glyph is **certified** when it matches a glyph of that typeface whose character
is the one its name gives, and **certified by shape** when it has no usable name and matches exactly one character
of that typeface, which must be a Hebrew or Arabic letter or mark and not a mirrored character (a right-to-left PDF
draws a bracket with its mirror image; a dagesh can share the dotted circle's outline). Empty outlines and `.notdef`
never certify. Anything else (no match, another typeface, two characters, a name the shape contradicts) is not
decoded; the unmatched shapes go to the owner's review by eye, by outline class.

**Repaired copy (step 2).** A font without `/ToUnicode` gets one with an entry for each drawn code whose certified
character the standard decoding does not already give; codes left out keep MuPDF's own decoding (checked: a partial
map changes only its own codes). A font with a map keeps every entry and gains only certified codes it lacks; an
entry that a glyph certified by name and shape contradicts is a conflict and holds every row using that PDF. The
map gives ONE character per code (the reference glyph's own code point, a presentation form for a pointed letter or
an Arabic shape), so the extractor still sees one glyph; decisions 1-2 are applied after extraction, to decoded
glyphs only: a Hebrew presentation form becomes the letter plus its points (NFD), an Arabic one its plain letters
(NFKC). A row whose other glyphs already carry presentation forms is held instead.

**Extraction and tracing (step 4).** The text is made exactly as the stored text was: `extract_pdf_pages`, pages
joined by a blank line and stripped, then `_collapse_blanks` (`scripts/fgp_rebuild_text_and_credit.py:312`). A traced
copy of `_extract_one_page_rawdict` calls the same helpers and tags each glyph by page, origin, font, size and box;
for every PDF it must reproduce the real extractor exactly ("faithful"), or the rows are held.

**Checks** (`D` = the glyphs whose character differs between the untouched reading U and the repaired one R):

- **C1**: U equals the row's stored text before the vocabulary repair; otherwise held.
- **C2**: rows are matched by their non-D glyphs; a row without D must come out identical; in a row with D, character
  counts (whitespace aside, a bracket pair counted as one class) must change by exactly what D's glyphs change, and the
  other characters must keep their order once whitespace and D's character classes are set aside (the native
  occurrences of those classes are the check's blind spot, counted). Every decoded glyph must produce a character of
  the injected map. Pages must be skipped alike, rows grouped alike, and no page with a D glyph may fall back to the
  blocks text. A failure holds the row and names its pattern (brackets re-paired; digits or Latin reordered; Hebrew or
  Arabic reordered; the row changed direction; rows grouped differently; a stray character). A held row can ship only
  as a recorded exception the owner approves, tied to its U and R hashes.
- **C3**: on the benchmark (every PDF row whose text gate 3 replaces with Word or line text), the tokens the re-read
  changes are aligned to the Word/line text (token alignment; tokens compared after NFD with edge punctuation and sigla
  brackets set aside), weighted by changed characters. Passes when at least 90% of the weight is aligned and judged and
  at least 99.5% of the judged weight agrees; a disagreement where R and the Word text differ only where the re-read
  changed the token is a decoding error, and holds its row.
- **C4**: every map entry comes from a certified decision (by construction; counted).
- **C5**: recovery against the census: the Hebrew letters a scope PDF draws that the candidate text lacks (79,030 in
  1,425 distinct texts when declared) are counted as recovered (an accepted row now has them), held (the row is held)
  or unresolved (still missing in what would ship). **Passes when at least 50% are recovered and at most 25% are
  unresolved**; a run that recovers nothing fails.
- **C6**: on the rows with the vocabulary repair, each repaired code is compared with what its glyphs decode to: agree,
  names differ (the names win), or left undecoded (the row is held: a vocabulary letter is never silently undone).
- **C7**: two runs, each with a fresh scratch directory and fresh repaired copies, give one digest over every row's
  decision, reasons and resulting text, plus the registry, reference and extractor versions.

**Development runs.** The rules above were tuned on the four spike rows and on a 400-PDF trial (`measure --limit 400`:
77 rows accepted, 40 held, 2 unchanged; C3 judged 96% of the weight and 99.4% of it agreed, below the declared
99.5%: of the 118 disagreeing, 113 are tokens where the PDF and the Word text differ outside the decoded characters).
The thresholds of the plan were kept as declared.
The thresholds were fixed after those runs and before the measured run over all PDFs.

**Measured run (2026-10-27, `python scripts/fgp_glyph_reread.py census|certify|measure|summary`):** two runs, the
second from a fresh census, certification and repaired copies, give the identical digest `dec17b24673e798a`
(registry `2026-10-27.1`, reference fonts `564b97e293ff99b0`, extractor `bffcb189925f17f7`); 5,506 PDFs re-read in
about 90 seconds, no errors. Of the 10,856 rows that ship PDF text: 9,075 have nothing to decode, **930 accepted**,
**845 held** (215 `original` rows by decision 4; 630 by the checks), 6 unchanged.

- C1 **pass**: the extractor reproduces every stored text it was asked to; the traced copy equals the real extractor
  on every PDF.
- C2 holds, by pattern: digits or Latin reordered 218, brackets re-paired 192, decoded and native presentation forms
  in one row 128, rows grouped differently 117, a stray character 83, Hebrew or Arabic reordered 73, the row changed
  direction 57, characters changed beyond the decoded glyphs 1.
- C3 **FAIL**: 96.1% of the weight judged (needs 90%), 99.41% of it agrees (needs 99.5%); 70 decoding errors (each
  holds its row); 1,526 of the 1,615 disagreeing weight are tokens where the PDF and the Word text differ outside the
  decoded characters.
- C4 pass by construction (every map entry from a certified decision).
- C5 **FAIL**: of the Hebrew letters missing from the candidate text (79,275 recounted; 79,030 declared), 37,962
  recovered (48.0%; needs 50%), 36,816 in held rows, 4,497 unresolved (5.7%; at most 25%).
- C6: 1,748 repaired codes agree, 36 differ (the names win), 18 left undecoded (held).
- C7 **pass**.

385 classes of unmatched or ambiguous glyph shapes await the owner's review by eye. Review page:
`_tmp/fgp_glyph_reread_review.html`. The candidate is unchanged; step 6 waits for the owner.

**Owner's exceptions (2026-10-27, after the per-pattern review `_tmp/fgp_glyph_reread_patterns.html`).** Approved as
recorded exceptions: brackets re-paired, rows grouped differently, decoded and native presentation forms in one row,
Hebrew or Arabic reordered, and C6 except rows that lose correct vocabulary letters. Kept held: digits or Latin
reordered, a stray character, the row changed direction (and every other reason). A held row ships as an exception only
when every one of its hold patterns is approved; a C6 row is excluded when the re-read leaves a box where the
vocabulary repair made a word of three letters or more attested in the clean FGP texts (rows 32507 and 34561, 31 such
words each: ובין, אהרן, ומנין ...). `python scripts/fgp_glyph_reread.py exceptions` writes the list
(`fgp_data/inventory/glyph_reread/exceptions.json`), each row tied to the SHA-256 of its untouched and re-read texts;
the summary applies an exception only while both hashes still match.

Re-measured with the exceptions (summary digest `bae19e46182cf792`, twice): **1,227 rows accepted** (930 plus 297
exceptions), 548 held (215 by decision 4, 333 by the checks: digits or Latin reordered 218, a stray character 83, the
row changed direction 57, and rows where an approved pattern comes with one of these). **C5 pass**: 53,687 of the
79,275 missing Hebrew letters recovered (67.9%), 20,863 in held rows, 4,725 unresolved (6.0%). C3 unchanged (fail,
99.41%); C1, C4, C7 pass.

**Owner's ruling on C3 (2026-10-27).** A disagreement where the PDF and the Word text differ outside the decoded
letters is a difference between the two texts, not a decoding error: it is counted and reported apart, and the 99.5%
bar applies to decoding errors only (agreement = agreeing weight / (agreeing weight + decoding-error weight)). The
90% judged coverage is unchanged, and every decoding error still holds its row. Recomputed (summary digest
`bae19e46182cf792`, unchanged): judged 96.1% of the weight; **decoding agreement 99.97%** (273,330 of 273,419
characters; 70 decoding-error tokens, 89 characters) - **C3 pass**; texts that differ: 1,526 characters (0.56% of the
judged weight; 99.41% agreement if they were counted as before). With the exceptions, C1, C3, C4, C5 and C7 pass.

**Fixes after the owner's review (2026-10-27, "fix both and re-measure").**

- *Number ranges.* A decoded hyphen between digits made the right-to-left reordering reverse the range (today "12□25",
  re-read "25-12"). `repair_ranges` restores today's order on the re-read row (`A-B` where today's text has `A<code>B`
  and the re-read has `B-A`, digits required on both sides), then rebuilds the reading from its rows; a reading that
  does not rebuild exactly is held ("rows do not reassemble into the reading").
- *Typeface ties.* When two reference families matched a font's glyphs equally often, the choice between them followed
  Python's string hashing and changed from run to run (76 entries; C7 did not see it because its digest covered rows,
  not decisions). Families that tie are now all the font's typeface, and **C7's digest now also covers every
  certification decision** (status, character, form, typeface). Shown able to fail: the two old certifications give
  different digests.
- *Stray characters.* A bracket that MuPDF merely mirrored is not counted as a stray character. A trial rule that let
  a point alone certify only by name was withdrawn: it removed 60 accepted rows (3389, 16546, 16608, 20851 ...) whose
  fonts carry ~9,000 real vowel points without names, and freed no row. On row 13734 (traced) the stray characters are
  brackets the untouched reading lost to MuPDF's point-attachment step, beside a dagesh-shaped glyph with a full
  advance width that the PDF draws inside lacunae ("[ ]ּּ"); these rows stay held.

Re-measured twice under different Python hash seeds, the second from a fresh census, certification and repaired
copies: identical digest **`2b91c704cd79f4a2`**, no errors, every PDF faithful. Of the 10,856 rows: 9,075 nothing to
decode, **1,368 accepted** (1,043 plus 325 exceptions), **407 held** (215 by decision 4, 192 by the checks), 6 unchanged.
Hold patterns (a row can have several): brackets re-paired 192, mixed presentation forms 128, rows grouped differently
117, a stray character 83, digits or Latin reordered 73 (was 218), Hebrew or Arabic reordered 73, the row changed
direction 57, C6 18, characters changed beyond the decoded glyphs 1. **C3 pass** (judged 96.1%; decoding agreement
99.97%, 70 decoding-error tokens; texts that differ 0.56%). **C5 pass**: 66,015 of the 79,275 missing Hebrew letters
recovered (83.5%), 8,209 in held rows, 5,051 unresolved (6.4%). C1, C4, C7 pass. The candidate is unchanged.

**Owner's ruling on the stray characters (2026-10-27, after the page of all 83 rows,
`_tmp/fgp_glyph_reread_stray.html`): "not perfect but good enough."** A stray character is an approved exception
pattern. That page showed every one of the 660 stray glyphs is a glyph the untouched extraction dropped or folded into a
neighbour and the repaired copy brings back; none is a glyph that changed character. Known imperfections accepted with
it: dotted circles drawn in lacunae decoded as a dagesh (13734), a point moved by the reordering (4713). Re-summarised
twice (digest **`94854a74221e0c94`**): 392 exceptions; **1,435 rows accepted** (1,043 plus 392), **340 held** (215 by
decision 4, 125 by the checks: those with digits or Latin reordered, a change of direction, a decoding error, C6 lost
words, or changes beyond the decoded glyphs). **C5 pass**: 67,784 of 79,275 missing Hebrew letters recovered (85.8%),
6,148 in held rows, 5,343 unresolved (6.7%). C3 pass (99.97%); C1, C4, C7 pass. The candidate is unchanged.

**Step 6: the re-read in gates 3 and 4 (2026-10-27, owner: "integrate the re-read into gates 3-4 and rebuild the
candidate").** Gate 3 runs steps 1-5 inside each run, in a fresh working folder (`<out-dir>/gate3_glyph_reread/`), on
its own rows: every row it keeps with today's text from a PDF (10,889, not retired, not replaced; S = today's text
with the vocabulary repair) and every replaced PDF row with a Word or line text as the C3 benchmark (27,714). It
writes `glyph_reread` (each row's decision, reasons, exception, C6 result, and the re-read text when accepted),
`glyph_map` (every injected entry with its certification evidence) and `glyph_reread_exceptions`; its digest takes in
the re-read's own digest (which covers every certification decision and the input versions), every row's decision
and shipped-text hash, and the map; new checks T11 (C1: no extraction error, every trace faithful, no row held for
C1), T12 (C3), T13 (C5 against the recounted denominator of this scope) and T14 (C4: every injected entry certified).
Gate 4 writes an accepted row's re-read as its text (`text_provenance` `gate3:glyph_reread`, `text_source`
`<source>+glyphs`, replacing the vocabulary repair) only on a row without a page split; a held row keeps today's text
with the vocabulary repair. Gate 4's digest now also covers `sections` and `text_source`. The vocabulary step stays:
its letters remain on held rows and C6 reports where the names differ.

Expectations declared before the run, from the standalone run over the same scope (digest `758b97fd7d6e38ca`): gate 3
1,046 accepted, 394 exceptions, 346 held, 9,097 nothing to decode, 6 unchanged; gate 4 1,435 rows re-read (the five
other accepted rows, 5404, 17670, 18882, 18883, 35933, are rows gate 4 excludes), 161 with the vocabulary repair
alone, none with a page split. **Gate 3 PASS 79/79**, two runs digest `50ae45e325cd59f0`, its re-read digest equal to
the standalone run's; C3 decoding agreement 99.97% (273,327 of 273,416 characters), C5 85.4% recovered (68,392 of
80,042), 6.9% unresolved, no errors. **Gate 4 PASS 85/85**, two runs digest `086690023883b4ae`, every expectation
held. **New candidate**: 59,056 rows, sha256 `e024a45c54ba1cde...`; against the previous candidate
(`0b1fb5fa...`, kept as `fgp_transcriptions.candidate.pre_glyph_reread.db`) exactly the 1,435 re-read rows change,
each equal to its measured re-read text, and no other row or field; `scripts/check_fgp_sidecar.py` passes it.

**Step 7: checks in both apps and Codex review (2026-10-27).** Web (the candidate behind a local server) and desktop
(the transcription view's own `ResultDialog._htmlify` / `_apply_source_highlights` in a `QTextBrowser`): pointed
Hebrew, Arabic joining and a Latin heading render; the text the reading view holds equals the stored text (sha256) on
every sample; a search phrase the re-read restored now makes the FGP edition the default and is highlighted (unpointed
text). Found, older than the re-read: the web highlight does not match through vowel points; the web version menu
appears only on pages with an automatic transcription; the desktop takes `*` in a text for a highlight marker.

Codex reviewed the integration in two rounds (`_tmp/codex_gs276_step6_r1.log`, `_r2.log`; both SHIP WITH FIXES, no
HIGH). Folded in: gate 4 attaches a re-read only with the exact accepted decision, the hash of the stored text gate 3
read, the hash of the re-read itself, and the same PDF path and sha256 (`--today-files`); anything else keeps today's
text and is counted (`rows.glyph_reread_stale`, declared 0). A decoding error on a PDF's Word-text row (C3) holds every
kept row of that PDF. T11 counts measurement and certification errors and unfaithful kept and benchmark traces, with
no escape when nothing was measured; unreadable PDFs are counted (`glyph_reread.unreadable_pdfs`, declared 0). The
re-read's digest also covers its own code, the pdfminer / fontTools / Unicode versions and every PDF read with its
sha256. `reset()` restores the module's paths and environment after gate 3's step. Tests make each of T11-T14 fail
and a stale re-read (other text or other PDF) be refused.

**Final run on that code:** gate 3 PASS 80/80, two runs `9ae25fa88349ddf6` (re-read digest `8a0d515927a71c41`), every
expectation held; gate 4 PASS 86/86, two runs `086690023883b4ae`; candidate sha256 `cdcc2268b1a6699a...`, its 59,056
rows identical to the first step-6 build; `scripts/check_fgp_sidecar.py` passes it.
