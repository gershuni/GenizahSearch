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
formats; unknown formats counted); footnote references numbered in order of appearance and the footnotes
returned apart and appended after a blank line as `<n> <text>`; header and footer text returned apart
(`PAGE` fields removed); the column labels `טור_א` … become the blank line that separates columns; one
rendering of a text box (`mc:Choice`). The structure census per unit (`structure`): paragraphs, tables,
footnotes, superscript runs, bold runs, breaks, tabs, numbered paragraphs, hidden runs, tracked changes,
images, column labels, header parts, numbering the reader could not render.

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
| `no_comparator` | a new row (§4) | 0 | by policy |
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
link and DOCX structure census, and no comparator. Under `--new-rows-policy hold` (the default) none is
accepted (`hold_reason = needs_acceptance`). Under `plain`, folder-B text (`rule:folder_b_text`) and a
DOCX without tables, footnotes, superscripts, hidden runs, tracked changes, images or unrendered numbering
(`rule:plain_docx`) are accepted by rule; a structured DOCX is held (`docx_structure`). Gate 3 never marks a
new row publishable: credit and visibility for new rows are the import's job (assessment §6.2). Descriptions
are never read (check `T07`).

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
FreeDescription unit or a `_D` file · `T08` every row is a transcription or translation by FIST · `E_*` the
declared expectations. The tests prove the gate fails when a lossy difference is accepted (`T06`), when a
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
  3,790, publishable with text 40,990, digest `496575a202e34af2`, PASS 56/56) (gate 2: 41,807 — 3,393 of those
  are held here, 2,731 come back through the lifted holds).
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
