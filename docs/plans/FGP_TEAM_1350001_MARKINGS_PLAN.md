# FGP: reading team 1350001's Word files with their completions and doubtful letters

Status: **done** (2026-10-28). Approved as written (owner, 2026-10-28: "Approve as written, compact and proceed"); rules
and run results in `FGP_TEXT_PIPELINE_RULES.md` §10: 176 candidate rows ship the marked Word text, 6 new rows enter,
4 rows held by the letter check. The owner's answers the same day: superscript = interlinear additions (`\ /`); the
Rashi font unknown, "mark somehow" (`⸤ ⸥`); the circle stays on a hollow `X`; the marks need no legend. Raised by the owner while reviewing the rows held by gate 3's re-read
(`_tmp/fgp_held_review.html`): "The Ktav Yad font symbolizes letters that are not there - completion (perhaps should be
between brackets). The hollow letters, that are currently not read, should be a letter with question mark or a dot
above (doubtful reading)." Belongs to GS-276; rules go to `FGP_TEXT_PIPELINE_RULES.md` (a new section) before the run.

## What was measured

One FGP team, folder `1350001` (the Yerushalmi transcriptions; 164 Word files, 191 gate-3 rows, 180 of them in the
candidate), marks its text by formatting in the Word files. Census of every run in the 164 files
(`team1350001_census.py`, `team1350001_examples.py`, session scratch):

| Formatting | Runs / letters | Meaning | In the shipped text today |
|---|---|---|---|
| font **Guttman Yad** (cursive) | 4,648 spans, 65,754 letters, in 64 files | completion: letters not in the manuscript | plain letters, unmarked |
| Narkisim with Word's **outline** effect (hollow) | 15,954 segments, 26,862 letters, in all 164 files; 13,231 inside a word, mostly one letter | doubtful reading | **missing**: the PDF draws them as strokes, not text |
| cursive *and* outline | 63 runs | doubtful completion | as above |
| **strikethrough** | 95 runs (`למ{ל}חצה`, `דגן {מ}זמ`) | apparently letters the scribe deleted | plain letters, unmarked |
| **superscript** | 283 runs (`ב'ש{וק}{טנים}`) | apparently interlinear additions | plain letters, unmarked |
| font **Rashi** | 695 letters, single letters inside words (`ונ{כ}נס`, `מו{צא}י`) | unknown | on lines of their own (the PDF draws them at another size) |
| underline / color | headings / one run | layout only | — |

No other team uses the cursive font in a sample of 4,000 Word files; 5 of them use the outline effect, meaning
unknown, so the rule is scoped to this team.

**Why these rows ship the PDF text.** All 191 rows' Word texts were read by gate 3 and held (`unclassified_diff`
against today's PDF text). The PDF text is the damaged one: the hollow letters are missing, headings in fake bold come
out with every letter four times (`דדדד`), number ranges as control characters, small-font letters on lines of their
own. With the outline letters set aside, the unclassified blocks fall from 1,315 to 528, but 156 rows still have some,
all of them PDF damage. A line diff against this PDF cannot vouch for the Word text.

**A check that can.** Count each Hebrew letter on both sides (NFKD, fake-bold repeats once, order and spacing
ignored): the Word text without its doubtful letters against the PDF reading (the re-read where gate 3 has one). Of
the 182 rows with a PDF, **179 agree on at least 99% of their letters**; the other 3 fall between 90% and 98%; 9 rows
are new units with no PDF.

## Decisions for the owner (recommendation first)

1. **Completions** -> square brackets around each cursive span that holds letters: `טעו[א]`, `[מה את]`. A span that
   is only a lacuna marker (`<..>`, 123 spans hold brackets) is left as written; no doubled brackets where the span
   already sits in brackets.
2. **Doubtful letters** -> the letter followed by **U+05AF HEBREW MARK MASORA CIRCLE** (the circellus editions use for
   a doubtful letter): `עבד֯`. Alternative: U+05C4 HEBREW MARK UPPER DOT (`עבדׄ`). Both are in David, Frank Ruehl,
   Narkisim, Times, Arial and Segoe UI (checked), and search already folds U+0591-U+05CF out, so `עבד֯` is found by
   `עבד`. Not recommended: `?` after the letter, which splits the word for search and highlighting. A doubtful
   completion gets both: `[ד֯]`.
3. **Struck letters** (95 runs) -> recommended: mark as deleted with `⟦ ⟧` (`למ⟦ל⟧חצה`); alternative: leave plain.
4. **Superscript** (283 runs) -> owner to confirm they are interlinear additions; then `\ /` around them (Leiden), or
   leave plain.
5. **The Rashi font** (695 letters) -> owner to say what it marks; until then read as plain letters, in place.
6. **Release** -> recommended: do this before v9.11.0 (it fixes 180 rows the release would otherwise ship damaged);
   estimate half a day of work plus one gates 3-4 run.

## Method

1. **Reader** (`scripts/fgp_gate3_text_pipeline.py::read_docx`): a marking profile per FIST team, applied only to
   team `1350001`. Runs are classified by their resolved run properties (direct `rFonts` / `outline` / `w14:textOutline`
   / `strike` / `vertAlign`, and the run style's), and adjacent runs of one class are merged before marks are added,
   so a span split across runs gets one pair of brackets. The fast path is kept for every other team. `structure`
   counts each marking (`completion_spans`, `doubtful_letters`, ...).
2. **Text rule**: decisions 1-5 applied in reading order; marks never cross a line; spaces kept as written.
3. **Gate 3 check (new, T15)**: for this team, the line diff against today's PDF text is reported but does not hold
   the row; instead the row is accepted when the letter inventory of its Word text (doubtful letters, brackets and
   marks set aside) agrees with the PDF reading on >= 99% of letters; a row below that, or with no PDF, is held for the
   owner's review by row. Shown able to fail: a Word file of another manuscript, and one with a line removed.
4. **Gate 4**: these rows ship the Word text (`text_source` `gate3:docx`, provenance naming the marking profile); the
   PDF re-read no longer applies to them.
5. **Expectations** re-declared from run 1 before run 2, with the reason, as for the re-read.

## Checks before shipping

- Tests on synthetic DOCX files: a completion inside a word, at a line start, a lacuna-only span, a span over two
  runs, a doubtful letter, a doubtful completion, a struck letter; the profile not applied to another team.
- The letter-inventory check fails on the two planted cases above.
- A review page of a sample (20 rows: PDF crop beside the new text) for the owner before the gates run.
- Both apps: the marks render (web reading view, desktop SBL Hebrew), search finds a word with a doubtful letter and
  one with a bracket inside it, the "?" marks legend still matches.

## Effect

180 candidate rows switch from the PDF reading to the Word text with their markings; their boxes go (the Word text has
none) and the 26,862 doubtful letters come back. 12 of them are on the 51-row review page and need no decision there:
43175, 34370, 31, 32, 148, 33, 6574, 31140, 25473, 36471, 36472, 5030.

## Later

A "FGP's typeset page" view (light images of FGP's PDFs beside the text): about 34,000 PDFs, 68,000 pages; ~3 GB as
stored images or far less rendered on demand; needs FGP's agreement. Separate plan.
