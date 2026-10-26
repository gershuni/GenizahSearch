## P2 data: FGP text drops the part of a line before a gershayim
Where: `fgp_data/fgp_transcriptions.db` (`text_source='pdf_rtl'`), `shared/local_indexer.py::extract_pdf_pages`, `scripts/fgp_rebuild_text_and_credit.py`
Opened: 2026-10-25

The PDF-extracted FGP text loses everything on a line before a `"`: `ועד י"ג שנים ואף על פי שלא` is stored
as `ג שנים ואף על פי שלא`, `הה"ד` as `ד`, and the missing prefix is nowhere in the row. Measured on 805
random FGP images whose own line text exists in FGP's position export
(`C:\FGPTranscriptions_WithPOSInfoAndImages\C<n>\data.js`, matched to the row through FIST
`dbo_UnitTranscription.ImageTranscriptionFolder` → `FileName`, lines aligned with `difflib`): 63 lines
in 23 documents lose a prefix; 6 catchword lines (`%אמור%`) and 2 short lines are absent; 10 long lines
are stored as two; 288 lines have mirrored lacuna brackets and 214 doubled backslashes (`{ה\/\/ב}`).
Gate 3 (`scripts/fgp_gate3_text_pipeline.py`, 2026-10-25) found more on the rows whose source is the
DOCX: final letters left as C0 control characters or symbols (`שלו\x0e ולכ\x04`, `ואיל"` for `ואילך`) or
dropped outright (`דנו` for `דנום`), vocalised letters as control characters, line numbers re-ordered by
the RTL fix (`.1` for `1.`, `-1` for `1-`), mixed-direction lines scrambled (`12.528: TS ראה`), Arabic
alef dropped, Arabic lines reduced to control characters. The same source also explains GS-60 (`&deg;`
entities). Fix sketch: rebuild the text from the structured sources (folder-B line text, else DOCX)
per `docs/plans/FGP_SOURCE_FOLDERS_ASSESSMENT.md` §6.1, gated per changed line (§8.3,
`docs/plans/FGP_TEXT_PIPELINE_RULES.md`), never from the PDF.

## P2 data: FGP visual XML is applied by image number, not by source record
Where: `scripts/fgp_rebuild_text_and_credit.py::build_xml_index`, `fgp_data/fgp_transcriptions.db` (`text_source='xml'`)
Opened: 2026-10-25

The XML-primary rebuild keys the Talmud-project visual XML by C-number and writes it into every row
of that image, whatever file the row came from. FIST attributes 5 of the 6,381 `xml` rows to a
different team's edition (C192908 row 3717, whose own text reads `למשתה` / `בתר` where the XML reads
`למשתי` / `כתר`) and 3 to the wrong `_G` region of the same photograph (rows 24067, 36404, 36406
carry the G1 region's text); 3,099 team-200 rows have no FIST folder link and are unverified. Those
rows show another edition's text under their own file's credit. Fix sketch: tie XML and line text to
the source record (FIST `ImageTranscriptionFolder` ↔ `FileName`), never to the image; re-attribute the
6,381 rows before any further rebuild (assessment §2, §7).

## P2 data: FGP credit heuristic disagrees with FIST on 1,551 / 6,151 files
Where: `scripts/fgp_fill_credits_bilingual.py`
Opened: 2026-10-25

Credits derive the team from `image_id[:3]` and the catalogue/book from `image_id[3:7]`, but the
14-digit id is not unique (8,131 ids carry 2–8 files) and FIST `dbo_Signature.SourceId/SubId`
disagrees on 1,551 source prefixes and 6,151 sub-ids of 69,330 files. Concrete: rows 20076
(T-S G2.98) and 22207 (T-S Misc.35.108) share `image_id` 60000010010951 and both show
"תרביץ: רבעון למדעי היהדות" from sub-id `1`, while FIST assigns book sub-ids 3308 and 6413. Fix
sketch: take team / catalogue / book from FIST (`FileName` → `SignatureId` → `SourceId/SubId`) at
build time and keep the team-matched transcriber enrichment the script already adds (assessment
§2, §6.2).
