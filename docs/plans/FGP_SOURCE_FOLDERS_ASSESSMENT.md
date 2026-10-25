# FGP source folders — technical assessment

**Date:** 2026-10-25 · **Revision 4** (after Codex gpt-6-astra rounds 1–3; §10 is the revision history) · **Status:**
assessment only, no code changed · **Branch:** `worktree-fgp-source-assessment`

Two new FGP deliveries were examined against what GenizahSearch serves today
(`fgp_data/fgp_transcriptions.db`, 45,034 rows extracted from PDFs in June 2026):

| Folder | What it is | Size |
|---|---|---|
| `C:\FGPTranscriptions_NoPOSInfo` | **The FIST file store**: the transcription / translation / description files FIST's unit tables point at, as PDF + DOCX + DOC (with export gaps, §3.4) | 219,520 files, 7.5 GB |
| `C:\FGPTranscriptions_WithPOSInfoAndImages` | **Position data**: per-image word/row coordinates in two formats, plus the image crops and rendered layouts | 97,505 files in 23,484 dirs, 25.4 GB |

Every number below comes from read-only scripts run in this session (`fgp_census2.py`, `probe_fist*.py`,
`verify_a–g.py`, `census_b1_geometry.py`, `overlay.py`, `compare_pdf.py` in the session scratchpad) against the two
folders, today's sidecar, `FIST.db`, `fjms_enrichment.db` and `nli_crossref.db`. Codex reproduced the headline counts
independently (Node SQLite, read-only) in two review rounds.

## 1. Bottom line

1. **Today's import holds 68% of FIST's transcription files.** FIST's `dbo_UnitTranscription.FileName` matches
   54,528 NoPOS basenames; 37,208 of them are in today's DB. **17,320 transcription files and 778 translation files
   are not served**, and — keeping every FIST association — the transcription files reach **31,439 manuscripts
   (AlmaIds) against 24,940 sys_ids today: 6,694 manuscripts have FGP text we do not show.** These are *candidate*
   associations until the import validates them (7,954 files carry more than one AlmaId). Eight sampled shared PDFs
   were byte-identical to today's copies.
2. **The position folder carries the text the PDFs were rendered from, already segmented by line, for 24,615 images.**
   Compared source-record to source-record (FIST links the folder to the file) and line-aligned, on 805 random B2
   images: 46% of documents identical, **91.1% of lines identical**; the differing lines are punctuation/spacing (474),
   mirrored lacuna brackets (288), doubled backslashes (214), and three real losses in today's text — **63 lines
   missing the text before a gershayim (`ועד י"ג שנים` stored as `ג שנים`, in 23 documents)**, 6 catchword lines
   (`%…%`) and 2 other short lines absent, 10 long lines wrapped. No genuine reading differences were found in the
   source-matched sample.
   Today's rows on those images are **coverage candidates** (22,378 rows: 15,977 `pdf_rtl` eligible for replacement,
   6,381 `xml` needing source re-attribution first, 20 `original`), not demonstrated corrections.
3. **Word-level image alignment is real and verified on one image per format**; the B1 census gives **1,074,196
   positioned entries with real text across 4,171 directories** (entries, not unique words: 879,400 in dict form,
   194,796 in list form; 98.9% are referenced by the Talmud-line `src` map). Coordinates are FGP-master pixels (B1) or crop-relative (B2); master
   sizes exist for 738,060 images in `fjms extra_info`. Whether the NLI IIIF rendition is the same photograph at the
   same orientation is **unverified, and 1,179 of the images carry a recorded rotation of ~90°/180°/270° or other**
   — a landmark check comes before any overlay work.
4. **The join keys are exact.** NoPOS basename == FIST `FileName` (69,330 / 72,098); WithPOS dir == FIST
   `ImageTranscriptionFolder` (20,825 rows); FGP number → sys_id through `fjms extra_info` (24,561 / 24,615) and →
   the exact page through `nli_images` (24,598 / 24,615; 89 numbers map to two AlmaIds there). Exact lookup is not
   unique identity: 1,645 basenames have several FIST unit rows, 14-digit ids collide (8,131), and **image identity is
   not edition identity** — today's XML rebuild already put another edition's text on 5 rows and another region's text
   on 3 (§2).
5. **10,890 of the NoPOS files are free descriptions, not transcriptions** — and descriptions are *already shipped*:
   `fjms_enrichment.db catalog_free_desc` (303,638 rows, 170,432 AlmaIds, exported by
   `scripts/export_fist_enrichment.py::export_catalog_free_desc`, read by `shared/fjms_service.py::get_catalog_detail`).
   Whether the file-backed ones add content beyond `FreeDesc` is unverified; they must stay out of the edition chooser.

## 2. Baseline — what is served today

- `fgp_data/fgp_transcriptions.db`: 45,034 rows (41,692 editions / 3,342 translations), 40,049 distinct `image_id`,
  20,564 distinct `c_number`, 24,940 distinct `sys_id`. `text_source`: 38,122 `pdf_rtl`, 6,381 `xml`, 531 `original`.
  3,603 C-numbers carry two or more edition rows (duplicate scans and the same transcription filed by two teams, e.g.
  C100000 as `20000010072771_…` and `20000040000011_100000_Tc`); `dedupe_fgp_sources` collapses them per
  `(c_number, doc_relation, language)` keeping the longest, and drops whole-document rows when per-page rows exist.
- Text comes from PyMuPDF over the FGP PDFs, re-extracted RTL (`shared.local_indexer.extract_pdf_pages`); the 3,278
  visual XML files found in the June export were parsed with `scripts/fgp_rebuild_text_and_credit.py::parse_fgp_xml`.
- **Defect of the current XML policy:** `build_xml_index` keys the XML by C-number and the rebuild applies it to every
  row of that image. Of the 6,381 `xml` rows, FIST attributes 3,251 to the Talmud-project folder (the XML's own
  source), **5 to the image's `C<n>` folder (a different team's edition — e.g. C192908 row 3717, whose own text reads
  `למשתה`/`בתר` where the XML reads `למשתי`/`כתר`)**, and 26 to a `_G` region folder of the same image — 23 of those
  match their own region's XML, **3 (rows 24067 `100446_G2`, 36404 `456630_G2`, 36406 `456738_G2`) carry the G1
  region's text instead**; 3,099 are team-200 files with no folder link whose equivalence to the XML is unverified.
  So 8 rows are known to show another edition's or region's text under their own file's credit, and 3,099 are
  unverified.
- `sections` are per PDF page / canvas (`shared/fgp_service.py` ~210), not lines — a line-level text source does not
  slot in without a contract change.
- Page alignment uses `c_number ↔ nli_images.FGPImageNumberId` where a C-number exists (24,184 rows), else the
  `image_side` label, else "whole document on every page".
- Credit is derived from `image_id[:3]` (team) and `image_id[3:7]` (catalog/book id) in
  `scripts/fgp_fill_credits_bilingual.py`, with team-matched individual transcribers kept where the DataSource text
  matches. Against FIST's `dbo_Signature.SourceId/SubId` the heuristic disagrees on **1,551 source prefixes and
  6,151 sub-ids** of 69,330 files; the collision pair below both carry "תרביץ: רבעון למדעי היהדות" from sub-id `1`,
  while FIST assigns book sub-ids 3308 and 6413.
- The uniqueness constraint on today's rows is `rel_path`, not `filename`; the web app stores per-user selections as
  `fgp:<id>` (surrogate key).

## 3. Folder A — `FGPTranscriptions_NoPOSInfo` (the FIST file store)

### 3.1 Structure

```
<SourceId(3)><SubId(4)>/          56 dirs, e.g. 2000000, 2000007, 1320000, 6000000, 5000266
   pdf/        <FileName>.pdf            75,461 files, 4.41 GB
   word/       <FileName>.docx           72,035 files, 1.18 GB
   word_old/   <FileName>.doc            72,024 files, 1.93 GB
```

- The 7-digit dir name is the FIST source: `200`=Genuzos transcription team (41,429 PDFs across 2000000/2000004/2000007),
  `132`=Goitein documentary team (7,431), `600`=books (6,741), `500`=catalogues (5,432), `108`=Milikowsky (3,296),
  `107`=Firkovitch (2,786), `102`=Stroumsa (1,378), `101`=Maman (1,111), `151`=Glick (997), `105`=Sklare (947) …
  It is `dbo_UnitTranscription.FolderPath` verbatim (`'2000000\'`).
- `word/` and `word_old/` are the same documents as DOCX and legacy DOC; PDF ↔ DOCX basename sets differ by 124 / 52 files.
- Junk to filter: 1,106 Word lock files (`~$…`), `Copy of …`, `New Microsoft Word Document.doc`, `inv.pdf`,
  `rrrr.pdf`, `test.docx`, `temp8_…`, empty dirs `1510000A`, `New folder`, `tsclient`, and two 0-byte `.txt` at the root.
  After filtering: 72,098 distinct PDF basenames.

### 3.2 File naming — the lookup key is the FIST `FileName`, not the 14-digit id

`<FileName>` has four shapes (basenames; gate 1 counts — revision 5 corrects the census, whose suffix pattern only
matched a suffix followed by `.scan`):

| Shape | Count | Example |
|---|---|---|
| `<id>_<Library> <Shelfmark>_<Tc\|Tl\|D>` with a suffix | 31,986 | `20000070000181_CUL_T-S NS 329.533_Tc` |
| `<id>_<Library> <Shelfmark>` with **no** suffix | 19,728 | `10800010000031_RNL_Yevr_ III B 71` |
| `<id>` only | 20,430 | `50002300001701` (catalogue/book-derived; no shelfmark in the name) |
| C-number named | 6 | `C252953` |

- `Tc` = transcription, `Tl` = translation, `D` = description. Where a suffix exists it agrees with the FIST kind in
  every case (0 disagreements), but 40,164 basenames carry none, so **the kind must come from FIST**, not the name.
- **The 14-digit id is not unique**: 8,131 ids have 2–8 PDFs. `60000010010951` is both `T-S G2.98`
  (AlmaId 990051182080205171) and `T-S Misc.35.108` (990051125920205171), different content. 5,095 ids are 15 digits.
- **The basename is an exact lookup key but not a scholarly identity**: 1,645 matched basenames occur in more than
  one FIST unit row (all of the same kind; gate 1: every one of them is several signatures of *different* signature
  sets, none a duplicate row). A refresh must carry FIST's typed unit id and `SignatureId`, not the name.
- **`.scan.pdf` twins**: 3,432 files; 3,354 sit next to a born-digital twin, 77 are orphans with no DOCX/DOC sibling.
  Checked samples are image-only (0 text chars, Acrobat Distiller / PDFCreator) — scans of typed sheets. Whether the
  77 orphans have text anywhere else in FIST is unchecked.

### 3.3 What the files are, per FIST

Matching the 72,098 basenames (`.scan` folded) against FIST (Codex reproduced these counts independently):

| FIST unit table | FIST rows with a FileName | NoPOS basenames matched |
|---|---|---|
| `dbo_UnitTranscription` | 54,608 | **54,528** |
| `dbo_UnitTranslation` | 3,913 | **3,912** |
| `dbo_UnitFreeDescription` | 97,174 | **10,890** |
| unmatched | — | 2,768 (1,857 of them in `2000000`) |

These are PDF basenames. Gate 1 (§8.1) also counts the **52 basenames that exist only as DOCX**: 8 are FIST
transcriptions (7 of team 133 "Judeo Persian" in `1330000`, 1 in `8500000`) and 44 are unmatched, so the inventory's
totals are 69,338 matched / 54,536 transcriptions / 2,812 unmatched.

Every matched file has a `dbo_Signature` row. The chain
`FileName → SignatureId → SetSignatureId → dbo_InventorySignature.InventoryId → dbo_InventoryAlma.AlmaId`
resolves **68,865 / 69,330** files to at least one manuscript (68,871 / 69,338 with the DOCX-only files); **7,954
files resolve to more than one AlmaId** (7,957), through
48 distinct inventories that carry more than one AlmaId (164 set–inventory associations reach them) — joined /
multi-part shelfmarks, not errors, but each needs a decision, since a chooser row has one `sys_id`.
`dbo_Signature.SourceId/SubId` gives the team/catalogue/book (provenance, not a finished display credit);
`dbo_UnitTranscription.HebCopyRight/EngCopyRight` carries explicit acknowledgment text for 600 rows (Sussmann / Israel
Academy); 84 rows have `tmp_DoubleNotToDisplay=1` (meaning unknown — those rows are not published until it is).

### 3.4 Delta against today's DB

| | Basenames | Of which, by FIST kind |
|---|---|---|
| In NoPOS and in today's DB | 40,342 | 37,208 transcriptions, 3,134 translations (8 sampled files byte-identical) |
| **NoPOS only** | **31,756** | **17,320 transcriptions**, 778 translations, 10,890 descriptions, 2,768 unmatched |
| Today's DB only | 19 | all 19 are still in FIST (team `850` site users, Birmingham, Haifa) — missing from this export, not dropped |

- New transcription files by dir: `2000000` 11,726 · `1070000` (Firkovitch) 1,431 · `2000004` 892 · `6000000` 764 ·
  `2000007` 749 · `1510000` (Glick) 478 · `1080000` (Milikowsky) 407 · `1020000` (Stroumsa) 243. 14,917 of them carry a
  shelfmark in the name; 2,403 are id-only and need the FIST chain.
- Manuscripts: transcription files → **31,439 AlmaIds (all associations) vs 24,940 sys_ids today → 6,694 candidates
  not in the DB** (31,440 / 6,695 with the DOCX-only files); description files add 5,223 manuscripts with a
  description but no transcription file.
- Why today's import missed them: the June export was keyed on the FGP *shelfmark folders* (`…/Trans/*_trans.pdf`);
  files FIST holds only in the flat team store never appeared there. The export itself has gaps the other way: the
  19 rows above (their FIST directories are in the export, the files are not), and 72 FIST transcription FileNames
  plus 1 translation with no file here (44 in exported directories, 29 in directories the export lacks; 8 more
  transcriptions exist only as DOCX).

### 3.5 The Word files

The DOCX is the editable original the PDF was printed from, one RTL paragraph per manuscript line (verified identical to
today's text for `T-S NS 329.533`). It is **not uniformly plain**: on 2,000 random DOCX, 648 use bold, 270 colour, 258
tabs, 88 have a footnotes part (66 with references in the body), 83 numbering, 45 tables (37 of them in `1510000`,
Glick), 38 explicit breaks, 25 superscripts, 7 images. A DOCX reader therefore has to be structure-aware (tables →
rows, footnotes kept apart, superscript marked), not a tag-stripping regex. Even so it beats PDF extraction: exact
line breaks, no RTL re-ordering, no loss before a gershayim. The legacy DOC copies were not examined; they are assumed
redundant with the DOCX until a conversion check says otherwise.

## 4. Folder B — `FGPTranscriptions_WithPOSInfoAndImages` (positions)

23,484 directories (one stray entry `V`) in **two unrelated formats**; 97,505 files; jpg 16.5 GB, png 5.7 GB,
js 3.25 GB, xml 43 MB, json 0.9 MB, 109 `data.js.bak`. Directory names: FGP image number(s), `C`-prefixed for format
B2, several numbers for joined fragments, and `_G<n>` / `.G<n>` for a second text region cut from the same photo.
Parsed correctly: **24,615 distinct image numbers (B1 5,308, B2 19,314, 7 in both), 695 multi-image dirs, 169 region
dirs.** FIST links 5 numbers to both a B1 and a B2 folder — two editions of one image (C192908 is one).

### 4.1 Format B1 — Talmud-project directories (4,171 dirs, 5,308 FGP numbers)

| File | Content |
|---|---|
| `N.xml` | The visual XML (`<root><info><fgp><lib><shelf><content>…<col><row u num m p id><t c><w u>`) — **identical bytes** to the 3,278 XML files in `fgp_data/transcriptions/…/visual_C*/` (a strict subset: none missing here). 106,806 rows, 1,223,333 `<w>` (1,147,316 non-blank, 1,055,207 non-lacuna). |
| `N.json` | Page classification: `type` (עמוד 3,351 / מיפתח 781 …), `cols`, `nik` (ללא ניקוד 3,356 / ניקוד חלקי 785 / מלא 4), divine-name style (`shem`, `shem4`), `toc`, `notes` (366 non-empty). |
| `N.js` | `var alldata = {fgps:{<n>:{lib,shelf}}, box:{imgid,x1,y1,x2,y2, fn:"…/T_0145/T_S_AS_78_353__L1F0B0S1.jpg&x=…"}, cols:{…box, mass (tractate), kname (Sussmann catalogue no.), content}, ziruf/zimage for joins}` then `coldata["<n>_<col>"] = {box, imagesize:[W,H], rows:[{id, pnts:[[x,y]×4], grp}], boxs:{<uid>:{r,e,w,p,s,t, box:{0:[x,y,w,h],1:[x,y,w,h]}}}, aca:{html}, src:{<Talmud line ref>:[uids]}}`. Not strict JSON in join dirs (unquoted keys such as `from2:0`) — parse with a lenient reader, never by executing it. |
| `N.xml.js` | The XML wrapped as a JS string (viewer variant; `u` moved from `<w>` to `<t>`). Redundant. |
| `N_<col>.png`, `_l.png` | The transcription rendered at the manuscript's word positions (grey = lost text); `_l` = low-res. |
| `img.jpg`, `img_l.jpg` | Crop of the FGP master at `alldata.box`; joins add `all.jpg` / `allb.jpg` / `imgb.jpg` (composite of the joined fragments, with/without background). |

Geometry census over all 4,173 `N.js` (0 parse failures): 5,051 columns, 106,806 rows, 2,665,251 box entries. Boxes
come in two representations — a dict `{0:[x,y,w,h],1:[x,y,w,h]}` (1,749,898 entries) and a list `[[x,y,w,h],[x,y,w,h]]`
(384,470 entries; e.g. dir `100052 100054 99944`, uid 6, `ר`); 530,883 entries carry no box (row containers, spacers,
lacuna runs). **1,074,196 entries carry a box and real (non-lacuna) text** — 879,400 dict-form + 194,796 list-form;
these are entries, not a count of unique words. 1,061,923 of them (98.9%) are referenced by the `src` Talmud-line map
(867,846 of the dict-form ones) — the map is nearly, not fully, complete (on `100000` it omits uid 9, the positioned `ש`
used as the example below).

Verified on `100000`:
- **Coordinates are FGP master-image pixels**: master 2232×2712 (= fjms `PixelWidth/Height`), crop origin (35,22),
  `img.jpg` 2000×2292 = the crop; uid 9 (`ש`) box (987,173) → (952,151) on the crop, on the letter.
- `boxs` is keyed by the XML `<w u>` uid: all 52 positioned entries with non-blank text match a `<w u>`; 134 entries
  carry a box in all. **Alignment is per word, but the box set is not a 1:1 token list.** `rows[].pnts` are 4-point
  baselines (they follow skew). `box.0` is the tight word box, `box.1` the full row band.
- `alldata.box.fn` / `img.php?fn=` names the **source image file** (`T_S_AS_78_353__L1F0B0S1.jpg`) = `nli_images.ImageName`.
- Libraries (from `<lib>`): CUL 3,428, JTS 584, Oxford 408, Manchester 190, BL 184, Budapest 71, AIU 49, Toronto 44,
  Mosseri 42 …; 2,315 distinct shelfmarks; ~2,470 sys_ids.

### 4.2 Format B2 — `C<n>` directories (19,314 dirs)

One dir per FGP image, three files: `data.js` (`fgp = {…};`), `C<n>_1.png` (rendered layout; 2–6 PNGs when
multi-column), `img.jpg` (the column crop, scaled by `fact`, typically 0.16).

`data.js` fields: `info{Shelfmark, libheb, fgpNumber, textDetails, addInfo}`, `date` (2012: 5,997 / 2013: 13,317),
`cols{<n>:{x1,y1,x2,y2 on the master, img, fact, mm}}`, `img{src:"/img.php?fn=00359/T_S_NS_329_533__L1F0B0S2.jpg&x1=…"}`,
**`ac[].rows{<line>:{ndx, txt}}` = the academic transcription line by line** (240,886 lines; `&nbsp;` for spacing,
sigla inline), `rows[]` = per-line boxes + 4-point baseline `xy`, `boxs[]` = ~10 segment boxes per line (`txt`,
`x1..y2`, `row`, `sug`, `type`, `pop` for supralinear) — produced by FGP's layout editor, with artefacts to filter
(`id` suffixes `split(ROW)`, `test`, negative `x1`). **Geometry is crop-relative**: a box needs the column's
`x1/y1` offset on the master and the `fact` scale, not a scale alone.

Verified: row and segment boxes overlay correctly on `img.jpg` once scaled by `img.jpg.width / (x2-x1)` (= `fact`).
Libraries: Cambridge 9,243, JTS 6,447, BL 1,449, AIU 928, Pennsylvania 395, Strasbourg 326, Budapest 271, Vienna 102,
HUC 95, JNUL 53 …; ~9,319 sys_ids. 1,987 dirs have 2 columns.

### 4.3 Text in folder B versus today's text — source-matched and line-aligned

Sample: 1,000 seeded random B2 dirs. The DB row for each was found through FIST (`ImageTranscriptionFolder` → `FileName`
→ today's `filename`), never by best textual match: 805 compared (158 dirs have no FIST folder link, 37 link to a file
with no DB row). Lines were normalized (entities decoded, whitespace collapsed) and aligned with `difflib`.

| Measure | Result |
|---|---|
| Documents identical | 369 / 805 (46%) |
| Lines identical | 10,818 / 11,876 (91.1%) |
| Replaced lines, by cause | 474 punctuation/spacing · 288 mirrored lacuna brackets · 214 doubled backslashes (`{ה\/\/ב}`) · **62 text before a gershayim missing** (49 by the single-`"` rule, 13 on lines with several gershayim, each inspected) · 0 other |
| Structural | 6 catchword lines (`%אמור%`, `%ולא%`, `%כמגדלות%`, `%סמאל%`, `%לבד%@`, `%שוקט%@`) and 2 other short lines (`ד'צ'ך' ע'[ד]'ש'`, `וג'&`) absent from today's text · 10 long lines stored as two · 2 two-line ↔ three-line regroupings, one of them hiding a 63rd gershayim loss |

The 63 gershayim losses occur in 23 documents; the missing prefix (`ועד י"ג שנים` → `ג שנים`, `הה"ד` → `ד`) is nowhere
in the row. All differences in this sample are artefacts of the PDF route; no reading difference between the
folder-B text and the same source record's PDF text was found. (The one apparent reading difference seen earlier,
C192908, was a comparison across two *different* editions of the same image — see §2.) Codex's independent 20-image
sample agrees on the classes. So the structured text is a correction of a lossy extraction *for the same source
record*; the gate is still per changed line, because this is a sample, not a proof, and because rows without a
comparator (new files) need their own acceptance.

### 4.4 Coverage of folder B against today

| | Numbers | sys_id via fjms | Rows in today's DB (by `c_number`) | Their text source |
|---|---|---|---|---|
| B1 (rich) | 5,308 | ~5,290 | 3,281 numbers | mostly `xml` |
| B2 (C) | 19,314 | 19,282 | 15,901 numbers | mostly `pdf_rtl` |
| **All** | **24,615** | **24,561** (11,782 manuscripts) | **19,177 numbers = 22,378 rows** | 15,977 `pdf_rtl` · 6,381 `xml` · 20 `original` |

- 5,438 numbers have no `c_number` row today — rows that exist without a C-number (CUL direct-`Trans/` layout) and
  images not transcribed in the June export; **1,938 of the 11,782 manuscripts have no FGP row at all today**.
- `nli_images.FGPImageNumberId` hits 24,598 / 24,615; 89 numbers map to two AlmaIds there although `ImageName` agrees.
- FIST ties the folders together: `dbo_UnitTranscription.ImageTranscriptionFolder` (21,527 rows; 21,519 distinct
  values once 8 empty strings are dropped) names the B dir; 20,825 exist, 694 do not (e.g. `1996_G1`…), and 2,660 B
  dirs are not referenced by any FIST transcription row.
- Recorded rotation (`extra_info.Rotation_Angle_deg`) for the 24,615 images: 12,267 within ±10°, 552 ≈180°,
  274 ≈90°/270°, 353 other, 11,169 null.

### 4.5 Scaling positions onto what the viewer shows

Positions are in the FGP master's pixel grid (B2 after adding the column offset). Scaling to an IIIF canvas is
`iiif_width / PixelWidth` **only if** the NLI IIIF master is the same photograph at the same orientation. Same
`ImageName` makes that probable, but 1,179 images carry a large recorded rotation, so the check must compare visible
landmarks, not dimensions: for 20 images spanning both formats, libraries, joins, regions, multi-column pages and
rotations (B1: 100000, 100022, 100446 `_G2`, 2043 `_G1`, 456547, 250429, 421459, 76563, 2857, 4505; B2: C100181,
C127133, C198397, C641, C250201, C73736, C67507, C92956, C59238, C92970 — the last three recorded at 270°/182°/270°),
fetch the rendition each app actually shows, and match ≥6 distributed landmarks against the FGP crop. Record a
per-image transform (rotation/reflection/translation) where it differs; suppress overlays where unresolved.

## 5. Side by side

| | Today | Folder A (FIST store) | Folder B (positions) |
|---|---|---|---|
| Text source | PDF → PyMuPDF (+3,278 XML applied by image) | DOCX (line-exact, structure-aware read needed) / PDF | Line-segmented academic text (`ac.rows`, XML), source-linked via FIST |
| Transcription files | 37,208 | **54,528** | 24,615 images |
| Manuscripts | 24,940 | **31,439** candidates (+6,694) | 11,782 (+1,938 new) |
| Translations | 3,134 | 3,912 | — |
| Descriptions | already shipped via `catalog_free_desc` (303,638 rows) | 10,890 file-backed (overlap with `FreeDesc` unverified) | — |
| Kind / team / credit | inferred from file name (1,551 / 6,151 disagreements) | **FIST `SourceId/SubId`, copyright text** | per-dir `info` |
| Manuscript key | fjms C-number / shelfmark cascade | **FIST inventory chain (68,865 / 69,330; 7,954 multi)** | fjms C-number (24,561 / 24,615) |
| Page alignment | C-number ↔ `FGPImageNumberId`, else `image_side` | — | **`ImageName` in the JS + C-number** |
| Positions | none | none | **~1.07M positioned text entries (B1) / segment boxes (B2) + baselines** |
| Joins knowledge | none | multi-Alma signatures (7,954 files) | **695 multi-image transcriptions + composites** |

## 6. What this enables, in order of value per effort

1. **One text pipeline with several source readers, run against a separate output DB, keyed on the source record.**
   Readers: folder-B line text (B1 XML, B2 `ac.rows`) **only where FIST links the folder to that row's own file**,
   DOCX (structure-aware) for the rest, PDF as last resort; XML never regresses; provenance recorded per row. Scope
   today: the 15,977 `pdf_rtl` rows on folder-B images are eligible candidates, the 6,381 `xml` rows are first
   re-attributed to their source record (8 are known to carry another edition's or region's text, 3,099 unverified). The gershayim,
   catchword, bracket and backslash artefacts are expected to disappear for every row that passes the per-line gate.
2. **Import the 17,320 missing transcriptions and 778 translations**, keyed on FIST's typed unit id + `SignatureId`
   (basename and `rel_path` kept as lookup keys), `sys_id` from the inventory chain with every association retained and
   multi-Alma files resolved explicitly, team/catalogue from `dbo_Signature`, and credits rebuilt from FIST while
   keeping the team-matched individual-transcriber enrichment the current script adds. +6,694 candidate manuscripts.
3. **Exact page alignment** for every B image from `ImageName` — replaces the `image_side` fallback for those rows and
   removes the "whole-doc on every page" bleed for them.
4. **Highlight-on-image**, after the §4.5 check. Storage: a per-(image, column, text-revision) compressed JSON payload
   holding uids, row ids, text spans, boxes/baselines, crop offset, master size and transform — relational only for
   the lookup keys; measured against a row-per-box table on installed size, download size, largest payload, peak
   memory and read latency, with the limits in §8.5 deciding. The web viewer transforms the `<img>` element itself
   (`manuscript_viewer.js::applyTransform`), so an overlay needs a wrapper that carries the same transform; desktop a
   QGraphicsItem layer over the pixmap. Scope: 24,615 images (~10% of FGP-transcribed images; the 4,171
   Talmud-project directories FGP delivered are all in it).
5. **Joins pilot**: 695 B1 transcriptions span several images (plus 7,954 multi-Alma FIST files). Multiple images
   may be facing pages, re-photography, regions, codex association or a scholarly reconstruction — retain FGP's
   assertion, composite and constituents as evidence; a relationship becomes a "join" only after scholarly
   classification, never automatically.
6. **Descriptions**: reconcile the 10,890 file-backed ones with `catalog_free_desc`; import only content absent from
   `FreeDesc`, in the existing catalogue-detail surface. No new store, no chooser category.
7. **Research by-products** (not for the apps directly): ~1.07M positioned text entries in B1 (HTR / line-detection
   training data; entries, not unique words); the `src` map ties 98.9% of them to a Talmud line reference; `N.json` page
   classifications — subject to the data owner's permission for that reuse.

## 7. Risks and gaps

- **Identity.** The 14-digit id collides (8,131); the basename is a lookup key, not a permanent identity (1,645 with
  several FIST rows; a re-export can rename, re-associate or retire). Rows need a project-owned durable id carrying the
  FIST unit id, `SignatureId`, `SetSignatureId`, original path, file hash (as revision evidence, not identity),
  associations, delivery and parser version; every existing public `fgp:<id>` keeps its meaning (aliases + explicit
  retirement), and a corrected text must not reset a user's saved selection.
- **Image identity ≠ edition identity.** Today's XML policy already put another edition's text on 5 rows and another
  region's on 3 (§2). Any
  replacement or overlay must be tied to the source record (FIST `ImageTranscriptionFolder` ↔ `FileName`), and the
  5 images with both a B1 and a B2 edition need an explicit precedence rule.
- **Mixed kinds.** 10,890 description files and 2,768 unmatched files sit beside transcriptions with no suffix in the
  name. Kind must come from FIST; unmatched files are quarantined with a reason, never guessed.
- **Behaviour changes without UI changes.** `dedupe_fgp_sources` merges rows per `(c_number, kind, language)` keeping
  the longest and drops whole-document rows when per-page rows exist — a second, independent edition of the same image
  would vanish; `choose_default_source` weighs coverage, overlap and the searched phrase, so longer or additional rows
  change defaults. Both need review and tests before the sidecar grows.
- **`sections` contract.** Line arrays do not fit the per-page `sections` shape; a line-level source needs a new
  field or a documented change at every consumer (`fgp_service`, browse, search results, desktop dialogs).
- **FIST.db (3.2 GB) and the folders are local-only.** Build-time only; nothing at runtime may depend on them. The
  importer streams (bounded memory, resumable manifest, explicit failures — never 72K documents or 3.25 GB of JS in
  memory) and lives under `scripts/`, unlike the untracked `fgp_data/fgp_*.py` producers.
- **Parsing.** `alldata` is not strict JSON; `boxs` in B2 carry editor artefacts; transcription text contains literal
  scholarly marks (`<~>`, `<י>`, `&nbsp;`) that must be decoded as entities, never treated as HTML.
- **Orientation/scale vs IIIF** unverified; 1,179 images have large recorded rotations (§4.5).
- **Credits, visibility and permission.** `SourceId/SubId` is provenance, not a display credit; FIST's copyright text
  (600 rows) must be shown; **the 84 `tmp_DoubleNotToDisplay=1` rows are withheld from publication until the flag's
  meaning is confirmed with FGP**; displaying word boxes over library images and any training-data reuse are new uses
  of FGP data — confirm with the data owner before they are public.
- **Export gaps.** 19 of today's rows and 72 FIST transcription FileNames (+1 translation) are not in this export,
  and 8 transcriptions exist only as DOCX; 77 `.scan.pdf` orphans have no text form in the folders; the DOC copies are
  unexamined. All go on the list for the FGP data owner.
- **Size.** Today's 405 MB sidecar is already bundled; a fuller text DB and a positions DB add to the installer, so
  positions should be an optional download on the desktop and `run.io_bound` reads on the web must cover fetch +
  decode. The post-install sidecar updater does not know FGP yet (open tracker row).

## 8. Suggested order of work and gates (for discussion, not committed)

Each gate names what makes it fail; "measured" or "reviewed" alone is never a pass.

1. **Association inventory.** Produce the per-file table: basename, FIST unit id(s), signature(s), source, AlmaId(s),
   B-folder dir, kind, visibility flags, status. *Fails if:* any count does not reconcile across physical files,
   basenames, unit rows, images and manuscripts; any multi-association lacks a resolution rule; any region suffix or
   description is miscounted; any of the 19 missing / 80 absent / 2,768 unmatched files lacks a reason.
   **Run 2026-10-25 — PASS, 53 / 53 checks** (`scripts/run_fgp_association_inventory.ps1` →
   `scripts/fgp_association_inventory.py`, expectations in `scripts/fgp_association_inventory.expect.json`; output
   `fgp_data/inventory/` — `inventory.db`, `summary.json`, `REPORT.md`; 22 s; `tests/test_fgp_association_inventory.py`
   proves the gate fails on a basename of two kinds and on a missed expectation). Every count partitions; 0 mixed
   kinds, 0 missing signatures, 0 suffix / kind disagreements. The 72,150 basenames: 40,311 served · 15,188 candidates
   (14,524 transcriptions, 664 translations) · 2,528 candidates with several AlmaIds · 10,890 descriptions · 331
   transcriptions / translations without an AlmaId (467 files in all: 463 whose inventory has no AlmaId, 4 whose set
   has no inventory) · 90 held by `tmp_DoubleNotToDisplay` (84 transcriptions, 6 translations; 23 served today) ·
   2,812 unmatched, each with a reason (1,865 whose id prefix is a FIST file of another name, 910 with no FIST trace,
   29 whitespace drift from a FIST name, 6 C-named, 2 scan-only). Multi-association classes, each with a declared
   rule (proposals for gate 2; a file may fall in several): `set_multi_inventory` 6,639 ·
   `multi_signature_different_sets` 1,645 · `inventory_multi_alma` 236 · `dup_unit_rows_same_signature` 0 ·
   `multi_signature_same_set` 0. Corrections the run made to this report: the §3.2 shape table, the 52 DOCX-only
   basenames (§3.3–3.4), 695 → 694 position folders (§4.4).
2. **Identity, credit, visibility and refresh rules.** *Fails if:* any of the 45,034 existing rows lacks an explicit
   outcome (kept / re-sourced / retired-with-alias); any public `fgp:<id>` changes meaning; any row with an unresolved
   credit, an ambiguous association, or an unknown visibility flag (`tmp_DoubleNotToDisplay`) is marked publishable.
   **Run 2026-10-25 — PASS, 7 checks + 29 declared expectations** (`scripts/run_fgp_gate2_rules.ps1` →
   `scripts/fgp_gate2_rules.py`; rules and open rulings in
   [`FGP_IDENTITY_CREDIT_VISIBILITY_RULES.md`](FGP_IDENTITY_CREDIT_VISIBILITY_RULES.md); output
   `fgp_data/inventory/gate2.db`). Outcomes: 39,176 kept · 5,808 re-sourced (2,678 credits from FIST instead of
   the file-name prefix — 1,463 books, 531 of them with titles FIST holds only in English; 1,051 named contributors
   from the single matching FJMS DataSource part; 11 bibliographic references composed from FIST; 74 site users and
   6 personal lists named by `SubId` from the live site; teams 111 and 161 verified there — and 3,130 XML rows whose
   text must come from the row's own record) · 50 retired-with-alias (17 exact duplicates with a same-meaning
   successor, 28 unreachable rows without a `sys_id`, 5 whose `sys_id` the FIST chain contradicts). Publishable:
   41,807 under the owner's ruling of 2026-10-25 that rows whose FIST source has no verified credit (338 rows, 5
   sources after the live-site refinement) carry the generic FGP attribution; 41,472 under the strict reading. Held
   and named per row: 3,130 XML rows, 23 `tmp_DoubleNotToDisplay`, 19 files absent from the delivery, 7 unconfirmed
   associations.
3. **Unified text pipeline on a separate output DB.** *Fails if:* two runs differ; any replaced row lacks a proven
   source correspondence (FIST link); any line-level diff against today's text is unaligned or has an insertion /
   deletion without a disposition; any replaced line falls outside the accepted classes (restored prefix, restored
   catchword, unwrapped line, escape, bracket, whitespace) without an approved reading; any new row without a
   comparator is published without its own acceptance (DOCX structure accounted for: tables, footnotes, superscripts);
   any `xml` row regresses; any description becomes an edition.
4. **Chooser validation with the larger sidecar, both apps.** *Fails if:* an independent edition becomes
   unselectable; a saved choice is lost; a page assignment changes without approval; a default-selection change is
   not dispositioned; import peak memory exceeds the declared limit (to be set against the 63 GB dev machine and the
   server's 5–6 GB baseline) or manuscript-load latency exceeds the declared budget on the reference manuscripts.
5. **Positions prototype after the 20-image geometry check.** The landmark tolerance and the size / payload / latency
   limits are declared *before* the check; an undeclared limit is itself a fail. *Fails if:* for any of the 20 images
   the **residual** landmark error after the recorded transform exceeds the tolerance (recording a transform is not a
   pass — the transformed overlay has to land); any overlay text span is not tied to the selected edition's revision;
   an unresolved mapping still shows an overlay; installed size, largest payload or cold read latency exceed the
   declared limits for the chosen storage — in which case the other schema is taken or the scope cut.
6. **Joins pilot and description reconciliation**, each separately reviewed. *Fails if:* a published relationship
   lacks provenance or an approved scholarly classification; a reconciled description is unattributed, duplicates an
   existing `catalog_free_desc` row, or displaces existing text or translations.

## 9. Evidence files (session scratchpad, not in the repo)

`census2.json`, `verify_a.json` … `verify_i.json`, `b1_geometry.json`, `b1_geometry2.json` (full counts),
`census2_withpos_numbers.tsv`, `census2_nopos_basenames.tsv`, `overlay_rich_100000.jpg`, `overlay_c_C100181.jpg`. The four
Codex reviews were kept verbatim in the assessment worktree's gitignored `_tmp/` and went with it when the worktree was
removed after the fast-forward; their findings and dispositions are recorded in §10. Since revision 5 the §3 and §4
association counts are reproduced by the gate-1 script (§8.1), whose output in `fgp_data/inventory/` is regenerated
in under a minute.

## 10. Revision history

**Revision 2 (after Codex round 1, REWORK)** — all factual corrections re-checked and folded in: coverage 68% not 56%;
`_G1` parsing bug (24,615 numbers / 695 multi-image dirs); 19,177 numbers = 22,378 rows; "identical" withdrawn; FIST
chain with all associations (7,954 multi-Alma; 31,439 / 6,694 manuscripts); credit heuristic disagreements; descriptions
already shipped; the 19 DB-only rows are in FIST; DOCX not plain; viewer transforms the `<img>`; `sections` are pages;
`dedupe_fgp_sources` / `choose_default_source` risks; identity model; per-page position payloads; landmark check;
phasing re-ordered with gates.

**Revision 3 (after Codex round 2, ACCEPT WITH CHANGES)** — §4.3 redone source-matched and line-aligned on 1,000 images
(the gershayim loss is 62 lines / 19+ documents, not "every difference"; catchword and wrap losses added; no reading
differences); the XML-by-image defect found and quantified (31 rows carry another edition's text, 3,099 unverified);
§6.1 restated as candidates with source-record linkage; 48 inventories / 164 associations; B1 5,308 numbers, 7 shared;
"1.2M word boxes" replaced by the census (879,400; `src` 98.7%); softened "every file", "DOC adds nothing", "subset of
FreeDesc"; 84 visibility-flag rows withheld; every §8 gate given an explicit failure condition.

**Revision 4 (after Codex round 3, ACCEPT WITH CHANGES)** — each correction re-verified and confirmed exactly: the 26
"other folder" XML rows are `_G` region folders, 23 correct and 3 carrying the G1 region's text, so the defect is
5 cross-edition + 3 wrong-region rows (not 31), with 3,099 unverified kept apart; the 8 deletions are 6 catchwords +
2 other short lines, and a 63rd truncation hides in a regrouped block (63 across 23 documents); the B1 census now
counts both box representations (1,074,196 real-text entries, 98.9% in `src`; dict-only numerator 867,846) and says
"entries, not unique words"; "all of the Sussmann Talmud material" bounded to the 4,171 delivered directories; §8.5
fails on the residual error after the transform and on undeclared limits.

**Revision 5 (gate 1 run, 2026-10-25)** — `scripts/fgp_association_inventory.py` reproduces the report's association
counts as declared expectations (53 / 53 checks). Two counting differences found and folded in: the §3.2 name-shape
table (the census pattern only matched a suffix followed by `.scan`; 31,986 basenames carry a suffix, 40,164 do not,
and every suffix agrees with the FIST kind), and 52 DOCX-only basenames the PDF census never saw (8 FIST
transcriptions, so the full totals are 69,338 / 54,536 / 68,871 / 7,957 / 31,440 / 6,695 and the absent FIST
transcription files are 72, not 80); 8 empty `ImageTranscriptionFolder` strings had been counted as a missing
directory (694, not 695). Statuses, unresolved reasons, visibility holds and the multi-association classes with their
proposed rules are in §8.1.

**Revision 6 (gate 2 run, 2026-10-25)** — `scripts/fgp_gate2_rules.py` gives each of the 45,034 rows an outcome under
the identity, credit, visibility and refresh rules of `FGP_IDENTITY_CREDIT_VISIBILITY_RULES.md` (7 checks + 29
expectations, PASS). Findings: today's rows are already one per (file, manuscript, image) and 44,975 of them carry a
`sys_id` the file's FIST chain reaches, so the FIST unit is found by basename + `sys_id`; 50 rows cannot stand (17
duplicates, 28 unreachable, 5 contradicted); 961 credits change once derived from FIST `SourceId`/`SubId` (932 book
rows the `image_id[3:7]` heuristic could not reach); 2,078 rows have no verified credit source (18 FIST sources, listed
for ruling); the 3,099 "unverified" XML rows are, for 3,073, Genuzos units whose image folder FIST assigns to another
Genuzos unit of the same manuscript. Gate 2 changes nothing in production.

**Revision 7 (credit ruling, 2026-10-25)** — the owner accepted the generic FGP attribution for rows whose FIST
source has no verified credit (`--credit-policy generic`, the gate's default; a per-source refinement from the live
FGP site is pending, checklist in the rules doc §6). Preparing that checklist showed that nine of the ten "book
`TitleId`s without a title" have English titles in `CODE_Title` that `build_book_map` never read; it now falls back
to them (+531 verified book credits: Gil's *Pious Foundations*, the India Book, Goitein's *Letters*, Mann, Ginzberg).
Publishable 41,807 (strict 40,285); 1,547 rows over 10 sources carry the generic credit.

**Revision 8 (live-site refinement, 2026-10-25)** — the owner checked three examples on FJMS and confirmed that the
single matching DataSource part names the contributor (BL OR 10578S.16 → Avi Shmidman; AIU III.B.109 → Emanuel
Friedberg's team line) and that T-S F5.49 is Simcha Emanuel's team (FIST source 111, now a verified team credit).
Gate 2 credits 1,087 rows from that part (`DATASOURCE_PART_TOKENS`, exactly one match required); 459 rows over 8
sources stay generic. T-S Misc.35.108: FJMS lists Abramson's edition and *Ginzei Qedem* (old) — the latter a Hebrew
translation in FIST — and nothing for the two rows on `TitleId` 6413, which have no title in FIST either.

**Revision 9 (FIST source semantics, 2026-10-25)** — the owner's screenshot of Or.1080 6.17 showed FJMS crediting a
bibliographic reference that our copy of the manuscript's DataSource lacks, which led to FIST's `CODE_Source` (names
for every source: 100 צוות אורחים, 300 רשימות אישיות, 601 הפניות ביבליוגרפיות …) and to the meaning of `SubId` per
source: for 601 it is a `dbo_UnitBibliographyReference` id, from which the gate now composes the credit FJMS shows
(all 11 rows); for 160 the journal only; for 300 a list id (51 Ben-Sasson, 53 unnamed); for 850 a user id outside
this FIST copy. Team 161 (Emanuel Friedberg) is a verified team credit for all 60 rows. 418 rows over 6 sources stay
generic, with the shelfmark to check on FJMS listed per source in the rules doc §6.

**Revision 10 (live-site lookups, 2026-10-25)** — eleven FJMS lookups through the owner's Chrome session read the
"Source:" line of each remaining generic source: source 850's `SubId` is a site-user id FJMS resolves to a name
(eight ids → 74 rows: Brody, Oren, Ber, Greenstein, Ilan, Katzman, Gershuni, Ventura), source 300's `SubId` 53 is
Milikowsky's personal handlist (6 rows), source 160's own source line on FJMS is empty (generic is what FJMS does),
source 240 is a demonstration-team entry, and every source-100 file checked (four manuscripts, BL and CUL) was Avi
Shmidman's — not applied by inference, since two JTS rows of that source name other users. 338 rows over 5 sources
remain generic; publishable 41,807 (strict 41,472).
