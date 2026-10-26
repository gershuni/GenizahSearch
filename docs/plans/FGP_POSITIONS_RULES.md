# FGP positions prototype (gate 5)

Gate 5 of [`FGP_SOURCE_FOLDERS_ASSESSMENT.md`](FGP_SOURCE_FOLDERS_ASSESSMENT.md) §8.5: the 20-image geometry
check, then the positions prototype. Script `scripts/fgp_gate5_positions.py`, wrapper
`scripts/run_fgp_gate5_positions.ps1` (two runs, one digest), tests `tests/test_fgp_gate5_positions.py` over the
position world of `tests/fgp_fixture_world.py`, expectations `scripts/fgp_gate5_positions.expect.json`. Inputs:
folder B (`C:\FGPTranscriptions_WithPOSInfoAndImages`), the gate-1 inventory (`b_dirs`, `b_numbers`), gate 3's
`text_rows` (which folder each candidate row's text was read from), gate 4's candidate sidecar
`fgp_transcriptions.candidate.db` (`durable_key`, `content`), `libraries.csv` (the library of a manuscript), the NLI
IIIF manifests and images the apps display. Outputs under `fgp_data/inventory/`: `positions_payload.db` and
`positions_relational.db` (the two candidate schemas), `gate5.db`, `gate5_summary.json`, `GATE5_REPORT.md`, and the
fetched manifests and images under `gate5_cache/` (reused by the second run; `--refetch` empties it).

**The tolerance (§2) and the limits (§4) were declared before the first run and never moved; an undeclared limit is
itself a fail (§8.5).** §1 and §3 state the transform family and the sampling as finally declared, each amendment
with its cause — every amendment came after a failing record run and before the next, and §7 keeps the sequence.
The gate fails (exit 1) when any check or declared expectation fails.

## 1. What is measured against what

The FGP positions live in a **frame**: for B1 the image the Talmud-project editor drew on (`coldata.imagesize`,
named by `alldata.box.fn` — the NLI photograph for 100000, but a derivative for region directories, 100446_G2's being
exactly half its master, and for the multi-image directories a **composite** (`ziruf`) of several photographs glued
in the editor — 76563 39617's is 2761×3641, 100022 100024 100038 120582's 11075×7743 — whose fragments may sit at any
angle, which no rigid transform of the whole frame describes); for B2 the FGP master **as FGP turned it** — the fjms
`PixelWidth` × `PixelHeight` through `b_numbers`, swapped when the recorded rotation is 90/270 or when the columns
fit only the swapped size (AIU I.A.48, C73736: columns reach x = 4842, y = 3226 of a 5428 × 7230 master, so the
editor's frame is 7230 × 5428; found in the first development run, corrected here before the record run) — with
the column's `x1`/`y1` added to the crop-relative row and segment boxes.
Region and multi-image directories are therefore **unresolved by rule** (`region_dir`, `composite_frame`): they get
no payload, and the joins pilot (§6.5) owns their geometry. A B2 image without a recorded pixel size has no frame
and is unresolved too.

The apps display, for the manuscript page whose gallery image (`nli_crossref.db`, `get_folio_images` order) is the
FGP image, the NLI IIIF rendition `https://iiif.nli.org.il/IIIFv21/FL<id>/full/2000,/0/default.jpg`, where the FL
id is the canvas at the same position in the manuscript's manifest
(`DOCID/PNX_MANUSCRIPTS<sys_id>-1/manifest`) — `web/api.py::_fetch_nli_image_bytes` on the web,
`genizah_app.py::_build_nli_iiif_url_for_page` on the desktop (its width varies; the residuals below are stated at
width 2000 and as a fraction of the row height, which is width-free). The gate fetches exactly that: the gallery of
the first AlmaId `b_numbers` lists for the image, the gallery index of the image, the manifest, the canvas at that
index, the 2000-wide rendition. The canvas must be **confirmed as this image's page**: the gallery and the manifest
hold the same number of images and the canvas label carries the gallery image's folio (`Frag. 001v` ↔ `1v`). When it
is not — Hamburg AS 13 (61897): 4 gallery images against 8 canvases, the canvas at the index another leaf; JNUL
577.4.24 (62519): canvases labelled `Page 1`–`4` in an order that is not the image names', the canvas at the index
the other side of the leaf — the apps' positional page mapping (the open per-folio alignment defect) shows another
photograph there, nothing about positions can be measured on it, and the image is `page_unconfirmed`: recorded with
its manuscript for the owner, and the stratum draws its next seeded image (§3). Recorded per image, not pass
criteria: whether the candidate row's `sys_id` is the gallery's AlmaId, whether the frame is the master.

The **transform** frame → canvas is a similarity: one of eight orientations — rotation 0 / 90 / 180 / 270 × mirror
no / yes — a uniform scale, a small angle and a translation, found in two stages by **normalised cross-correlation**
of the FGP photograph (`img.jpg`, whose place in the frame is the crop box), oriented and scaled, slid over the
displayed image, both grey and Gaussian high-passed, a display window with under 5 % of the template's variance
counting as no match, a template under 384 displayed px on a side or under 3 % of the display's area counting as
too small to trust. Stage 1, at 1/16 of the displayed scale (high-pass σ = 2, where a tilt of a degree costs about a
pixel): every orientation at 1/32-octave steps within 1.5 octaves either side of its dimension ratio (canvas size /
rotated frame size; 0.35× to 2.8× of it), candidates ranked by the peak's **prominence** — its height above the
highest value outside a 3-px neighbourhood (a true alignment stands alone; a small template's spurious peaks come
in crowds; a template as large as the display has one position and its peak counts whole); every orientation's most
prominent pair and the five most prominent overall go on, and so does every orientation's dimension-ratio scale whatever its
rank (a frame that is the canvas is the common case, and a tiny photograph of a few giant lines gives prominence
nothing to rank). Stage 2, at 1/4 (σ = 3), for each: the angle in whole degrees to ± 10°, then the scale in 4 steps of
1/64 octave either side, then the angle in quarter degrees to ± 1°, then the scale in 4 steps of 1/256 octave (a 1 %
scale error is 20 px at the edge of a 2000-px image: Strasbourg 4075.5, 92964, landed all 18 landmarks 25 px RMS off
at ×2.89). Stage 3: every
candidate — each seed as the sweep left it and as stage 2 refined it (a photograph of 293 × 757 px, BL Or 12299.25,
gives stage 2 only noise to refine on), best peak first — is tried against the landmarks of
§2 — with one translation correction, the median displacement of the found landmarks (ruled text gives the
correlation a peak one line pitch off: NLI 577.2.22, C62378, sixteen landmarks each 72–77 px away, the pitch 74.6),
after which the landmarks are searched afresh — and the candidate the words confirm (most found, then lowest RMS,
then highest peak) is the transform; the trial stops early when a candidate finds every landmark. Scale, angle and
orientation are never fitted to the landmarks. Recorded with every stage's peaks and every trial's landmark count.
**Recording a transform is not a pass**: the landmarks below decide, under the fixed tolerance — a spurious candidate
cannot pass by being chosen, it would need six real matches within a third of a line.

Why this family, and how it got here: the dimension ratio alone is the scale only when the NLI canvas is the FGP
master at another size (T-S AS 78.353, 100000: it is, exactly). The FGP master is often a crop cut from a larger
library photograph (Strasbourg 4075.9, C92956: 3150 × 3570 from 6500 × 10500; the AIU photographs sit on a grid board
with a ruler, their text crop at 1.34× of the dimension ratio), so the scale must be searched; FGP's recorded
rotations include small angles (−2°, 4°, 182°) and the NLI 577.2.22 photograph (C62378) is visibly tilted against
its master, so the angle must be searched; the first attempt's phase correlation lost the true peak (0.02 against
0.015 noise) under the AIU grid boards and a half-off-canvas placement, so the correlation is normalised; and at 1/4
scale a one-degree tilt already halves the true peak (AIU IV.C.220, C80459: 0.077 at its scale and orientation
against 0.16 for a tiny spurious template) while at 1/16 it stands clear (0.368 against 0.239), so the sweep runs
coarse and the angle is searched before the scale is refined; and on Vienna H 137 (93021), a tall thin fragment
whose template fills the whole display and whose NLI rendition has a black background where the FGP photograph has
a grid, the correlation peak at the true scale (0.14) lost to a spurious mirrored candidate, while the words matched
at 0.8–0.9 — so the landmarks, which are the signal the gate is about, choose among the refined candidates; and on
the same image a template that fills the whole display loses its peak to a 4 % scale step, so the sweep runs in
1/32-octave steps (smoothing the sweep images was tried and dropped: it inflates the correlation of small spurious
templates). The
first development run used the dimension ratio and phase correlation only; the scale search was added after it; the
record runs of 09:48 (10 of 20 PASS) and 10:50 (15 of 20) on 2026-10-26 showed the remaining weaknesses one by one,
and the family above is the one the record run of §7 used. The tolerance in §2 never moved.

## 2. The landmark tolerance (declared)

Let `h` be the image's line pitch in displayed pixels: the median spacing of consecutive row baselines (B1
`rows[].pnts`, B2 `rows[].xy`, first point), falling back to the median row-box height when the column has one row;
scaled by the transform.

- **Candidates**: positioned boxes whose text carries at least one letter (Hebrew, Arabic or Latin — a lacuna run of
  dots or a bare siglum is positioned in the store but has no ink to match; the first development run on 100000, a
  badly damaged fragment, chose dot-runs and found nothing) — B1 `box.0` of a `<w>` with text; B2 segment boxes whose
  `txt` is non-blank, dropping the editor artefacts (`id` ending in `_test`, `x1 < 0`; the `split(ROW)` boxes are the
  words themselves — the assessment's §4.2 listed the suffix among the artefacts, which the first development run
  disproved: 304 of C100181's 440 boxes carry it, every `_test` box is blank). The text region's
  bounding box is cut into a 3 × 3 grid; per cell up to 2 boxes — words of two or more letters before single letters
  (a lone נ on AIU IV.C.144 matched its twin two words away, 31 px off, in the first record run), then by the
  variance of their patch in the FGP photograph; at least 8 landmarks are attempted when the image has them.
- **Patch**: the box enlarged to a square of side `max(1.2 h, 96 displayed px)` — never more than a third of the
  display's shorter side (BL Or 12299.25, 301957: four lines 1,549 px apart on a 2,967-px frame) — centred on it, cut
  from the FGP photograph. Matching runs at the **coarser** of the two resolutions (the B2 `img.jpg` is at 0.16 of the master):
  both patch and search window are brought to that scale, grey, high-passed, and compared by normalised
  cross-correlation inside a window of ± 1.0 h (never more than a sixth of the display's shorter side) around the
  predicted centre.
- **Found**: NCC peak ≥ 0.45. The image needs ≥ 6 found landmarks in ≥ 4 distinct grid cells spanning at least two
  grid rows and two grid columns.
- **Window widened, once**: a trial that finds fewer than three landmarks looks once more with the window at a third
  of the display's shorter side (the registration of a tiny photograph can be off by more than a line).
- **Translation correction, once**: when three or more landmarks are found, the median of their displacements
  (found − predicted) is added to the transform's translation and the landmarks are searched again from the
  corrected prediction. Ruled text gives the coarse correlation a peak one line pitch off (NLI 577.2.22, C62378:
  sixteen landmarks found, each 72–77 px away, the pitch 74.6 px), which a translation set by the words themselves
  catches; the scale, angle and orientation are never touched by it, and the residuals below are measured against
  the corrected transform with landmarks found afresh. Recorded as `translation_corrected` with the correction in
  displayed px.
- **Residual** `r_i` = distance between the predicted centre (the transform applied to the box centre) and the found
  centre, in displayed pixels at width 2000.
- **PASS** iff found ≥ 6 (as above), `max r_i ≤ 0.35 h` and `RMS r_i ≤ 0.20 h`. Anything else — too few landmarks, a
  candidate transform that lands a word on the next line, a different photograph — is a FAIL for that image, and
  check G1 fails the gate. Nothing is retried with another tolerance.

Why these numbers: a word box moved by a third of a row height still lies on its own line; half a row height puts it
in the interline gap; the RMS bound keeps a transform whose scale is slightly off from passing on the centre rows
alone. The 0.45 floor is above the spurious peaks a text-textured window gives at the coarse scale.

## 3. The 20 images (declared sampling)

The §4.5 hand list is replaced by a seeded sample so the choice is reproducible and not ours. Frame F = the
`b_numbers` rows whose number has a position directory with geometry (B1 `has_js`, B2 `has_data_js`) that is
**neither a region nor a multi-image directory** (§1: their frames are a region image or a composite, unresolved by
rule; check G2 verifies over the whole build that they get no payload), with an NLI gallery row (`nli_alma_ids`), with
a frame (a B2-only image needs its fjms pixel size — Vienna H 118, C73133, has none and fell out of the first record
run as `no_geometry`), and with at least one candidate row whose `c_number` is the image. Library = the `libraries.csv`
`library_code` of the first gallery AlmaId. Rotation class from `rotation_deg`: `small` (null, or within ± 10° of 0 /
360), `180` (± 10° of 180), `90/270` (± 10° of 90 or 270), `other`.

`random.Random(20261026)`; libraries are visited in a seeded shuffle, round-robin, one image per library per round
(`rng.choice` over the sorted eligible numbers), so no library fills a stratum:

| stratum | quota | pool |
|---|---|---|
| A | 7 | B1 (incl. images in both formats), `small` |
| B | 3 | B1, large rotation: one from `180`, one from `90/270`, one from any large class |
| C | 6 | B2, `small`; the first draw from the two-column directories (`png_count ≥ 2`) |
| D | 4 | B2, large rotation: two from `180`, two from `90/270` (an empty pool falls back to `other`) |

A pool with fewer images than its quota is drawn in full and the shortfall is reported, not refilled. Each stratum
draws 10 further images in the same order as a **reserve**: when NLI does not serve a drawn image after five
attempts (503 on FL201910354, FL202604859 and FL196670395 through the first record run — the apps show nothing or a
Rosetta thumbnail for such a page), there is no displayed image to check; when the canvas is not confirmed as the
image's page (§1), there is nothing of this image to check either. The image is recorded as `undisplayable` with its
reason and the stratum's next seeded draw takes its place; check `G1_sample_size` fails when the quota is not measured.
The first twenty of the draw are the same with or without the reserve. `--images` replaces the sample (tests and
reruns of one image), and the record says so.

## 4. The positions store: two schemas, declared limits

Scope of the build: every position directory of F's numbers (B1 columns by `coldata`, B2 columns by `cols`), i.e.
every image a candidate row from folder B can show. Per (image, column) the geometry is parsed once and tied to a row
by §5; the two schemas hold the same tied content.

- **Schema P** (`positions_payload.db`, table `positions(durable_key, text_hash, image, col, frame_w, frame_h,
  transform_status, overlay_allowed, payload BLOB)`, index on `(durable_key, text_hash)`): one zlib (level 6) JSON
  payload per (row, image, column) — frame, frame file, crop box, rows (band box, 4-point baseline), words
  (`[start, end, row_no, x, y, w, h]` in frame pixels), transform (or null).
- **Schema R** (`positions_relational.db`: `position_images(pk, durable_key, text_hash, image, col, frame_w,
  frame_h, crop…, transform_json, transform_status, overlay_allowed)`, `position_rows(image_pk, row_no, x, y, w, h,
  baseline_json)`, `position_words(image_pk, row_no, start, end, x, y, w, h)`, indexes on `(durable_key, text_hash)`
  and `image_pk`).

| limit | P | R | why |
|---|---|---|---|
| installed size | ≤ 512 MB | ≤ 512 MB | an optional desktop download beside the 405 MB text sidecar; the deploy copies it |
| largest payload (one (image, column) read) | largest BLOB ≤ 1 MB | JSON length of the rows one read returns ≤ 1 MB | one page turn must not pull megabytes |
| cold read latency | p95 ≤ 25 ms, max ≤ 200 ms | same | a fresh `mode=ro` connection per read, after the build (the OS cache is as it is), one (row, image, column) lookup + decode, over the 20 images and 200 seeded others |
| build peak working set | ≤ 2,048 MB | — | one streaming pass over 3.25 GB of JS |

**Choice rule**: the schema with the smaller installed size among those within every limit; P on a tie. If neither
is within its limits the gate fails and the owner decides the scope cut (§8.5). Build time is reported, not limited.

## 5. Tying an overlay to the selected edition's revision

An overlay may exist only for a **resolved** (row, image, column): the candidate row's `c_number` is the image, gate
3 read the row's text from this very directory (`text_rows.source_path`), and `sha256(candidate.content)` equals the
hash of the text re-read from the directory by the same reader (`parse_fgp_xml` / `read_b2`), i.e. the sidecar still
carries this revision. The payload is keyed by `(durable_key, text_hash)`, so a corrected text (a new revision) finds
no payload and shows nothing until positions are rebuilt. Spans are offsets into `content`: a span is kept only when
`content[start:end]` equals the word's (B1) or segment's (B2) text; the rest are dropped and counted. A directory
whose text cannot be reproduced with offsets, whose hash differs from every candidate row, or whose rows carry no
candidate, is unresolved with a reason (`no_candidate_row`, `hash_mismatch`, `text_not_reproduced`, `no_frame`,
`parse_error`, `region_dir`, `composite_frame`) and gets no payload. `transform_status` is `verified` for the images G1 passed and
`unchecked` elsewhere; **`overlay_allowed = 1` only when verified** — the viewer contract this prototype proposes
(the owner rules whether an unchecked image may show its overlay under an identity transform; nothing here assumes
it).

## 6. Checks

| check | fails when |
|---|---|
| G1 geometry | any of the 20 measured images is not a PASS under §2; `G1_sample_size`: fewer than the quota were measured |
| G2 no overlay without a tie | a payload exists whose `(durable_key, text_hash)` is not a candidate row with that content hash; a payload exists for an unresolved directory; a region or multi-image directory has a payload; `overlay_allowed = 1` on an image G1 did not pass |
| G3 spans | reading the stores back, any span's text differs from `content[start:end]` |
| G4 two runs | the digest (the 20 images' transforms and residuals, every payload's key and BLOB hash, every unresolved reason) differs from the previous run's |
| G5 size | the chosen schema's installed size is over its limit, or neither schema is within every limit |
| G6 payload | the chosen schema's largest payload is over its limit |
| G7 latency | the chosen schema's cold read p95 or max is over its limit |
| G8 memory | the build's peak working set is over its limit |
| G9 schemas agree | the two stores differ in resolved keys, word counts or span texts |
| E_* | a declared expectation is missed |

The tests prove the gate fails on a displayed image that is another photograph (G1), on a payload whose text hash no
candidate carries (G2), on a changed digest (G4) and on a missed expectation.

## 7. Run record

**2026-10-26 11:52 — FAIL on G1: 17 of the 20 measured images PASS; G1_sample_size and G2–G9 PASS; 39 counts
declared from this run (`scripts/fgp_gate5_positions.expect.json`); run 2 (§7.1) reproduced the digest
`77df7365b12e4973` and every expectation.** 69 minutes per run (about a minute per registration, 112 s for the
build), the candidate sidecar of 11:15 (50,681 rows). Three record attempts that day, each with the method as
declared at the time: 09:48 (dimension-ratio scale, phase correlation, no angle: 10 of 20), 10:50 (normalised
correlation, scale search, angle, three stage-2 seeds: 15 of 20; gate 4 rebuilt the candidate sidecar during the
run, which the one-pass read now rules out), 11:52 (the family of §1). The tolerance of §2 and the limits of §4
never moved; every amendment and its cause is in §1–§3.

The draw: 30 images drawn, 20 measured, 10 given way — 5 that NLI did not serve after five attempts (Manchester
435874 FL201910354 and 440380 FL201912706, Mosseri 451348 FL202604859, Geneva 69898 FL202844522 and 69694
FL196670395: 503 every time), 4 whose canvas is not confirmed as the image's page (Hamburg AS 13 61897: 4 gallery
images, 8 canvases; JNUL 577.4.24 62519 and 577.4.26 62526: canvases labelled `Page n`; AIU 77095), 1 whose
manifest is shorter than the gallery (Katz 67839, `canvas_missing`).

| image | stratum | library | frame | canvas | transform | h | landmarks | max r | RMS | result |
|---|---|---|---|---|---|---|---|---|---|---|
| 116367 | A | CUL | 3712×1528 | 3684×1552 | 0°, ×0.990 | 62.9 | 15/15, 8 cells | 2.1 | 1.2 | PASS |
| 192869 | A | Munich | 2958×2004 | 3944×2677 | 0°, ×1.330 | 90.0 | 18/18, 9 | 2.2 | 1.4 | PASS |
| 249538 | A | Schocken | 2701×2021 | 2380×1811 | 0°, ×0.881 | 73.3 | 18/18, 9 | 0.8 | 0.6 | PASS |
| 299157 | A | BL | 4477×5532 | 4524×5610 | 0°, ×1.013 | 87.8 | 18/18, 9 | 3.0 | 1.5 | PASS |
| 72470 | A | Strasbourg | 5428×7230 | 5428×7230 | 0°, ×1 | 60.8 | 18/18, 9 | 0.7 | 0.4 | PASS |
| 87917 | A | HUC | 1948×2636 | 5412×7216 | 0°, ×1.938 | 70.9 | 18/18, 9 | 3.7 | 2.0 | PASS |
| 93021 | A | Vienna | 3570×3990 | 4958×11731 | 0°, ×2.996, +0.75° | 68.3 | 13/18, 7 | 19.6 | 10.7 | PASS |
| 111895 | B | CUL | 2790×2985 | 2745×2870 | 0°, ×0.984 | 101.1 | 12/13, 7 | 0.6 | 0.4 | PASS |
| 301905 | B | BL | 3500×4645 | 3493×4523 | 180°, ×0.998 | 86.3 | 18/18, 9 | 0.7 | 0.3 | PASS |
| 73499 | B | Vienna | 7216×5428 | 5428×7230 | 270°, ×1 | 77.0 | 18/18, 9 | 0.6 | 0.4 | PASS |
| 13185 | C | JTS | 4984×6658 | 4984×6658 | none found | — | 0/18 | — | — | FAIL too_few_landmarks |
| 296844 | C | BL | 2865×3679 | 2871×3650 | 0°, ×1.002 | 107.5 | 18/18, 9 | 3.9 | 1.6 | PASS |
| 61457 | C | HAS | 3579×4661 | 3600×4693 | 0°, ×1.006 | 70.7 | 18/18, 9 | 2.9 | 2.5 | PASS |
| 68732 | C | Katz | 2624×3554 | 2624×3554 | 0°, ×1 | 156.3 | 18/18, 9 | 0.5 | 0.3 | PASS |
| 92964 | C | Strasbourg | 3150×3570 | 5900×9115 | 0°, ×2.889 | 96.0 | 18/18, 9 | 41.4 | 20.7 | FAIL max_residual |
| 92988 | C | Vienna | 4200×3570 | 13641×9319 | 0°, ×2.587, −0.75° | 25.8 | 18/18, 9 | 2.7 | 1.5 | PASS |
| 197693 | D | CUL | 3365×3143 | 3342×3126 | 0°, ×0.993 | 137.0 | 14/16, 8 | 0.3 | 0.2 | PASS |
| 301957 | D | BL | 2967×5692 (swapped) | 5671×2987 | 270°, ×0.996 | 544.3 | 5/8, 4 | 1.6 | 0.8 | FAIL too_few_landmarks |
| 79576 | D | AIU | 7230×5428 (swapped) | 5428×7230 | 90°, ×1.325 | 65.4 | 18/18, 9 | 2.3 | 2.1 | PASS |
| 92956 | D | Strasbourg | 3150×3570 | 6500×10500 | 180°, ×2.856 | 66.8 | 18/18, 9 | 4.0 | 3.1 | PASS |

(h, max r, RMS in displayed px at width 2000; a scale ≠ 1 with a frame that is the master means the NLI canvas is
the master at another size or a larger photograph the master was cut from.) On the 17 passing images 288 landmarks
were found, mean residual 1.5 px, 16 of the 17 under 4.1 px at the worst landmark; the scale was searched on 10,
the angle is non-zero on 2 (Vienna 93021 and 92988, ±0.75°), the frame swapped on 2 (AIU 79576, BL 301957), the
translation corrected on 15 (by 1–14 px; CUL 197693 by 216 px after the widened window; BL 301957 by 100 px),
the frame is the fjms master on 17 of 20. Recorded rotations and found orientations agree where the canvas is the
master (301905: 179° recorded, 180° found; 73499: 272°, 270°; 79576: 90°, 90°; 197693: 180° recorded, 0° found —
the NLI canvas is already turned).

The three failures, each a finding rather than a tolerance miss by a hair:

- **JTS ENA 2017.8 verso (13185)**: the canvas is label-confirmed as the page, and it is another capture of it: no
  similarity transform of the FGP master correlates (best stage-1 prominence 0.014; at the most plausible candidate
  the words match at NCC 0.1–0.2). The overlay cannot be shown on NLI's photograph of this page.
- **Strasbourg 4075.5 recto (92964)**: the transform is found (×2.889, 18 of 18 landmarks); 13 landmarks sit within
  5 px, but the 5 landmarks of the first two lines sit 40–47 px (0.42–0.5 h) to the left of their words — a per-row
  offset in FGP's geometry for those rows (or a local deformation of one of the photographs), which the rule `max r
  ≤ 0.35 h` is there to catch.
- **BL Or 12299.25 (301957)**: four lines of giant letters, eight positioned words; the transform (270°, ×0.996)
  lands 5 of the 8 within 1.6 px (RMS 0.8 px), but three short words on the 293 × 757-px photograph fall under the
  0.45 floor — FAIL under the declared minimum of six landmarks, which this image cannot reach.

The store (both schemas built over the 19,659 directories of the frame; `positions_payload.db` chosen):

| | count |
|---|---|
| directories resolved (text reproduced with offsets, hash equal to a candidate row) | 19,608 (3,358 B1, 16,250 B2) |
| unresolved: `hash_mismatch` (the candidate row's text is not the folder's) | 46 (6 B1, 40 B2) |
| unresolved: `no_candidate_row` (C134410, C192906–C192909: the five images with both a B1 and a B2 directory; the B2 directory has no row of its own) | 5 |
| unresolved by rule: `region_dir` | 2 |
| payloads (row, image, column) | 22,319 — 4,040 B1 columns, 18,279 B2 — on 19,608 images, 19,608 rows |
| positioned words / rows | 2,275,222 (B1 933,132; B2 1,342,090) / 318,772 |
| spans dropped (B2 segments whose text the line does not hold) | 2,157 |
| `overlay_allowed` | 19 payloads: the 17 passing images' columns |

| measure | P | R | limit |
|---|---|---|---|
| installed size | 53.3 MB | 136.4 MB | 512 MB |
| largest payload | 15.9 KB (1,158 words) | 68 KB | 1 MB |
| cold read, 223 reads | p50 0.43 · p95 0.69 · max 1.76 ms | p50 0.69 · p95 1.22 · max 2.61 ms | p95 25 ms, max 200 ms |
| build peak working set | 865 MB | | 2,048 MB |

Open for the owner:

1. The verdict. G1 fails on three images of twenty, two of them by findings the tolerance was written to catch and
   one by a capture NLI replaced; the store's contract (`overlay_allowed` only on verified images) already keeps the
   overlay off all three. Whether the gate's pass condition should stand as declared, exclude pages NLI has
   re-photographed (feature-based registration would be the next method for those), or carry a smaller landmark
   minimum for images with fewer than ten positioned words, is a ruling.
2. The 19,591 unchecked images: at a minute per registration the whole frame is two weeks of compute; a sampled
   acceptance per library (the residuals on the 17 cluster by library: CUL, BL, Strasbourg, Vienna, HAS, Katz,
   Munich, Schocken, HUC, AIU all under 4 px except Vienna 93021 at 19.6) or a build-time check are the options.
3. Page confirmation: 4 of the 30 drawn images sit on manuscripts whose manifest cannot be aligned to the gallery by
   folio label; the apps show another photograph on those pages today (the open per-folio alignment row), and the
   overlay contract should require a confirmed page.
4. NLI answered 503 for 5 of the 30 drawn FL ids through five attempts over a minute; the apps fall back to a Rosetta
   thumbnail there.
5. The 46 `hash_mismatch` directories: gate 3 held these rows (21 with lines the folder has and today's text lacks,
   18 with an unclassified difference, 7 with typed sigla), so the candidate carries today's PDF-route text while the
   positions belong to the folder's; nothing can be tied until the folder text is the edition shown or the rows are
   approved.
6. The per-row offset on Strasbourg 4075.5 is worth a look in FGP's editor data before positions ship for that
   library.

### 7.1 Run 2

13:05 the same day, from the same cache and candidate sidecar (built 11:15), with `--previous-summary` run 1's and
the 39 expectations declared from it: digest `77df7365b12e4973` reproduced, every expectation met, the same three
images failing G1 — **48 of 49 checks** (G1 alone), 68 minutes. The record run's outputs are the ones under
`fgp_data/inventory/` (run 1's under `gate5_run1/`); the wrapper stops after a failing run 1, so run 2 was started
by hand with the wrapper's own arguments. Cold read latency (not in the digest) measured p95 1.7 ms / max 3.5 ms on
run 2 against 0.7 / 1.8 on run 1, both two orders under the limit.
