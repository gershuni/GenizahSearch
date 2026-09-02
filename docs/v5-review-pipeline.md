# How the identifications were made — the algorithms and the full pipeline

*Companion to `v5-review-quickstart.md`. That file tells you how to open the
database and what each column means; this one tells you how the rows came to
exist: what was compared against what, by which rule, with which thresholds,
and what each stage was measured to do.*

**How this document was written.** Every rule, constant and count below was
read from the code that ran or from the run's own report or the database's
`meta` table, and the source is named beside it (a script path, or a `meta`
key). Where something could not be established from the code it is said so.
Paths are relative to the project repository; the `same_work_spike/` tree is
the research workspace that is not part of the public repository but is the
code that actually produced these matches. Two reference corpora are
restricted and appear here only under their codenames, **M-source** and
**R-source**, exactly as they appear in the database.

The database was built between 2026-07-10 (the corpus and the first matching
run) and 2026-09-02 (the final packaging). Run dates are given where they
matter.

---

## 0. The shape of the whole thing

The question the pipeline answers, once per manuscript page and reference
work, is: *does the text on this page reproduce text from this work, and if so
where, how much, and is it a copy of the work or a quotation of it?*

```
 Transcriptions.txt ──► pages (667,411) ──► search text per page ──┐
   (HTR corpus)          §1.1               (HTR, or a human        │
                                            transcription, §1.2)    │
                                                                    ▼
 reference works ──► letter streams ──► k-gram index ──► TRACK-1 MATCHER  §4
 (4 corpora, §3)        (§2)             (masked, §3.6)   seed → diagonal → extend
                                                          → verify → tier A / B
                                                                    │
                                                                    ▼
                         CLAIMS  §5: shadowing on the page, chronology, the
                         coverage router (copy vs quotation), routing_status,
                         main pool; labels: novelty, adjudication, scripture,
                         formula
                                                                    │
                                                                    ▼
                         REVIEW DATABASE  §7: both texts, both file addresses,
                         one row per alignment (519,382); two halves merged
                                                                    │
                                    §8 identities and cards ◄───────┤
                                    §9 HTR re-alignment      ◄───────┤
                                    §10 viewer, Access, gate ◄───────┘
```

The database is the union of **two independently rendered halves**: the
**base** half (Sefaria, JA and M-source works, 292,703 rows, rendered from the
V4.2 "lit" build artifact `discovery-v42lit.db`) and the **R-source** half
(226,679 rows, rendered from the gen-2 `g_r` run through its adapter
`gr-adapter.db`). Both were rendered by the same script and merged by a
merge that refuses to publish on any inconsistency (§7). Source: `meta` keys
`base.built_from_artifact`, `rsource.built_from_artifact`, `merged_from`.

| Headline number | Value | Source |
|---|---|---|
| pages in the corpus | 667,411 | `fullcorpus_extract_report.txt` |
| pages searched as a human transcription rather than HTR | 18,982 (fgp 14,932 / pgp 4,050) | `stage0_report.md` |
| reference works in the base index | 1,352 source files, 1,353 witnesses | `meta base.source_files`, `base.reference_witnesses` |
| R-source works matched | 344 files (of 354 ingested, 1,679 in the corpus) | `meta rsource.source_files`; `rsource/REPORT.md` |
| evidence rows in this file | 519,382 (base 292,703 + R-source 226,679) | `meta rows`, `base.rows`, `rsource.rows` |
| cards (one per page and identity) | 433,911 over 211,510 pages, 55,383 manuscripts | `meta card_grain.*` |
| identities (known works) | 1,447, from 1,736 corpus-work members | `meta work_registry.*` |
| rows with a manuscript-side address in `Transcriptions.txt` | 493,116 direct + 26,180 by re-alignment; 86 withheld (NFC) | `review_row.ms_provenance_status`, `htr_realign.*` |
| rows with a reference-side file address | 519,104; 278 fall back to a letter stream | `review_row.ref_provenance_status` |

---

## 1. The manuscript side

### 1.1 From `Transcriptions.txt` to pages

`Transcriptions.txt` is the HTR corpus: 1.47 GB, 948,549 records, each a
header line `==> {page_id} <==` followed by the page's transcription lines.
A `page_id` has the shape `{system number}_{IE id}_{P folio}_{FL image}`, for
example `990000412990205171_IE104549337_P000002_FL104549340`; the system
number before the first underscore is the manuscript.

`same_work_spike/probe/scripts/extract_full.py` streams the file once,
reconstructs each page as its lines joined by newline and stripped, and
applies three filters from `stage0.py` before keeping a page:

| Filter | Rule | Dropped |
|---|---|---|
| short | fewer than 80 Hebrew letters (`MIN_STREAM_LETTERS`, `stage0.py:35`) | 231,679 |
| target sheet | 4 or more of 5 microfilm target-card template words, or 3 with under 400 letters (`stage0.py:56`) | 9,007 |
| duplicate photograph | the same `FL` image id already seen (`fl_of`) | 40,452 |

948,549 − 231,679 − 9,007 − 40,452 = **667,411 pages** kept
(`fullcorpus_extract_report.txt`). A library-stamp filter exists in the same
function but caught nothing in this run. Two further hygiene tiers the
`stage0.py` docstring names (same-shelfmark duplicates, line-break agreement)
are not applied at extraction; they need cross-page comparison and are left to
later stages.

### 1.2 The search text: HTR, or a human transcription

Before matching, `same_work_spike/probe/scripts/mapv2_stage0.py` (run
2026-07-10, 40.7 minutes) replaced a page's HTR text with a **human
transcription** where one existed and passed a gate: FGP digital editions
(`doc_relation='Digital Edition'`; translations excluded) and PGP documentary
transcriptions (longer than 100 characters, tied to a manuscript through the
fragment table). The intent, stated in the script's docstring, is that every
downstream measurement should read the best available text, and that a
partial human transcription must never replace a fuller HTR.

The gate, per candidate transcription and page (`gate_candidate`,
`mapv2_stage0.py:248-283`; constants at `:92-113`):

1. Both letter streams must have at least **200 letters** (`MIN_LEN`).
2. A cheap 5-gram containment pre-filter skips pairs that cannot reach the
   score floor (`GRAM_PRE_MIN = 0.08`).
3. Length ratio (transcription ÷ HTR): above **12.0** rejected outright
   (`RATIO_MAX`); below **0.70** rejected as partial without aligning
   (`PREFILTER_MIN`).
4. `rapidfuzz.partial_ratio_alignment` score must be at least **70.0**
   (`MIN_SCORE`).
5. If the transcription is **shorter** than the HTR, the aligned window must
   cover at least **80 %** of the HTR stream (`COVER_MIN`), else
   `partial_coverage`.
6. If the transcription is **longer** than 1.3 × the HTR (`WINDOW_RATIO`,
   the multi-page case), only the aligned window plus 10 letters of padding
   each side is stored, so a page never carries another page's text.
7. Within a manuscript, passing pairs are assigned **greedily one-to-one by
   score**, so each transcription substitutes at most one page and each
   page receives at most one transcription.

Outcome (`same_work_spike/probe/results/stage0_report.md`): **18,982 pages
substituted** (14,932 FGP, 4,050 PGP); 5,419 windows cropped. Among the pages
of manuscripts that had a candidate but were *not* substituted: 8,731 too
short, 8,617 with no usable candidate, **10,033 rejected as partial**, 3,873
by the ratio cap, 37,623 below the score floor, 2,763 lost the one-to-one
assignment. Scores of the substitutions: 2,642 in 70–79, 6,731 in 80–89,
9,598 in 90–99, 11 at 100.

**The HTR text of every substituted page still stands in
`Transcriptions.txt`.** The substitution changed what the matcher *searched*;
it deleted nothing. All 18,982 pages are present in the file with their
recorded length: `scripts/attach_htr_realignment.py` checks both, and pins the
file's size and modification time to the offsets index. (A one-off
byte-for-byte comparison against the untouched v1 corpus during the
2026-09-01 audit found no difference; that comparison is not part of the
shipped code.) This is why the affected rows carry two manuscript-side
addresses (§9).

**What the gate was audited to miss.** An independent audit the same day
(`results/agent_stage0_audit.md`, verdict "GATE-LEAKY") showed that the
coverage test measures the *span* of the aligned window, not the letters that
actually match, and is checked independently of the score; a constructed
human draft that skips the middle 20 % of a page, or covers 82 % with 30 %
internal divergence, passes while preserving about 57 % of the page. A
recompute against the untouched HTR (`results/mapv2_substitution_risk.md`,
2026-07-11) then measured the real damage: of the 18,982 substitutions,
15,420 have a human text at least as long as the HTR and 3,562 a shorter one;
**no page lost half or more of its HTR content**; 152 of the shorter-text
pages and 57 of the longer-text pages kept only 50–70 % of it; 595 pages were
flagged risky by any of three criteria (span coverage < 0.85, score < 75, or
coverage × score < 0.75). The median shorter-text substitution keeps 90.3 % of
the HTR letters. The gate itself was not changed after the audit.

### 1.3 The character-offset index

`scripts/index_transcriptions_offsets.py` streams `Transcriptions.txt` a
second time and records, for every page whose search text is the HTR, the
character span of its text in the file. The coordinate contract is fixed and
stored in the index's `meta`: the file is decoded as UTF-8 with
`errors='replace'` in Python text mode (universal newlines), and **offsets are
character indices into that decoded stream, not bytes**. The reconstruction
rule (`"\n".join(lines).strip()`) is the same one that built the pages, so the
recorded slice *is* the page text; the pass compares them anyway and fails on
any mismatch. Result (index `meta`): 648,429 HTR pages indexed, 0 mismatched,
0 missing, 18,982 skipped by design because their search text is not in this
file.

---

## 2. The letter stream

Everything is compared as a **space-free stream of Hebrew base letters**
(`same_work_spike/probe/scripts/normalize.py::norm_stream`, lines 27–45):

- NFC-normalize;
- keep only code points in the Hebrew letter block א–ת (U+05D0–U+05EA);
  every vowel point, cantillation mark, punctuation sign, digit, Latin letter
  and space is dropped;
- fold final forms to their medial letters (ך→כ, ם→מ, ן→נ, ף→פ, ץ→צ);
- keep a parallel array of offsets, one per surviving letter, pointing back to
  its position in the NFC text, so any span in the stream can be projected
  onto the readable text and, from there, onto the file.

Spaces are dropped because HTR word segmentation is unreliable. The same
function normalizes manuscript pages and reference works, so a letter offset
means the same thing on both sides. Because a Genizah page is compared with a
printed edition, matching text typically differs by about 0.4 edits per
character (orthography, abbreviations, real variants); the thresholds in §4
are set against that noise.

---

## 3. The reference side

### 3.1 Four corpora

| Codename in the database | What it is | How it entered |
|---|---|---|
| `sefaria` | the *display* label for Bible, Targum, Mishnah, Talmud and other public works | Sefaria itself supplied the matching index with Targum, a liturgy set used for masking only, and named gap works, staged with license and structure maps (§3.3); the Bible and rabbinic texts the matcher indexed came from M-source (§3.2, §3.5). Of the 233,292 rows carrying this label, 164,235 are addressed against an M-source file, 41,506 against a Sefaria staging file and 27,273 against a V4 JSON re-projection file (§7.3) |
| `ja` | a Judeo-Arabic corpus, one text per document | files read directly, 150-letter floor |
| `msource` (M-source) | a restricted classical Hebrew corpus | files read with header stripping (§3.2) |
| `rsource` (R-source) | a restricted rabbinic library, 1,679 works | its own eligibility, dedup and masking track (§3.7) |

The first three form the base reference (`ref_corpus_v2.pkl`, 171 MB at run
time); R-source was added as two extra index shards.

### 3.2 M-source and JA ingestion

`same_work_spike/probe/scripts/track1_build_ref.py` reads each M-source text,
strips its `##…##` header blocks *before* normalizing so that division labels
and provenance notes never enter the matching stream, skips works whose stream
is under **150 letters** (`MIN_LETTERS`, `:32`), and records author, title,
date and genre from the file name. The header regex
(`##(?:[^#\n]|#(?!#))*##`, `:24-28`) replaced a naive one that had deleted
30,677 letters from one work and leaked 342 header letters across sixteen
other short works (`results/ref_header_bug.md`). JA files carry no such
headers and are normalized as they are.

### 3.3 Sefaria texts

`ref1_fetch_sefaria.py` picks, for each text, the Hebrew version with the most
permissive license (public domain, then CC0, CC-BY, CC-BY-SA; non-commercial
and no-derivative licenses are quarantined), strips publisher rubrics and
markup, and stores a body plus a structure map (`*.versemap.json`) that records
each verse's or section's character span, so a match can later be cited by
chapter and verse. `ref2_build.py` admits a staged work into the reference
only if its stream has at least **200 letters** and it is not a duplicate
(§3.4). Whole Tanakh books and rabbinic tractates fetched by `ref3` and `ref4`
are **display re-projections only**, for showing an identification against a
republishable edition; their docstrings state they never enter the matching
index.

### 3.4 Duplicates and version twins

Two works that are the same text would otherwise compete for the same page.
`ref2_build.py` (`:49-52`, `:102-235`) compares each new work with every
existing one by **5-gram containment** (shared distinct 5-grams ÷ the smaller
gram set): at or above **0.98** the new work is a duplicate and is dropped;
between **0.85 and 0.98** the two are "version twins", both kept but joined
into one version group. A containment hit against a Bible work is never a
twin (the "same-kind gate"): a liturgy unit containing Hallel is quotation,
not identity.

### 3.5 Canonical monoliths split into divisions

Three M-source works are whole canonical corpora in one file (39, 63 and 61
divisions). `split_canon_works.py` splits each into per-division works using
the `##division…##` headings, and refuses to write anything unless the
divisions' streams concatenate back to the stored parent stream byte for
byte, the lengths sum exactly, the division count matches the hand-verified
number, and every child is non-empty and uniquely named (`:83-168`). This is
why a match can name a tractate or a biblical book rather than "the Bible".

### 3.6 Masking quoted scripture inside reference works

A commentary quotes the verse it comments on; a legal code quotes the
Talmud. Left alone, the matcher would identify such a work on any page that
merely carries the quoted canonical text. `mask_ref_canon.py` therefore
marks, inside every non-canonical reference work, the spans that are
themselves Bible, Mishnah, Tosefta, Bavli or Yerushalmi text, and the index
drops every k-gram that overlaps a masked span. **A work is identifiable only
through its own words.**

Method (`mask_ref_canon.py:36-156`): the work's stream is cut into windows of
5,000 letters (200 overlap); 5-grams are looked up in a sorted array of all
canonical 5-gram codes; hits are grouped by canonical segment and by
"diagonal" (work position minus canonical position, in buckets of 20), so
that only runs at a near-constant offset survive; a run needs at least 2
anchors; its hull is verified by bounded Levenshtein distance, accepted at a
normalized distance of at most **0.28** (spans under 100 letters) or **0.32**
(longer); a long hull that fails is bisected and each half re-tested (depth
4), because paraphrase or interleaved commentary can hide a tight quotation
inside a loose hull; accepted spans get a 20-letter margin and merge when
within 30 letters. The distance cutoff was loosened from an earlier setting
that found only 104 spans across 5,271 works, because embedded canonical text
carries real orthographic drift. Midrash is deliberately **not** masked: a
midrashic quotation may be a shared source or an unrecognized witness, and
that judgement is left to later stages.

### 3.7 R-source

R-source is a born-digital library of 1,679 works (6.34 GB). Three things had
to be decided before any of it could match a Genizah page
(`same_work_spike/probe/rsource/`, report `REPORT.md`, policies
`ELIGIBILITY-POLICY.md` and `DEDUP-DESIGN.md`).

**Eligibility by composition date** (`r_eligibility.py`). A work is classified
by when its *text* was composed, never by its edition: E1 (to about 1050),
E2 (about 1050–1470, the core layer), E2L (1470–1550, flagged late), GE (a
modern edition of Genizah texts, usable as a known-witness channel); E3
(1550–1800) deferred; X (after about 1800, or institutional) excluded; XC
(anthologies whose mass is quotation of earlier works) excluded as claim
producers regardless of date. Classification uses about 120 explicit per-work
rulings, else a per-category default table. Eligible: **409 of 1,679 works**;
79 % of the corpus by volume is anachronistic for the Genizah.

**Deduplication against the other three corpora** (`r_overlap.py`,
`r_dedup_decide.py`). Distinct 5-grams saturate on a corpus this size
(about 2.71 million distinct codes; every eligible work already covers 95 % of
them), so 5-grams only *nominate* up to 100 counterpart works; confirmation
uses distinct **8-grams**: a counterpart if the R-source work contains at
least 50 % of the counterpart's 8-grams, or 70 % of the smaller set (a twin).
Coverage of the R-source work by the union of its counterparts decides:
**≥ 0.90 drop** (already present), **≥ 0.50 twin** (kept, grouped), else
keep. Result on the 409: 44 dropped by construction (the whole-Bible file and
four canonical aggregates the base already holds split), 11 dropped as
duplicates, 41 kept as twins, 313 kept as unique; **354 works, 299.2 million
letters**, packed into two index shards under the 65,536-segment ceiling.
When a version group spans corpora, the canonical identity is minted from the
member with the highest source priority (Sefaria > JA > M-source > R-source).

**Masking** (`gen2_mask_par.py`, the same algorithm as §3.6 with the anchor
minimum raised from 2 to 6 for speed, measured to keep 99.5 % of the masked
letters): 91.5 minutes, **305 of 354 works masked, 11,547,468 letters
(3.9 %)**. Anthology-flagged works get a second pass against the non-canonical
base reference (the midrashim they quote). The first R-source matching run
used these masks; the run that feeds *this* file used a corrected, harder
mask (`mask2_hardmask.json`) after a review of the first run found that, of
the 38,071 base rows an R-source witness had displaced in shadowing, 91.2 %
(34,705) changed to a *different* canonical work rather than to a twin of
the same work — the signature of masking that was canon-only rather than
full-reference (`REPORT.md` §6).

---

## 4. Track-1: finding a work on a page

The matcher is `same_work_spike/probe/scripts/mapv2_track1_run.py`, with the
index built by `track1_match.py::build_ref_index`, the k-gram encoder in
`engine_np.py`, and the per-page decision in `track1_membership.py`. It ran
over the whole corpus on 2026-07-10 in 197 minutes (`MAPV2-RUN-LOG.md`).

**Index.** Each reference work's stream is cut into segments of 3,800 letters
(200 overlap; `SEG_LEN`, `SEG_OVERLAP`, `track1_match.py:35`) and every
**5-letter gram** (`K = 5`) is encoded as a base-27 integer. Grams that
overlap a masked span (§3.6) are dropped. Any gram code occurring in more
than **128** reference positions is dropped from the index entirely
(`REF_DF_CAP`, `track1_match.py:37`). That cap is a raw count, not a
fraction, so adding works to the reference can push a gram over the cap and
remove it: **the matcher's recall is not monotonic in reference size**, a
documented pitfall (`MAPV2-15m-PLAN.md`).

**Seed.** A page's stream is 5-grammed and every gram is binary-searched in
the sorted index (`np.searchsorted`), yielding (page position, reference
segment, reference position) hits: 10,327,397,373 hits over the corpus
(`fullcorpus_v2.db` `mapv2_meta.stats`).

**Diagonal binning.** Hits are bucketed by `(page_pos − ref_pos) ÷ 20`
(`BAND = 20`). Within one page and reference segment, adjacent occupied
buckets are chained, and a chain needs at least **2** anchors
(`MIN_ANCHORS`) to become a candidate: 750,349,696 candidates.

**Extend.** The candidate's page and reference extents are padded by **30**
letters each side (`MARGIN`); a span shorter than **30** letters on either
side is rejected (`MIN_SPAN`).

**Verify.** Levenshtein distance between the two spans, divided by the longer
length, gives the edit **density**. Candidates above **0.55** are discarded
(`WIDE_CUTOFF`; 744,319,073 rejected). The rest are kept as verified hulls.

**Accept.** The production boundary is `accept_density = 0.28` for spans under
100 letters and `0.35` otherwise (`track1_match.py:43-44`). Hulls at or under
it are **tier A**; hulls between the boundary and 0.55 go to **tier B** with a
calibrated probability.

**Membership on the page** (`track1_membership.resolve_membership`). For each
work with a tier-A hull the best hull is the lowest-density one. Another work
is a *competitor* if its best hull overlaps this one by at least half of the
shorter (`OVERLAP_FRAC = 0.5`); the *margin* is the density gap to the
nearest competitor. **Two-page merge flag**: if two works' widest tier-A hulls
are disjoint (overlap under 10 %) and sit in opposite halves of the page, the
page is weak-flagged; if in addition FGP counts more folios for the
manuscript than the corpus does (`stage0_sys_flags.fgp_disagree`), the page is
treated as a scan that merged two leaves and its tier-A rows are held in
tier B with `flag='merge_page'`: 594 pages merge-flagged, 25,839 weak-flagged.

**Output per (page, work).** Tier-A hulls are merged when within 30 letters
(`GAP_MERGE`) and written once to `track1_matches` with `matched_letters`
(sum of merged span lengths), `best_density`, `n_spans` and the span list:
**381,341 tier-A rows**. Everything else goes to `track1_candidates` with a
margin band and a probability: **1,900,171 tier-B rows** (rows below
`P_MIN_STORE = 0.05` are dropped and counted, never silently).

**Calibration** (`cal1_calibration.py`, `p_calibration_final.json`, 6
minutes). The probability of "same work" for a tier-B hull is an isotonic
curve fitted per margin band and length bin (40, 60, 80, 100, 150, 200, 300
letters) on 753,701 labelled rows from 916 pages and 12,824 synthetic crops;
singletons (no competitor) use a decoy-null CDF instead, because only 12 of
921,000 pilot rows were true singletons.

**R-source runs.** The same runner, imported unchanged, matched the two
R-source shards against all 667,411 pages on 2026-07-22 (141 and 170 minutes;
226,664 + 62,416 tier-A rows) for a research frame. The rows in *this* file
come from a **later re-match** (2026-08-28/29, `gen2_track1_pilot.py`) with
the corrected hard mask, storing the exact page↔reference alignment
(CIGAR) that the later chronology and routing steps need: 130,684 + 42,287
staged rows on 77,468 + 25,597 pages. The base half's frame came the same
way through `scripts/discovery_v4_match.py`, a wrapper that pins the same
runner and its calibration file and refuses to run with a page allowlist.

---

## 5. From matches to claims

### 5.1 Shadowing: competition on one page

Two reference works often match the same span (a paraphrase of Deuteronomy
against Deuteronomy itself; two editions of one work). `track1_shadow.py`
sorts a page's rows by density and keeps a row live unless a strictly better
row already kept overlaps its best span by at least **60 %** of its length
(`OVERLAP_FRAC = 0.6`) and beats its density by at least **0.03**
(`MIN_DENS_GAP`); the loser records `shadowed_by = <winner>`. The production
pipeline ports this rule with the same constants (`scripts/discovery_v4_match.py`).
The review database reads only rows that are not shadowed.

### 5.2 The gen-2 pipeline: ingest, shadow, chronology, bake

`same_work_spike/probe/rsource/scripts/gen2_discovery_run.py::run_pipeline`
turns a match frame into **claims** (one per manuscript and canonical work)
and **evidence** rows (one per aligned span), in one fenced, fingerprinted
run:

- **ingest** (`gen2_evidence_ingest.py`): canonicalizes work ids through the
  version-group registry and flags thin claims;
- **shadow** (`gen2_shadow.py`): the page competition of §5.1;
- **chronology** (`gen2_chrono.py`): when two works claim the same page span
  and their composition-date intervals are strictly ordered, the *later*
  work's hull is demoted with reason `later_shared_text` **only if** what
  remains of it outside the overlap is below a floor, i.e. the later hull is
  essentially a repeat of the earlier text; a later work's distinctive
  material is never touched, and undated or near-tied pairs never fire. The
  R-source run took the *widest* interval when a work's members disagreed on
  date, so chronology can only under-demote. Dates come from
  `discovery_data/composition_dates.json` and a Sefaria/JA date table;
- **bake** (`gen2_bake.py`): derives each claim's `routing_status`
  (`shipped` / `review_only` / `contested`, strict precedence) and a
  content-hashed manifest.

For the R-source half (`gen2_g_r_launch.py`, run `g_r`): 172,971 claims /
226,679 evidence rows; 39,076 rows shadowed; 3,917 chronology pairs examined,
2,882 demoted; routing 131,957 shipped / 41,014 review_only
(`results/gr_pipeline.log`). The base half went through the same pipeline in
its own run (`g_launch3`), whose router evidence the V4.2 build consumed.

### 5.3 The coverage router: copy or quotation?

`router_verdict` is **the relation** the database asserts, and the only
witness-versus-quoter axis that was validated. It is decided by **how much of
the page the match covers**: `gen2_coverage_router.py` computes, per (page,
canonical work), `page_coverage = max matched_letters ÷ page letters`, and
labels the group `same_work` at or above a threshold, else `parallel`;
groups with no shipped claim are `not_shipped`. The threshold was **fitted
at 0.2984** by maximizing accuracy against **1,395 owner-graded claims**
(accuracy 0.795; precision of `same_work` 0.887 design-weighted). On the
R-source run: 36,373 same_work / 95,287 parallel / 40,630 not_shipped groups.
The review builder reads the verdict verbatim rather than recomputing it
with its own older threshold (0.45), because recomputing would have demoted
30,899 of 160,095 validated witness rows (`scripts/v3_routing_ingest.py:9-22`).
Distribution in this file: same_work 224,397; parallel 224,040; not_shipped
68,688; shared_text 2,149; no verdict recorded 108.

Two things `router_verdict` is **not**:

- `claim_type` (`direct_witness` / `quotes_this_work`) only says which matched
  span is *largest on this page*; a page with a single match is
  `direct_witness` however short. 45,149 rows the router called quotations
  carry `direct_witness`. Use `router_verdict` for the relation (`meta
  doc.claim_type`).
- `routing_status = shipped` on an R-source row describes the matching run's
  tiering; R-source is not on the public site (`meta doc.routing_status`).

### 5.4 Pools and the display relation

**`main_pool`** (`shared/discovery_main_pool.py::main_pool_decision`) is the
public site's rule for one *identification* (manuscript × work across all
its pages), applied in order with no compensation between steps: a human
confirmation wins; no `direct_witness` anywhere → not main; best evidence in
a screening band or review-only → not main; every page carries an unresolved
near-tie → not main; **two or more matched pages → main**; a single page needs
at least **150 matched letters** and **80 % page coverage** (the coverage
floor is marked provisional in the code). NULL means the rule was never
evaluated for the row. In this file: main 271,157 rows, not main 160,587,
never evaluated 87,638.

**`triage`** is the *review tool's* pool, a deterministic SQL rule tuned on an
80-card blind deck graded by the owner on 2026-08-30 (`scripts/serve_v3_review.py`,
`TRIAGE_SQL`): `main` when the router says `same_work`, the scripture flag is
off, the formula kind is not an embedded section or standalone unit, no owner
ruling excludes the work, and page coverage is at least **85 %** (**75 %**
for R-source); `citation` for `parallel` rows; `shared_quotes` for
scripture-flagged, embedded-formula or `shared_text` rows; else `unclear`.
Counts: main 117,608; citation 158,700; shared_quotes 100,369; unclear
142,705. The two pool rules disagree on 112,844 cards, which is why the viewer
derives a card's pool from its own rows' triage.

**The display relation** on the public site (`shared/discovery_relation_matrix.py`,
frozen 2026-08-12) is six rules applied in order: no shipped evidence →
uncertain; routing reason `later_shared_text` or `co_citation` → shared_text;
(inactive) whole footprint on known non-discriminative text → shared_text;
work on the curated quoter list → quotes_this_work; coverage unknown →
uncertain; otherwise the stored relation. No page-level chip may claim more
than its identification's rendered relation.

### 5.5 Novelty: does this add anything to what the catalogues record?

`novelty_status` is computed in two stages (`shared/discovery_novelty.py`;
spec `docs/specs/discovery-novelty-v1.md`):

1. A **mechanical funnel** reads the free text of every checked source —
   the catalogue prose, the Friedberg bibliography, PGP, FGP, and M-source's
   own attributions — and can only resolve a pair to `confirms` (a genuine
   name match under alias normalization) or leave it in the residual; a
   pair with literally no source text becomes `fills_gap` without a model
   call. Mechanical resolution is one-way: a false `confirms` is never
   re-examined.
2. A pinned **LLM gate** (gemini-3.7-flash, reasoning effort low, **10 cases
   per call**) judges only the residual into ten shades: `confirms`,
   `refines_granularity`, `aid_more_specific`, `diverges_work`,
   `diverges_part`, `container_predicts`, `fills_gap`, `extends`,
   `alias_merge`, and `not_checked` (the fail-closed default). The cache key
   hashes the model, prompt and every input field, so a contract change
   invalidates every cached verdict; a batch reply that misaligns case
   numbers is rejected whole. Batch size 10 was chosen because false
   `fills_gap` promotions measured +2 at 10 versus +10 at 20 and +11 at 40
   over the same 300 cases. Measured cost: **$0.000727 per case** ($40.12
   over 55,184 residual cases on the v2 asset; $42.28 for 67,079 pairs on
   v3). The model was moved to 3.7-flash after a 127-case blinded audit
   preferred it 64 to 27.

Distribution in this file: diverges_work 177,301; not_checked 120,220;
confirms 105,870; refines_granularity 50,451; fills_gap 39,067;
container_predicts 12,362; aid_more_specific 11,458; diverges_part 2,622;
alias_merge 31. The R-source half was scored on 2026-08-30 (`meta
rsource_novelty.*`); 31,135 of its rows remain `not_checked`.

### 5.6 Two adjudication gates: labels, never verdicts

`scripts/divergence_adjudication_gate.py` (gemini-3.7-flash, effort low,
batch 10, a hard cost ceiling checked against real usage before every call,
every prompt masking-scanned before it leaves) read, for the strongest page
of each pair, the catalogue's own prose, the bibliography, PGP, the aligned
excerpts and the computed signals:

- **divergence** (where the catalogue and the computation disagree):
  catalogue_right_match_is_quotation 18,977; computed_right_catalogue_mismatch
  1,876; catalogue_too_general 1,014; overlapping_works 1,013;
  both_right_multiple_works 856; catalogue_right_claim_mistaken 246. Validated
  on 117 owner-graded cases: "catalogue right" verdicts 98.8 % confirmed;
  **`computed_right_catalogue_mismatch` went 0 for 4 when contested** — read
  it as "needs a person", never as a finding.
- **new finds** (pairs no finding aid records): credible_new_identification
  7,588; weak_match_generic_text 2,393; wrong_identification 543;
  plausible_needs_expert_check 426; actually_recorded 22. Validated on 84
  graded cases: precision of `credible_new_identification` 58 of 60. Each
  verdict carries a "doubt", the one thing an expert should verify.

`divergence_correctness` (who is right when the two disagree) is **human-only
and empty**: the model scored 8 of 28 on it, at or below chance for three
options (`meta doc.divergence_correctness`).

### 5.7 Two computed labels on the text itself

**Scripture flag** (`scripture_fact`, version 3; thresholds in `meta
scripture_fact.thresholds`): a match is flagged when at least half of its
20-letter grams occur verbatim in the Bible or in Mishnah, Tosefta, Talmud or
Targum; or a citation formula sits at the match boundary (90 characters of
context plus 30 of the match's own edge, never its middle) and the match is
under 150 letters; or at least half the span lies inside quotation intervals
the pre-matching hard mask already caught (mask data exists for R-source
only). Computed for every corpus except works that are themselves scripture:
344,640 rows examined, 80,218 flagged. A flagged match may rest on text both
sides are quoting.

**Formula kind** (`formula_fact`): `embedded_section` (24,712: the matched
section inside a non-liturgical work is a fixed prayer or notarial formula),
`standalone_unit` (7,973: the claimed work is itself a liturgy unit),
`documentary_page` (932: the page is catalogued as a legal document; shown as
context only). Nothing is hidden by these labels.

### 5.8 A known weakness

The screen that suppresses matches resting on shared scripture is stale and
misses part of the reference corpus, so some matches sit on a verse or
liturgical formula many works quote. **Treat short matches on such text with
suspicion** (`meta doc.known_weakness`, 2026-08-09).

---

## 6. Track-2: page-to-page reuse (not in this file)

The same research chain also ran a page-to-page search with no reference work
involved (`rehearsal_run.py`, 50 minutes), and built views over it: manuscript
clusters, a reuse graph, multi-page chains, many-to-many passage units, and
two retrieval passes that turn discovered passages back into corpus queries
(`motif_query.py`, 96 minutes; `work_query.py`, 39 minutes). None of these
write to the review database; they are separate research products and are
mentioned only so that the run log (`MAPV2-RUN-LOG.md`, steps 6–13) is not
mistaken for part of this artifact's lineage.

---

## 7. Building the review database

### 7.1 Two halves, rendered then merged

`scripts/build_v3_review_db.py` renders one review database from one build
artifact: for the base half, `discovery-v42lit.db` (the V4.2 "lit" build of
`scripts/build_discovery_sidecar.py`, itself fed by the V4 match frame, the
gen-2 router evidence, the re-scored novelty verdicts, composition dates and
the owner-approved work list); for the R-source half, `gr-adapter.db`
(`gen2_gr_adapter.py`, which translates the `g_r` run into the builder's
schema, keys every witness to the **raw** R-source file so each evidence
row's offsets stay meaningful against exactly one file, and asserts five
structural gates before writing). `scripts/merge_v5_review_db.py` then merges
the two halves (2026-08-29).

### 7.2 What a row is

One `review_row` is one alignment: a page, a work, a matched span, and both
texts around it. The builder reads the shipped evidence rows
(`evidence_source='track1_direct'`, `w_start` present), cuts the matched span
out of each side with **320 characters of context** before and after
(`CONTEXT`, `build_v3_review_db.py:97`; the six columns `ms_before/match/after`,
`ref_before/match/after`), and computes an address on each side.

**The offset contract.** Every offset is a **character index into the
NFC-normalized text, 0-based, end exclusive**. On the manuscript side
`page_char_*` index the page's own text and `file_char_*` the whole
`Transcriptions.txt`; `ms_provenance_status` says which case a row is:
`ok` (493,116 rows), `offsets_missing` (26,180: the page was searched as a
human transcription, so these rows have no address in the file — but see
§9), `nfc_shift` (86: the page's raw form differs in length from its NFC
form, so a file address would be off; page offsets are kept, file offsets
withheld). Bounds are checked, never clamped: an out-of-range span becomes a
status, not a wrong excerpt.

### 7.3 Reference-side addresses

The reference side is harder because each corpus has its own cleanup. The
builder loads each work's raw file, applies the corpus's cleaner (M-source:
`##…##` headers; R-source: `###` locus headers, `+…+` apparatus, later
layers) **with an offset map**, composes it with the normalizer's map, and
projects the matcher's letter-stream coordinates (`w_start`/`w_end`) back
onto raw file characters (`ref_char_start/end`). For V4-era JSON sources,
which are concatenations of numbered units, the row also carries the start
and end unit ordinals and intra-unit offsets, because 107 of 12,454 such spans
cross a unit boundary. A split work (a division of a canonical monolith,
§3.5) locates its slice in the parent file by a single unambiguous find;
ambiguity degrades to a stream fallback rather than a guess.
`ref_provenance_status`: `ok` 519,104; `stream_fallback` 278 (Sefaria rows
whose text came from a re-derived stream, shown unspaced and flagged).

**Witnesses and files.** `reference_witness` names the exact file that
produced a row's offsets (one work can have several witnesses);
`source_file.ref_id` is a frozen codename for the two masked corpora
(`M:Ytext…`, `RS:…`) and never a path. Ids are content-derived (`sha1` of
kind and reference id, first 16 hex), so two independently rendered halves
agree on them by construction. The codename-to-path map is written to a key
file **outside the repository** and never travels with the artifact.

The row's `source_corpus` is the work's *display* label and can differ from
the kind of file the row is addressed in (measured on this file):

| `source_corpus` | witness file kind | rows | files |
|---|---|---|---|
| `sefaria` | M-source (masked codename) | 164,235 | 235 |
| `sefaria` | Sefaria staging text | 41,506 | 272 |
| `sefaria` | V4 JSON re-projection | 27,273 | 92 |
| `msource` | M-source (masked codename) | 33,787 | 647 |
| `rsource` | R-source (masked codename) | 226,679 | 344 |
| `ja` | JA text | 19,365 | 85 |
| `ja` | Sefaria staging text | 6,259 | 21 |
| (any) | no witness: letter-stream fallback | 278 | — |

So 424,701 rows (81.8 %) are addressed in a masked file, resolvable only
through the Access index's file names; the rest carry the real file name in
`source_file.display_ref`.

### 7.4 Independent verification

`scripts/verify_v3_review_offsets.py` re-derives every offset with a
differently shaped algorithm (a boolean mask over removed regions, then one
walk keeping letters) and the same cleanup rules restated: for each witness
with `ref_provenance_status='ok'` it checks that the oracle's stream slice
equals the letters of `ref_match` and that the oracle's raw position of
`w_start`/`w_end` equals the stored `ref_char_*`; for `ok` manuscript rows it
streams `Transcriptions.txt` once and compares the file slice to `ms_match`.

### 7.5 Merge safety

`merge_v5_review_db.py` refuses unless both inputs carry schema
`discovery-v3-review/2` with identical columns and **disjoint** evidence ids;
copies parents before children inside one transaction with foreign keys on;
asserts the row count equals the exact sum of the inputs; rolls back and
deletes the half-written file on any error; runs `integrity_check` and
`foreign_key_check` before accepting; and namespaces `meta` per input
(`base.*`, `rsource.*`).

### 7.6 Citable loci and row metadata

`locus_label` is the address a scholar cites. For base rows it comes from the
sidecar's locus module (`shared/discovery_locus.py`), which maps letter-stream
offsets onto per-work unit tables built from each work's own structure. For
R-source rows `scripts/attach_rsource_locus.py` indexes every `###` section
header's NFC position in the raw file, bisects to the innermost header at or
before `ref_char_start`, and turns its breadcrumb into a label (capped at 200
characters); every distinct label passes the masking scanner before the
update commits. Resolved on all 226,679 R-source rows (`meta rsource_locus.counts`).
Shelfmarks and library codes were backfilled from the display table, then
`libraries.csv`; 226,555 R-source rows carry one.

### 7.7 Counts

| Table | Rows | Meaning |
|---|---|---|
| `review_row` | 519,382 | one alignment each |
| `facet_row` | 519,382 | the slim projection the viewer filters on |
| `source_file` / `reference_witness` | 1,696 / 1,697 | files and the witnesses cut from them |
| `scripture_fact` | 344,640 | scripture screen results (80,218 flagged) |
| `formula_fact` | 33,617 | formula kinds |
| `gate_verdict_fact` | 35,042 | LLM adjudication verdicts, pair grain |
| `locus_unit` | 122,952 | unit tables behind the loci |
| `known_work` / `known_work_member` | 1,447 / 1,736 | identities (§8) |
| `card` / `card_member` | 433,911 / 519,382 | cards (§8) |
| `htr_page` | 18,982 | HTR text and address of every substituted page (§9) |
| `work_author_ruling` | 215 | owner rulings on authors and titles |

---

## 8. Identities, cards, authors

**Identities** (`scripts/build_work_registry.py`). Several corpus works are
one literary work (an R-source whole-work file and the base corpus's per-book
works; two editions; two halves of one midrash). The registry decides
identity **only from a pinned upstream census plus explicit owner rulings,
never from text similarity**: it verifies the hash of its own pin manifest
against an operator-supplied trusted root, re-hashes each pinned input, type-
checks every field, refuses any owner question still open, and merges work ids
by union-find from the census groups, four owner merges, the vetted alias
links and five declared work groups. Anthology containers are the only
multi-member works; their rows are routed to member identities by the longest
`locus_label` prefix in a pinned scope map, and a row that matches no prefix
aborts the build. Result: **1,447 known works from 1,736 members**; 1,329
singletons, 63 census canonicals, 20 clusters, 17 families, 9 minted, 5 work
groups, 4 owner merges (`known_work.title_basis`).

**Cards** (`scripts/attach_review_cards.py`). One card per (page, known
work): the unit of the question "is this page this work?". A card **never
merges evidence**: `card_member` keeps every row with its raw work id and
witness scope; a summary column whose rows disagree reads `mixed`, never a
majority; a locus is set only when all rows agree. 433,911 cards; 12.6 %
hold more than one row.

**Authors.** One canonical string per person across the database
(`scripts/attach_author_authority.py`); no author derived from a title alone
(`scripts/drop_title_derived_authors.py` cleared 34 works / 38,760 rows); 215
owner rulings recorded in `work_author_ruling` (209 authors, 6 titles). Where
witnesses of one identity disagree on the author the build aborts rather than
choosing.

---

## 9. The HTR re-alignment

Section 1.2 left 26,180 rows on 7,890 pages (5,172 manuscripts) whose
manuscript-side offsets index a human transcription the reader does not hold.
`scripts/attach_htr_realignment.py` (2026-09-01) located each of those matched
spans in the **HTR text of the same page** and stored a second address:

| `htr_align_status` | Rule | Rows |
|---|---|---|
| `exact` | the matched letters occur verbatim in the HTR page, once | 760 |
| `realigned_htr` | best `partial_ratio_alignment` window, score ≥ 90.0 | 16,279 |
| `realign_uncertain` | best window, score below 90, shown with the score | 9,141 |
| `ambiguous` | the letters occur more than once: no address | 0 |
| `unalignable` | fewer than 10 letters: no address | 0 |

(`REALIGNED_MIN`, `MIN_QUERY_LETTERS`, `attach_htr_realignment.py:81-82`;
counts in `meta htr_realign.status_counts`.) The status is decided on the same
one-decimal score the row shows. The FGP-based offsets stay untouched: two
addresses, each into the text it belongs to. `htr_page` holds the HTR text and
file address of all 18,982 substituted pages.

Gates: `Transcriptions.txt` must match the offsets index in size, modification
time and record count; 500 indexed pages are re-derived and must reproduce
their spans; every substituted page's captured length must equal the corpus's
own record of it; every alignment window must reproduce the aligner's score
when re-scored independently (a content check, since re-slicing through the
offset map alone is a tautology) and must slice back to its letters; counts
are reconciled after the write or the transaction rolls back. The low scores
are a property of the HTR, not of the match: on 7,884 of the 7,890 affected
pages the human text is at least 80 % as long as the HTR (corpus `n_chars`
against `htr_n_chars`), so the human transcription is rarely the shorter
text; a handful of low-score rows read by hand while building the pass were
the same passage with garbled HTR.

**What this does not do.** It re-addresses the spans the human text produced.
The HTR of those pages was never *searched*; a match present only in the HTR
where the human transcription is thin is not in this file (§1.2 measures how
often that could matter; it is recorded as an open item).

---

## 10. Delivery

**The viewer** (`serve_v3_review.py`) is a single standard-library script. It
never sends the raw `source_corpus` code to the browser (the label is computed
server-side and the code deleted from the payload); it applies the live
site's display-only marker cleaning to the six text pieces; it offers a
Sefaria link only where a work title and locus parse under a documented,
non-guessing rule (tractate or book table, Hebrew numerals accepted only in
non-increasing letter order, chapter ≤ 150, folio ≤ 200); the card grain and
the HTR pane are offered only when their tables exist **and** their recorded
counts match the live row counts, so a stale projection is hidden rather than
served. Grades and notes go to a separate `.grades.db` beside the database,
never into it.

**The Access index** (`export_access_index.py`) carries every table except the
text columns, plus the real base file name for each masked source, by the
owner's authorization for a reviewer who already holds those corpora. Every
R-source file name carries its provider's name, which is why the quickstart
asks you to treat that file as the sensitive one.

**The packaging gate** (`package_review_artifact.py`) must pass before the
folder leaves the machine: no process holds the database, no unfinished
transaction files, `integrity_check` and `foreign_key_check` clean on both
databases, a masking scan of every file in the bundle, no key file in or
beside the bundle, and a size-and-SHA-256 manifest written with
`--bundle-out` (the `MANIFEST.txt` in your folder). The scan proves
the absence of the loaded patterns, not the completeness of the pattern list;
the gate proves structural soundness, not semantic correctness — that is what
each stage's own reconciliation gates are for.

---

## 11. What this file cannot tell you

- **Recall on substituted pages.** The HTR text of 18,982 pages was never
  searched (§9). The measured content loss is small but not zero (§1.2).
- **Reference-size effects.** The k-gram posting cap makes Track-1 recall
  non-monotonic in reference size (§4); a work added later can remove grams
  that identified another.
- **Short matches on shared text.** The scripture screen is stale (§5.8).
- **Citation-only pairs were never model-adjudicated.** The two LLM gates
  ran only on pairs with a main-pool or unclear row; the 70,456 manuscript ×
  work pairs whose every row sits in the citation bucket carry no verdict
  (counted on this file: pairs with no row outside `triage='citation'`, none
  of which has a `gate_verdict_fact` row) — an absence, not a clean bill.
- **Which side is right** when catalogue and computation disagree is a
  human question here, by measurement (§5.6).
- **Two rules for "main".** The site's `main_pool` and the review tool's
  `triage` disagree on 112,844 cards (§5.4); the viewer shows triage.
- **Upstream identity groups.** The R-source work-id registry that the gen-2
  run used for canonical ids (`gen2_workid_registry.json`) chains 19 of the
  354 works into over-merged version groups, one of 293 members joined
  through shared liturgical and biblical text. This file's own identity table
  was built from the pinned census and owner rulings and does not reproduce
  that grouping (its largest identity has 40 members), but the canonical ids
  under which R-source claims were shadowed, dated and routed (§5.2) came
  from the upstream registry. Offsets are unaffected either way: they are
  keyed to the individual file.
- **R-source novelty** is `not_checked` on 31,135 rows.
- **The gate audits were not all closed.** The stage-0 gate's leak (§1.2)
  and the canon-only masking issue of the first R-source run (§3.7) were
  measured and, in the second case, remediated for the run that feeds this
  file; the stage-0 gate itself was not changed.

---

## Appendix A. Constants at a glance

| Stage | Constant | Value | Where |
|---|---|---|---|
| pages | minimum Hebrew letters | 80 | `stage0.py:35` |
| substitution | score floor / min letters / coverage / window ratio / ratio cap | 70.0 / 200 / 0.80 / 1.3 / 12.0 | `mapv2_stage0.py:92-104` |
| reference | M-source & JA min letters; Sefaria min letters | 150; 200 | `track1_build_ref.py:32`; `ref2_build.py:50` |
| reference | duplicate / twin containment (5-gram) | 0.98 / 0.85 | `ref2_build.py:51-52` |
| masking | gram / band / anchors / density (<100, ≥100) / margin / gap | 5 / 20 / 2 / 0.28, 0.32 / 20 / 30 | `mask_ref_canon.py:35-47` |
| R-source dedup | counterpart 8-gram containment; drop / twin coverage | 0.50 (or 0.70 twin); 0.90 / 0.50 | `r_dedup_decide.py:48-50` |
| R-source masking | anchors | 6 | `gen2_mask_par.py:46-52` |
| Track-1 index | gram / segment / overlap / posting cap | 5 / 3,800 / 200 / 128 | `track1_match.py:34-37` |
| Track-1 | band / anchors / margin / min span | 20 / 2 / 30 / 30 | `mapv2_track1_run.py:79-80` |
| Track-1 | accept density (<100, ≥100) / wide cutoff / tier-B floor | 0.28, 0.35 / 0.55 / 0.05 | `track1_match.py:43-44`; `mapv2_track1_run.py:84-86` |
| Track-1 | competitor overlap / merge-page overlap | 0.5 / 0.10 | `track1_membership.py:26-27` (the module both runners import; `mapv2_track1_run.py:89-90` carries an unused copy) |
| Track-1 | span merge gap | 30 | `mapv2_track1_run.py:91` |
| shadowing | overlap / density gap | 0.6 / 0.03 | `track1_shadow.py:31-32` |
| router | same_work coverage threshold | 0.2984 | `results/gr_coverage_router.log` |
| main pool | single-page letters / coverage | 150 / 0.8 | `shared/discovery_main_pool.py:113,123` |
| triage | coverage bar (R-source) | 0.85 (0.75) | `serve_v3_review.py` `TRIAGE_SQL` |
| novelty | model / effort / batch | gemini-3.7-flash / low / 10 | `shared/discovery_novelty.py:492-582` |
| scripture flag | gram / share / flank / edge / max letters / mask fraction | 20 / 0.5 / 90 / 30 / 150 / 0.5 | `meta scripture_fact.thresholds` |
| review db | excerpt context | 320 chars | `build_v3_review_db.py:97` |
| HTR re-alignment | trusted score / min letters / score tolerance | 90.0 / 10 / 0.5 | `attach_htr_realignment.py:81-84` |

## Appendix B. Where each stage lives

| Stage | Code |
|---|---|
| pages, filters | `same_work_spike/probe/scripts/extract_full.py`, `stage0.py` |
| search-text substitution | `same_work_spike/probe/scripts/mapv2_stage0.py`; audit `results/agent_stage0_audit.md`; recompute `results/mapv2_substitution_risk.md` |
| offset index | `scripts/index_transcriptions_offsets.py` |
| normalizer | `same_work_spike/probe/scripts/normalize.py` |
| reference build | `track1_build_ref.py`, `ref1_fetch_sefaria.py`, `ref2_build.py`, `split_canon_works.py`, `msource_clean.py`, `mask_ref_canon.py` |
| R-source track | `same_work_spike/probe/rsource/scripts/r_eligibility.py`, `r_overlap.py`, `r_dedup_decide.py`, `r_build_gen2_ref.py`, `gen2_mask_par.py`, `gen2_track1_run.py`, `gen2_track1_pilot.py`, `gen2_g_r_launch.py`, `gen2_coverage_router.py`, `gen2_gr_adapter.py`; `REPORT.md` |
| Track-1 | `mapv2_track1_run.py`, `track1_match.py`, `engine_np.py`, `track1_membership.py`, `cal1_calibration.py`; production wrapper `scripts/discovery_v4_match.py` |
| claims | `gen2_discovery_run.py`, `gen2_evidence_ingest.py`, `gen2_shadow.py`, `gen2_chrono.py`, `gen2_bake.py`; `track1_shadow.py` |
| pools, relation, novelty, gates | `shared/discovery_main_pool.py`, `shared/discovery_relation_matrix.py`, `shared/discovery_novelty.py`, `scripts/divergence_adjudication_gate.py`, `scripts/attach_scripture_facts.py`, `scripts/attach_formula_flags.py` |
| review database | `scripts/build_v3_review_db.py`, `scripts/verify_v3_review_offsets.py`, `scripts/merge_v5_review_db.py`, `scripts/attach_rsource_locus.py`, `scripts/v3_routing_ingest.py` |
| identities, cards, authors | `scripts/build_work_registry.py`, `scripts/attach_review_cards.py`, `scripts/attach_author_authority.py`, `scripts/apply_work_author_rulings.py`, `scripts/drop_title_derived_authors.py` |
| HTR re-alignment | `scripts/attach_htr_realignment.py` |
| delivery | `scripts/serve_v3_review.py`, `scripts/export_access_index.py`, `scripts/package_review_artifact.py` |
