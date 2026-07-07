---
id: SEED-029
status: dormant
planted: 2026-07-02
revised: 2026-07-06 (three-agent critical review — codebase/data grounding + literature research + adversarial critique — substance inlined below; the original LSH-centric method stack is REPLACED by a seed-and-extend architecture)
planted_during: A /gsd-new-milestone exploration (2026-07-02). Scoped end-to-end with the user + a Codex methodology review, then parked. Revised 2026-07-06 after a parallel three-agent review (grounding, research, adversarial critique) requested by the user.
trigger_when: A dedicated research/experimentation milestone when the user is ready to explore automated fragment identification via text. Standalone — no dependency on other seeds. Feasibility-FIRST (week-1 separability probe → prototype → eval → go/no-go), likely an INTERNAL milestone (no public version bump) until proven. COORDINATE WITH MiDRASH/Dicta FIRST (see "Strategic context").
scope: large (research + prototype + evaluation feasibility spike; new `same_work_spike/` pipeline; no web/desktop UI integration this cycle)
---

# SEED-029: Fragment textual similarity — shared-passage detection (feasibility spike, REVISED)

> User intent (2026-07-02): "Investigate the possibility to identify similarity between pairs of
> fragments. Assigning them a fingerprint and judging by score... also search the research for
> relevant methods and what's working." Goal: **identify unknown Genizah fragments by their text**
> — the "inside the corpus" complement to `/corpus_mapper`. Feasibility, not ship-a-feature.

## Goal
Detect **shared passages / citations** between Genizah pages over the MiDRASH HTR text of the whole
corpus — robust to HTR noise, spelling variation, and partial overlap. From passage-level evidence,
infer same-work relationships and identify unknown fragments. Long citations surface as valuable
indirect textual witnesses.

## What the 2026-07-06 review changed (summary)
The seed's instincts survive (lexical-first, multi-view normalization, canonical-as-feature,
embeddings-last, honest eval). Its ENGINE does not: **windowed-Jaccard + MinHash/LSH is replaced by
seed-and-extend** (DF-banded exact char-n-gram seeds → diagonal two-hit filtering → local-alignment
verification — the BLAST/Passim/Shmidman architecture). Three independent lines converged:
1. **The math**: at realistic HTR noise (CER ~10%, both sides), *identical* text lands at
   char-4/5/6-gram Jaccard ≈ 0.27/0.21/0.17. The planned 32×4 LSH banding captures only **2–15%**
   of true pairs there; the "stricter 16×8" has threshold s*≈0.71 — a near-duplicate setting,
   flatly wrong for this. Widening bands to catch s≈0.2 pulls in the 0.03–0.05 background Jaccard
   of *unrelated* Hebrew windows → 10⁸–10⁹ noise candidates.
2. **The literature**: Silcock et al. (ICLR 2023, arXiv:2210.04261) *measured* true-OCR-duplicate
   Jaccard at 0.23–0.30 (3–5-grams); 19% of true pairs share NO 10-gram. Turku's BLAST recovered
   2–3× more reuse than Passim on noisy OCR because seed-and-extend *scores* mismatches instead of
   requiring set overlap.
3. **The citation use-case**: a 50-char quote inside a 300-char window is Jaccard ≤ 0.09 with ZERO
   noise — window set-similarity structurally cannot see the headline use-case. Seed-and-extend
   measures the right objective: "does a contiguous-ish aligned span exist."

Fates: **MinHash/LSH → demoted to stage-0 near-dup detection only** (the one place its regime is
correct). **Windowed char-TF-IDF → demoted to a 50-line sklearn eval baseline** (not a pipeline
stage). **Local-alignment verifier → promoted to the load-bearing stage.**

## Corpus ground truth (verified 2026-07-06 — corrects the original seed)
- `Transcriptions.txt` (repo root, 1.47 GB, UTF-8): records delimited by `==> <ID> <==` headers.
  **The unit is a PAGE, not a fragment**: **948,549 page records** across **216,911 manuscripts**
  (220,813 sys_id+IE). ID = `{sys_id}_{IE…}_{P######}_{FL…}`; the sys_id prefix joins directly to
  fjms `AlmaId` / pgp+fgp `sys_id`.
- Length: median **171 tokens/page** (~821 chars), mean 181, p90 298; only ~4.6% under 20 tokens
  (head-of-file sample of 2,000 — re-sample randomly at trigger time).
- Noise is **segmentation-shaped**: whole lines of bare `][`, single-letter word shards, split/merged
  tokens; JA combining upper-dot U+0307; stray OCR glyphs. ⇒ word-tokenized processing is fragile;
  **space-stripped char-level is the default**. Plan for CER ~3–10%+ (worse on JA/documentary hands)
  — but MEASURE it (see "confusion-matrix asset").
- The Zenodo dump (10.5281/zenodo.17734473, CC-BY-4.0, Nov 2025, v0.8) self-describes as
  "very preliminary": segmentation/reading-order errors, weak vertical text + Arabic script.
  (HTR *model* is CC-BY-NC-SA — check licensing if outputs feed a commercial surface.)
- `AllGenizah_OLD.txt` (1.62 GB) is an older dump — stage-0 dedup must handle re-transcription
  overlap (V0.7/V0.8), which the original seed assumed solved but is an UNBUILT stage.
- Root-level `pgp.db` / `fjms_enrichment.db` / `nli_crossref.db` / `FIST.db` are **0-byte stubs**;
  live copies are `pgp_data/`, `fist_data/`, `nli_data/`, `fgp_data/`.

## Architecture (revised pipeline)

**Stage 0 — corpus hygiene (mandatory, new):** drop empty/`][`-only pages; page-level near-dup
detection (high-Jaccard regime — strict LSH/exact hashing is CORRECT here); exclude same-`sys_id`
pairs from discovery (recto/verso, refoliations — Passim's `series` concept); 20-line char-frequency
language ID (Hebrew vs Judeo-Arabic) per page. **Per-language DF tables + eval strata are required**
— JA function-shingles won't be suppressed by global statistics.

**Normalization — ONE union view, not parallel indexes:** NFC → strip nikud/cantillation/combining
marks (incl. JA upper dot) → fold geresh/gershayim/quotes → **final-letter fold ךםןףץ→כמנפצ** →
space-stripped. None of NFC / final-letter fold / matres handling exists in the codebase today
(`shared/text_normalize.py` has nikud-strip + diacritic-fold only) — this is new code. Matres-light:
emit those shingles **into the same shingle set** (union), not a second index; validate the gain
(~ΔJ +0.06–0.08 expected) on known plene/defective witness pairs before keeping.

**Track 1 — fragment ↔ canon (identification):** run **Shmidman-Koppel skip-grams against CLEAN
reference corpora** (Maagarim/Sefaria: Bible, Mishnah, Bavli, Siddur/liturgy). Method (arXiv:1602.08715):
per-word reduction to the **2 rarest letters** (order kept; matres/prefixes are frequent letters →
plene/defective collapses free); 4-word skip-grams — from every 5-word window, 4 variants each
omitting one word; collision-free 64-bit codes (22⁸ < 2⁶⁴); cluster validation i=3 matches / gaps ≤8
words / span ≥20 words (Talmud all-pairs ≈ 11 s single-threaded). **Two HTR adaptations the 2018
paper lacks: a maxDF posting cap + relaxed clusters (i=2).** One-sided noise (noisy fragment vs
clean canon) → signature survival (1−p)² instead of (1−p)⁴ — the method's sweet spot (two-sided at
CER 5% ≈ 44% skip-gram survival; at 15% ≈ 7% — fragile frag↔frag, fine frag↔canon). Output:
**canonical span labels per page** (feeds Track 2 masking) + direct identifications ("unknown page =
Mishnah Shabbat 3"; "quotes Bavli Berakhot"). This REPLACES `canonical_filter.py` — the existing
asset is an **exact-match pickle** of 2.8M normalized 5-word chunks (121 MB): one HTR error in 5
consecutive words kills the lookup. Reusable as a day-1 labeling pass only.

**Track 2 — fragment ↔ fragment (discovery, the engine):** direct **inverted index over DF-banded
char-5-grams** (positions kept; per-language drop DF > ~5K and DF=1 singletons), canonical spans
**masked at the character level before insertion** (the commentary half of a half-quote stays in).
Candidate pair = **≥2 shared seeds within a ±20-char diagonal band** (BLAST two-hit heuristic). At
~700M char positions and ~30% post-band keep-rate, exact posting lists (~2–3 GB) are tractable in
RAM — approximate LSH is solving a problem we don't have; exact is debuggable + append-incremental.
**Union in Shmidman word-codes as a second cheap candidate generator** (~200 lines) — catches
orthographic-variant pairs exact char-grams miss; the two generators fail differently. Masking-
before-insert (not label-after) dissolves the liturgy bucket explosion structurally.

**Verifier — the arbiter:** banded local alignment (edlib / parasail / rapidfuzz — none in
requirements yet) → span offsets, aligned length, **coverage-of-the-SHORTER-side**, edit density.
Acceptance start: ≥25 aligned chars ∧ edit density ≤0.3, ROC-tuned on Tier-1 vs random pairs.
Refinement: **confusion-weighted substitution costs** from the measured HTR confusion matrix
(ד/ר, ב/כ, ה/ח…). Score accepted spans Tesserae-style: inverse-frequency of shared material ×
density. Aggregate window/span hits → fragment-pair evidence via diagonal binning. Short pages
(<40 tokens = disproportionately the *unidentified* fragments we care about): whole-page unit,
acceptance relative to shorter side, explicit short-page eval stratum.

**Externals:**
- **Passim v2** (Python/PySpark, github.com/dasmiq/passim; char n-grams default n=25 → lower to
  ~10–15, `--floating-ngrams`, `--min-match 3`, lower `--min-align`, tune `--maxDF` down;
  `series`=sys_id; KITAB/OpenITI Arabic recipe is the template; CHR 2024 Syriac paper = exact
  precedent for "HTR → align vs e-text corpus → identify the work") — **week-1 recall floor + eval-
  pool contributor, NOT the engine** (Turku measured Passim at ~⅓ of BLAST's recall on noisy OCR).
- **BLAST** (Turku textreuse-blast, aclanthology W17-0510): phase-2 fallback if custom recall
  disappoints. Hebrew's 22 folded letters map 1:1 onto the amino alphabet more cleanly than the
  Latin cases it was built for; proven recall to ~60% char error; cost = custom-compiled NCBI BLAST
  + heavy CPU (Turku burned ~150K core-hours on 1.95M pages).
- **Embeddings — deferred.** Off-the-shelf dense retrieval does NOT beat lexical for verbatim-ish
  reuse under noise (every published "win" is on paraphrase/allusion gold sets); degrades from ~5%
  WER; MiqraBERT's 87%→<9% narrative→poetic recall cliff (arXiv:2606.19638) is the standing warning
  for piyyut. If lexical recall plateaus on Tier-1: **MsBERT (dicta-il/MsBERT — manuscript-domain,
  67M words, Genizah-evaluated; Hillel is a co-author) + contrastive fine-tuning on synthetically
  HTR-noised pairs weak-labeled by Track 2 itself.** Never a bigger multilingual model. Legitimate
  quarantined side-channel later: Hebrew↔JA translation pairs / heavy paraphrase, separately labeled.

## Canonical-text handling (user's key requirement — now structural)
Two tracks, two indexes (replaces mask-vs-label agonizing): Track 1 *identifies + labels* canon
(separate channel — "quotes Bavli Berakhot" is a finding, canonical-copy identification is a win);
Track 2 *never sees* canonical characters (masked pre-insert) so frag↔frag similarity is scored on
distinctive shared wording only. DF-banding independently suppresses residual formulaic shingles.
Keep a whitelist path for wanted-but-formulaic classes (piyyut incipits) searchable deliberately.

## Evaluation (revised)
- **Fix 1 — Tier-1 positives:** same-work pairs with non-overlapping passages are unfindable BY
  CONSTRUCTION. Run the verifier once over all known same-work pairs and split: **Tier-1 = pairs
  with a verified shared span** → the recall denominator; the remainder reported as a corpus fact
  ("X% of same-work pairs share no wording"), not a miss. This reconciles the joins tension: fjms
  joins are gold for *same manuscript*, meaningless for *shared passage* until verifier-filtered.
- **Positives inventory (verified counts):** fjms `joins` — 14,517 join-groups ≥2 members (8,852
  pairs; 20,136 AlmaIds). fjms `catalog.GenizahTitleId` — 555 title-groups spanning ≥2 MSS (74,467
  MSS carry a title id; canonical-heavy: Bible=38,708 — best for canon labeling/negative mining).
  PGP multi-fragment documents — 1,609 (physical joins). FGP `title_he` — 23,249 labeled
  transcriptions (257 works) + `domain` (Liturgy 4,185 / Bavli 3,626 / Piyyut 2,687) for strata.
- **The confusion-matrix asset (missed by the original seed):** **45,034 human FGP transcriptions**
  (99.9% sys_id-resolved) + PGP transcriptions cover pages the HTR also covers. Align HTR vs human →
  **empirical CER + confusion matrix per script/genre**. Powers: (a) synthetic-noise eval at
  5/10/20/30% CER sampled from REAL confusions, (b) confusion-weighted alignment costs, (c)
  data-driven normalization decisions. Run this in week 1 — it's independent of everything else.
- **Precision:** graded labels (verbatim / near-verbatim / paraphrase / shared-formula / topical /
  unrelated — binary looks artificially bad); stratified sampling ~200 pairs per score×language×
  length band (±7% CI); TREC-style pooling — every system (custom, Passim, TF-IDF baseline,
  any embedder) contributes top-k to the pool BEFORE judging; keep qrels reusable.
- **One exhaustively-annotated micro-domain** (~1–2K pages, one composition or series) → single
  "complete-truth" ARI / pairwise-F1 number (Silcock pattern).
- Report per genre × language × CER band × length; unknown hits stay "unverified until sampled."

## Week 1 — the separability probe: ✅ RUN 2026-07-06 — VERDICT: GO
Full results: **`same_work_spike/probe/PROBE-RESULTS.md`** (code in `same_work_spike/probe/scripts/`).
Pilot = 17,228 pages (1,393 Birkat-Hamazon witness pages from the Shmidman BH index docx — 471/484
sigla resolved — + joins + title-groups + FGP-overlap + 10K random background). Headlines:
- **Candidate recall 1.00 on all three GT families** (joins/titles/BH) — the seed stage lost nothing.
- **Empirical letter-CER 16–20%** (209 HTR↔FGP-human aligned pages; י↔ו/ד↔ר/ב↔כ dominate) —
  high-noise regime confirmed; the discarded LSH design would have been blind here.
- **Verifier is the only loss point**: density ≤0.30 sits at the two-sided noise floor; BH witness
  connectivity 24%→**69%** as density 0.30→0.45. Fix = sloped length×density acceptance + per-genre
  calibration + confusion-weighted costs (matrix measured, in `results/confusion_matrix.json`).
- **DF-banding HELPS liturgical recall** (anchor-budget effect) — whitelist worry dissolved.
- **Joins share wording in 1% of groups** (corpus fact; eval design confirmed). Title groups: 64%.
- **New stage-0 classes found**: exact 997…-sys_id catalog duplicates (same IE/P/FL), microfilm
  target-sheet/catalog-card false parallels.
- **Unplanned discovery**: two background MSS of the same halakhic work (אונאה text) auto-linked.
- **Scale frontier**: candidate volume (31.7M pairs at pilot; two-hit needs diagonal-keyed
  accumulation before the 948K-page run; Python dicts → numpy/Rust postings).
**Round 2 (same day):** diagonal-keyed distinct-gram two-hit candidates (recall stays 1.00;
volume 31.7M→11.4M; `engine.build_diag_pairs`/`verify_span`) + fitted acceptance boundary —
**density ≤0.30 under 100 letters, ≈0.39–0.42 above** (`results/roc_boundary.md`); production
profile recall = joins 1.00 / titles 0.984 / BH 0.974; **BH witnesses connected 241/428 (56%)**
vs 82 at flat 0.30. Remaining: stage-0 module → 200-pair graded precision sampling (focus the
100–300-letter × 0.35–0.45 overlap region) → 100K numpy scale rehearsal → Track 1 + full corpus.
**Round 3 (2026-07-07):** first 19 human grades on the FP-frontier stratum: **68% canonical /
16% formula / ~11% spurious** — routing, not precision, is the issue; Track-1 masking absorbs
most of it. NEW required verifier feature — **flank-contrast classifier** (Hillel's heuristic):
align ~150-letter flanks of every accepted span; flanks-align → same-work; flanks-dissimilar →
ISLAND → quotation/formula; island∧non-canon = citation of a non-canonical work (indirect
witness — the highest-value class). Review tool: `same_work_spike/probe/review/review.html`.
Next steps (ordered, handoff-ready) in PROBE-RESULTS.md §Next steps.

## The 10 decisions that matter (start values + cheap validation)
| # | Decision | Start | Validate by |
|---|---|---|---|
| 0 | Positives | Tier-1 via verifier over all known pairs | one cheap pass, thousands of pairs |
| 1 | Objective | seed-and-extend, not set-similarity | seed-count vs Jaccard separation on 50+50 known pairs |
| 2 | Unit | char windows/positions (spacing-noise-proof) | synthetic spacing-noise recall, token vs char |
| 3 | Seed length | char n=5 exact (n=4 fallback view) | sweep n=4–7: hit-recall vs volume |
| 4 | DF band | drop DF>~5K per language + DF=1 | known-pair seed recall flat while volume −10× |
| 5 | Two-hit filter | ≥2 seeds, ±20-char diagonal | candidate precision pre-alignment |
| 6 | Views | base + matres-light UNION, one set | ΔJ on known plene/defective pairs |
| 7 | Canon | two-track; mask chars pre-insert; Track 1 vs clean canon | posting-length dist; canon-ID accuracy on identified liturgy |
| 8 | Stage-0 dedup | strict LSH/exact-hash near-dup FIRST | manual check 50 top clusters |
| 9 | Verifier | edlib banded; ≥25 chars ∧ density ≤0.3; coverage-of-shorter | ROC on Tier-1 vs random |
| 10 | Language | char-freq ID; per-language DF + strata | 100-page manual check |

## Strategic context (weigh BEFORE building)
Text-based intertextuality over this exact corpus is a **funded, roadmapped, not-yet-published
MiDRASH work package** (ERC Synergy 101071829; Avi Shmidman = NLP PI — already this seed's domain
steer). The Haifa group (Miller/Kuflik/Lavee) has published the HTR-noise-tolerant reuse machinery
since 2021 (LDK 2025; Applied Sciences 2025 — SW + embedding + HTR-typo correction, +11% F1;
benchmark arXiv:2512.23504: Dicta F1 0.78 vs Passim 0.62 on clean Midrash). CHR 2024 Syriac paper
(ceur-ws.org/Vol-3834/paper110.pdf) = step-by-step precedent for "HTR → align vs e-texts → identify
the work." **All published Genizah join/identification work is image-based (Wolf/Dershowitz IJCV
2011 → FGP join suggestions; "Bag of Bags" 2026) — the textual lane is open.** Position this spike
as Dicta's contribution to / coordination with the MiDRASH work package, not a parallel effort that
gets scooped within months.

## Build posture
Clean **`same_work_spike/`** pipeline: read `Transcriptions.txt` → stage-0 hygiene → normalize
(union view) → Track 1 (canon ID + span labels) → Track 2 (DF-banded seed index → diagonal two-hit)
→ verify (alignment) → emit pair evidence / spans / canon labels to SQLite or Parquet. Reuse
`corpus_mapper` *infrastructure* (ResultsDatabase checkpointing, LibrariesDB, logging) — NOT its
engine, NOT its exact-match canonical pickle (labeling pass only). **Compute: the dev box (12C/24T,
63 GB RAM) holds everything in RAM; do NOT run on prod EC2** (memory-constrained, hosts the web
app). Inner loops must be numpy-vectorized or Rust-backed; pure Python dies on ~10¹¹-op stages.
New deps: edlib or rapidfuzz (+parasail optional); PySpark only if the Passim baseline runs locally.
No NiceGUI/PyQt integration this cycle. Internal milestone, no version bump, until green.

## Requirement themes (draft → REQUIREMENTS.md at trigger)
1. Stage-0 corpus hygiene (empty pages; near-dup/V0.7-V0.8; same-sys_id exclusion; language ID).
2. Union-view normalization (NFC; nikud/diacritics; final-letter fold; space-strip; matres-light union).
3. Track 1 canon identification (Shmidman skip-grams + maxDF + i=2 vs clean Maagarim/Sefaria; span
   labels; direct canonical-copy identification; separate reporting channel).
4. Track 2 discovery engine (DF-banded char-5-gram inverted index; canon-masked; diagonal two-hit;
   Shmidman-code second generator).
5. Alignment verifier (edlib/parasail; span/coverage/edit-density; confusion-weighted costs; short-
   page handling).
6. Evaluation harness (Tier-1 verifier-filtered positives; FGP/PGP confusion-matrix asset; synthetic
   CER bands; graded labels + stratified sampled precision; pooling; micro-domain ARI; strata).
7. External baselines (Passim v2 week-1 recall floor; TF-IDF pilot baseline; BLAST phase-2 option).
8. Go/no-go: the week-1 separability probe gates all scale engineering.
9. MiDRASH/Dicta coordination checkpoint before (or alongside) the spike.

## Pointers (verified 2026-07-06)
- Corpus: `Transcriptions.txt` (root, 1.47 GB; `==> ID <==` page records). Older dump `AllGenizah_OLD.txt`.
- `corpus_mapper/` — reuse `runner.py::ResultsDatabase` (SQLite + checkpoints), `LibrariesDB`,
  logging; `canonical_filter.py` (exact-match pickle, `corpus_mapper_output/canonical_fingerprints.pkl`,
  2.8M chunks / 121 MB) = labeling pass only.
- Normalization today: `shared/text_normalize.py` (`strip_nikud`, `strip_search_diacritics`),
  `shared/search_tokenizer.py` (`hebword`). Missing: NFC, final-letter fold, matres view, space-strip.
- Ground truth: `fist_data/fjms_enrichment.db` (`joins`: AlmaId/JoinGroupId; `catalog.GenizahTitleId`),
  `pgp_data/pgp.db` (`document_fragments` multi-fragment docs; transcriptions), `fgp_data/
  fgp_transcriptions.db` (45,034 human transcriptions; `title_he`/`domain`). Root DB files are 0-byte stubs.
- Key references: Shmidman-Koppel-Porat arXiv:1602.08715 · Silcock arXiv:2210.04261 · Passim
  github.com/dasmiq/passim (+KITAB kitab-project.org/methods/text-reuse) · Turku BLAST
  aclanthology.org/W17-0510 · MsBERT aclanthology.org/2024.ml4al-1.2 · Midrash-reuse benchmark
  arXiv:2512.23504 · CHR-2024 Syriac ceur-ws.org/Vol-3834/paper110.pdf · Loci Similes arXiv:2601.07533
  · MiqraBERT arXiv:2606.19638 · corpus Zenodo 10.5281/zenodo.17734473.

## Open questions (remaining at trigger time)
- Final seed length n + DF cap values (week-1 sweep decides).
- Whether the Shmidman second generator earns its keep frag↔frag, or stays Track-1-only.
- Matres-light union: keep or drop after the plene/defective validation.
- The MiDRASH coordination outcome — complement, contribute, or coordinate-and-defer.
- Naming + release posture (proposed: internal, no bump) — unchanged.
