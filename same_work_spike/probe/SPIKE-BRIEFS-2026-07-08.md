# SEED-029 Spike Briefs — 2026-07-08 (wave 1 + wave 2)

Execution briefs for the unblocked spikes from `SYNTHESIS-AND-PLAN.md`. Each brief is
handed to one implementation agent. This file goes through Codex review BEFORE dispatch.

## Global context (applies to every spike)

- **Working dir:** `same_work_spike/probe/scripts/` — run everything as
  `python -X utf8 -u <script>.py` (Hebrew console output breaks otherwise).
  Reports go to `../results/*.md`, review pages to `../review/*.html`.
- **Main DB:** `../data/fullcorpus.db` (3.1 GB, gitignored) — tables: `pages`
  (667,411 rows), `track1_matches` (Track-1 identification rows; **column
  `shadowed_by` — NULL = live row**. NOTE: `track1_match.py` CREATEs the table
  WITHOUT this column; `track1_shadow.py:36-38` adds it via ALTER TABLE — any
  fresh Track-1 rebuild drops it until shadowing reruns, so consumers MUST use
  the PRAGMA compat gate. **`density` is a DISTANCE — LOWER is better**;
  acceptance boundaries are upper bounds), `pairs_*` (Track-2 accepted pairs,
  see `rehearsal_run.py` PAIRS_TABLE), `passage_units_*` / `passage_unit_members_*`
  (see `passage_units.py`), `motifs_pilot` / `motif_members_pilot` (see
  `motif_pilot.py`), `motif_query_hits` + `motif_query_ckpt` (see `motif_query.py`).
- **Liturgy subcorpus:** `../data/liturgy.db` (456 MB) — same schema, 139,694 pages;
  its `track1_matches` copy may PREDATE the `shadowed_by` column (extraction ran
  before shadowing) — always use the PRAGMA compat gate pattern from
  `track1_testimonies.py:113-118`.
- **Reference corpus:** `../data/ref_corpus.pkl` via `track1_build_ref.py` (8,233
  Maagarim + 92 JA works; includes Tanakh and the canonical corpus). Maagarim
  source txt lives under Dropbox and needs the `\\?\` long-path prefix to open.
- **Catalog metadata:** `fist_data/fjms_enrichment.db` at repo root (the copy in
  root dir `fjms_enrichment.db` is a **0-byte stub — never use it**); `libraries.csv`
  (col 0 = sys_id, col 7 = Hebrew title); title-equivalence logic (translation /
  acronym / JA↔Hebrew classics) already exists in `track1_bib.py` (`title_bucket2`
  and friends) — REUSE, don't reinvent.
- **Graded data:** `../review/grades_hillel_2026-07-07.json` (164 pair grades,
  1 spurious) + Track-1 ID grades (58/3/0) in `../results/track1_id_grades_round1.md`.
- **Compute discipline:** `mask_ref_canon.py` (PID 2592) is running until late
  tonight. Light SQLite reads/writes and per-page analyses are fine. Do NOT launch
  full-corpus index scans / multi-hour jobs in wave 1. Anything >30 min estimated:
  deliver the code + a scoped verification (e.g. on `liturgy.db` or a sample) and
  flag the full run for the wave-2 queue.
- **Do NOT git-commit.** Leave changes in the working tree; the orchestrator
  reviews and commits. Do not modify files outside your spike's scope.
- **Report contract:** every spike ends with `../results/<spike-id>_report.md`:
  what was done, numbers, examples, verdict vs the acceptance gate, open issues.

---

## Wave 1 (dispatch now, parallel, file-disjoint)

### A1 — `shadowed_by` filter into remaining consumers

**Goal:** all live consumers of `track1_matches` respect competitive span
assignment; mask-side consumers get an explicit semantic decision, not a blind patch.

1. Patch with the compat-gated live filter (mirror `track1_testimonies.py:113-118`):
   - `df_damage.py:43` (census query)
   - `passage_units.py:208` (member-role assignment)
   - `map_with_ref_edges.py:79` (reference-edge layer)
2. **Do NOT blindly patch** the mask-side consumers — `rehearsal_run.py:86`
   (mask-span loading), `mask_severity.py:52`, `classify_canonical_edges.py:93`.
   Write a short semantic analysis in the report instead: for MASKING, a shadowed
   canonical span is still real text on the page (the 11QT case: Deut spans won,
   11QT rows shadowed — but if a canonical row is shadowed BY an edited work,
   masking it anyway is conservative). Recommend keep-all-spans vs live-only per
   consumer, with the 11QT and ibn-Tibbon cases traced as evidence.
3. Rerun `df_damage.py` on fullcorpus.db (it's a read-and-report script; should be
   minutes) → regenerate `results/df_damage_full.md`; diff the 0%-cohort list
   before/after in the report. If `passage_units.py` full rerun estimate >30 min,
   verify the patch on `liturgy.db` instead and flag the full rerun for wave 2.

**Acceptance (per-script, before/after captured in the report):**
- `df_damage.py` (its own filters: `matched_letters >= 200`, ≥2 witness MSS):
  total pages/works counted before vs after; the delta must be explainable as
  shadowed-row removal (11QT-class and translation-double-count works LOSE
  members; e.g. מגילת המקדש should drop sharply from 176-MSS scale). Regenerated
  `df_damage_full.md` 0%-cohort diffed against the old one.
- `map_with_ref_edges.py` (excludes canonical rows + suspect ≥150-MSS webs):
  edge count before vs after + directional explanation.
- `passage_units.py` (Track-1 used for label propagation only): unit/member
  structure UNCHANGED; only labels may change — assert unit and member counts
  identical, report label-change count.
- Mask-side decision documented with the 11QT + ibn-Tibbon cases traced.

### A3 — interleaved Bible+Targum/Tafsīr class probe

**Goal:** confirm (or refute) that the top unidentified motif-query gainers are
verse-by-verse interleaved Bible+translation texts, and estimate the harvest of
aligned Hebrew-verse↔JA/Aramaic-block pairs (this feeds the RamBERT gold set).

1. Get the top ~10 unidentified gainer motifs (reuse `growth_unidentified.py` /
   `build_growth_review.py` logic; the +91/+47/+45/+40 gainers are the suspects).
2. Sample 2–3 member pages per motif (~20–25 pages). For each page pull the full
   text from `pages` and ALL `track1_matches` spans (including shadowed and
   citation-tier rows — interleaved pages defeat the testimony tier, but verse-level
   Bible spans should exist as low-coverage rows; check `spans_json`).
3. Classify the BETWEEN-span text — **on the ORIGINAL page text from `pages`,
   never on `norm_stream`** (normalization strips/garbles spacing; see
   `normalize.py` docstrings). Cues: Judeo-Arabic (particles פי/מן/אלדי/אלא,
   definite אל־), Aramaic Targum (ית/די/הוו), Hebrew. Every language call gets a
   confidence label HIGH/MED/LOW; particle hits alone are NOT sufficient for
   HIGH — the evidence card must quote the actual text so a human can verify.
   Judge alternation regularity (verse → block → verse → block).
4. Per-page verdict cards in the report (span layout sketch + quoted text +
   language calls with confidence + verdict interleaved/medley/other). Then
   estimate at class level: across ALL
   unidentified gainers, how many pages look interleaved, and how many aligned
   (verse, following-block) pairs are extractable in principle.

**Acceptance:** clear verdict per the kill criterion — if the gainers are NOT
verse-alternation (e.g. florilegia/medleys), say so explicitly and describe what
they are; the class taxonomy is the deliverable either way.

### A4 — set-cover vs greedy shadowing disagreement probe

**Goal:** decide whether the greedy competitive-span-assignment heuristic in
`track1_shadow.py` needs the set-cover/assignment formulation from the ACL wave-2
convergence. **Read `track1_shadow.py` FIRST and match its actual semantics:** the
unit of assignment is the whole `track1_matches` ROW (shadowed via one selected
span; overlap ≥0.6 of the worse row's span; density gap ≥0.03; **density is a
DISTANCE — the LOWEST-density row wins**).

1. READ-ONLY probe: re-derive the assignment input (per page: all rows incl.
   currently-shadowed) and implement an alternative global assignment at the SAME
   unit (whole rows). Objective: minimize total covered-letters-weighted density
   (equivalently maximize coverage quality with lower-distance rows preferred);
   a weighted-interval-scheduling DP or ILP over row-spans is acceptable — state
   the formulation and why it matches/improves the greedy's intent.
2. Compare over **ALL 276,296 rows**, not just the 61,922 currently shadowed: count
   shadowed→live AND live→shadowed flips; pages whose best work changes; per-work
   witness-count deltas (the census-visible effect). Stratify disagreements by
   density gap and overlap fraction.
3. Spot-check: 11QT (must stay ~23 live, not regress toward 176) and the
   ibn-Tibbon-translation vs JA-original class must stay collapsed.
4. Write disagreement examples (10 cards with page text + competing spans).

**Do not UPDATE `track1_matches`** — write probe output to a JSON/aux table.

**Acceptance/kill:** total flips <2% of all rows AND per-work witness deltas
negligible AND no regression on the two known cases ⇒ recommend KEEP GREEDY and
close the item. Otherwise characterize where the global assignment wins and
recommend scope for a v2 implementation.

### A5 — conformal + FDR acceptance thresholds

**Goal:** replace the hand-tuned density boundary with a statistically grounded,
FDR-bounded operating point (ACL wave-2 convergence 4), per-genre.

Design (Codex-revised; justify any deviation in the report):
1. **Null distribution — TARGET-DECOY, candidate-conditioned.** Hillel's grades
   (1 negative / 164) cannot calibrate a null, and a null of random NON-candidate
   pairs is NOT exchangeable with the tested population (accepted pairs are
   selected downstream of seed/DF/two-hit candidate generation + best-segment
   verification — such a null understates FDR). Instead, build DECOY pages:
   chimeric pages assembled from line/window snippets sampled across many
   different same-domain pages (preserves local 5-gram and letter statistics,
   destroys genuine extended reuse). Mix decoys into a small corpus slice, run
   the SAME engine end-to-end (candidate generation + verification), and take
   best-segment (length, density) of decoy-involved candidates as the null —
   candidate-conditioned by construction (the proteomics target-decoy pattern).
   The null must be LENGTH-CONDITIONAL — the existing boundary slopes with
   length for exactly this reason. Report decoy candidate volume; if decoys
   yield too few candidates, that itself bounds the null and must be discussed.
2. **Conformal p-value** per accepted pair = rank of its (length-conditioned)
   density within the null; then Benjamini–Hochberg across the accepted-pair set
   to get FDR-bounded acceptance at q ∈ {0.01, 0.05}.
3. **Stratify by genre/domain** (liturgy vs documents vs other): separate nulls
   per stratum if the distributions differ materially (test this and report).
4. **Validate, don't calibrate, on the human grades:** at the chosen operating
   points, what do the 164 graded pairs and tier-1 ground-truth recall
   (`ground_truth.py` families) look like vs the current hand-tuned boundary?

**Compute scope (HARD, wave 1):** design + implementation + dry run on
`../data/probe.db` (17K pages) ONLY. **Do NOT call `build_candidates`/the spill
engine on fullcorpus.db or liturgy.db in wave 1** — the box is busy and the full
engine is a 35-min/1.5B-hit job. Full calibration = wave 2, queued behind the
running job.

**Acceptance:** on probe.db — a working (length, genre) → threshold pipeline with
a defensible exchangeability argument, validated against tier-1 recall + the
graded pairs; plus a written wave-2 scale-up plan. Or an honest negative result
explaining why the null construction fails.

### B3 — fragmentary-tail catalog auto-validation

**Goal:** mechanize the Yefet-ben-Eli validation pattern over the 1,219
fragmentary-tail motif gains (+1/+2 new members on motifs that had ≤4 MSS).

1. From `motif_query_hits` + the growth analysis (reuse `growth_inspect.py`
   logic), enumerate (motif, new_member_MS) pairs in the fragmentary tail.
2. For each motif, derive its existing identity from Track-1 labels of its OLD
   members (live rows only). **Conflict policy:** members with conflicting
   `author|title` labels → majority label + a CONFLICT flag (report count);
   no labels → MOTIF-UNIDENTIFIED bucket.
3. For each new member MS, pull catalog metadata: `libraries.csv` title (col 7),
   FJMS catalog rows + bibliography from `fist_data/fjms_enrichment.db`
   (AlmaId == sys_id). Missing everywhere → NO-CATALOG bucket (do not guess).
4. Score agreement between motif identity and new-member catalog description.
   NOTE: `growth_inspect.py` keeps labels only as `author|title` strings — the
   agreement scorer does NOT exist yet; build it on top of `track1_bib.py`'s
   equivalence machinery (`title_bucket2`, acronym/translation/JA↔Hebrew pairs),
   don't reinvent the equivalences. Buckets: AGREE / PARTIAL / DISAGREE /
   NO-CATALOG / MOTIF-UNIDENTIFIED.
5. Report: bucket counts; 20 example cards (10 AGREE = validated identifications,
   10 DISAGREE-or-interesting = the discovery queue); ranked CSV/JSON artifact for
   a future review page.
6. **Spot-check gate:** manually verify 10 AGREE + 10 DISAGREE cards by reading
   the page text vs the catalog entry; report the scorer's spot precision.

**Acceptance:** every fragmentary-tail gain classified; AGREE rate + spot-check
precision reported (together = an external precision estimate for motif-query at
the +1/+2 tail).

---

## Wave 2 (prepared now, dispatched after wave 1 / after the box frees up)

### A2 — DF-policy v2: work-keyed second pass (COMPUTE-GATED)

**RE-ANCHORED by A1 (2026-07-08):** the pre-shadow "0% cohort" was mostly
shadowing artifacts (see `results/a1_shadow_consumers_report.md`). Target cohort =
the REGENERATED `results/df_damage_full.md` short-work cohort (60 works, 77%
overall, floor 14–30%: תפילת פסוקים לאחר ערבית 14%, תפילה ליום כיפור 15%, צלותא
21%, קידוש לרגלים 30%) plus the sub-30% ≥20-MSS damaged list (Yannai qedushtaot,
Rambam Mishnah-commentary orders, ראב"ש מלכים א). Feed these works as queries
through the `motif_query.py` mechanics (per-query DF immunity, two-sided
boundary), using each work's REFERENCE text (from ref_corpus) as the query.
Measure: cohort pairing rate before/after (target: sub-30% works reach ≥60%),
added pair volume, precision spot sample (20 pairs). Must run detached
(`Start-Process`), BelowNormal, checkpointed, AFTER mask_ref_canon.py finishes.
Compare against the liturgy-subcorpus-pass numbers where the cohorts overlap.

### A6 — motif v2: community detection on the segment graph

Replace transitive gap-merge chaining with Leiden/Louvain communities over the
segment co-occurrence graph (strict links), targeting the liturgy-pass mega-motif.
Gates: brakhah-level granularity preserved on the known BH motifs (birkat ha-zan /
הטוב והמטיב stay separate); BH acceptance ≥ the 119/71 pilot baseline. Needs
`python-louvain`/`igraph+leidenalg` (pip-install OK). Moderate compute — OK on the
busy box if it stays in-process on liturgy.db.

### B2 — residue mining: most-copied unidentified texts

Rank unidentified passage units by distinct-witness count (units + members tables,
live rows, continuum unit excluded); attempt auto-labels via catalog title
cross-ref (B3's machinery — dependency: reuse its agreement scorer); produce
`results/residue_most_copied.md` + a review HTML with evidence snippets. The 70%-RNL
Karaite liturgy signal should be re-verified on the full-scale data.

### C1-prep — Phase 0 JABERT training data

Extract the JA reuse slice: accepted pairs where both pages are JA-domain, with
density scores as graded labels; mine hard negatives from (a) rejected candidates
in the ambiguous band just below the boundary, (b) shadowed rows, (c) random JA
pairs as easy negatives. Emit train/dev/test JSONL (page-pair texts + score),
stratified by genre, plus a `sentence-transformers` fine-tune script implementing
cosine-regression (MiqraBERT parity) AND Margin-MSE (our upgrade) variants.
Training run itself deferred (GPU unverified). Deliverable: dataset + script +
README; no training.

---

## Post-wave gates

- Codex reviews THIS FILE before any dispatch (plan gate).
- After wave 1: Codex code-diff review of A1's patches (it touches pipeline
  consumers) + spot review of new probe scripts; then orchestrator commits.
- Wave-2 dispatch decision folds in wave-1 findings (A3 verdict reshapes A2/C2;
  A5 thresholds may reshape A2's acceptance gate).
