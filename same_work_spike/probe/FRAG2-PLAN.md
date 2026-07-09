# FRAG-2 PLAN — recall-first, probability-graded small-fragment discovery + Map v2

**Date:** 2026-07-09 · **Status:** REVISED per Codex plan-gate (verdict REVISE,
4 BLOCKER + 8 HIGH — all folded in; see "Codex revision log" at bottom)
**Supersedes/extends:** `FRAG-ID-PLAN.md` (goal pivot doc). Findings base:
`SYNTHESIS-AND-PLAN.md`, `results/frag1_truncation.md`,
`results/frag1_grades_analysis.md`, `results/discovery_grades_analysis.md`,
`results/residue_naming.md`, `results/a2_work_query_full_report.md`.

## Governing directive (Hillel, 2026-07-09)

1. Goal is **same-work witness finding**: more granularity, more precision AND
   recall, identify **small/fragmentary** fragments.
2. The graded findings so far were **large-text-biased by construction**
   (most-copied residue ranking; highest-coverage new? exemplars). "Many of the
   new discoveries will be of smaller texts, and **we prefer recall over
   precision — maybe with probability grade — to find new items**."
3. Reference universe is CLOSED: Maagarim + Sefaria + JA only.

Design consequence: keep the **census** at the precision-first boundary
(tier A, unchanged), and add a **discovery tier (tier B)**: accept far down the
confidence scale, stamp every candidate with a **calibrated P(same-work)** and
a novelty signal, rank recall-first lists by probability instead of dropping
near-misses. Validated basis: Hillel's density_fail grading = 10/10 near-misses
correct; crop precision 20/20 at the 60–100-letter knee; decoy FDR machinery
already built (A5, `probe_conformal_fdr.py`).

---

## Step 1 — CAL-1: probability calibration (PILOT now; FINAL re-fit on the
## frozen Map-v2 state)

**Codex BLOCKER fold-in (ordering):** the deployed model must be calibrated on
the EXACT corpus/text/reference state Map v2 queries (ref_corpus v2, version
groups, FGP-preferred text). So CAL-1 runs TWICE from the same script:
- **PILOT (now, pre-REF-2):** validates the machinery, produces preliminary
  curves and operating-point brackets, and surfaces stratum divergence. All
  outputs labeled PILOT.
- **FINAL (inside step 3, after the REF-2 freeze):** identical script re-run on
  the frozen v2 index/query path → the model tier B actually deploys. Cheap
  (~1 h), fully scripted.

**Goal:** a calibrated, validated mapping `P(same-work | density, length)` +
measured wrong-work contamination at every candidate-tier threshold. This is
the instrument that makes "recall over precision" navigable.

**Method (new script `scripts/cal1_calibration.py`, reusing
`frag1_truncation.py` infra — build_reference / query_batch):**

1. **Labeled candidate generation (synthetic truncation, ground truth known).**
   Sample ~1,000 source pages (stratified by cat, round-robin across works)
   that have EXACTLY ONE distinct live Track-1 work (`shadowed_by IS NULL`,
   best_density ≤ 0.15, span-coverage ≥ 0.85) and **crop ONLY inside the
   verified matched span** (spans_json; largest span, ≥340 letters) — label
   purity by construction, closing the embedded-quotation contamination
   channel (Codex BLOCKER 2). Crop lengths {40, 60, 80, 100, 150, 200, 300} ×2.
   Query each crop against the reference index at a WIDE verification cutoff
   (0.75) and record **ALL distinct-work candidates — no top-K censoring**
   (Codex HIGH: rank caps bias contamination downward; deployment rank policy
   is applied downstream, consistently). Row = `(crop_id, crop_len, alen,
   density, is_correct = candidate work_id == labeled work_id)`. No
   title-equivalence in truth labels (Codex HIGH: track1_bib matching is a
   catalog heuristic, not an equivalence relation); version-group identity
   replaces raw work identity only in the FINAL run, after REF-2 builds
   explicit version groups.
2. **Null arm (decoys).** Chunk-shuffle each crop (CHUNK=25, A5 protocol)
   through the IDENTICAL query path → chance-candidate rate per length.
   Reported SEPARATELY from wrong-work contamination (Codex HIGH: decoys
   measure chance alignment only, not related-work confusion — the labeled
   arm's wrong-work rows are the real hard negatives).
3. **Leave-work-out arm (Codex BLOCKER 3 — the "work absent" failure mode).**
   For every crop, drop its true work's candidates post-hoc (identical to
   removing the work from the index for the accept surface) → per length ×
   threshold: how often a WRONG work would be accepted when the right one is
   absent. This is the mis-attribution rate the 82%-no-reference orphan
   population actually faces.
4. **Fit.** Per crop-length bin: weighted PAVA isotonic (non-increasing in
   density) of `is_correct` on density, **each true WORK contributing equal
   total weight per bin** (Codex HIGH: raw rows overweight candidate-rich
   works) + **work-granular train/holdout split**. Pooled fit first; per-cat
   (Bible/Bavli/Maagarim/JA) empirical precision reported at the pooled
   operating points — if strata diverge materially, Map v2 deploys per-stratum
   curves (candidate cat is deployment-known) or the conservative lower
   envelope (Codex HIGH: 2 features may be too thin — checked, not assumed).
5. **Stress test vs Hillel's graded cards (NOT a reliability sample —
   Codex HIGH: those grades are selection-biased and plan-adaptive).** The
   density_fail cards (10/10 correct just above the boundary) must get HIGH
   predicted P; discovery new_sample cards (33/34 correct-work) likewise.
   Proper reliability = the holdout split now + a FRESH BLINDED deck drawn
   stratified from tier-B probability buckets after Map v2 (grading reviewer
   already built).

**Known bias, stated up front:** crops come from full-page HTR of pages good
enough for Track-1 to label — real orphan fragments are noisier at equal
length, so P values are an **optimistic bound**. Mitigations: (a) stress test
vs real graded cards; (b) optional noise-injection arm (measured 16–20% CER
confusion matrix) if the stress test shows material miscalibration; (c) the
fresh blinded post-Map-v2 deck is the true reliability instrument.

**Deliverables:** `data/p_calibration.json` (+ raw rows),
`results/p_calibration.md` (wide-cutoff recall ceiling, isotonic curves,
holdout reliability, contamination + leave-work-out + decoy sweeps, proposed
tier-B operating points). Read-only vs fullcorpus.db; no map tables touched.

---

## Step 2 — REF-2: reference ingestion + novelty cross-checks (data prep)

Everything the rebuild consumes; no map mutation yet.

1. **Ingest REF-1 Stage-1 acquisitions** (refs_staging/): Targum
   Onkelos/Jonathan, statutory liturgy, Sefaria gap texts — license manifest
   kept; version-groups formed by stream near-dup detection (title equivalence
   only as candidate filter).
2. **Reference-gap works from residue naming** (~19 clusters:
   `results/residue_naming.md`): look up each named work (tafsīr al-alfāẓ
   al-ṣaʿba, Ibn Janāḥ Kitāb al-Afʿāl, JA Esther qiṣṣa, Qiṣṣat Ḥanna, …) in
   Maagarim/Sefaria/JA per the CLOSED universe; works absent from all three are
   logged as unrecoverable-by-reference (stay discovery-tier). Hillel's 6
   COMPETING-cluster decisions (same work vs parallel works) gate how their
   references are grouped.
3. **מסירה cross-check table.** Parse `##המסירה:##` headers across the 8,233
   Maagarim files → normalize shelfmarks → match against libraries.csv
   call-number variants → `(work_id, sys_id, confidence)` known-witness table.
   **Confidence tiers (Codex MEDIUM: naive normalization over-demotes —
   collapses adjacent call numbers, ignores folio/range detail):** exact
   library+collection+classmark(+folio where present) = high → auto-demote;
   classmark-only / range-overlap = low → flag for review, never auto-demote.
   Consumers: new?-queue demotion (fixes the 6/34 leak Hillel found) + the
   **"relocated source-of-edition" report** (Wertheimer class): new?/new?known
   rows whose work's מסירה shelfmark is unlocatable/lost.
4. **Stage-0 additions:**
   - **FGP-preferred text** with hard consistency rules (Codex HIGH: silent
     coverage corruption): the indexed text becomes `search_text` with
     `search_len` + `provenance` (fgp|htr) columns; **every downstream
     coverage/length/span computation reads search_text, never pages.text**,
     and span offsets are search_text offsets. Substitution gated by a
     page-level FGP↔HTR sanity check (length ratio + shared-gram overlap;
     fail → keep HTR, log) (Codex MEDIUM).
   - **Two-page-merge detection** (two distinct work-labels on disjoint
     halves + FGP page-count disagreement where FGP exists): flagged pages are
     **excluded from tier A** (kept, flagged, in tier B) — no destructive
     splitting in v1 (Codex MEDIUM).

**Deliverables:** ref_corpus v2 (with version groups + provenance),
mesirah_witnesses table (tiered), stage-0 flags + search_text provenance.
Each ingestion batch gets a small extract report (counts, dedup hits, license
notes). **REF-2 completion = the FREEZE point: FINAL CAL-1 runs here.**

---

## Step 3 — MAPV2: the rebuild (one overnight, Codex-gated, checkpointed)

Single consolidated rerun so every lever multiplies the others:

0. **FINAL CAL-1** on the frozen REF-2 state (identical script; version-group
   identity as truth relation) → the deployed p_calibration model.
1. Rebuild Track-1 index over ref_corpus v2 (ref-side canonical masks v2 where
   applicable). Rerun Track-1 full (667K pages, search_text). Then — order is
   load-bearing (Codex BLOCKER 4) — **tier assignment FIRST, shadowing on
   tier A ONLY**:
   - **Tier A** = current boundary → `track1_matches` → greedy shadowing (A4
     closed) → census. Tier-B rows can never shadow or displace a tier-A row.
   - **Tier B** = below the boundary down to calibrated **P ≥ p_B** (operating
     point from FINAL CAL-1) → **separate `track1_candidates` table** with P
     stamped, non-destructive. Census consumers NEVER read it (Codex HIGH:
     existing consumers read track1_matches live rows unfiltered — tier B must
     be physically elsewhere, not a column flag).
2. Demotions: bib_signal (existing) + מסירה cross-check (new) + canonical/
   Scripture guard (a bare canonical-category match cannot identify a
   non-canonical work — kills the Bible→colophon class).
3. Track-2 canonmask rerun + A2 work-query pass + motif-query pass (all
   checkpointed; sequential BelowNormal; Start-Process detached — PC-crash
   lessons codified).
4. Liturgy-agglomeration handling: the 17 flagged residue clusters + a
   liturgy-share flag on passage units (MARC-keyword heuristic from
   residue_naming) so unit-level products can exclude/split them; masking
   decision per-unit, not global.
5. **Headline product — small-fragment discovery list:** tier-B candidates +
   unit residue, filtered to SHORT spans / few-witness works, each row =
   fragment, proposed work, P(same-work), novelty signal
   (rarity-gated: witness-count threshold + no bib/מסירה/known-witness match),
   link. Ranked by P × novelty, delivered as a grading-reviewer deck (reusing
   the discovery reviewer UI) so Hillel's verdicts feed calibration round 2.

6. **Tier-A delta report (Codex LOW):** v1→v2 census diff attributing changes
   to (a) new references, (b) FGP text, (c) demotions — so boundary stability
   is separable from corpus/text changes.
7. **Fresh blinded grading deck** drawn stratified from tier-B probability
   buckets (the true reliability sample; existing grades are stress tests
   only).

**Gates:** PILOT CAL-1 report reviewed by Hillel (operating-point bracket) →
REF-2 freeze → FINAL CAL-1 → Codex code review of the rebuild scripts →
launch. Compute: Track-1 full ~4–5 h, Track-2 ~35 min, A2 ~hours; overnight
window.

## Risks / open items

- CAL-1 optimism bias (stated above; bounded by stress test + fresh blinded
  post-Map-v2 deck; noise-injection arm if stress test fails).
- Interleaved Bible+Targum/Tafsir pages: single-label sampling excludes them
  from calibration; their identification depends on REF-2 Targum ingestion.
- Version-group explosion in statutory liturgy: rarity gate keeps the
  discovery queue from flooding (REF-1 binding semantics).
- Two-page-merge: flag-only in v1 (destructive splitting deferred; flagged
  pages excluded from tier A).
- The 6 COMPETING clusters need Hillel's call before their refs are grouped.
- no_reference sizing still under-graded (2/12 — Codex MEDIUM): more
  no_reference cards graded (hard, needs images + expertise, Hillel's pace)
  before REF-2 gains are interpreted as recall-recovered vs discovery-only.

## Codex revision log (plan-gate, 2026-07-09 — verdict REVISE, all folded)

- BLOCKER 1 (calibrate on deployed state) → CAL-1 split PILOT/FINAL; FINAL
  runs at the REF-2 freeze inside step 3.
- BLOCKER 2 (crop-label ambiguity) → crop only inside verified spans,
  coverage ≥0.85, density ≤0.15, exactly-one-work pages.
- BLOCKER 3 (work-absent failure mode invisible) → leave-work-out arm.
- BLOCKER 4 (tier B shadows tier A) → tier assignment before shadowing;
  shadowing on tier A only; tier B in a separate table.
- HIGH: no top-K censoring in calibration rows; per-work weighting in the
  isotonic fit; no track1_bib equivalence in truth labels (version groups at
  FINAL); decoys reported separately from wrong-work hard negatives; Hillel
  grades = stress test not reliability; search_text/provenance consistency
  rule; per-stratum divergence check.
- MEDIUM: mesirah confidence tiers; FGP sanity gate; merges excluded from
  tier A; more no_reference grading before REF-2 interpretation.
- LOW: tier-A v1→v2 delta report.
