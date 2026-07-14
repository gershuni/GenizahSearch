# MAPV2-15m — Flank / citation-vs-witness detector: implementation plan

Grounded in the recon workflow (`flank-plan-recon`, 5 agents) + Codex design
gate (`CODEX-CRITIQUE-flank-realign.md`) + confirmed DB facts.

## Goal
For each discovery-bucket match `(page, work_id, page-side spans)`, decide
whether the page is a **same-work witness** (its text *continues* the work past
the matched span ⇒ a real "this fragment contains work X" find) or an
**embedded citation/quote/formula** (the span is an island in otherwise
different-work text ⇒ not a discovery). Emit an **advisory** verdict + score
multiplier — never a hard filter. Preserve the fragmentary tail by abstaining
when there isn't enough text to judge.

## What EXISTS (reuse) vs what must be BUILT
Reuse (verbatim):
- `normalize.norm_stream(text)->(stream,offsets)` (letter stream + 1:1 NFC
  offset map); `project_span(offsets,s,e,nfc_text)` for readable evidence.
- `build_smoke_preview2.RefText` (loads a work's SOURCE text + stream by
  work_id; `passage()` = readable ref window) and `snippet(text,spans)` (page
  side + matched stream slice). Used for the human-facing evidence line.
- Reference STREAM by work_id for relocation: load `ref_corpus_v2.pkl` once →
  `{w['id']: w['stream']}` (main session only; agents may not). The pkl stores
  the stream (all we need for alignment); RefText is only for readable display.
- `idiom_search.gram_anchors(stream,grams)` (exact k-gram start positions =
  the re-seed primitive) and `best_window_edits` (near-anchor windowed
  Levenshtein — starting point, but O(anchors·slack·L), no banding).
- `engine_np._gram_codes` (K=5 base-27 codes) if we need fast anchoring.
- `classify_canonical_edges.overlap_frac / merge_intervals` (merge page spans
  with gap<15; flank-vs-canonical overlap).
- `work_query.accept_density(len)->0.30/0.386/0.418` (two-sided HTR boundary);
  the CER context (micro 20% / median 16.6%) that sets the "wide ambiguous
  band" rationale.
- `mapv2_deck.flank_class(pstream,p0,p1,wstream)` — the EXISTING page↔ref flank
  fn (relocate via rapidfuzz `partial_ratio_alignment` + equal-length flanks).
  **We augment it, not replace: add edge-gap tolerance + reconvergence.**
- `score_validation.w(no)` post-stratification weight; `build_validation_cards`
  blind-card + freeze pattern.
- `discovery_score.py` is the fold point (docstring already reserves
  `discovery_flank.py`).

Must BUILD (the recon's "gaps"):
1. **Banded, edge-gap-tolerant semi-global aligner** of two letter streams
   (the one true missing primitive — Codex's key fix). Returns best aligned
   continuation block (length, norm-edit, leading-gap) allowing an unmatched
   edge gap ≤ G on either side.
2. **Ref-side span relocator**: given page span text + target work stream,
   return up to N credible `(r0,r1,edit)` relocations (k=5 gram anchors →
   diagonal vote → banded verify), keeping multiples (duplicate passages).
3. **Flank reconvergence test** (uses 1+2): walk ≤220 letters outward each side
   (reverse left), tolerate edge gap ≤60, label side continuation / ambiguous /
   island / short-edge.
4. **Multi-span monotone chain** (preserve page-order AND ref-order; discordant
   gaps ⇒ ambiguity, not citation).
5. **Whole-page target support (B)**: adjusted coverage incl. recovered
   continuation blocks.
6. **Competing-work flank (C)**: query `track1_candidates` for OTHER works
   covering the flank regions (data confirmed: up to ~90 works/page, page-offset
   spans), with title/author equivalence bucketing + canonical-competitor
   exclusion.
7. **Decision rule + abstention gate.**
8. **`discovery_flank.py`** score fold + the **calibration harness**.

## Algorithm (Method A primary, B supporting, C confirmatory)
Per `(page_id, work_id)` from `data/discovery_scored_gated.jsonl` (bucket2 ∈
{discovery} first; extendable):
0. Load page stream (`pages.text`→norm_stream) + `spans_json` (page offsets
   `[[s,e,dens],…]`); merge spans with gap<15 → span chain.
1. **Relocate** each merged span in the target work stream (ref_corpus_v2
   stream): k=5 grams from the trimmed span core (fallback k=4), diagonal vote
   (bin ~25), banded verify → keep relocations with span norm-edit ≤0.38 (≤0.42
   for <80 letters) & ≥2 anchors. Keep ALL close; **if any credible relocation
   shows continuation, do NOT demote.**
2. **Monotone chain** the spans on (page,ref); pick the best chain; flanks =
   outside its first/last span.
3. **Flank reconvergence** each side: ≤220 letters out (reverse left), edge gap
   ≤60, best continuation block (min 60, pref 80-120). Side = continuation
   (block≥60 & edit≤0.42; weak≤0.48 w/ ≥2 anchors & gap≤40) / ambiguous
   (0.48-0.58) / island (>0.58, enough text) / short-edge.
4. **B**: adjusted target coverage = union(orig spans ∪ recovered blocks) ÷ page
   letters. Positive witness if page≥200 & cov≥0.55, or aligned-outside-span
   ≥120 letters. Low coverage alone ≠ citation.
5. **C**: other-work coverage of the flank regions from track1_candidates.
   Strong C = non-equivalent, non-canonical competitor (p_same_work≥0.65 or
   matched_letters≥100) covering ≥90 letters outside target span or ≥45% of a
   judged flank. Canonical competitor never triggers C for a non-canonical
   target; a non-canon competitor on a CANONICAL target = strong citation.
6. **Verdict:** target_continuation (any strong reconvergence OR strong B, no
   stronger contradictory C) / likely_citation_strong (no continuation AND ≥2
   independent negatives: both-island; island+strong-C; strong-C-both-flanks +
   low B on long page) / mixed_multiwork (continuation + strong other-work →
   route separately) / abstain (else).
7. **Score fold** (advisory, disc_score2 unchanged): add flank_verdict,
   flank_strength, flank_multiplier ∈ {strong-cont 1.10, weak-cont 1.04,
   abstain/ambiguous/mixed 1.00, weak-citation 0.75, strong-citation 0.45},
   flank_evidence_line, `disc_score2_flank = disc_score2 * flank_multiplier`.
   Strong citation may route to an "Other/citation review" facet but STAYS
   visible with its score+evidence.

## Abstention (fragmentary-safe — Hillel's hard constraint)
Abstain (multiplier 1.00, never penalize) when: relocation fails/ambiguous;
page text outside the span chain <120; no side has ≥60 usable page AND ref
letters; only one side island w/o strong C; any ambiguous side; edge with one
weak flank; only canonical/common-source competitor. Short fragments may still
be BOOSTED by a clear continuation, never demoted for lack of flank.

## Calibration & validation (leakage-free)
Derived flank target from the 8-class grades (no human flank label exists):
- **must-not-demote (continuation)** = {discovery, witness, known}
- **desired citation demotion (island)** = {citation, shared, formula, norel}
- **acceptable abstain** = {tsarich} + short/fragmentary
Sets (join by card_no / no):
- **132 gold** (discovery/witness-enriched; dist discovery26/witness33/shared30/
  known18/citation12/tsarich8, formula0/norel0): used to CALIBRATE + enforce the
  **false-demotion constraint**. Hard target: **rescue the 49 must-not-demote
  cards the naive island label buries** (26 witness+13 discovery+10 known);
  allow ≤1 false strong-demotion of a same-work witness.
- **held-out 100** (corpus base rate; known34/shared31/citation17/discovery7/
  formula5/witness3/tsarich2/norel1): FROZEN, run ONCE, post-stratified via
  `score_validation.w`, for honest corpus citation precision/recall +
  discovery-safety + abstention rate + top-slice rank churn.
Protocol: grid-search on 132 ONLY (flank_min {50,60,80}, gap {40,60,80},
continuation {0.40,0.42,0.45,0.48}, island {0.56,0.58,0.62}); objective = max
citation recall s.t. ≤1 false witness-demotion; Wilson intervals; stratify by
length/flank-availability. FREEZE thresholds+multipliers. Run the 100 once. If
it fails, revise on 132 + new dev data, never on the held-out.
Pre-freeze spot-check: the 'shared' grade is mapped to island, but a
shared-source page can still be a genuine witness of the *sharing* work — hand
-check a few 'shared' cards' derived labels before trusting them.

## Risks / pitfalls & mitigations
- **Maximal-span boundary** (the whole reason naive flank fails, proved by
  continuation 0/88) → edge-gap ≤60 reconvergence.
- **Unequal-flank distance floors at the length ratio** (documented fix_flanks
  bug) → clip both sides to equal length before scoring.
- **REF_DF_CAP=128 raw cap drops a repetitive single work's postings; seg pos
  is 12-bit (<4096)** → do NOT reuse `build_ref_index` for one whole target
  work; relocate directly against the work stream (gram_anchors + banded verify),
  no segment packing.
- **Recension divergence ≠ citation** → bounded reconvergence + wide ambiguous
  band + never demote on one side alone.
- **formula/norel island-demotion is UNCALIBRATED on dev** (132 has zero of
  both; they exist only in the held-out, which can't tune) → lean on the
  coverage/short-flank heuristics for them; report separately; do not overfit.
- **Circularity with the canon_mass penalty already in disc_score2** → flank
  stays an independent signal; canon-only competitor never adds a citation
  penalty.
- **Duplicate photographs inflating witness counts** in validation → apply
  `stage0` dedup (line_agreement/same_shelf/fl_of) when counting witnesses.

## Files & artifacts
- `scripts/flank_align.py` — the banded edge-gap aligner + relocator (new
  primitives; unit-tested on synthetic continuation/island pairs).
- `scripts/discovery_flank.py` — orchestrates A+B+C per match, writes
  `data/discovery_scored_flank.jsonl` (adds flank_* + disc_score2_flank) +
  `results/discovery_flank_report.md`.
- `scripts/calibrate_flank.py` — grid-search on 132 (derived labels), freeze
  `data/flank_thresholds.json`, then score the 100 held-out once →
  `results/flank_calibration.md` (rescue-of-49, false-demotion, held-out
  precision/recall, abstention, rank churn).

## Scope / perf
Run over the discovery-bucket rows (~27,678), prioritizing the ranked top
(disc_score2≥0.15). Relocation reuses cached per-work ref streams; page texts
batched from the DB (main session). Expected minutes, not hours.

## REVISIONS folded after Codex pre-flight (verdict was REVISE → now build-ready)
Full pre-flight in `CODEX-PREFLIGHT-15m.md`. All 6 required changes applied:
1. **Rejoin spans + page text from the DB.** `discovery_scored_gated.jsonl` does
   NOT carry `spans_json` (discovery_score.py wrote only its `keep` fields). The
   flank pass reads spans + page text fresh from `track1_matches`/
   `track1_candidates` + `pages` (main session).
2. **C = `track1_matches` UNION `track1_candidates`.** Candidates is tier-B only;
   tier-A competitors live in `track1_matches`. Union both; tier-A rows lack
   `p_same_work` → treat as strong (they are the confirmed tier).
3. **Local gap-aware span merge.** `merge_intervals` merges only overlaps, and
   stored `spans_json` is already merged at gap≤30 — so treat stored spans as
   coarse and apply a local merge (gap<15) only for chain assembly; don't claim
   merge_intervals does gap<15.
4. **Calibration set = 127 human-graded cards** from the 132-card deck (5
   ungraded: 14,15,16,17,24). Distribution sums to 127. Exclude the 5.
5. **`score_validation.w` is not import-clean** (runs at import) → reimplement
   the post-stratification weight (`frame_cells[cell]/samp[cell]`) locally in
   `calibrate_flank.py`; do not `import score_validation`.
6. **Multi-span evidence.** `snippet()` returns only the longest span's slice and
   `RefText.passage(wid,page_slice,pad=40)` takes that slice — so build the
   evidence line per span/recovered-block, not from snippet's single slice.

Open-question resolutions folded into the algorithm:
- **Relocation perf:** cache per-work k-gram positions via `_gram_codes`, cap
  very-frequent in-work grams, verify only top diagonal windows (no naive
  `gram_anchors` in the hot loop on long works like Mishneh Torah).
- **Banded aligner recurrence:** enumerate leading edge shift `delta ∈ [-G,G]`,
  banded Levenshtein around the shifted diagonal, band = `ceil(thr*L)+slack`
  (edit-cutoff based), trailing ends free, endpoint block ≥ min_len.
- **'shared' safety:** spot-check ALL 30 dev `shared` cards before freezing the
  derived island label; keep `known` as a dev-only false-demotion safety class.
- **formula/norel:** report separately, require the same strong-evidence rule,
  never tune thresholds from held-out formula/norel.
- **Leakage:** never use the old broken `flank_class`/`flank_dist` as features;
  freeze on the 127 dev cards only; run the 100 held-out once.

## Open questions for Codex pre-flight (ANSWERED — see CODEX-PREFLIGHT-15m.md)
1. Is relocating directly against the full work stream (gram_anchors + a new
   banded verify) sound vs reusing the segmented index — any missed scale/perf
   trap for very long works (e.g. Mishneh Torah)?
2. The banded aligner: band width and the edge-gap search — DP band = ±(gap+
   slack)? Preferred concrete recurrence to avoid O(n·m).
3. Is the grade→flank-target mapping safe for 'shared' (spot-check size), and
   how to treat 'known' (already demoted by the identified-gate — does it even
   reach the flank pass)?
4. Given formula/norel are uncalibrated on dev, should strong-citation demotion
   be withheld for those classes until more dev data, or trusted via coverage?
5. Any additional leakage risk in deriving the target from grades that the
   detector's own features (coverage/canon) also depend on?
