# Codex thorough code review — MAPV2-15 corpus-wide annotation pipeline

You gave the DESIGN gate for this pipeline earlier (verdict: PASS-WITH-CONDITIONS;
see `C:\Genizahsearch\same_work_spike\probe\results\CODEX-CRITIQUE-corpuswide-annotation-v2.md`).
This is now a **code review + assessment** of the implementation. Be thorough and
adversarial: find correctness bugs, statistical/methodology flaws, leakage, and places the
code does not match its claims. Do NOT modify files. Do NOT open any `data/*.db` or the large
`data/*.pkl`. Read-only.

## What the pipeline does (one paragraph)

Scale a scholar's 132 hand-grades (8 classes: discovery / witness / known / shared / citation
/ formula / norel / tsarich) to ~218k candidate page↔reference-work matches. Design:
(1) a catalog-only "metadata scope" detector, (2) a frozen stratified audit sample drawn from
the RAW frame, (3) a two-stage grader = narrow deterministic rule vetoes + AI adjudication on
the residual, (4) canonical-quotation ("shared") detection via a rarity-weighted match against
the Maagarim classical canon. Discoveries are never auto-accepted (abstention). The scholar's
132 gold is used to REPORT agreement only; we do not tune the rules to it, and the frozen 467
audit sample is never tuned against.

## Files to review (all under C:\Genizahsearch\same_work_spike\probe\)

1. `scripts/metadata_scope.py` (15a) — ScopeGate: per-ms regime (single_work / homogeneous_
   anthology / miscellany / ambiguous) + per-page tri-state resolution, from catalog metadata
   ONLY (NLI title + FJMS GenizahTitleOrgTitle + NumFolio). n_matched_works is only a weak
   leave-target-out tie-break (must not be circular). Reuses TitleGate from `scripts/title_gate.py`.
2. `scripts/build_audit_sample.py` (15b) — frozen stratified sample from the raw match frame
   (track1_matches non-shadowed, matched_letters>=40 = 218,680) BEFORE stitching/routing.
   Strata = genre bucket x match size x stitch status; records scope/title/bib/resolution;
   deterministic sha1 draw; frame cell sizes stored for post-stratification; component_key +
   page_text_hash for leakage-safe splits. Output data/audit_sample_v1.json (frozen).
3. `scripts/grader.py` (15c) — rule_grade() narrow vetoes (known via catalog TITLE only;
   statutory-unit witness; canonical-quotation -> shared) + full_grade() = rule else AI;
   measure() scores the full grader vs the 132 gold (AI layer = the already-produced critic
   grades, reused so no new model calls); frame() runs the rule tier over the 467 audit sample
   with post-stratified corpus estimate. _canon_scores() fetches spans from track1_candidates
   (the discovery pool; a prior version wrongly used track1_matches).
4. `scripts/shared_source.py` (15d) — CanonIndex: per-page canonical intervals from track1's
   OWN Bible/Talmud identifications + overlap_frac. (SUPERSEDED by canon_rarity for the deck;
   still present. Assess whether it should stay.)
5. `scripts/build_canon_corpus.py` (15e) — ingest the local Maagarim export (Bible + Talmud +
   Midrash) via the Academy classification xlsx (סוגה column), fine-cat by title, normalize
   to the matching stream. Midrash is ingested but held OUT of the mask (discovery-bearing).
6. `scripts/canon_rarity.py` (15f) — CanonRarity: IDF-weight each canon 8-gram over canon
   works (crc32 keys, cached); span rarity-mass/len; is_canonical() at SHARED_TH=1.5.

## Measured results the code should support

- Grader vs 132 gold: full agreement 77% (98/127); DISCOVERY recall 96% (25/26) / precision
  78% (25/32). Rarity rule at TH=1.0 dropped discovery recall to 88% and flipped 6 witness ->
  shared; TH=1.5 flips 0 discoveries — chosen for discovery safety.
- Scope detector vs gold: 26/26 discoveries fall in ms_scope_ambiguous (0 leak into demoting
  tiers).
- Canon corpus: 393 works, 14.3M CANON letters (Bavli 6.67M / Yerushalmi 3.55M / Mishnah /
  Tosefta / Bible); rarity separation shared median 1.11 vs discovery 0.22.
- Frame (467): rules settle 49%, residual 50%.

## Review asks (be specific; cite file:line)

1. **Correctness bugs** in any of the 6 files — off-by-one in span/interval math, the
   deterministic sampling floor/top-up logic in build_audit_sample.py, the largest-span
   selection, norm_stream offset assumptions (spans_json offsets are into norm_stream(page)[0]
   — verify that assumption is safe), crc32 collision risk in canon_rarity (7M distinct
   8-grams in 32-bit space), IDF over only 96 canon works (Bavli/Yerushalmi/Mishnah/Tosefta/
   Bible) — is df over 96 documents a sound rarity model?
2. **Statistical / methodology soundness**: is the post-stratification weighting in frame()
   correct (weight = frame_cell / sampled_in_cell)? Is the audit sample's stratified draw
   biased in a way that breaks the weighting? Is component_key sufficient for leakage-safe
   splits, or are there leakage paths (shared page_text_hash across sys, stitched runs)? Does
   measuring the grader on the 132 gold while the AI layer (critic grades) was itself produced
   with knowledge of those cards constitute leakage / optimistic bias in the 77%?
3. **Grader logic**: are the rule vetoes actually "narrow high-precision" (the known-via-title
   rule still mislabels 2 witness as known; the canon rule at 1.5 still flips a couple)? Is
   full_grade defaulting residual to 'tsarich' when the AI grade is missing sound? Should the
   threshold choice (1.5) be treated as a tuned hyperparameter that biases the reported 77%?
4. **Scope detector**: is the regime logic genuinely non-circular (n_matched_works gated)? Are
   the confidence numbers meaningful or decorative? Does resolution() correctly implement the
   tri-state you specified (page_resolved_known can veto; global_ms_likely argues-against-
   discovery; ms_scope_ambiguous weak-only)?
5. **Canon ingestion**: the fine_cat() title heuristic buckets 297/393 works as Midrash — is
   that mapping sound, and does mis-bucketing a Bavli/Mishnah tractate into Midrash (excluded
   from mask) silently lose canonical coverage? Any encoding/normalization risk reading the
   Maagarim files?
6. **Overall assessment**: is this a sound, honest foundation to build the corpus-wide
   annotation on, or are there load-bearing flaws? What are the top 3 things to fix before
   trusting a corpus-wide run? End with a one-line verdict: SOUND / SOUND-WITH-FIXES (list) /
   REWORK (what).
