# Pre-registration: parallels holdout run — DRAFT, NOT YET FROZEN

**Status: DRAFT.** Becomes FROZEN by a dedicated commit that fills in the
freeze block at the bottom. Until then nothing here licenses anything, and the
holdout stays untouched (the write-once ledger enforces that mechanically).

## What this run decides — and what it deliberately does not

The product decision is already made on tune evidence and does not wait for
this run: **passage ships as a second selectable method in both apps, no
default flip, surface designed so a union view is a natural extension.** The
tune evidence: statistically indistinguishable recall on both instruments,
40–50× speed, non-overlapping precision intervals, and a complementarity
structure in which 71% of passage's exclusive same-text finds are structurally
unretrievable by the incumbent while most of the incumbent's exclusive finds
are merely demoted in passage's list.

What the holdout licenses is the **public wording** — `/help`, release notes,
the methods page. Every claim below is phrased exactly as it would be
published, with its endpoint and its failure mode. A claim whose endpoint is
not met is not published; the feature ships either way.

## Instruments and inputs (exact artifacts)

| input | identity |
|---|---|
| passage index | `full_v1/spool`, layout_version 1, normalizer_version 1, 702,466 records, 593,785,092 postings |
| passage policy | `standard-40`, policy_id `pp1-dfd44076cf548ea5` |
| incumbent config | `chunk:3:exact:100`, equal-eligibility (`-elig`), the shipping `search_composition_logic` |
| FGP query set | `fgp_queries_lang.jsonl`, sha256 `77ac7a52c96c350b…`, 19,090 queries, `language` stratum |
| witness query set | `witness_queries_v2_lang.jsonl`, sha256 `de4cc6e45d91aea8…`, 2,164 queries, 573 works, deduped |
| split rule | group-aware (query id up to first `#`), salt `v1` — `shared/retrieval_eval.py`, pinned by `tests/test_retrieval_eval_split.py` |
| analysis code | `scripts/eval_methods.py` + `shared/retrieval_eval.py` at the freeze commit |

Config provenance: `standard-40` vs `flat-25` were identical on tune;
`chunk_size=3` beat 5 by 13 recall points on tune. Both choices were made on
tune data, which is what tune exists for; neither may be revisited after the
freeze.

## Sampling — declared before drawing

- **FGP holdout:** n = 500, **stratified by `language`: 250 Hebrew + 250
  Judeo-Arabic**, evenly spaced within stratum (the eval harness's spacing
  rule). Proportional sampling would leave the Judeo-Arabic class underpowered
  (~185); language is a protected class and gets equal allocation.
- **Witness holdout:** n = 500 evenly spaced (Hebrew-only by construction;
  language claims never come from this instrument). Analysis clustered by
  work: bootstrap resamples works, not queries.
- **Grading deck:** 60 evenly drawn holdout FGP queries, `per_method_k = 3`,
  pooled, deduplicated, blinded, hash-shuffled — same builder, salt
  `deck-holdout-v1`. The grader may stop at any point; a prefix of the
  shuffled order is a near-random draw and is analysed as such (lesson from
  the tune deck, where the "spread" description was wrong).
- **Depth probe (automated, no grading):** every exclusive same-text card is
  probed against the other method's full uncapped list, exactly as in the
  2026-08-21 re-examination. "Missed" is reported at both grains: not-in-top-3
  and absent-from-full-list.

## Endpoints — the published claims and their tests

All intervals 95%. Paired analyses use the per-query outcome dumps
(`--dump-outcomes`); the bootstrap is seeded (seed 20260821, 10,000
resamples), clustered by work on the witness instrument.

**C1 — speed (expected to pass trivially).**
"Passage answers in well under a second; the incumbent takes seconds to
minutes." Endpoint: passage p95 < 1,000 ms on both instruments' holdout runs.

**C2 — precision per returned result.**
"When the passage method returns a manuscript, it is almost always the same
text (strict precision ≥ 0.9 on blinded grading); the incumbent's top results
are real matches but mostly formulas and quotations on formulaic queries."
Endpoint: passage strict (`same_text`+`paraphrase`) Wilson lower bound ≥ 0.85
on the holdout deck; incumbent strict upper bound < passage lower bound.
Vocabulary and `REAL_STRICT` definition identical to the tune deck; the label
"strict" is always glossed as "same text or a paraphrase of it".

**C3 — recall parity (the wording behind "finds the same texts").**
"On held-out benchmarks the two methods retrieve attested witnesses at
statistically indistinguishable rates." Endpoint, per instrument: paired
recall@50 difference (passage − incumbent), one-sided 95% bootstrap lower
bound ≥ −3 points, in EVERY protected class: length band and language on FGP;
length band and witness band on witness. Multiplicity: the claim is published
only if all classes pass; a class with fewer than 100 holdout queries blocks
by insufficiency and the published wording then names the classes it covers.
Power note: at n=500 paired with discordance ≤ ~15%, a true tie yields a lower
bound around −2.6 points, inside the margin; the 250-query language strata
carry roughly ±4-point resolution and may block by insufficiency — that is an
accepted outcome, not a reason to relax the margin.

**C4 — complementarity.**
"The methods find overlapping but distinct sets of same-text witnesses; the
combined view finds more than either alone." Endpoint: on the holdout deck's
graded same-text union, each method's exclusive share ≥ 10%, and the depth
probe run and reported at both grains. No numeric gate on the probe — it
qualifies the wording ("misses" vs "ranks lower"), it does not pass/fail.

**Not claimed, ever:** absolute recall (unmeasurable — the corpus's witnesses
are unknown); anything sourced from catalogue yardsticks; precision deeper
than the graded depth.

## Procedure

1. Freeze (block below), commit.
2. One run per instrument: `eval_methods.py --split holdout` with the exact
   configs above, `--dump-outcomes`, ledger armed. The ledger's write-once
   rule makes a second scoring of the same (config, split, query set) raise.
3. Build the holdout deck, owner grades ≥ 150 of 240 cards (or to exhaustion).
4. Analysis script reproduces every published number from the dumps + the
   graded export; numbers land in `docs/specs/parallels-method-comparison.md`
   under a "Holdout (pre-registered)" heading.
5. Any deviation from this document is recorded in that section as a
   deviation, with the reason.

## Freeze block — empty until the freeze commit fills it

- [ ] Freeze commit: `________________`
- [ ] Ledger checked immediately before freeze: zero holdout rows.
- [ ] Chain results reviewed (suite green, witness v2 + FGP tune runs sane)
      with NO harness change required. Any harness change reopens the draft.
- [ ] Stratified-by-language sampling implemented in the harness and
      mutation-tested (the current sampler is even-spacing only).
