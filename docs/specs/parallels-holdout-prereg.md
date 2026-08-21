# Pre-registration: parallels holdout run — SHELVED BY OWNER RULING, holdout unspent

**Status: SHELVED (owner ruling, 2026-08-21).** The owner decided the public
wording for the passage method will be QUALITATIVE rules of thumb — (a) it is
fast; (b) it can potentially find witnesses the incumbent misses and/or be
more precise per result; (c) the two methods complement each other — hedged
language that does not require certified measurement. No certified claim is
planned, so this pre-registration is NOT scheduled to run.

Everything below is kept intact and current: the holdout split is UNSPENT,
the ledger's reservation discipline is live, and the tooling (strict paired
analyzer, hardened scorer, sys-grain deck builder, depth probe) is committed
and tested. If a certified claim is ever wanted, freezing this document is
the only sanctioned path — do not design a new one.

Historical status line: v2 DRAFT, post-Codex; v1 was reviewed externally
(CHANGES-REQUIRED) and every blocking finding is resolved in this revision or
in the code it pins.

## What this run decides — and what it deliberately does not

The product decision is already made on tune evidence and does not wait for
this run: **passage ships as a second selectable method in both apps, no
default flip, surface designed so a union view is a natural extension.**

What the holdout licenses is the **published wording** — `/help`, release
notes, the methods page. Every claim below is phrased as it would be
published, each clause with a measured endpoint. A claim whose endpoint is
not met is not published; the feature ships either way.

## Instruments and inputs (exact artifacts, full hashes)

| input | identity |
|---|---|
| passage index | `full_v1/spool`; manifest sha256 `8b01f9e26af8494239faa117a5acb2645b13245264ad08dac76867378cd5ad60` (layout 1, normalizer 1, 702,466 records, 593,785,092 postings) |
| passage policy | `standard-40`, policy_id `pp1-dfd44076cf548ea5` |
| incumbent config | `chunk:3:exact:100`, equal-eligibility (`-elig`), the shipping `search_composition_logic` |
| FGP query set | `fgp_queries_lang.jsonl`, sha256 `77ac7a52c96c350bfaa590d8e6d26a8c1f4f9d265b95e00daa206783d650d792`, 19,090 queries |
| witness query set | `witness_queries_v2_lang.jsonl`, sha256 `de4cc6e45d91aea873370a98affafb240aabf4ddcba2439c7f0fa86c68098bbe`, 2,164 queries, 573 works, deduped |
| split rule | group-aware (query id up to first `#`), salt `v1`; pinned by `tests/test_retrieval_eval_split.py` |
| expected cells | `docs/specs/parallels-holdout-cells-fgp.json`, `-witness.json` (committed, passed verbatim to the analyzer) |
| analysis code | `scripts/eval_methods.py`, `shared/retrieval_eval.py`, `scripts/analyze_paired_outcomes.py`, `scripts/score_grading_deck.py`, `scripts/build_grading_deck.py`, `scripts/probe_exclusive_depth.py` — all at the freeze commit |
| bootstrap | seed 20260821, 10,000 resamples, clusters = work (query id before `#`) for retrieval, query for deck cards |

Config provenance: `standard-40` vs `flat-25` identical on tune; `chunk_size=3`
beat 5 by 13 recall points on tune. Chosen on tune, not revisitable after the
freeze.

**"language" means classifier-assigned language** (Codex #4). The classifier's
measured operating point is recall 0.868 / FPR 0.0076 on 600-character windows
(`scripts/detect_query_language.py`); the negative control (an all-Hebrew set)
labelled 0.9% `ja` against a fitted 0.76%. Implications, stated up front:
assigned-`ja` is ~98.8% truly Judeo-Arabic, but ~9% of assigned-`he` is truly
Judeo-Arabic, so a deficit specific to true-JA queries would be diluted, not
hidden, in the `he` cell. Published wording says "classifier-assigned"; the
`unknown` label is excluded by construction (the stratified draw picks only
`he` and `ja`). A blinded manual label audit is NOT pre-registered; if one is
ever run it is reported as exploratory.

## Sampling — declared before drawing, sized from measured tune discordance

Measured on tune (n=300 each): FGP discordance@50 = 31.3% overall (he 35.7%,
ja 25.0%), diffs he +8.2 / ja +0.9 points; witness discordance 20%, diff −0.7.

- **FGP holdout: n = 1,400 — equal allocation, 700 `he` + 700 `ja`**, drawn by
  the runner's `--stratify-by language --strata-values he,ja` (equal-allocation
  sampler, fatal on under-fill; commit `61b4d51a`). Power: at ja's measured
  discordance 0.25 and diff +0.9, the expected one-sided LB is ≈ −1.8 points —
  inside the 3-point margin with buffer. Proportional sampling would have left
  the class undecidable.
- **Witness holdout: the full split, --limit 0** (~1,080 queries; exact n
  recorded at run time). Power is TIGHT and said so: at discordance 0.20 and
  diff −0.7, expected LB ≈ −2.9 against −3. If it lands NOT SHOWN, the witness
  parity clause is simply not published — an accepted outcome.
- **Grading deck: 60 evenly drawn holdout FGP queries, `--grain sys`** (one
  card per (query, manuscript); the tune deck was record-grain and carried 27
  extra-page cards — Codex #8), `per_method_k = 3`, pooled, deduplicated,
  blinded, hash-shuffled, salt `deck-holdout-v1`. The deck key retains
  `is_source` per card. The grader may stop at any point; a prefix of the
  shuffled order is analysed as a near-random draw (stated in advance this
  time). Minimum 150 graded, ENFORCED by the scorer (`--min-graded 150`).
- **Depth probe: `scripts/probe_exclusive_depth.py`** (committed, tested,
  deterministic) over the graded exclusive cards, both grains. Its report
  wording is fixed: "not returned by this configured retriever under its
  internal caps" — never "unretrievable" (both methods have internal caps;
  Codex #11).

## Endpoints — the published claims and their tests

All decisions use the paired analyzer's UNROUNDED bootstrap lower bounds
(`analyze_paired_outcomes.py --strict --expect-cells <file>`); its strict mode
makes a mismatched query set or A/B strata disagreement fatal, and any
missing/undersized declared cell yields `overall_verdict: INSUFFICIENT-BLOCKED`
(Codex #3, mutation-proven).

**C1 — speed.** Published: "Passage answers in under a second at the 95th
percentile on held-out benchmarks; the incumbent's median on the same queries
is over five seconds." Endpoints: passage p95 < 1,000 ms on BOTH instruments'
holdout runs, AND incumbent p50 > 5,000 ms on the same runs (both clauses
measured; Codex #10 — no unmeasured clause may appear in the sentence).

**C2 — precision per returned result, scoped to non-source manuscripts.**
Published: "When the passage method returns a manuscript OTHER than the one
the query was transcribed from, blinded grading finds the same text or a
paraphrase of it in ≥ [measured]% of cases; the incumbent's corresponding rate
is [measured]%." Endpoints, all on QUERY-CLUSTERED bootstrap intervals (cards
of one query are correlated; Codex #7): passage non-source strict LB ≥ 0.80;
passage non-source strict LB > incumbent non-source strict UB. Source and
non-source populations are reported separately (`is_source`; Codex #9); tune
showed non-source 0.958 vs 0.450, so the split strengthens rather than
launders. "strict" is always glossed "same text or a paraphrase of it".
No sentence about WHAT the incumbent's other returns are (formulas,
quotations) is published unless the graded relation counts are shown next to
it (Codex #10).

**C3 — recall parity (classifier-assigned language).** Published: "On held-out
benchmarks the two methods retrieve attested material at statistically
indistinguishable rates, overall and within classifier-assigned Hebrew and
Judeo-Arabic strata." Endpoints: paired recall@50 difference (passage −
incumbent), one-sided 95% cluster-bootstrap LB ≥ −0.03 (unrounded), required
in: FGP overall, FGP `language=he`, FGP `language=ja` (cells file, min_n 700
each). The witness instrument's parity clause is separate and gates on its
overall cell only (min_n 1,000): its other strata are REPORTED, not gating —
the plan's protected classes were length/genre/language/CER, of which the
witness set supports none non-degenerately (length is 100% one band, language
99% one value). A gating cell with adequate n whose LB misses the margin
kills its clause; an INSUFFICIENT cell blocks the whole claim (fail-closed,
never silent narrowing).

**C4 — complementarity.** Published: "The methods overlap substantially and
each also finds same-text witnesses the other does not; a combined view finds
more than either alone." Endpoints on the graded holdout deck's same-text
union at sys grain: each method's exclusive share ≥ 10%, AND the shared
(both-methods) share ≥ 15% (Codex #10 — without this, zero overlap would have
passed a claim asserting overlap). The depth probe qualifies the wording
(misses vs demotions) and is reported at both grains; it has no numeric gate.

**Not claimed, ever:** absolute recall; catalogue-yardstick evidence;
precision deeper than the graded depth; anything about true-language strata
beyond the classifier-assigned qualification above.

## Procedure (each step's exact command is pinned at the freeze commit)

1. Freeze commit fills the block below. Ledger checked: zero holdout entries.
2. FGP run: `eval_methods.py --split holdout --stratify-by language
   --strata-values he,ja --limit 1400 --ledger ... --dump-outcomes
   fgp_holdout_outcomes.jsonl` (reservation is automatic and precedes the
   first query; `--baseline` is refused by the runner on holdout).
3. Witness run: same, `--limit 0`, no stratification, dump to
   `witness_holdout_outcomes.jsonl`.
4. Analysis: `analyze_paired_outcomes.py --strict --expect-cells <cells file>
   --dump <dump> --a passage-standard-40-pp1-dfd44076cf548ea5
   --b chunk-c3-exact-f100-elig --k 50 --seed 20260821` per instrument.
5. Deck: `build_grading_deck.py --grain sys --salt deck-holdout-v1` over the
   FGP holdout sample; its `prereg.json` (with full input fingerprints — Codex
   #6) and manifest hashes are recorded in the results section before grading
   begins. Owner grades ≥150 of 240.
6. Scoring: `score_grading_deck.py --deck-dir <dir> --verdicts <export>
   --min-graded 150` (deck-id + key-hash verification, duplicate/orphan/
   unknown-grade fatal — Codex #7). Depth probe per above.
7. Results land in `docs/specs/parallels-method-comparison.md` under
   "Holdout (pre-registered)"; any deviation from this document is recorded
   there as a deviation, with the reason.

Machine cost estimate, accepted in advance: ≈ 13 h (FGP) + ≈ 4 h (witness) of
incumbent time, ≈ 10 min total of passage time, ≈ 35 min deck build.

## Freeze block — filled only by the freeze commit

- [ ] Freeze commit: `________________`
- [ ] Ledger checked immediately before freeze: zero holdout entries
      (reservations or records).
- [x] Stratified-by-language sampling implemented and smoke-tested
      (`61b4d51a`); ledger reservation implemented and mutation-proven
      (`f7cc129d`); analyzer strict mode, scorer hardening, sys-grain deck,
      depth probe committed with 56 passing tests and 36 killed mutations
      (`93e08e08`, `b8e8e4a4`, `50c2b4ad`, `3982666f`).
- [x] Tune-side measurements this document's sizing relies on are recorded in
      `parallels-method-comparison.md` (discordances, per-language diffs).
- [ ] The Phase 145 merge is NOT a freeze dependency and may land before or
      after; the harness files above must be untouched between freeze and run.
