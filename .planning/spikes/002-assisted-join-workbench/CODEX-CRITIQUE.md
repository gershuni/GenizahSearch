# Joins Lab — Scholar critique + Codex cross-review + validation probes

> Produced during the v8.0.0 Genizah-scholar design-critique session (2026-06-02).
> Triangulates: (a) Claude-as-scholar critique, (b) corrected data probes against the
> REAL indexed corpus, (c) Codex independent evidence-based review, (d) decisive
> validation probes Codex recommended. Brief sent to Codex: `_tmp/codex-joins-lab-brief.md`.
> Raw Codex transcript: `_tmp/codex-joins-lab-response.txt`. Probe scripts: `_tmp/*.py`.

## 0. The error that was corrected mid-session

First-pass probes wrongly measured `pgp.db` (7,395 human diplomatic editions) + FJMS catalog
descriptions. The join hunt actually rides on **`Transcriptions.txt`** (1.47 GB, 948,549 page
records, 216,911 sys_ids) — the Tantivy-indexed corpus. All numbers below are the corrected
probes against the real corpus + the FJMS joins/domains/measurement sqlite tables.

## 1. Corrected evidence (real corpus)

| Measure | Value |
|---|---|
| Pages with line-edge tear markers | 72.4% (42.9% clear the ≥15%-torn bar) |
| Brackets mid-line-only (editorial) | 4.6% — markers are genuinely tear/lacuna, not editorial |
| Whole-line lacunae (`][` etc.) | 33.9% of pages |
| Side mapping: start-`]` vs start-`[` | 2,253,942 vs 273,840 = **8.2 : 1** |
| Side mapping: end-`[` vs end-`]` | 2,410,402 vs 719,286 = **3.35 : 1** |
| Known size-2 joins with BOTH sides transcribed | **85.4%** (neither: 4.0%) |
| Recorded joins literary-only / documentary-only | 89.1% / 7.2% |
| Join group sizes | pairs ~60% of groups but only ~36% of memberships; 450 groups >10 |
| Physical metadata coverage (measurements) | ~90% of catalog; **99.8% of physical-join members** |

**Confirmed: JWB-05's bracket→side rule is inverted.** Corpus + prior research code both use
**start-`]` = left half (beginning torn), end-`[` = right half (end torn)**. JWB-05 says the
opposite and keys off the minority bracket at each position.

## 2. Codex independent review — verdict

> "Your conclusion is broadly right, but sharpen it from 'algorithms fail, compare wins' to
> 'search supplies candidates; the product value is disciplined physical-textual triage.'"

- **JWB-05 bug — agreed (strong).** Add UI nuance: phrase as "text-beginning missing / text-end
  missing" first, then map to physical side (RTL/reading-order confuses users).
- **"Discrimination > coverage" — agreed but I over-worded it.** 85.4% both-transcribed kills the
  coverage objection, but **"seeding works on 85%" is NOT proven** — both sides having text ≠ the
  edge lines are useful, the OCR is reliable, or the true partner is retrievable. POC ceiling stands:
  Recall@50 33–47%, 40% no-parallel. Candidate *generation* is still a hard constraint.
- **Cruel inversion — directionally right, rhetorically too strong.** Drop "Component B is useless
  where joins matter most" → "Component B is *least trustworthy exactly where human confirmation is
  most valuable*." Documentary joins can still benefit from names/formulae/rare phrases/metadata.
- **JWB-08 under-specified — agreed (best critique).** Make it metadata-heavy. Caveat: do NOT make
  dimensions HARD filters early — **fragment width differs between true vertical-tear halves** (each
  half is narrower than the whole leaf). Use evidence badges + soft mismatch warnings.
- **Recto/verso — solid, but MVP must degrade gracefully** (image/page-map messy; don't require it).
- **Unit-of-join — solid + correction.** Pairwise = ~60% of groups but only ~36% of memberships.
  MVP = pairwise same-leaf vertical tear; display all known-join types; don't claim dispersed codices.

**Codex MVP spine:** JWB-01/02/03/04, fixed-05, editable-06, 07, metadata-heavy-08, basic-09.
**JSA:** keep JSA-01 only (manual trigger); **spike JSA-03**; **CUT JSA-02** (corpus completion is
noisy/expensive without a purpose-built positional/completion index).

## 3. Validation probes (Codex-recommended, run this session)

**Probe #1 — side-lean excluding whole-line lacunae (948,549 pages):** clean LEFT 5.8%, clean RIGHT
11.8%, BOTH 13.0%, neither 69.5%. Of torn pages, 57.6% cleanly one-sided (lacunae were inflating
"both"). Cleaner than the first pass.

**Probe #2 — complementary-side prediction on 2,178 known Physical/Partial-Physical size-2 joins
(THE test of JWB-05's value):**
- complementary (clean L+R): **2.5%** (55 pairs)
- consistent (≥1 member "both edges"): **55.3%** (1,204)
- contradictory (L+L or R+R): **3.7%** (80)
- one-side silent (neither): 13.5% ; both-silent: 6.0% ; untranscribed: 19.1%
- **Of the 1,339 pairs where both sides got a torn verdict: complementary-or-consistent 94.0%,
  contradictory only 6.0%.**
- **Reading:** the classifier rarely *misleads* (6% contradictory), but a clean *directional*
  signal ("left half → find right half") fires on only **2.5%** of even known physical joins. The
  dominant actionable verdict is "both edges torn" (55%). So JWB-05 is a **minor, conservative
  assist — not a headline.** Keep it, fix the mapping, make "both edges" a first-class verdict, stay
  silent the ~38% of the time it can't give a two-sided reading.

**Probe #6 — ID alignment (Codex's #1 underweighted risk), sys_id → measurements:** 95.7% of the
transcribed corpus has measurements; **99.8% of physical-join pair members (3,702/3,711)**. Risk
**refuted** — the compare-pane physical metadata will materialize on the exact join targets.

## 4. Converged recommendation (Claude + Codex + probes)

**Reframe (final wording):** *Search supplies candidates; the product value is disciplined
physical-textual triage.* The workbench's job is helping the scholar disambiguate the true physical
partner from textual look-alikes — not finding the join algorithmically, and not (per Codex)
claiming the text seed retrieves the partner.

**MVP spine:** JWB-01, 02, 03, 04, **05 (mapping fixed + "both edges" verdict + conservative)**,
**06 (editable seed)**, 07, **08 (metadata-heavy, load-bearing: dimensions/inner-dims/line-count/
density/material as evidence badges + soft mismatch warnings, NOT hard filters; recto/verso/
page-neighbor where available, degrade gracefully)**, 09 (basic).
**JSA: JSA-01 only (manual trigger).  Spike JSA-03.  CUT JSA-02 from MVP.**
**Unit:** pairwise same-leaf vertical tear; display all known-join types; do not claim dispersed codices.

## 5. Remaining probes before locking the roadmap

Already run: corpus bracket prevalence, side mapping, side lean (±lacunae), join-pair coverage,
documentary/literary split, ID alignment, complementary-side validation.

Still worth running (Codex #3/#4/#5/#7 — they need live search or image APIs):
1. **Seed-template realism:** for N known physical joins, run the actual existing-search seed
   (line-end → line_start, with variant expansion) and record candidate count, **true-partner rank**,
   and latency. This measures whether JWB-06 *retrieves* the partner, not just seeds.
2. **Metadata-delta discrimination:** physical-join pairs vs random same-text literary candidates —
   do dimensions/material/line-count actually separate true from false partners? (Validates JWB-08.)
3. **Bracket behavior stratified by transcription source/family.**
4. **Recto/verso availability:** how reliably can the app expose both sides for the anchor + candidate.
