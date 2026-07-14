# Codex design gate — citation-vs-witness detector for page↔reference matches (MAPV2-15m, "Way 2")

## Context
SEED-029 builds a corpus-wide **discovery** product over ~667K Cairo Genizah
manuscript pages. Track-1 matched pages against a reference corpus (Maagarim +
Judeo-Arabic) by **seed-and-extend** (char k-gram anchors → extended aligned
spans). Each match = (Genizah page, reference work_id, one or more matched spans
on the PAGE). We score every match for **discovery-likelihood** and rank; the
small top slice is the mineable discovery DB.

A "discovery" = a fragment whose content the catalog does NOT predict. The
scoring already has: confidence (calibrated `p_same_work` from size+density+
margin), novelty (catalog-silence via a metadata-scope gate × work rarity),
minus a shared-canonical-source penalty, and an "already-identified" gate
(Friedberg bib + printed catalogs + FGP + PGP) that demotes named fragments.

## The remaining signal we need (Hillel's own heuristic)
Distinguish, per match:
- **same-work witness** — the page IS a copy of the work; the matched span is
  part of a continuous copy. This is a real discovery ("this fragment contains
  work X"). KEEP / rank up.
- **citation / embedded quote / formula** — the page is a DIFFERENT work that
  merely quotes work X in the span; beyond the span the page is other-work text.
  NOT a discovery. Rank DOWN (route to "Other").

Hillel's phrasing: "if the flanks are different it goes down — more likely
citation/shared source/formula."

## Why the naive flank test is wrong
We have a page↔page flank detector (`fix_flanks.py`): compares 150 letters on
each side of a shared span between two Genizah pages; low edit-distance ⇒
continuation (same-work), high ⇒ island (citation). For page↔**reference**
matches this breaks:
1. Track-1 stores only **page-side** span offsets (`spans_json` = [start,end]
   in the page's normalized letter stream); the **reference-side** offset is NOT
   stored.
2. The span is the MAXIMAL seed-and-extend result, so page and reference
   diverge *at the span edge by construction* — the immediate flank always looks
   like an "island", so a naive comparison labels everything a citation.

## Hard design constraint (Hillel, load-bearing)
Expected yield = **hundreds/thousands of genuine discoveries — dozens at least
substantial (long), many more FRAGMENTARY**. The detector must **preserve the
fragmentary tail**: short pages have short/absent flanks. When there is not
enough neighboring text to judge, it must **ABSTAIN (neutral)**, never
auto-reject. Recall is preferred over precision (every survivor is
human-reviewed before publication).

## Data available (read-only)
- `data/discovery_scored_gated.jsonl` — per match: page_id, sys_id, work_id,
  cat, genre, title, author, matched_letters, best_density (= matched ÷ page
  letters), n_spans, p_same_work, margin_band, canon_mass, work_nms, regime,
  resolution, bucket2, disc_score2, spans_json (PAGE-side offsets).
- `data/fullcorpus_v2.db` — `pages(page_id, text)`; also `track1_candidates`
  (1.34M rows: page_id, work_id, matched_letters, best_density, margin,
  n_competitors, p_same_work, spans_json, flag) and `track1_matches` (270K,
  shadowed_by). **A page can have candidate matches to MULTIPLE works.**
- Reference work text: `ref_corpus.pkl` / `ref_corpus_v2.pkl` (work_id ("M:Ytext<N>"/
  "J:<name>") → normalized letter stream + metadata), or re-resolve from source
  files. `normalize.norm_stream(text)` → (letter_stream, offset_map).
- `scripts/engine.py` / `engine_np.py` — the seed-and-extend matcher (k-gram
  anchors, one-sided/two-sided boundary acceptance). `track1_match.py` builds
  the ref index.

## Candidate methods (critique + choose + refine — this is the ask)
**A. Gapped ref re-alignment (literal flank).** Locate the span in the ref work
(re-seed with a k-gram from sa[a0:a1]); walk outward on both sides allowing a
small gap (lacuna/HTR noise); measure whether the page RE-CONVERGES to the work
(continuation) or stays divergent (island). Faithful but needs ref-side
relocation + gapped alignment; thresholds for "re-converges".

**B. Whole-page-vs-work coverage.** Align the ENTIRE page against the target
work (not just the span). Same-work witness ⇒ the page aligns to the work at
many positions / high total coverage; citation ⇒ aligns only at the span. Close
to best_density/n_spans but measured directly against ref text.

**C. Competing-work flank (cheap, uses data in hand).** A page's OTHER Track-1
matches: if the page also matches a DIFFERENT work Y in the regions flanking the
span (Y ≠ target X) ⇒ the page is Y quoting X ⇒ citation. If the page matches
ONLY X (or nothing else) across its length ⇒ witness. Computable from
track1_candidates (all works per page + their span offsets) with NO
re-alignment. `n_competitors` is a coarse existing proxy.

**D. Coverage + span-structure only (the cheap proxy we're NOT choosing).**
best_density + n_spans, no ref text. (Baseline to beat.)

## Questions for Codex
1. Which method (or combination) is soundest and most robust to HTR noise
   (~16-20% CER), lacunae, and recension differences? Is C (competing-work
   flank, free) strong enough to be primary, with A as refinement — or is A
   necessary?
2. Specify the concrete algorithm for the chosen method: relocation of the span
   in the ref, flank window size, gap handling, the continuation vs island
   decision rule, and thresholds (state how to calibrate them — we have Hillel's
   132 gold grades with island/edge/continuation-style labels and a 100-card
   held-out set).
3. How exactly to ABSTAIN for short-flank / fragmentary pages so recall on the
   fragmentary tail is preserved (Hillel's constraint). What minimum flank
   length, and neutral vs penalize?
4. How to fold the result into disc_score2 (multiplier? separate flag +
   evidence line? both)? It must stay an *advisory* signal (human verdicts),
   never a hard filter.
5. Pitfalls: a work that legitimately quotes canon (Bible-in-piyyut) so its own
   flanks are canon; palimpsests/multi-work pages; the page being a DIFFERENT
   witness/recension of the same work (flanks diverge but it IS same-work —
   must NOT be called citation); circularity with the shared-canon penalty
   already in the score.
6. Validation plan: how to check the detector against the 132 gold + 100
   held-out without tuning-on-test leakage.

Be concrete and prescriptive. Output a recommended method + algorithm spec +
thresholds + abstention rule + validation plan + a ranked pitfall list.
