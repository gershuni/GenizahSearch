# Mask-strictness investigation — full corpus (2026-07-08)

Question (Hillel): is the Bible/canonical mask too strict — and is masking
the right mechanism at all? Investigated with three full-corpus regimes:
**raw unmasked**, **edge-excluded** (spans classified by Track-1 canonical
overlap on both sides; only both-canonical edges dropped), and
**char-masked** (`maskcanon` — canonical spans removed at index time).

## The three-regime table (667,411 pages)

| | raw unmasked | edge-excluded | char-masked (canonmask) |
|---|---|---|---|
| accepted page pairs | 3,445,923 | 1,665,841 kept | 1,342,277 |
| clean MS pairs | 1,192,148 | 632,612 | 437,989 |
| connected MSS | 73,132 | 70,570 | 62,414 |
| giant component | 69,276 | 66,560 | 58,131 |
| **CUL–RNL citation links** | **361,231** | **130,807** | **5,134** |
| BH witnesses connected | 156 | 152 | 166 |
| tier-1 titles recall | 0.9657 | ~same | 0.9635 |

Edge classes on the unmasked run: canonical 1,780,082 · mixed 485,300 ·
clean 1,180,541 (thresholds 0.70 / 0.30 both-sides Track-1 overlap).

## Findings

1. **Masking is exonerated on every count that worried us.**
   - BH connectivity is regime-INDEPENDENT (166 / 156 / 152): the witness
     loss vs 100K (326) is entirely the **DF≤100 cap self-tightening** —
     liturgy grams exceed 100 documents at 667K pages and lose their
     anchor budget. Only 132 BH pages even carry a canonical mask
     (`mask_severity_full.md`).
   - Tier-1 recall is regime-independent (0.9657 vs 0.9635).
   - The feared over-masking of ~27K mid-band pages (50–80% masked,
     not canonical copies) does not measurably cost census connectivity.

2. **Candidate volume was a red herring; composition wasn't.** Unmasked
   and masked runs generate near-identical raw hits (1.475B vs 1.482B) —
   the DF cap already suppresses high-witness canonical text — but
   masking removes the **low-DF canonical tail** (passages with <100
   witnesses: rare verses, Talmud passages inside halakhic works), which
   is where the citation web lives: 3.44M → 1.34M accepted pairs.

3. **Edge-exclusion is NOT a substitute for masking.** It kills only the
   citation edges Track-1 actually identified on BOTH sides; CUL–RNL
   residue stays at 130,807 vs canonmask's 5,134 (Track-1 recall bounds
   the exclusion; masking at index time is categorical).

4. **The canonical edge layer is a product, not just noise** ("it's a
   tell"): the 1.78M canonical-class edges form a labeled CITATION map —
   which manuscripts quote the same canonical loci. Kept in
   `accepted_pairs_canonclass` for a future citation atlas.

## Decision

- **`maskcanon` stays the discovery-map regime** (unchanged).
- The `accepted_pairs` (unmasked) + `_canonclass` tables persist for
  citation-layer analysis.
- **The real open lever is DF policy**, not masking: the DF≤100 cap
  throttles precisely the most-witnessed texts (liturgy, formulae, any
  100+-witness work). Options, in rising cost order:
  1. **Per-domain second pass**: rerun candidates over restricted
     subcorpora (e.g., the ~150K liturgy/piyyut-domain pages) where the
     same absolute cap is proportionally ~4.5× looser — recovers BH-class
     connectivity without global volume blowup. Recommended next.
  2. Scaled cap (DF ≤ 0.05–0.1% of corpus): global fix, ~3–10× hit
     volume, needs bigger spill budget (disk).
  3. Motif-guided completion: use decomposed motifs (43,278 from the
     pilot) as retrieval queries against the full corpus — targeted,
     precision-friendly, and labels members as it finds them.
