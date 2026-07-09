# SEED-029 — Findings Synthesis & Forward Plan

Compiled 2026-07-08 (evening). This is the consolidation point: everything the probe →
rehearsal → full-corpus → remedy-passes arc established, and the concrete probes/spikes/builds
that follow from it. Companion docs: `PROBE-RESULTS.md` (pilot), `REHEARSAL-RESULTS.md` (100K),
`METHOD.md` (method report for Avi Shmidman), `results/` (per-run reports),
`ACL2026_papers/TRACK3-DECISION-BRIEF.md` + `RAMBERT-CROSSLINGUAL-EXPERIMENT-PLAN.md` (Track-3).

---

## Part I — What we have established

### 1. The engine is validated at full corpus scale

- **Architecture:** seed-and-extend over char-5-grams (DF≤100, two-hit, diagonal-keyed),
  density-based acceptance with a fitted sloped boundary (≤0.30 under 100 letters,
  ≈0.39–0.45 above), stage-0 hygiene (short pages, duplicate photography, target sheets,
  library stamps). MinHash/LSH was abandoned 2026-07-06 and the pivot was empirically right.
- **Recall:** candidate stage recall **1.00** on all ground-truth families (pilot);
  tier-1 recall **0.9635** at full corpus. Recall *rose* with scale.
- **Precision:** Hillel graded 164 pairs — actually-spurious **1/164 (0.6%)**;
  Track-1 identification cards **58 correct / 3 boundary / 0 wrong (≈100%)**.
- **Scale:** full effective corpus 667,411 pages (of 948K raw; stage-0 removed 231,679 short
  + 40,452 dup-photo + 9,007 target sheets + 3,077 stamp pages). 1.48B raw hits handled by
  the two-pass disk-spill engine (byte-exact parity vs in-RAM verified on 40.5M candidates).
  Track-2 full pass = **35 minutes**. Reruns are checkpointed after the 07-08 PC crash.
- **Noise model:** empirical HTR letter-CER 16–20% with a confusion matrix (י↔ו, ד↔ר, ב↔כ)
  — a reusable asset (see Part II, cross-cutting).
- **Two graded-in heuristics are now pipeline features:** flank-contrast
  (island ⇒ citation/formula vs continuation ⇒ same work) and line-break-agreement dedup
  (line breaks are a physical-page property; 100% precision / 74% recall duplicate detector).

### 2. The map (current numbers)

| Layer | Result |
|---|---|
| Accepted page pairs (canonmask, full) | **1,342,277** |
| Clean MS pairs / MSS in map | **437,989 / 62,414** |
| Track-1 identified pages | **176,444 / 667,411 (26.4%)** — Bible-domain 66.2%, Documents 3.6% |
| Census rows post-shadowing | **85,872** (MS, work) — testimony/partial/citation split |
| Canonical witnesses | **21,215 MSS** post-shadowing (21,797 pre-shadow; Bible/Bavli/Mishnah/Yerushalmi/Tosefta — current subtotals in `results/track1_full_testimonies.md`) |
| Edited-work testimonies | 25,801 |
| **new? discovery queue** | **1,168** (post-shadowing; edition source ≠ this MS) + 1,495 new?known |
| Passage units | **81,365** many-to-many units with member roles |
| Motifs | **43,278** at brakhah-level granularity; motif-query sweep grew 2,544 of them by +25,327 memberships |
| Multi-page chains | 9,279 (117 with ≥3 pages) — chains are identifications |
| Continuation giant | 43,387 MSS — the liturgy/piyyut continent NOT in reference corpora |

Real cluster finds beyond the giant: 51-MS Tadhkirat al-Kahhalin (medicine), Hilkhot ha-Rif
webs, a calendar cluster, מגילת מצרים, Semag+Mordechai BL cluster, 51-MS grammar treatise
across 5 libraries, Karaite ketubbah formulas.

### 3. Structural findings (the science)

1. **DF-cap self-suppression is the dominant recall disease, not masking.** At 667K pages the
   DF≤100 cap starves exactly the highest-witness texts (unmasked ≈ canonmask raw hits:
   1.475B vs 1.482B). The damage census (`results/df_damage_full.md`) shows monotone damage:
   short liturgical works with ≥25 witnesses pair at **0%** while long unique texts pair at
   89–100%. **The cure is proven twice:** (a) per-domain second pass — liturgy subcorpus
   (139,694 pages) recovered BH 166→**291** in 12 minutes; (b) motif-as-query with per-query
   DF immunity — recovered קדושת היום מוסף לרגלים 6→223, ברכות חתימה לתקיעות 6→170, מעין שבע
   3→143, אלו דברים 4→165.
2. **Competitive span assignment (shadowing) resolves the Temple-Scroll class.** Overlapping
   work-spans resolve to best density; 61,922 rows shadowed; 11QT 176→23 witnesses;
   ibn-Tibbon-translation vs JA-original double-counting collapsed; census −33%;
   new? queue purified 1,508→1,168. `track1_matches.shadowed_by` — **all consumers must
   filter `WHERE shadowed_by IS NULL`** (testimonies + review patched; units + df_damage not yet).
3. **A new text class discovered: Bible-with-Targum/Tafsīr INTERLEAVED.** Verse-by-verse
   alternation defeats both canonical masking and Track-1 (neither pure reference matches
   a full page). Surfaced as the biggest unidentified motif-query gainers (+91/+47/+45/+40).
   Also in class B: liturgical verse medleys, late hymns (בני היכלא, 94 MSS),
   dignitary/exilarch blessing formulas.
4. **Fragmentary-prize mechanism works: identification-by-join.** Motif-query's +1/+2 tail
   (1,219 motifs) attaches unlabeled fragments to tiny known clusters — e.g. motif 369002
   (JA on priestly courses) gained a witness NLI-catalogued as Yefet ben Eli's Deut
   commentary, at density 0.15 (far below any global gate). Catalog agreement = external
   validation for free.
5. **Units-level view is a data-quality microscope.** It exposed the NLI ownership-stamp junk
   class (2,618 fake "MSS") and the 18,676-MS chained-prayer continuum (quarantined —
   1.2% direct evidence for its label).
6. **Physical joins ≠ textual overlap.** Joined fragments share wording in only 1% of groups;
   all 36 "join anomalies" were duplicate photography. Joins are not recall positives.
7. **Semantics locked (Hillel):** same_text is judged at the textual-UNIT level; canonical =
   quotation inside a *different* work; Track-1 testimony vs citation split by page coverage
   (≥0.45 = the page IS a copy; <0.15 = mask-only noise).

### 4. Discovery products already in hand (scholar-facing value)

- **new? queue:** 1,168 (MS, work) rows where no edition/bibliography knows this witness.
- **71 unknown-BH-witness candidates** (phrase-anchored, review page shipped; Hillel round-2
  grades pending fold-in).
- **The residue:** high-witness unidentified units, 70% RNL, Karaite liturgy + piyyut —
  "the most-copied texts in the Genizah that no reference corpus contains."
- **Fragmentary-tail gains:** 1,219 motifs with +1/+2 new members = candidate identifications.
- **9,279 multi-page chains** — each multi-page chain is a strong same-work assertion.

### 5. Track-3 (semantic layer) and literature position

- **MiqraBERT** (arXiv:2606.19638) is the published existence proof of Track-3 — and its
  failure mode (narrative recall@10 87%, **poetic <9%**) is precisely our liturgy risk.
  Its two weaknesses (binary labels, random negatives) were forced by data scarcity we
  don't share: our density scores are graded labels, our near-misses/shadowed hits are
  mined hard negatives.
- **We own the base encoders:** JABERT (published, JA-only, lacuna-native [GAP]/[ONEGAP])
  and RamBERT (unpublished, combined Hebrew+JA). No public JA-in-Hebrew-script encoder exists.
- **RamBERT's missing benchmark = Track-3's cross-lingual validation** — one pre-registered
  difference-in-differences experiment (`RAMBERT-CROSSLINGUAL-EXPERIMENT-PLAN.md`).
- **ACL 2026 two-wave scan → 5 convergences** now wired into this plan: shadowing as an
  assignment problem; the stemma gap (over-determined, 4 methods); concept-erasure toolset;
  conformal+FDR gold-free thresholds; the HTR confusion matrix as a triple asset.

---

## Part II — Forward plan

Organizing principle: **one consolidated Map v2 rebuild** absorbs the proven remedies; around
it, short probes de-risk the unproven ideas before they earn a place in v2; discovery
deliverables ship off the current snapshot without waiting.

### Cross-cutting synthesis insights (new connections, not yet in any doc)

- **S1 — The interleaved Bible+Tafsīr MSS are themselves a verse-aligned Heb↔JA bitext.**
  The RamBERT experiment's gold backbone (§4 of the plan) was waiting on "is a digital
  Saadia Tafsīr reachable?" — but finding 3.3 above means we can *mine* aligned pairs from
  our own corpus: on interleaved pages, the Hebrew verse is Track-1-identified (Bible spans)
  and the JA text between two consecutive identified verses is that verse's Tafsīr, aligned
  by construction. A3 (interleaved-reference spike) therefore feeds C2 (gold build) directly.
- **S2 — The DF-cap disease has one general cure, already implemented.** The liturgy
  subcorpus pass and motif-query are the same mechanism (restore anchor budget for
  high-witness text) at different keys (domain vs motif). The generalization is
  **work-keyed second passes with per-query DF immunity** — motif_query.py already has the
  mechanics; it just needs work-spans (Track-1) as queries instead of motifs.
- **S3 — The HTR confusion matrix is a triple asset** (ACL wave-2): edit-cost prior for the
  matcher, visual-confusability hard negatives for Track-3, and orthographic-innovation
  weighting for stemma work (D1).
- **S4 — The residue is both product and training ground.** The unidentified high-witness
  units (B2) are paraphrase-heavy liturgy — exactly the stratum Track-3 must learn;
  its witness webs supply hard positives (low-overlap true parallels) no external
  dataset has.
- **S5 — BH's 291-witness web is a ready-made stemma playground** — dense, unit-scoped,
  rite-annotated (Bavli vs Eretz-Israel versions known to scholarship), with our own
  confusion-matrix noise model. Cheapest possible entry to the wave-2 stemma convergence.

### Workstream A — Map v2 (the spine)

| # | Item | Type | Cost | Gate / acceptance |
|---|---|---|---|---|
| A1 | Patch `shadowed_by IS NULL` into units + df_damage consumers | fix | hours | consumers agree with census totals |
| A2 | **DF-policy v2 spike** — work-keyed second pass (S2): feed Track-1 work spans of the df_damage 0%-cohort into the motif-query engine as queries with per-query DF immunity; compare against a second domain-subcorpus pass | spike | 1–2 days | 0%-cohort (68 short works, ≥10 MSS) pairing rate ≥60%; no volume blowup; decide the v2 policy |
| A3 | **Interleaved-reference spike** — probe 20 gainer pages to confirm verse-alternation; then build synthetic interleaved refs (Bible×Onkelos, Bible×Tafsīr where minable) as a Track-1 reference type | probe→spike | 0.5 day probe, 2 days build | gainer units get work-level IDs; harvest of aligned Heb↔JA verse pairs counted (feeds C2) |
| A4 | Shadowing v2 — weighted set-cover formulation (wave-2). **Probe only** for now: quantify greedy-vs-set-cover disagreement on the 61,922 shadowed rows | probe | 0.5 day | if disagreement <2% of rows, keep greedy and close the item |
| A5 | **Conformal+FDR acceptance thresholds** — calibrate on the 225 graded pairs (164+61), per-genre strata; replace hand-tuned density gates with FDR-bounded operating points | spike | 1–2 days | FDR ≤5% with tier-1 recall ≥ current; per-genre thresholds published in METHOD.md |
| A6 | **Motif v2 — community detection** (Leiden/Louvain on the segment co-occurrence graph, strict links) to stop mega-motif re-chaining under dense liturgy data | spike | 1–2 days | liturgy-pass motifs stay brakhah-granular; BH acceptance ≥ the 119/71 baseline |
| A7 | Page-chain extension (designed, unimplemented): promote chains to first-class evidence; propagate identifications along chains | build | 1 day | chain-propagated IDs sampled ≥95% correct (20-card grade) |
| A8 | **Map v2 consolidated rebuild** — one versioned snapshot folding A1–A7 winners + liturgy pass + motif-query growth + ref-edge layer + mask-ref-canon v2 masks; regenerate census / units / atlas / review pages from the one DB | build | 1 day compute + glue | all consumers read one snapshot; before/after diff report; this becomes the substrate for every B deliverable |

Order: A1 immediately; A2/A3/A4 in parallel (independent); A5/A6 next; A8 once winners are known.
A7 can ride into A8.

### Workstream B — Discovery deliverables (ship off current snapshot; refresh on v2)

| # | Item | Cost | Notes |
|---|---|---|---|
| B1 | **new?-queue product**: 1,168 rows → review page v3 (bib badges already in v2 tooling) → Hillel grading round 3 → graded "new testimonies" list | 0.5 day + grading | the headline scholarly deliverable; per-work grouping, evidence snippets |
| B2 | **Residue mining**: rank unidentified units by witness count = "most-copied unidentified Genizah texts"; auto-label attempt via NLI/FJMS catalog title cross-ref (the B3 trick) before human review | 1 day | 70% RNL, Karaite liturgy/piyyut; second headline product |
| B3 | **Fragmentary-tail auto-validation**: for the 1,219 +1/+2 motif gains, cross-check each new member's catalog description against the motif's known work; rank by agreement (Yefet-ben-Eli pattern) | 0.5 day | catalog agreement = free external validation; disagreements = the interesting queue |
| B4 | Fold Hillel's BH round-2 grades; finalize the 71-candidate unknown-witness list | hours | blocked on grades |
| B5 | **METHOD.md → Avi Shmidman**: add the motif-query results section (§9.2 exists), then send | hours | opens the MiDRASH bridge; do before C-workstream conversations |

### Workstream C — Track-3 semantic layer

| # | Item | Status |
|---|---|---|
| C1 | **Phase 0 — JABERT monolingual-JA smoke** (MiqraBERT recipe + our graded labels + mined/denoised hard negatives on a JA reuse slice; WD+overlap+AUC, genre-stratified) | **unblocked today**; this is Track-3's green-light gate |
| C2 | Gold build for the cross-lingual experiment | partially unblocked by S1/A3 (self-mined Tafsīr pairs); still ask Hillel re: external digital Tafsīr + Hebrew-only RamBERT-pipeline checkpoint |
| C3 | RamBERT difference-in-differences (the paper) | after C1+C2; pre-registered in `RAMBERT-CROSSLINGUAL-EXPERIMENT-PLAN.md` |
| C4 | Paraphrase-stratum hard positives from the residue webs (S4) | feeds C1/C3 training data; harvest during B2 |

### Workstream D — Stemma probe (exploratory, capped)

| # | Item | Cost | Gate |
|---|---|---|---|
| D1 | **Stemma spike on the BH 291-witness web** (S5): shared-innovation clustering with confusion-weighted edit costs; compare communities against known rites | 1–2 days, hard cap | communities correlate with Bavli/Eretz-Israel rite split ⇒ promote to a workstream; else park with a writeup |

### Workstream E — Infra & hygiene

- E1: mask-ref-canon-v2 rerun is live (resumed from checkpoint 12:15 after the crash;
  ~5,271 works, slow — hours to go). Output masks feed the next Track-1 index build (A8).
- E2: overnight discipline stands — Start-Process detached, sequential, BelowNormal,
  checkpointed. No concurrent full-load jobs.
- E3: repo hygiene — decide gitignore vs commit for the accumulated outputs
  (liturgy.db, parity npz, overnight logs) before A8 multiplies them.

### Sequencing (proposed)

1. **Now (this session / tomorrow):** A1 → B1 review page out to Hillel → A4 probe +
   A3 probe (half-day each) → B5 METHOD.md motif section.
2. **Next 2–3 days:** A2 spike, A3 build (if probe confirms), C1 Phase 0, B2+B3.
3. **Then:** A5, A6 → **A8 Map v2 rebuild** → refresh B products on v2 → D1 probe.
4. **Pending Hillel:** BH round-2 grades (B4), new?-queue grading (B1),
   Tafsīr / Hebrew-checkpoint answers (C2), and any re-prioritization of the above.

### Kill criteria / honesty notes

- A2: if work-keyed immunity blows up candidate volume nonlinearly, fall back to
  domain-subcorpus passes only (already proven) and accept per-domain coverage.
- A3: if the interleaved probe shows the gainers are NOT verse-alternation (e.g. florilegia),
  the class needs a different reference design — write it up, don't force it.
- C1: if fine-tuned separation does not beat off-the-shelf JABERT on the JA slice,
  Track-3 is deprioritized in favor of exhausting lexical remedies (A-workstream)
  before revisiting.
- D1: hard-capped exploratory; no stemma workstream without the rite-correlation signal.
