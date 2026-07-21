# Discovery Sidecar Schema v1

**Status:** PARTIAL — this document is built incrementally by plan 134-01.
Task 1 (this section) investigates the gitignored E1 research track and
records the "Resolved Design Questions" below. OQ2 and OQ3 are FROZEN
(autonomous defaults per RESEARCH.md). **OQ1 (the per-claim-family band
source, join key, raw->product-band translation, and the TOTAL
flank->claim_type routing) is PROPOSED ONLY in this section — it is NOT
FROZEN.** It requires an owner/researcher sign-off at the Task 2 blocking
checkpoint before Task 3 may freeze the schema's band-source and routing
sections. Do not build any distillation code against the OQ1 proposals
below until that sign-off is recorded.

**Provenance-masking note:** every artifact named below lives in the
gitignored `same_work_spike/probe/` research tree (dev-box only, never
committed, never shipped). This document and all product code reference
the restricted reference corpora ONLY by the masked codenames **M-source**
and **R-source** — never by real name, path, or filename stem. File paths
to gitignored research artifacts are cited here (in a FROZEN, committed
spec doc) only as an artifact identifier for the offline build step, not
as literal restricted content — none of the paths or field values quoted
below contain a restricted corpus name; `check_atlas_masking.py --scan-repo`
gates this doc on every task.

---

## Resolved Design Questions

### OQ2 — Shown-set derivation: reference-catalogue works only (FROZEN default)

**Decision (FROZEN, owner-confirmable):** the launch shown-set's *works*
come exclusively from the reference-catalogue identification tables —
`track1_matches` / `track1_candidates` / `work_query_hits_fullv2` — never
from unsupervised clustering (Louvain / connected-components) over
`accepted_pairs_canonmask`. Rationale (RESEARCH.md Landmine 8 / Assumption
A3): the launch shown-set is Sefaria + JA + M-source-literary (134-CONTEXT
D-05), and every one of those reference works already has a
`work_id`/page-identification record in Track 1 — there is no need to
*invent* works via graph clustering. Clustering (Louvain / force-layout) is
an ATLAS-layout concern (Phase 133), not a claim-model concern, and is
explicitly out of scope for the discovery spine.

**Owner-confirmable flag:** if a future gen-2 build wants discovery-only
works (works with NO reference-catalogue identification, discovered purely
from MS-MS connectivity), that is a new, separately-versioned frame — never
a retrofit of the v1 frame (mirrors D-03(d)).

### OQ3 — HTR snapshot-hash granularity (FROZEN default)

**Decision (FROZEN, low-risk implementation default):** the release
contract carries ONE corpus-level `htr_snapshot_hash` in `meta` (the
release-contract-level "did the underlying HTR corpus change since this
build" signal) **PLUS** a per-page hash-or-char-count on every evidence
row, for page-scoped drift detection at render time (Phase 136):

- `work_witness_pages` carries a per-page snapshot hash (or `htr_n_chars`
  equivalent) alongside `text_layer`.
- `ms_ms_alignments` carries the SAME thing on **BOTH sides**
  (`text_layer_a`/`text_layer_b` + `hash_a`/`hash_b`) — an MS-MS alignment
  spans two manuscripts, so drift on EITHER side must be independently
  detectable (N1 / DATA-03 fail-closed-on-drift covers both sides).

This is the cheapest sufficient granularity: a single corpus-wide hash lets
the release contract detect "the whole corpus text changed, do not trust
these offsets," while the per-page value lets a render-time check reject
ONLY the specific page(s) that drifted, without invalidating the whole
sidecar.

---

### OQ1 — PROPOSED per-family band source, join key, and routing (PENDING owner/researcher confirmation — Task 2)

**Everything in this OQ1 subsection is a PROPOSAL, not a decision.** It is
recorded here so the Task 2 checkpoint has a concrete artifact to confirm
or correct, per claim family, separately.

#### Two structurally separate engines (context for both proposals below)

METHOD.md §8.2 documents Track 1 and Track 2 as architecturally distinct:

- **Track 1** ("fragment ↔ clean canon / reference — identification"):
  matches noisy HTR pages against clean reference corpora (Sefaria / JA /
  M-source). Tables: `track1_matches` (live identifications;
  `shadowed_by IS NULL` filters out shadowed/nested rows — Landmine 9),
  `track1_candidates` (a wider, model-scored candidate pool below the
  live-identification cutoff), `work_query_hits_fullv2` (span/offset
  support). **This is the WORK-WITNESS claim family's source.**
- **Track 2** ("fragment ↔ fragment — discovery"): the seed-and-extend
  engine of METHOD.md §§5-6, output table `accepted_pairs_canonmask` (MS-MS
  page pairs; `a0/a1/b0/b1` offsets; `flank_class` in
  `{island, continuation, edge, ambig}`; verified counts island 582,599 /
  continuation 387,333 / edge 300,237 / ambig 61,930). **This is the MS-MS
  claim family's source.**

The **E1 research track** (`e1_band_frame.py`, all `e1_r*_frame.jsonl`,
`e1_ra_confirmed.jsonl`, `e1_rb_screening.jsonl`,
`e1_certification_registry.json`) — investigated directly this task —
**operates exclusively over Track 1** (`track1_candidates`/`track1_matches`
rows, keyed by `(page_id, sys_id, work_id)`). None of its rows carry the
`accepted_pairs_canonmask` MS-MS key `(page_a, page_b, sys_a, sys_b)`. This
is the single most important finding of this investigation: **there is no
E1-equivalent certified band-source artifact for the MS-MS family.** The
two families' proposals below therefore look very different in confidence
level.

#### (A) Work-witness family — PROPOSED band source, join key, translation

**Proposed authoritative band-source artifacts** (one file per band, all
under the gitignored `same_work_spike/probe/data/` tree):

| Product band | Proposed source artifact | Row count | Measured precision (95% CI or lower bound) | Certification status |
|---|---|---|---|---|
| `expert_verified` | `data/e1_ra_confirmed.jsonl` (`e1_status`/`band2=='R-A'` rows) | 1,570 | 0.889 [0.828, 0.942] | "confirmed-broad-valid-with-deviation, single-expert, **independent audit pending**" (`e1_certification_registry.json`) |
| `tier_a` | `track1_matches` WHERE `shadowed_by IS NULL` (**NOT an E1 artifact** — the base Track-1 identification layer, pre-dating and distinct from the E1 screening/confirmation work) | 275,894 rows / 198,238 distinct pages / 52,497 distinct sys_id / 4,093 distinct work_id `[VERIFIED against fullcorpus_v2.db this session — matches PROJECT.md's "275,894 tier-A page-level identifications on 52,497 MSS across 4,093 works" exactly]` | not a single number — this is the base canonical-reference-match layer, not an E1-graded sample | Machine-matched against clean reference text; not individually human-graded per row (structurally lower noise than Track 2 per METHOD §8.2, "one-sided noise") |
| `screening_rb` | `data/e1_rb_screening.jsonl` (`e1_status=='screening (uncertified)'`, `band2=='R-B'` rows) | 7,498 | 0.859 [0.780, 0.925] | "screening (uncertified)" |
| `screening_canon` | `data/e1_r3_frame.jsonl` (round-3 "clean canon-recovery" frame; ALL rows already `quilt_flag==0` — the round-2 quilt-flagged rows, measured at 0.100 precision, were excluded before round 3 ran) | 9,996 | row precision 0.647; physMS bootstrap 95% lower 0.576 | Per `E1-ROUND3-RELEASE.md` 2026-07-19 product decision: **"canon lane CLOSED at screening"** — ships as screening-grade leads, explicitly NOT certified (matches D-10's caveat requirement) |

**Proposed join key:** the natural key `(page_id, sys_id, work_id)` — this
exact triple is a literal field set on every row of all four sources above
(`e1_ra_confirmed.jsonl`, `e1_rb_screening.jsonl`, `e1_r3_frame.jsonl`, and
`track1_matches`), and matches the fields the `claim_id_work_witness`
recipe already hashes (`sys_id`, `work_id`; `claim_type` is the third
component). No additional join/lookup table is needed to stamp a
work-witness row's band.

**Proposed raw-label -> product-band translation** (apply in this
precedence order — DATA-02/Landmine 10, within a single `(sys_id, work_id)`
key):

1. Row's `(page_id, sys_id, work_id)` appears in `e1_ra_confirmed.jsonl` →
   `expert_verified`.
2. Else, row's `(page_id, sys_id, work_id)` appears in `track1_matches`
   WHERE `shadowed_by IS NULL` → `tier_a`.
3. Else, row's `(page_id, sys_id, work_id)` appears in
   `e1_rb_screening.jsonl` → `screening_rb`.
4. Else, row's `(page_id, sys_id, work_id)` appears in `e1_r3_frame.jsonl`
   → `screening_canon`.
5. Else → not a work-witness claim (row does not ship).

**Residual owner-confirmable assumptions (work-witness):**
- Whether `track1_matches WHERE shadowed_by IS NULL` really is the intended
  `tier_a` source (vs. a more specific "Track-1 tier-A export" the
  researcher may maintain separately) — the row/page/MS/work counts match
  PROJECT.md's published tier-A numbers exactly, which is strong but not
  certain confirmation.
- Whether R-A's "independent audit pending" status should gate anything
  at the DATA-02 sidecar-population level (this proposal says NO — D-09
  already says all four bands populate the sidecar; the audit-pending
  caveat is a Phase 135 methods-page/UI-label concern, per PROJECT.md
  "UI labels say 'expert-verified' until it passes").
- Whether round-3's `e1_r3_frame.jsonl` (9,996 rows, the guard-defect-fixed
  recovery pool) or round-2's broader `band2=='R-CANON'` population within
  `e1_r2_frame.jsonl` (26,157 rows, pre-guard-fix) is the intended
  shippable `screening_canon` population — round 3 is proposed as
  authoritative because it is the LATER, defect-corrected round, but the
  two frames are not row-identical.

#### (B) MS-MS family — PROPOSED band source, join key, routing (HIGH UNCERTAINTY)

**No E1-equivalent certified band-source artifact exists for this family.**
The only MS-MS-specific evaluation found in the research tree is:
- METHOD.md §10.3 — a single AGGREGATE (not banded) human-graded precision
  figure over a 164-pair pilot sample: "precision after stage-0 removes
  duplicates+junk: 110/111 = 99.1%"; "real-shared-text rate per density
  band 0.30→0.45: 100%/100%/100%/97%".
- `ROAD2-DESIGN-OPTIONS.md` — an explicitly-labeled **"design proposal"**
  (not a certification round) describing a `bucket2`/`disc_score2_flank`
  scoring scheme over `data/discovery_scored_flank.jsonl` (217,814 rows;
  `bucket2=='discovery'` = 16,753 rows). This was never graded for
  per-band precision the way the E1 work-witness bands were, and its
  status note says explicitly the pipeline output "never changes to serve
  the app" — i.e. it is a candidate INPUT artifact, not a frozen band
  registry.

**PROPOSED stand-in band rule** (pending owner confirmation — this is the
single highest-uncertainty part of this entire OQ1 proposal): derive MS-MS
bands directly from `accepted_pairs_canonmask.flank_class` +
canonical-overlap status, using the METHOD §8.1 flank-contrast semantics as
the qualitative justification (no dedicated per-band precision number
exists for MS-MS — the `tier_a` mapping below borrows the §10.3 AGGREGATE
99.1% figure, which is not composition-matched to `continuation` rows
specifically):

- `continuation` → `tier_a` (flanks also align → running witnesses of the
  same text; the aggregate-measured ~99% precision regime).
- `island` AND is-canonical → `screening_canon` (shares the canon-confusion
  risk profile of the Track-1 canon lane; canonical-quotation spans are the
  dominant residual noise class per METHOD §8.2).
- `island` AND NOT canonical → `screening_rb` (the project's highest-value
  discovery class — an indirect textual witness of a non-canonical work —
  but with no dedicated per-row precision measurement, so conservatively
  NOT `expert_verified`).
- `edge` → fold to the `continuation` routing (boundary/partial
  continuation) pending the owner's edge rule.
- `ambig` → EXCLUDE by default (screening only if the owner explicitly
  assigns a claim_type).
- **No MS-MS row is proposed for `expert_verified`** — there is no
  MS-MS-equivalent of the E1 single-expert-graded R-A deck at launch.

**Proposed canonical-overlap test:** whether the aligned span
(`a0:a1`/`b0:b1`) overlaps a live Track-1 canonical identification for that
page — a page/offset join against `track1_matches`/`work_query_hits_fullv2`
canonical rows. `accepted_pairs_canonmask.bucket_a`/`bucket_b` (values
`all`/`tier1t`/`bh` per RESEARCH.md) may carry a usable canon signal
directly on the row, avoiding the join, but its exact canon semantics need
researcher confirmation before Task 3 can freeze it.

**Proposed join key:** the `accepted_pairs_canonmask` natural key
`(page_a, page_b, sys_a, sys_b)` (already present verbatim on every row) —
no join to any E1 file is possible or needed for this family, since none
exists.

**Residual owner-confirmable assumptions (MS-MS — all high-uncertainty):**
- Whether the owner wants this flank_class-based stand-in rule at all, or
  prefers `data/discovery_scored_flank.jsonl`'s `disc_score2_flank`/
  `bucket2` scoring as the band signal instead.
- Whether the owner wants to defer MS-MS banding to a dedicated
  post-134 certification round rather than shipping an uncertified
  stand-in rule at launch (this would need a DATA-02/D-09 scope
  conversation, since D-09 currently says "all four bands populate the
  sidecar" for the spine as a whole, not per-family).
- The exact canon-overlap test (page/offset join vs. `bucket_a`/`bucket_b`
  reuse).

---

### TOTAL flank_class → claim_type routing — PROPOSED (MS-MS family)

`claim_type_for_flank(flank_class, is_canonical)` must be TOTAL — defined
for every one of the 4 × 2 = 8 combinations. Proposed table (frozen
claim_type codes: `direct_witness` | `quotes_this_work` | `textual_parallel`
| `direct_text_overlap`; `EXCLUDE` is a sentinel, not a shipped claim_type):

| flank_class | is_canonical=True | is_canonical=False |
|---|---|---|
| `continuation` | `textual_parallel` | `textual_parallel` |
| `island` | `quotes_this_work` | `direct_text_overlap` |
| `edge` | `textual_parallel` (folded to `continuation` semantics — PROPOSED pending the owner's E1-equivalent edge rule; MS-MS has none) | `direct_text_overlap` (folded to `island`/non-canon semantics) |
| `ambig` | `EXCLUDE` | `EXCLUDE` |

Rationale (METHOD.md §8.1, frozen semantics already recorded in the
134-01-PLAN interfaces section): flanks-also-align = running witnesses of
the same text = `textual_parallel`; flanks-dissimilar = a quotation/shared
formula rather than a common work = `quotes_this_work` when the shared
span matches the canonical index, else `direct_text_overlap` (the
project's most valuable discovery class — an indirect textual witness of a
non-canonical work). `edge` is a boundary/partial continuation with no
dedicated E1 rule for MS-MS (unlike work-witness, there is no E1 edge
precedent to borrow at all) — folded conservatively to the nearest
neighbor's semantics. `ambig` ships nothing by default.

**This entire table is PROPOSED, pending Task 2 owner/researcher
confirmation**, together with the OQ1 band-source proposal above.

### Work-witness claim_type rule — PROPOSED (non-flank)

`track1_matches`/`track1_candidates` carry no `flank_class` (Landmine — D2
confirmed this session: neither table has a `flank_class` column). Per
METHOD.md §8.3, Track 1 itself already distinguishes "this page IS work X"
(a direct identification) from "this page QUOTES work X" (a citation
embedded in a different primary text) as two kinds of output, but neither
table carries an explicit boolean flag for this distinction. Proposed
`claim_type_for_work_witness(...)` rule, keyed on relative span dominance
within a page (a page may carry more than one live work identification —
its own primary text plus embedded quotations of other works):

- If a `(page_id, work_id)` claim's span is the LARGEST (by
  `matched_letters`/`alen`) among all live work identifications for that
  `page_id` (i.e., it is that page's primary/dominant identified text) →
  `direct_witness`.
- If a `(page_id, work_id)` claim's span is NOT the largest for that page
  (i.e., a different work's claim dominates the page, and this claim is a
  smaller embedded span) → `quotes_this_work`.
- A page with exactly one live work identification always yields
  `direct_witness` for that claim (no competing span to be dominated by).
- `textual_parallel` and `direct_text_overlap` are PROPOSED to be MS-MS-only
  claim_types — a work-witness claim (canonical reference ↔ our
  manuscript) is never labeled as either, since both describe two OUR
  fragments sharing text without either being a "witness of a reference
  work."

**This rule is PROPOSED, pending Task 2 confirmation** — the "largest span
dominates" heuristic is this executor's inference from METHOD §8.3's
prose description, not a field the research tables expose directly.

---

*(Task 3 completes this document: full DDL for all 8 tables, the column
allowlist, the frozen claim_id/unit_id/work_id recipe, the FROZEN
per-family band source + routing derived from the Task 2 sign-off, band
precedence, the source_corpus codes, the membership-based
frame_content_hash recipe, the release-contract meta keys, the manifest.json
resolution, the DATA-10 unit×work projection rule, and the D-03(b)
source-extensibility note. This document is marked FROZEN only at the end
of Task 3.)*
