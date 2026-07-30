# Phase 136: Read Surfaces — Connections Panel & Work→Witnesses - Context

**Gathered:** 2026-07-30
**Revised:** 2026-07-30 — post mockup + Codex gate (both gates RUN in the discuss session, before planning)
**Status:** Ready for planning

<domain>
## Phase Boundary

The **first UI that renders discovery claims**: a browse-page "Computed identifications" panel
(PANEL-01/02/03), a `/work/{id}` witness-map page (WORK-01/02), computed identifications folded into
the existing `/catalog-browse` Browse-by-Identification page, and a **standalone corpus-wide findings
page with its own nav entry**. Everything is built **behind the discovery flag** — Phase 139 flips it
on (REL-01's plain reading; the curated-surface exception was declined by the owner 2026-07-28).

**Owner-authorized scope expansion (2026-07-30).** The phase opens with ONE data rebuild + ONE
production redeploy and absorbs two requirements previously homed after 136:

- **VIS-01** (public/private projection — already homed here)
- **NOVEL-01 + NOVEL-02** (the novelty axis + its provenance masking) — **moved INTO 136**, reversing
  the 2026-07-28 "Post-136" homing. Owner rationale: novelty is the axis that makes these surfaces
  worth using, the surfaces cannot honestly ship a novelty toggle over data where the flag is
  uncomputed, and the phase is already rebuilding + redeploying the asset once.

**Requirements delivered:** PANEL-01, PANEL-02, PANEL-03, WORK-01, WORK-02, VIS-01, NOVEL-01, NOVEL-02.
**Requirements AMENDED by this phase** (amendment is in scope here, not deferred): BAND-03, BAND-05,
NOVEL-01 wording, plus a narrow amendment to the frozen precision contract in
`docs/specs/discovery-sidecar-schema-v1.md` §1.6 and `docs/specs/discovery-band-labels-v1.md`.

**NOT in this phase:** community judgments (137 — 136 ships placeholder voting controls only), the
leads queue (138), atlas drill-down + homepage band + the REL-01 public flip (139), and the **gen-2 /
discovery-v2.1 evidence refresh**, which becomes its own later phase (D-01).

</domain>

<decisions>
## Implementation Decisions

### Asset & sequencing

- **D-01 — Build on the LIVE v2 asset; the gen-2 v2.1 refresh gets its OWN later phase.** The
  surfaces render `discovery_data/discovery-v1-33499c5b…db` (deployed at 135-08). The GEN2 handoff
  (`same_work_spike/probe/rsource/HANDOFF-TO-135.md`) asks the milestone to re-bake v2.1 from
  `g_launch3.db` — **not in 136.** Measured, and Codex-confirmed sound (F-15):
  - Only **34.25%** of v2.1's 236,497 shipped claims (**80,993**) name a work carrying an
    owner-reviewed neutral title (the approved-list ∪ crosswalk join; the exact recipe must be
    documented when that phase is planned — an earlier figure of 34.7%/82,156 used a crosswalk-keys-only
    join and is superseded). DATA-04 is fail-closed, so v2.1 at full breadth needs a NEW owner
    curation round over ~2,670 works; at constant curation the renderable surface would SHRINK from
    ~152K to ~81K claims.
  - v2.1 **strands CERT-01** (different shipped population → re-registration + a fresh blind draw +
    owner grading time).
  - v2.1 has **no MS-to-MS / shared_text family at all** (every claim is `direct_witness`) and `band`
    is NULL. A v2.1 bake must MERGE v2's propagated/shared_text inputs alongside v2.1's direct
    evidence; that is a data phase, not a file swap.
  - Carried into that later phase: the **reference-granularity stage** (Bible → chapter,
    Mishnah/Tosefta → tractate+chapter, Talmud → folio+amud — actionability, measured NOT to raise
    precision) and the **witness-vs-quoter lever**.
- **D-02 — ONE rebuild + ONE production redeploy, as the first execution gate, before any surface.**
  Payloads:
  1. **The tier-A default-visibility authorization** — see D-02a. NOT the precision figure.
  2. **Coverage**: an indexed fixed-point `coverage_ppm` + a validity status, plus the page-length
     input, for the DIRECT family only (D-08a).
  3. **The novelty assessment for ALL evidence families**, tri-state (D-23a), + NOVEL-02 masking.
  4. **Materialized sort keys and indexes** the new surfaces need (D-10a).
  Asset-first, human-approved, ONCE, per `docs/specs/discovery-deploy.md`.
- **D-02a — Store the AUTHORIZATION, not the number (owner, post-Codex F-01).** Codex proved the
  rebuild as originally written **cannot be built**: `_validate_precision_spec`
  (`scripts/build_discovery_sidecar.py:3865-3890`, enforced at 4019-4027) *requires* `tier_a`
  precision to be NULL; the release verifier independently rejects a non-null `tier_a`
  (`scripts/verify_discovery_sidecar.py:553-560`); and the `band_precision` insert
  (`scripts/build_discovery_sidecar.py:4419-4427`) writes only `measurement_status` of the five
  registry columns. **Owner decision:** write ONLY `measurement_status='measured_pass'` and
  `ci_low = 0.9084` for `tier_a`; `precision` stays NULL. That is exactly what
  `is_default_eligible()` reads, so the band goes default-visible while the asset stores the
  authorization rather than an estimate — consistent with D-06's no-numbers posture, and it preserves
  the contract's real intent (no unearned `tier_a` figure in the asset). Requires a NARROW dated
  amendment permitting those two fields for this band, applied in lockstep across the frozen §1.6
  row-set, `_frozen_real_band_precision_rows`, `_validate_precision_spec`, the release verifier, the
  insert column list and the schema tests — with fixtures proving BOTH the pass and the fail branch.
- **D-02b — Rebuild-preservation gate (Codex F-04; strengthens the gate identified in-session).**
  Population-hash equality is necessary but NOT sufficient: `population_hash` /`cluster_map_hash`
  (`scripts/cert01_frame.py:293-306`) key only on `(page_id, canonical_work_id, stratum)` /
  `(…, unit_key)`, and the frame hash omits novelty, routing, precision, snapshot hash and
  `matched_letters` — so `matched_letters` could drift within a stratum undetected. The gate is an
  **exact old/new allowlisted diff**: `works`, claims, witness units, routing audit and every
  pre-existing evidence column byte-identical, with ONLY the new coverage columns, the authorized
  novelty changes and the `tier_a` registry row permitted to differ; PLUS recompute
  population/stratum/cluster hashes; PLUS bind every graded card to the same work, claim,
  display-evidence, span and snapshot; PLUS an externally pinned expected frame hash (the deploy
  runbook currently reads the expected frame from the *candidate's own* manifest —
  `docs/specs/discovery-deploy.md:79-91,123-138` — which cannot detect a wrong rebuild). The runbook's
  rebuild command (lines 211-231) also omits the live v2 pinned inputs and must be corrected.
- **D-02c — The pre-registration stays IMMUTABLE.** `scripts/verify_cert01_grading.py:206-212`
  pins the CURRENT asset's `db_content_hash`, so every rebuilt byte-stream fails its check 10 by
  design. Do NOT weaken that check. Publish a **separate compatibility attestation** for the rebuilt
  asset (original hash + new hash + the allowlisted-diff result + the recomputed population hashes),
  and keep the original hash and report id in the immutable pre-registration.
- **D-04 — Phase shape: ONE phase, six execution gates** (Codex F-12, accepted): (1) requirement +
  contract amendments and the frozen semantics for visibility, novelty, coverage and evidence;
  (2) offline build + verification of BOTH projections (compatibility, masking positive-control,
  reconciliation, performance); (3) owner approval + the paired asset-first deploy; (4) the panel +
  a tier-first work page; (5) the catalogue integration + the findings page; (6) PANEL-03 last.
- **D-05 — Both process gates RAN in this discuss session, before planning, and a SECOND mockup pass
  ran after them.** Artifacts: `136-MOCKUP.html` (single manuscript) and `136-MOCKUP-MULTI.html`
  (seven manuscripts, every agreed rule applied), with their generators; plus `136-CODEX-REVIEW.md`
  (**VERDICT: REWORK**, 3 BLOCKER / 9 HIGH / 3 MEDIUM; every finding dispositioned below). All data is
  real — the deployed asset + real HTR text from `Transcriptions.txt`.
  - **The second pass earned its keep:** it exposed the D-13d granularity flaw, the D-13g
    human-confirmed/routing bug, and the D-13i catalogue-juxtaposition trap — none of which were
    visible in the single-manuscript pass or in any document. **Lesson to carry: mock a SPREAD of real
    manuscripts, not one.** The seven cases (clean / commentary / Judeo-Arabic multi-register /
    expert-reviewed / the problem siddur / page-relation-heavy / 427-identification) are the standing
    regression set for any future panel change.
  - **Coverage routing evidence recorded for the v2.1 lever:** `low_coverage` accounts for **100,159**
    of the review-only display rows, and on Moss. V,374 it demoted SIX correct Rashi identifications —
    one at 1,329 matched letters — because a commentary occupies only part of a densely-written page.
    Not fixable in 136; it is direct evidence for the deferred witness-vs-quoter/coverage work.

### Bands, numbers and honesty

- **D-06 — NO precision percentages on ANY surface, and no by-source breakdown.** Owner
  (2026-07-30): *"the exact percentage may be misleading and may include sources the user will never
  see. What's important is the tiers, and the user can judge for themselves."*
- **D-06a — The conflicting requirements are AMENDED IN THIS PHASE (owner, post-Codex F-03).** Codex
  showed the conflict is live NOW, not only at 139: BAND-03 requires screening precision reachable via
  the band tooltip, BAND-05 requires the methods page to publish population, sample size, weighted
  estimate and CI, and `web/pages/help.py:245-260` **already renders estimates and intervals**.
  Owner chose to amend both and rewrite the methods page **qualitatively**: each tier explained in
  words plus the non-percentage facts — that grading happened, the unit, the sample size, the grader,
  the date, the method, the audit state, and the immutable report identifier. No percentage, no CI, no
  strata table. `docs/specs/discovery-band-labels-v1.md` §3 needs a dated amendment to match.
  - **Still owed at 139:** REL-01/CERT-02 also require tier-A to go public *with* its measured number.
    That clause must be amended or satisfied at the release gate; 136 publishes nothing, so nothing is
    violated now.
- **D-06b — The public projection is NOT certificate-covered (Codex F-02).** The measurement is on the
  all-source population; the public projection is a structurally different one, and `135-09-CERT01-MEASUREMENT.md`
  itself records the Sefaria-only figure as descriptive, not pre-registered. Never copy the all-source
  measurement into the public asset as if frame-matched. **OPEN, must be settled before the 139 flip:**
  either pre-register and measure a public estimand before the public bake, or formally amend REL-01 to
  justify the transfer. For 136 the full/private asset carries the D-02a authorization; whether the
  public projection may inherit it is part of that same open decision.
- **D-08 — A coverage percentage IS displayed** ("Matches ⟨work⟩ · 68% of page"), sortable and
  filterable. It is NOT a precision number and must never read as one.
- **D-08a — Coverage is DIRECT-FAMILY ONLY and explicitly labelled (owner, post-Codex F-08).** The
  metric is matched Hebrew **base letters** ÷ the normalized page stream
  (`scripts/build_discovery_sidecar.py:532-568`) — not character coverage and not the displayed span:
  **9,549** shipped direct rows have multiple spans while only the largest is stored, and **all**
  42,776 shipped propagated evidence rows have NULL `matched_letters`. So: show, sort and filter the
  percentage for the direct family only; label it as matched-letter coverage, never bare "68% of page";
  store `coverage_ppm` (indexed, fixed-point) plus a validity status; propagated rows show no
  percentage at all.

### Panel (PANEL-01/02/03)

- **D-09 — Scope: both page and manuscript.** One panel, two groups: **"On this page"** first, then
  **"Elsewhere in this manuscript"** (collapsed). Served via `page_id IN (…)` over the browse page's
  own page list — `discovery_evidence` has no `sys_id` index, and Codex confirmed the page-list plan
  does use `ix_discovery_claim_page_id`, so it is the right approach. Per manuscript: median **2**
  shipped claims / **1** distinct work; max 427 claims / 47 works; **429** manuscripts over 50 claims
  → the manuscript group needs pagination.
- **D-10 — Nested structure.** Top level = identification rows. "Other manuscripts matching ⟨work⟩"
  nests per work as an on-demand expansion via `get_work_witnesses(work_id, anchor_sys_id=…)`.
  "Pages matching this page in other manuscripts" is a separate section below.
- **D-11 — Related-pages section: header + count by default, rows behind the toggle.** No amendment to
  the band-label contract — nothing unevaluated is asserted as an identification.
- **D-11a — Count semantics (Codex F-13).** The earlier "20,435" was display-claims whose *selected*
  evidence is shared text, not the relation population. Real figures: **40,968** shipped shared-text
  evidence rows, **37,397** directed page pairs, **30,539** unordered pairs. The header counts
  **distinct opposite pages for this anchor**, deduplicated, and is labelled as *unevaluated candidate
  alignments* so an aggregate cannot evade the screening disclosure.
- **D-12 — PANEL-03 (evidence view): highlight where offsets exist, and only there (owner,
  post-Codex F-09).** Mechanism: slice the RAW page text at the stored offsets, escape each part, wrap
  the middle — **not** a reuse of `web/pages/browse.py::highlight_text`, which escapes first and then
  substitutes search *terms* (`browse.py:1577-1601`), so it cannot consume offsets and would corrupt
  them. Fail closed per side on snapshot-hash drift: the identification and its tier still show, only
  the span is withheld. **The b-side of a page relation has no stored offsets by design**
  (`discovery-sidecar-schema-v1.md:165-175`), so when the viewed page is the b-side the section shows
  the relation and its stats with an explicit "the passage location was not recorded" note. PANEL-03
  ships in execution gate 6, last.
- **D-13 — The entry control is hidden only on a SUCCESSFUL zero (Codex F-14).** Only 44,375 of ~255K
  manuscripts carry shipped claims (~17%), so hiding on zero is right — but today's wrappers collapse
  timeout, overload, unavailable sidecar and genuine zero all to `[]`, which would hide the panel
  during an outage as though the manuscript had nothing. The service must return an envelope
  (`{status, items, total}`); an outage shows a visible temporary-unavailable state with retry.
- **D-13a — Collapse duplicate canonical works at display time (mockup M-1).** The mockup's real page
  showed the SAME work twice under two titles — `w000190` (M-source-sourced title) and `w001382`
  (Sefaria) — even though `canonical_work_id = w001382` records the merge, because claims key on
  `(page_id, work_id)` and dedup runs per claim key. Corpus-wide: **921 row-pairs**. Owner decision:
  **collapse by `canonical_work_id`, and the canonical work's own title wins** (the other title is
  dropped from view). Applies to the panel, the work page and the findings page alike, and to every
  count derived from them.
- **D-13b — One row per passage; competing attributions nest beneath (mockup M-2).** **1,558**
  span-groups carry 2–8 shipped claims on byte-identical offsets (1,245 pairs, 208 triples, 57 quads,
  32 quints, 15 six-way, one 8-way). Owner decision: the strongest attribution is the row; the others
  appear as "↳ the same passage also matches …". This stops one passage inflating a manuscript's match
  count. The lead-attribution rule must be deterministic (tier rank, then the existing total order).
- **D-13d — Identical-span groups are pulled OUT of the identifications entirely (owner, second mockup
  pass).** Not merely nested (which D-13b proposed) — the whole group leaves the identifications
  bucket. Behind the toggle it reads "one passage (offsets a–b, N letters) appears in **N works**: …".
  Rationale: several works claiming byte-identical text with identical matched length is the signature
  of generic shared text (a verse-chain, a liturgical formula), not of a witness. Owner's case: a
  prayer book whose page-6 verse-chain pulled in Tur Orach Chaim (twice, under two titles) and Yalkut
  Shimoni on Nevi'im on offsets 0–555. Scope: 1,558 groups / 3,600 claims = **2.5%** of the shipped
  direct set; matched length is identical within the group in 1,495 of the 1,558.
  - **⚠ KNOWN FLAW, must be fixed at gate 1 (found by the second mockup pass).** The rule as stated
    conflates two different cases. On T-S Misc. 12.31.14, `רש"י על התורה` (w000171) and
    `רש"י על בראשית` (w001281) sit on the IDENTICAL span 0–962 — the same work at two granularities,
    carrying DIFFERENT `canonical_work_id`s, so D-13a's merge collapse does not catch them and D-13d
    files them as "generic shared text". Net effect: that manuscript renders **1** identification where
    it should render **2** (Rashi + Genesis, on genuinely different passages). The rule must separate
    *same work at different granularity* → collapse like a duplicate, from *different works on one
    passage* → generic. This is the reference-granularity gap the GEN2 handoff defers to v2.1,
    surfacing a phase early; a display-time alias/containment test is needed, not a data fix.
- **D-13e — A third bucket: "Also shares text with" (owner, second mockup pass).** The panel has THREE
  disclosure levels, not two: (1) **Identifications** (default); (2) **"Also shares text with /
  חולק טקסט גם עם"** — collapsed by default, holding the D-13d generic-passage groups and the
  related-pages count, explicitly NOT presented as identifications; (3) the existing **"Show more
  possible matches"** toggle for screening bands, review-only and short-passage rows. Owner's words for
  the middle bucket: *"perhaps interesting but should be hidden by default"* — it is neither an
  identification nor low-quality screening, so it gets its own honest home.
- **D-13f — The review badge is DROPPED until provenance is established (owner, second mockup pass).**
  No row on any surface claims human review. The 121 `adjudication_status='human_confirmed'` rows (121
  claims across 116 manuscripts) keep their tier and lose only the badge, because **Phase 134's own
  closeout left their provenance open — "internal deck vs owner", never resolved.** Until we can name
  who reviewed a row and when, "Expert-reviewed ✓ / נבדק בידי מומחה" asserts more than we can source —
  the same discipline that governs the band names. Consequence: every row on the surface reads
  "unreviewed · algorithmic estimate", which is at least uniform and true. Requires a dated amendment
  to `discovery-band-labels-v1.md` §2 (the review overlay). `review_overlay()` and
  `serialize_banded_claim` keep computing the value — the surfaces simply do not render it.
  - **New task:** establish the provenance of those 121 rows (their source is
    `e1_adjudicated_a.jsonl`, 174 individually-adjudicated cards). If it turns out the owner graded
    them, the badge can return with a sourced wording. Until then it stays off.
- **D-13g — A human-confirmed row is shown by default even when routing demoted it, flagged as
  low-coverage (owner, second mockup pass).** **This is a real bug, not a preference.**
  `shared/discovery_service.py::get_claims_for_page` filters `routing_status='shipped'` in SQL by
  default, but `is_default_eligible()` returns True for `human_confirmed` **unconditionally, before it
  checks routing**, and `discovery-band-labels-v1.md` §4 says the same. So **19 of the 121**
  human-confirmed rows are dropped by the query before the predicate meant to protect them ever runs.
  Live symptom in the mockup: on Moss. V,374, P22's human-confirmed `רש"י על איכה` is hidden while
  P23's human-confirmed `רש"י על אסתר` shows — two rows a human confirmed, treated differently. Fix:
  the query must not filter them out; the row renders with an explicit low-coverage note.
  - **Interaction with D-13f (accepted by the owner):** with the badge dropped, the reader sees an
    ordinary row carrying a low-coverage note and no stated reason for its presence. The inclusion rule
    keys on `human_confirmed` internally; the note is about coverage, not review.
- **D-13h — "Elsewhere in this manuscript" NAMES the works, not just a count (owner, second mockup
  pass).** It reads "Rashi on Song of Songs (5 pages), Rashi on Lamentations, Halakhot Gedolot" rather
  than "8 more on 7 pages". Rationale from the mockup: **manuscript-level coherence is the context that
  makes a single claim judgeable by a reader.** Moss. V,374's page-23 Esther identification looks
  arbitrary alone and obviously right once you see that P2–P8 carry Rashi on Song of Songs and P22
  Rashi on Lamentations — a Rashi-on-Megillot codex in the standard order. (This is a READER aid only;
  it must never feed band assignment or routing, which would be circular.)
- **D-13i — A shelfmark's catalogue description must never sit unlabelled beside a page-level claim.**
  The second mockup pass produced a false alarm this way: Moss. V,374's catalogue line reads
  *"a) Legal document regarding a bill of divorce. b) Court record in the hand of Hillel b. Eli"* —
  which describes OTHER leaves of a composite volume, not page 23. Presented next to the Rashi-on-Esther
  claim it read as an absurd mismatch, when the claim is verifiably correct (the span ends with the
  colophon `תם כל הפירוש`). On composite shelfmarks a catalogue description is simply not about the
  folio. Either omit it beside claims or label it explicitly as describing the shelfmark. Same trap as
  `feedback_catalogue_never_evidence`, in the other direction: the mismatch looked like a false
  positive when it was the catalogue being coarse.
- **D-13c — Short-evidence rows go behind the "show more" toggle (mockup M-3).** Under 150 matched
  letters: **6,558** direct rows (4.5% of 144,294) and **5,630** propagated rows (25% of 22,243). The
  mockup's real case is a siddur whose four liturgical matches share one **66-letter** span; the
  thinnest shipped direct match in the whole asset is **37** letters. Owner chose the toggle over
  marking. **The threshold itself is set at execution gate 1 with counts on the table** — note the
  honest counter-argument the owner accepted: for a prayer book a short liturgical passage may be
  exactly the correct identification, so the threshold must be defensible and the rows stay reachable,
  never deleted.

### Work page (`/work/{id}`, WORK-01)

- **D-14 — Sort is a three-way toggle, tier-first by default:** (1) strongest tier, then shelfmark;
  (2) library then shelfmark; (3) coverage. Novelty is NOT a sort — see D-15a.
- **D-15 — A novelty filter is first-class**, worded per D-23b.
- **D-15a — Novelty filters and (optionally) groups; it never orders by default (Codex F-07).** D-19's
  novelty-first ordering contradicted NOVEL-01/D-24's prohibition on novelty feeding ranking. Owner
  resolution: default order stays tier-first; novelty is a filter plus an explicit user-selected
  grouping that never changes confidence rank or styling. NOVEL-01's prohibition text is amended to
  say exactly that.
- **D-16 — Filters: tier + novelty + coverage are the important ones.** The library filter is included
  only if the existing web library-filter component drops in cheaply; otherwise deferred. Filters
  compose as AND; empty = all currently enabled tiers.
- **D-17 — Plain paging with the real total visible.** **Corrected figures (Codex F-11):** the heaviest
  work has **13,038 claim rows** but **4,796 distinct manuscripts** and **4,637 witness units**;
  medians across the 1,088 works are **9 claims / 5 manuscripts / 4 units**; only **64** works exceed
  200 witness units (not 120). WORK-01 totals count the post-DATA-10 **unit** projection.
- **D-17a — The work query needs display fields the service does not return (Codex F-11).**
  `shared/discovery_service.py:209-226` exposes only unit/page/work/claim/manuscript ids, family, band
  and band rank; the paged result (797-842) has no total, shelfmark, library, coverage or novelty — so
  server-side sorting by library and a visible total are impossible today. The rebuild adds a sidecar
  **manuscript-display lookup** with normalized library and shelfmark sort keys, and the service gains
  a count query using the identical grouped predicates.

### Findability, the catalogue page and the findings page

- **D-18 — `/catalog-browse` (Browse by Identification) gains computed identifications**, with both the
  strong and the weaker relation kinds present, visibly separated and separately worded — never under
  wording that asserts "is a copy of" versus "quotes". Note the integration problem: that page's work
  vocabulary is the FJMS catalogue's and resolves shelfmarks only after fetching a page
  (`web/pages/catalog_browse.py:262-309,376-438`), so the mapping is real work and will not be
  complete. **How computed rows are presented alongside catalogued ones is OPEN** → execution gate 5.
- **D-19 — The corpus-wide findings surface is its OWN page with its OWN nav entry** (owner,
  2026-07-30), flag-gated exactly as `/atlas` is (availability predicate, not the flag alone). It
  cannot be called "Discoveries" — `/discoveries` is the pre-existing **Community** page. The nav today
  is Home · About · Search · Find Parallels · Browse by Shelfmark · Browse by Identification ·
  Community · My Lists · Joins Lab · [Fragment Puzzle] · [The Genizah Atlas — Beta]. This surface
  gives **SEED-032** its home, closing the disposition `v9-PUBLICATION-STRATEGY.md` §8.6 left open.
  **The row unit is OPEN** → execution gate 5, with a mockup. Recommended default to test:
  **one line = one identification (manuscript × work)**, because tier, novelty, coverage and the
  future vote all attach to exactly that pairing; the alternatives are one line per manuscript
  (44,375 rows; 9,806 carry >1 work, so a novelty filter is ambiguous) or one line per work (1,088
  rows, but the individual find is hidden and giant works dominate by size).
- **D-20 — Work pages are reachable from** the manuscript panel, `/catalog-browse`, and the findings
  page.

### Relation wording

- **D-21 — "Match framing", heuristic-honest.** Owner-selected, with the Hebrew maqaf as U+05BE `־`:
  - Row: **"Matches ⟨work⟩ · 68% of page"** / **"התאמה ל⟨חיבור⟩ · 68% מהדף"** — the percentage only on
    direct-family rows (D-08a), labelled as matched-letter coverage.
  - Weaker relation: **"Partial match with ⟨work⟩"** / **"התאמה חלקית ל⟨חיבור⟩"**
  - Section: **"Other manuscripts matching ⟨work⟩"** / **"כתבי־יד נוספים התואמים ל⟨חיבור⟩"**
  - Section: **"Pages matching this page in other manuscripts"** /
    **"דפים התואמים לדף זה בכתבי־יד אחרים"**
  - PROHIBITED in display: "copy of", "quotes", "witness of". Owner: *"we are not sure that tier_a is
    same work and the next are parallels, just heuristics."* The stored vocabulary
    (`direct_witness` / `quotes_this_work` / `shared_text`) is unchanged — this is display only.
  - "Possible identification" stays RESERVED for screening-tier rows (BAND-03).
  - The panel title stays **"Computed identifications" / "זיהויים מחושבים"**.

### Public/private projection (VIS-01)

- **D-22 — Two visibility axes, derived at BUILD time, before raw ids are discarded (Codex F-05).**
  "Origin of the displayed assertion" is not representable in the shipped schema as written: the
  schema *requires* `discovery_claim.source_corpus` to equal the work's identity source
  (`discovery-sidecar-schema-v1.md:89-100`, enforced at `verify_discovery_sidecar.py:319-332`), and
  `discovery_evidence` carries only a family code — no assertion-origin and no licence provenance. So
  `works.source_corpus` IS exactly the proxy D-22 says is insufficient. The build must therefore derive
  **`assertion_visibility`** (from the raw evidence origin) and **`identity_visibility`** (for the
  displayed work) as separate fields; **public eligibility requires BOTH**. The public artifact carries
  only masked/public enums, never the raw origin.
  - Projection is a **closed graph**: FK closure, no unreachable works, routing-audit rows, counts,
    aggregates, sort behaviour and auxiliary tables all projected and verified — not claim rows alone.
  - Measured motivation: the restricted-corpus id prefix maps to **656 restricted-identity works AND
    235 open (Sefaria) ones**, so a corpus-keyed rule mislabels in both directions.
  - Reference numbers: shipped display claims by corpus × family — sefaria/direct 113,337; ja/direct
    15,821; msource/direct 15,136; sefaria/propagated 11,604; msource/propagated 6,564; ja/propagated
    4,075. Works: 1,269 total (sefaria 507, msource 656, ja 106); 1,088 with ≥1 shipped claim.

### Novelty (NOVEL-01/02)

- **D-23 — The full check.** The enumerable finding-aid set (FJMS + NLI catalogue and bibliography,
  titles, PGP, FGP, M-source shelfmark attributions), per (manuscript, specific work), with the LLM
  title judgement for cases a string comparison misses. The owner's own framing — "what's computed and
  not in the catalogue is new" — IS the funnel's catalogue arm. Handoff specifics to honour: compute at
  the **raw `ref_work`** grain, not the over-collapsed `canonical_work_id`; read the catalogue's own
  identification (`catalog.TitleHeb` / `GenizahTitleOrgTitle`), NOT `catalog_refs` (matched zero);
  keyed by `sys_id == AlmaId`; bibliography `published_full` and bare PGP descriptions **over-demote**
  (presence ≠ naming *this* work).
- **D-23a — TRI-STATE, fail-closed (owner, post-Codex F-06).** Values are
  **`known` / `not_found` / `indeterminate`**, defaulting to `indeterminate` whenever anything fails —
  a source is unavailable, an identifier does not normalize, a snapshot is incomplete, a cache is
  stale, or the model abstains. A boolean cannot distinguish "checked and found nothing" from "the
  check failed", and the live asset already contains **665 claims whose evidence rows disagree with
  each other** on `is_new` (of 29,054 multi-evidence claims). The assessment is centralized at a
  versioned manuscript–work key, and a verifier asserts every evidence row of a claim inherits ONE
  result.
- **D-23b — Wording: "Not found in the finding aids checked"** / the Hebrew equivalent, shown with the
  checked-source list and their dates. This replaces "not identified in any available finding aid",
  which over-claims relative to the finite set actually checked. "New", "new discovery" and "unknown to
  scholarship" stay PROHIBITED. NOVEL-01's wording is amended accordingly.
- **D-23c — The LLM gate needs a reproducible contract:** pinned prompt hash, exact model and version
  (`gemini-3.6-flash` + `reasoning:{effort:"low"}`), normalized input hash, **structured abstention**
  (→ `indeterminate`), an explicit cache-key specification, and a **substantially larger owner-labelled
  hard-case evaluation than the 40 cards** used so far — agreement on 40 is too weak for an axis this
  reputationally loaded. Cost ≈ $27 one-time; **do not downgrade the model**; always read real
  `usage.cost`. The verdict cache is a build-time artifact and never ships.
- **D-23d — Novelty needs its OWN reviewed identity key.** Raw source-work ids split aliases; the
  existing canonical key is documented as over-collapsing (one collapsed id covers 39 Bible books).
  Define a reviewed `novelty_work_key` with aliases and a deterministic "known via ANY alias ⇒ not
  novel" rule (Codex F-07).
- **D-24 — Novelty is structurally orthogonal to the tier** — never feeding band assignment, precision
  copy, ranking weight or certified styling. Enforced in the UI by D-15a (filter/grouping, not order).
- **D-25 — Prior-record provenance is masked.** Name the source where nameable ("recorded in the
  catalogue"); otherwise **"recorded in another reference source"**, with no corpus name anywhere —
  including copy/clipboard output, JSON payloads and error paths.

### Performance (PERF-01)

- **D-10a — The findings page cannot meet the current budget without new indexes (Codex F-10,
  measured).** A representative novelty/tier/coverage ordering scanned all display claims, used a
  temporary B-tree, and took **3.41–3.55 s** across four runs against a **1.5 s** cap
  (`discovery-budgets.md:35-42`); the count alone took 0.50–0.55 s. The rebuild must materialize
  sortable **`band_rank`** and **`coverage_ppm`**, add an index supporting findings ordering/filtering
  and a **unique index on `discovery_claim(display_evidence_id)`**, and index the novelty *status*
  rather than the legacy boolean. Every filter/sort/count combination is benchmarked before deploy, and
  the new query shapes plus their worst-case filtered totals are added to the **versioned**
  `discovery-budgets.md` (which today has no findings-page cap at all).

### Codex disposition (`136-CODEX-REVIEW.md`, VERDICT: REWORK)

| Finding | Severity | Disposition |
|---|---|---|
| F-01 tier-A precision forbidden by builder + verifier | BLOCKER | **Resolved** by D-02a (store authorization only) + the narrow lockstep amendment |
| F-02 certificate cannot authorize the public projection | BLOCKER | **Open by design** — D-06b; must be settled before the 139 flip, not in 136 |
| F-03 no-numbers conflicts with BAND-03/05 now | BLOCKER | **Resolved** by D-06a (amend both, methods page goes qualitative) |
| F-04 population-hash equality insufficient | HIGH | **Accepted** → D-02b exact allowlisted diff + card binding + external pinned frame hash |
| F-05 assertion origin not representable | HIGH | **Accepted** → D-22 two-axis build-time derivation + closed-graph projection |
| F-06 boolean novelty cannot express failure | HIGH | **Accepted** → D-23a tri-state, fail-closed |
| F-07 novelty-first ordering contradicts no-ranking | HIGH | **Accepted** → D-15a filter/grouping only + D-23d own identity key |
| F-08 coverage metric mis-described, absent for propagated | HIGH | **Accepted** → D-08a direct-family only, explicit label, `coverage_ppm` |
| F-09 PANEL-03 impossible for b-side; highlight_text unusable | HIGH | **Accepted** → D-12 offset renderer + b-side note; PANEL-03 last |
| F-10 findings query 3.4 s vs 1.5 s cap | HIGH | **Accepted** → D-10a materialized keys + indexes + versioned caps |
| F-11 work query lacks display fields; terminology wrong | HIGH | **Accepted** → D-17/D-17a; figures corrected |
| F-12 not one deliverable phase | HIGH | **Accepted** → D-04 six execution gates |
| F-13 related-page count semantics | MEDIUM | **Accepted** → D-11a |
| F-14 wrappers collapse outage into zero | MEDIUM | **Accepted** → D-13 envelope |
| F-15 title-coverage figure stale | MEDIUM | **Accepted** → D-01 corrected to 34.25% / 80,993 |

### Claude's Discretion

- Per-surface BAND-04 disclaimer variants (base sentence fixed in 135 D-12).
- The D-13c short-evidence threshold's exact value (set at gate 1 with counts).
- The D-13b lead-attribution tie-break beyond tier rank.
- Page sizes, timeouts, LRU sizing and query shapes within the PERF-01 caps; the
  overload/sidecar-absent copy.
- Whether to add a `discovery_evidence.sys_id` index — Codex confirmed the `page_id IN (…)` plan is
  already indexed, so add one only if it replaces that plan.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### This phase's own gate artifacts (read first)
- `.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-CODEX-REVIEW.md` — the
  adversarial review (REWORK) with file:line evidence for every blocker and high.
- `.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-MOCKUP.html` — the
  real-data mockup; `136-MOCKUP-extract.py` + `136-MOCKUP-render.py` regenerate it.
- `.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-DISCUSSION-LOG.md` — the
  alternatives considered (audit trail only).

### Owner strategy & milestone state
- `.planning/v9-PUBLICATION-STRATEGY.md` — open-first / visibility-gated output (§2), the gate at the
  packaging boundary (§3), the novelty axis (§4), gen-2 posture (§5), precision posture (§6), §8 open
  list (items 2, 3 and 6 answered here).
- `.planning/v9-REQUIREMENTS-ADDENDUM-DRAFT.md` — rationale for VIS-01/02 + NOVEL-01/02 and the
  DECLINED curated-surface exception (section C is declined-draft text — do NOT revive it).
- `.planning/STATE.md` · `.planning/REQUIREMENTS.md` · `.planning/ROADMAP.md` §Phase 136.

### GEN2 handoff (the D-01 deferral)
- `same_work_spike/probe/rsource/HANDOFF-TO-135.md` — the v2.1 package; **§6.1 is load-bearing for
  NOVEL-01**. `GEN2-HANDOFF.md` is the fuller engine-track record. `rsource/data/g_launch3.db` is the
  v2.1 evidence — **not consumed in 136**. (gitignored, local-only)

### Discovery contract specs
- `docs/specs/discovery-sidecar-schema-v1.md` — the frozen two-table model, id recipes, enums, §1.6
  precision row-set (D-02a amends it), the nullable shared-text b-side (F-09), the claim/work
  source-corpus equality constraint (F-05).
- `docs/specs/discovery-band-labels-v1.md` — §2 labels, §3 precision-presentation rules (D-06a amends),
  §3.1 coverage bands, §4 default-shown + D-18 amendment, §5 enum lockstep.
- `docs/specs/discovery-budgets.md` — PERF-01 caps; needs a versioned findings-page entry (D-10a).
- `docs/specs/discovery-deploy.md` — asset-first deploy/rollback; §211-231 rebuild command needs the
  live v2 pinned inputs (D-02b).
- `docs/specs/discovery-coordination.md` — session roles, the "v2 vs gen-2" naming rule, the
  pinned-artifact handoff rule.
- `docs/specs/discovery-frames.md` / `discovery-frames-v2.md` — the frozen frame; a rebuild needs a new
  dated frame doc with the public/private row-count reconciliation (VIS-02 input).

### Certificate
- `.planning/phases/135-precision-certificate-confidence-bands/135-09-CERT01-MEASUREMENT.md` — the
  measured 0.9382 [0.9084, 0.9644]; its own text records the public-scope figure as descriptive (F-02).
- `.planning/phases/135-precision-certificate-confidence-bands/cert01_prereg.json` — pins
  `db_content_hash` + `frame_content_hash` (D-02c).
- `scripts/verify_cert01_grading.py` · `scripts/cert01_frame.py` — the 12 checks and the hash recipes.

### Phase context
- `.planning/phases/134-discovery-data-spine/134-CONTEXT.md` — the two-table contract (C-1..C-9), band
  sources, the DATA-10 unit×work projection, the masked-title workflow.
- `.planning/phases/135-precision-certificate-confidence-bands/135-CONTEXT.md` — D-11 toggle wording,
  D-12 disclaimer base sentence, D-17 demotion, D-18 default-shown sequencing.

### Code the phase builds on
- `shared/discovery_service.py` — the async chokepoint; all four read paths exist and are UNUSED.
  Needs the D-13 envelope, the D-17a display fields + count query, and the D-13a/D-13b grouping.
- `web/discovery.py` — fail-open async wrappers, **zero callers**; this phase is their first consumer.
- `shared/discovery_band_labels.py` — `band_label`, `review_overlay`, `band_measurement_status`,
  `is_default_eligible` (reads `measurement_status` + `ci_low` — the D-02a target),
  `serialize_banded_claim` (SC#1, mandatory).
- `web/discovery_assets.py` — the fail-closed versioned loader + availability predicate.
- `web/pages/browse.py` + `web/pages/browse_enrichment.py` — the staleness-guarded enrichment path;
  `highlight_text` at 1577-1601 is NOT reusable for offsets (D-12).
- `web/pages/catalog_browse.py` — the Browse-by-Identification page to extend (D-18).
- `web/main.py:1753-1772` — the nav list the new findings page joins (D-19); `@ui.page` routes at
  2086-2907. The route is `/catalog-browse`, NOT `/catalog`.
- `web/pages/help.py:245-260` — currently renders estimates and intervals; D-06a rewrites this.
- `scripts/build_discovery_sidecar.py` — `_validate_precision_spec` 3865-3890 / 4019-4027, the
  `band_precision` insert 4419-4427, coverage 532-568, largest-span selection 2555-2570/2689-2713.
- `scripts/verify_discovery_sidecar.py` — release check M4 553-560 (tier-A precision NULL), the
  claim/work corpus equality 319-332.
- `scripts/check_atlas_masking.py` + `MASKING_SCAN_PATTERNS_FILE` — the DATA-05 gate; must pass on the
  rebuilt asset, the public projection and every new surface. **Blind spot: it scans git index/HEAD,
  never history.**
- `scripts/discovery_identified_gate.py` + `scripts/title_gate_llm.py` — the pre-built NOVEL-01 funnel
  and title gate (rewire per D-23c).

### Discipline (memory)
- `feedback_catalogue_never_evidence` — the catalogue is a recall yardstick, never acceptance evidence;
  in reverse for novelty: absence from a finding aid is not evidence of correctness.
- `project_msource_codename_rule` — restricted corpora appear ONLY as M-source / R-source.
- `reference_discovery_llm_gate_cost` — the validated cheap gate configuration and cost discipline.
- `reference_io_bound_safe_storage_trap` — background execution loses NiceGUI context; bind
  `page_client` at render time.
- `feedback_plain_english_decision_questions` — put owner decisions in plain English with real numbers.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **The read spine is built, tested and has zero callers** — `shared/discovery_service.py` +
  `web/discovery.py`. This phase is UI plus one rebuild, not data-layer design.
- **`serialize_banded_claim()`** raises rather than emitting a bandless presentation (SC#1).
- **`get_work_witnesses(work_id, enabled_bands, anchor_sys_id=…)`** already implements the DATA-10
  unit×work projection with pagination over units.
- **Browse's enrichment path** (generation-token staleness guard + `enrichment_refs`) is the
  established place for a lazily-loaded section.
- **The `/catalog-browse` facet worker** (`_CatalogFacetWorker`, off-UI-thread counts) is the model for
  the findings page's counts.
- **The atlas availability pattern** (`atlas_preview_available()` gating page + data routes + nav) is
  exactly the model for gating the new findings page and its nav entry.

### Established Patterns
- Content-hashed versioned artifacts; tunable-only-by-versioning contract docs; display labels rendered
  over stored keys; fail-closed loader; masking gate over asset + repo + every surface; asset-first
  deploy (DBs before code); fail-open service wrappers.

### Integration Points
- New page modules for `/work/{id}` and the findings page — **naming hazard:**
  `web/pages/discoveries.py` is the pre-existing community "Discoveries Center" and is unrelated; the
  nav label "Discoveries" is taken.
- Panel → `web/pages/browse_enrichment.py`; catalogue integration → `web/pages/catalog_browse.py`;
  methods rewrite → `web/pages/help.py`; nav + routes → `web/main.py`.
- Gate 1–3 touch the bake/verifier/id modules, the frozen specs, and produce a new schema-versioned
  asset + public projection + a new dated frame doc + the D-02c compatibility attestation.

### Measured facts (deployed asset, verified twice — in-session and by Codex)
- `page_id` = `{sys_id}_{IE…}_{P00000N}_{FL…}` → maps 1:1 onto a browse page.
- 1,269 works / 268,361 claims / 297,415 evidence rows / 166,537 shipped display claims.
- Shipped display claims: tier_a 134,449; `not_evaluated` 20,435; screening_canon 6,594;
  screening_rb 2,399; weak 1,078; high_confidence_algorithmic 852 (750 unreviewed + 102
  human_confirmed); corroborated 730.
- 44,375 manuscripts and 1,088 works carry shipped claims; 9,806 manuscripts carry >1 work.
- Relation population: 40,968 shipped shared-text evidence rows / 37,397 directed / 30,539 unordered.
- All 144,294 shipped direct rows have novelty false (uncomputed); 14,003 propagated rows are flagged;
  665 claims disagree internally.
- 9,549 shipped direct rows have multiple spans (only the largest is stored); all propagated rows have
  NULL `matched_letters`.
- Evidence thinness: 10 direct rows under 50 matched letters, 1,710 at 50–99, 4,838 at 100–149.
- No index on `discovery_evidence.sys_id`; `router_bucket` is NULL throughout; no page-length column.

</code_context>

<specifics>
## Specific Ideas

- Row wording: **"Matches ⟨work⟩ · 68% of page"** / **"התאמה ל⟨חיבור⟩ · 68% מהדף"** (direct family only).
- Panel groups: **"On this page"** then **"Elsewhere in this manuscript"**.
- Competing attributions: **"↳ the same passage also matches …"**.
- Novelty: **"Not found in the finding aids checked"** + the source list and dates; never "new".
- Big-work page: **"4,637 witnesses · page 1 of 24"** — count units, not claim rows.
- The mockup's teaching case: RNL Ms. EVR II A 684 (`סדור מנהג אשכנז`) page 6 carries *Tur Orach Chaim*
  twice under two titles plus *Yalkut Shimoni on Nevi'im* on byte-identical offsets 0–555, and four
  liturgical works sharing one 66-letter span — every row real, shipped and tier-A.
- Owner's framing: *"a big new amazing feature… maximum ability to see new findings"*, and the honesty
  constraint *"we are not sure that tier_a is same work and the next are parallels, just heuristics."*

</specifics>

<deferred>
## Deferred Ideas

**Open, resolved during execution (gate noted):**
- **Gate 1:** the D-13c short-evidence threshold; the D-13b lead-attribution tie-break.
- **Gate 5:** the findings page's row unit (recommended default: one identification per line); how
  computed rows are presented alongside catalogued ones on `/catalog-browse`.
- **Gate 6:** PANEL-03's final form, given the b-side has no offsets.

**Owed at Phase 139:**
- **D-06b** — whether the public projection gets its own pre-registered estimand or REL-01 is amended.
- REL-01/CERT-02's "tier-A goes public WITH its measured number" clause versus D-06.
- The correction/retraction policy that lost its home when the curated-surface exception was declined.
- VIS-02's positive control and the public/private row-count reconciliation.

**Later phases:**
- **The gen-2 / discovery-v2.1 evidence refresh** as its own phase (D-01), carrying the
  reference-granularity stage and the witness-vs-quoter lever, plus the ~2,670-work title curation
  round and the CERT-01 re-registration a population change forces.
- Community judgments (137); the leads queue (138).
- The work-page library filter if reuse is not cheap (D-16).
- Whether restricted-source rows ever move private → public (`v9-PUBLICATION-STRATEGY.md` §8.5),
  which decides if D-22's projection is permanent or a revisable policy label.

**Bookkeeping owed before/at planning:**
- `REQUIREMENTS.md`: NOVEL-01/02 traceability → Phase 136; the BAND-03, BAND-05 and NOVEL-01 wording
  amendments; note that 136 amends contracts.
- `ROADMAP.md`: rewritten Phase-136 goal + success criteria (four surfaces + the rebuild), and the
  findings-page ↔ leads-queue relationship at 138.

</deferred>

---

*Phase: 136-read-surfaces-connections-panel-work-witnesses*
*Context gathered: 2026-07-30 · revised the same day after the mockup + Codex gates*
