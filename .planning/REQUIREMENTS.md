# Requirements: GenizahSearch — Milestone v9.0.0 Discovery

**Defined:** 2026-07-20 (Codex 6-round convergence + full owner requirement walk)
**Core Value:** Researchers can find what they need in the Genizah corpus
**Milestone:** v9.0.0 Discovery — Same-Work Identification & Connection Atlas (web-only)

## v9.0.0 Requirements

### Claim Model & Data Spine

- [x] **DATA-01**: A canonical claim model defines TWO stored claim families: (a) **work–witness claims** (`sys_id`, `work_id`, supporting page IDs, page→witness aggregation rules) and (b) **MS–MS relation claims** (`sys_id_a` < `sys_id_b` canonical ordering, direct-alignment basis) carrying CHILD alignment records — (`page_id_a`, `page_id_b`, per-side evidence spans), queryable by page so PANEL-02's "pages related to this page" view is served directly — while the stable parent claim ID remains the voting target. `claim_id` is a deterministic content key namespaced by family — SHA-256 over the canonical UTF-8 serialization `{claim_family}|{sys_id}|{work_id}|{claim_type}` (work–witness) / `{claim_family}|{sys_id_a}|{sys_id_b}|{claim_type}` (MS–MS, `sys_id_a` < `sys_id_b`) — hash algorithm + serialization frozen in the schema doc, stable across rebuilds, uniqueness constraint on the key fields. Each stored claim carries a `claim_type` from a fixed relation vocabulary (direct witness / quotes-this-work / textual parallel / direct text overlap — the semantic mapping from engine flank-class is FROZEN before distillation; only bilingual display wording is deferred to the UX discuss-phase), a sidecar version, and exactly one band assigned at distillation. A manuscript — even a single page — can legitimately carry MULTIPLE work–witness claims (separate codicological parts; interleaved works such as Talmud+Rashi+Tosafot or Mikra+Targum+Tafsir; one main work plus quoted works): deduplication and band precedence operate only within a single claim key, never across different works. "Related manuscripts via shared work" lists are deterministic projections of work–witness claims (never stored as separate claims); a projection displays the WEAKER of the two claims' bands. Judgments target stored claims of either family. Text-relations are never presented AS physical joins (a same-work identification is not join evidence); known physical joins feed DATA-10 witness-unit aggregation instead.
  - **⟨SUPERSEDED 2026-07-21 — Phase-134 CONTRACT CORRECTION (`134-CONTEXT.md` C-1..C-9); RATIFIED by owner 2026-07-28 — the corrected two-table model is what shipped in the v1 and v2 assets and was verifier-confirmed⟩** The two-stored-claim-families framing above (separate work–witness + MS–MS claim tables; sys-level MS–MS claim IDs; `claim_id` hashing `claim_type`; the four-value relation vocabulary; "exactly one band assigned at distillation") is REPLACED by a **two-table** model: `discovery_claim` PK `(page_id, work_id)` — the REAL page, NO physical-MS claim-key collapse — 1-to-many `discovery_evidence` (PK `evidence_id`, with an `evidence_kind ∈ {witness, shared_text}` discriminator). `claim_type ∈ {direct_witness, quotes_this_work, shared_text}` is DERIVED from the evidence (`textual_parallel`/`direct_text_overlap` DROPPED into `shared_text`); `claim_id = SHA-256` over the STABLE key `(namespace, page_id, work_id)` — NOT `claim_type`. Recall-widening is an orthogonal `evidence_source ∈ {track1_direct, propagated}` axis (a propagated witness is HOW we know, not a separate claim). "MS–MS relation" becomes `shared_text` evidence still anchored to a catalogued `works.work_id` (anchorless MS-MS residue EXCLUDED per the frozen OQ2). A claim MAY carry MULTIPLE evidence rows with DIFFERENT bands; ONE deterministic `display_evidence_id` is selected for presentation. Physical-MS grouping is a DATA-10 unit×work PROJECTION via `witness_units`, never a claim collapse.
- [x] **DATA-02**: The band model is frozen in the requirements: allowed band set = `expert_verified` (R-A) > `tier_a` (algorithmic) > `screening_rb` (R-B) > `screening_canon` (R-CANON); the SAME claim key appearing in multiple frames takes the highest band by that precedence (exactly-one-band per claim key, post-precedence) — distinct works carried by the same manuscript are distinct claims and all ship with their own bands; MS–MS relation claims take the band of the frame that produced them under the same precedence. The distillation phase must emit a version-controlled **frozen-frame artifact** (`discovery-frames.md`: per-band deduplicated claim counts, page→claim dedup formula, overlap-resolution counts, frame content hash) BEFORE certificate cards are drawn; work IDs are opaque product IDs (no raw research IDs); schema is allowlisted with uniqueness + referential-integrity checks
  - **⟨SUPERSEDED 2026-07-21 — Phase-134 CONTRACT CORRECTION (`134-CONTEXT.md` C-4/C-5/C-7); RATIFIED by owner 2026-07-28 — the per-`evidence_source` band model is what shipped in the v1 and v2 assets and was verifier-confirmed⟩** The band model is per-`evidence_source`, NOT a single global precedence with "exactly-one-band per claim key": **track1_direct** → `{expert_verified, tier_a, screening_rb, screening_canon}`; **propagated** → `{corroborated, weak, not_evaluated}` (`not_evaluated` is a real enum for `shared_text`). Bands are assigned per evidence row; the "exactly-one-band-per-claim-key" invariant is DROPPED (a claim legitimately holds multiple evidence rows/bands, resolved to one `display_evidence_id`). The held-out **0.926 [0.875,0.968]** attaches at the **propagated witness COLLECTION** level (corroborated ∪ weak; 200-card draw = 90 corroborated + 110 weak, 176/190), NOT to `corroborated` alone; `corroborated` ranks above `weak` structurally but neither carries a separate manufactured band interval (both PROVISIONAL). The word "certified" is PROHIBITED until the independent audit gate (registry-gated `audit_status`). The frozen-frame artifact (`discovery-frames.md`) + the `band_precision` table (scope-discriminated — the 0.926 stored ONCE at `scope='collection'`) remain phase exit criteria.
- [x] **DATA-03**: Provenance masking is structural: the shipped sidecar cannot contain reference text, sigla, or provenance columns; claim evidence is stored ONLY as offsets into our own HTR text with the HTR snapshot hash recorded, offsets validated at render time, failing closed (no evidence shown) on text-version drift
- [x] **DATA-04**: The shipped work set is a curated SUBSET of the research works (all ~4,093 need not ship; the v9.0 launch may restrict to works sourced from open corpora — Sefaria + the JA corpus — deferring M-source-derived works; the selection is recorded at distillation); every shipped work carries a human-reviewed neutral title with NO fallback to research titles, and all displayable fields (title, author, genre) pass the same review
- [x] **DATA-05**: A permanent CI leak-vector guard scans the shipped sidecar (schema + all cell values) and every product surface — rendered pages, clipboard/copy output, atlas assets, JSON payloads, SEO/JSON-LD, sitemap, error messages, Supabase claim payloads — for provenance leaks; the guard also scans COMMITTED repo content (code, docs, planning artifacts — git index/HEAD, not gitignored research trees): the restricted source appears in committed material only under the internal codename M-source, never by name; the guard's first run includes the one-time cleanup verification (research trees untracked + gitignored — done 2026-07-20; no tracked occurrence remains) and any legacy uncommitted prototype strings (e.g., the current `genizah_translations.py` working-tree additions) must be scrubbed before they may be committed
- [x] **DATA-06**: All web access goes through one async DiscoveryService chokepoint with per-query timeouts, bounded concurrency with defined user-facing overload behavior ("temporarily unavailable", no hang), indexed bounded queries, LRU caching on the browse-enrichment path, and server-side pagination everywhere lists can grow
- [x] **DATA-07**: A feature flag gates all discovery surfaces (pages, panels, homepage band, sitemap/SEO discovery); when off or when the sidecar is absent, every surface hides cleanly with zero errors
- [x] **DATA-08**: The sidecar release contract includes schema version, source-DB hash, build date, data-as-of date, expected row counts, `PRAGMA integrity_check`, and a disk budget (**working target ≤ 300 MB — NOT a numbered cap**; owner size re-acceptance recorded 2026-07-28: v1 shipped at 368.5 MB and the frozen v2 asset is ~370 MiB, both accepted. The only HARD budget contract is PERF-01's RSS ≤ 250 MB, measured on the prod box at 11.2 MB); the sidecar filename is schema-versioned so old code never opens an incompatible snapshot; a corrupt/incompatible sidecar is rejected at startup while the rest of the app stays fully available; deploy = temp upload → verify → atomic rename → code deploy, rollback documented, full rebuild recipe reproducible
- [x] **DATA-10**: A **codicological witness unit** is defined over stored claims as a UNION of sys_ids: sys_ids merge into one unit via (a) catalogued codicological parts (e.g., an Oxford part ID grouping several sys_ids into one MS) and (b) known physical join groups (PGP/FJMS/user; joins whose basis is only "same scribe" do NOT merge); witness lists and counts group by unit (a joined/part-grouped codex counts as ONE witness) while claims stay stored per sys_id so claim IDs remain stable as join knowledge evolves; `unit_id` is deterministic per snapshot (hash over the sorted member sys_ids) and membership is recorded in the sidecar; the unit×work projection rule is fixed: a unit's row on a work page displays the HIGHEST band among its members' claims for that work (band filters act on that displayed band), member claims are visible on expansion, and same-unit members are suppressed from "other manuscripts of ⟨work⟩" lists (a joined fragment is not "another manuscript")
- [ ] **PERF-01**: A versioned acceptance-budget artifact (`discovery-budgets.md`) is a phase exit criterion, with initial numeric caps: browse-enrichment added latency p95 ≤ 150 ms; atlas drill-down response ≤ 1,500 nodes / 6,000 edges / ≤ 2 MB, server p95 ≤ 3 s, timeout 10 s; work/leads pages ≤ 200 rows per page, response ≤ 500 KB, server p95 ≤ 1.5 s, timeout 5 s; browse-enrichment query timeout 2 s; discovery adds ≤ 250 MB RSS on the prod box — measured before release, tunable only by versioning the artifact

### Public/Private Visibility

*(Added 2026-07-28 by owner-approved addendum. Source decision: `.planning/v9-PUBLICATION-STRATEGY.md`, owner 2026-07-27 — open-first, visibility-gated output.)*

- [ ] **VIS-01**: The bake emits **two assets from one normalized build** via deterministic projections — a PUBLIC asset restricted to open-licensable provenance (launch scope: Sefaria-direct matches ∪ all MS-relationship/`propagated` claims) and a PRIVATE asset carrying the full register — each with its own manifest, content hash, release-contract row counts, and per-projection invariant tests. M-source / JA / R-source remain permitted private **inputs** (dating, deduplication, canonicalization) while being excluded from public **outputs**. Exclusion is **structural**: private rows are ABSENT from the public artifact, never merely hidden by a UI filter or a query predicate. The projection keys on the origin of the DISPLAYED assertion, not on `works.source_corpus` alone, which may be insufficient after cross-corpus merge/dating/canonicalization. Implementation is ONE gate at the packaging boundary — never duplicated in the matching engine — and adding a new `source_corpus` / `evidence_source` code remains a dated amendment to `docs/specs/discovery-sidecar-schema-v1.md`
- [ ] **VIS-02**: A release-gate check proves the public asset is free of restricted provenance: the DATA-05 scan runs over the public asset's schema AND all cell values, plus every surface that reads it (rendered pages, copy/export output, JSON payloads, SEO/JSON-LD, sitemap, error messages), and a positive control confirms the scan would FAIL on a deliberately seeded restricted row (no silent green). A public/private row-count reconciliation is recorded in the frame artifact

### Novelty Axis

*(Added 2026-07-28 by owner-approved addendum. Homed AFTER Phase 136 by owner decision — Phase 136 stays read-surfaces-only; the novelty axis needs the LLM title-gate rewired to the validated cheap config, the heuristic funnel run, and the `track1_direct` coverage gap closed, which is a phase of work in its own right.)*

*(**MOVED INTO Phase 136 — owner, 2026-07-30**, `136-CONTEXT.md` D-15/D-23/D-23a-d/D-24/D-25. Reversing the 2026-07-28 homing: the novelty axis is the owner's primary reason the read surfaces are worth shipping — "new — or those that are not obvious from the title/identification — are the most important" — and the surfaces cannot be designed around a filter that does not exist yet. The work the addendum priced (rewiring the title gate, running the funnel, closing the `track1_direct` coverage gap) becomes execution gates inside 136 and rides the ONE authorized rebuild.)*

- [ ] **NOVEL-01**: Every claim carries a computed **novelty flag** decided per `(sys_id, work)` — NOT per work — by checking whether ANY available finding aid already ties THAT fragment to THAT work. The checked source set is enumerable, versioned, and recorded in the sidecar `meta`: FJMS and NLI catalogue + bibliography, titles, PGP, FGP, and M-source shelfmark attributions. The flag is computed for **ALL evidence families** (`track1_direct` AND `propagated`) — the frozen v2 asset computes it only for `propagated`, leaving all 254,612 `track1_direct` rows at `is_new = 0`, which is a coverage gap, not a result. Public display wording is **"not identified in any available finding aid" / "לא מזוהה באמצעי העזר הקיימים"**, shown with the checked-source list and the as-of date, and with the owner's confidence estimate stated AS an estimate. The wordings "new discovery" / "new" / "unknown to scholarship" are PROHIBITED on public surfaces (they assert a construct the check cannot establish). Novelty is a filterable axis on the panel and `/work/{id}`, and is **structurally orthogonal to the confidence band**: it must never feed band assignment, precision copy, ranking weight, or certified styling — absence from a finding aid is not evidence a claim is correct (the catalogue-never-evidence rule, applied in reverse)
  - **⟨AMENDED 2026-07-30 — Phase-136 discuss, owner (`136-CONTEXT.md` D-23a/D-23b/D-23d)⟩** Three changes. (1) The flag is **TRI-STATE, fail-closed**, not boolean: `not_in_finding_aids` / `already_recorded` / `not_checked`; only the first is filterable as novel, and an unrun or failed check reads `not_checked` — never "novel by default". (2) Display wording is **"Not found in the finding aids checked" / "לא נמצא באמצעי העזר שנבדקו"** (past tense, scoped to what was actually checked) — replacing "not identified in any available finding aid", which overclaims coverage we do not have; the checked-source list and as-of date still ship with it. **The owner's confidence estimate is DROPPED** from the surface, per D-06 (no percentages anywhere). (3) The check keys on a **reviewed novelty identity** (alias/containment-aware), not raw source work ids, which split aliases and would manufacture false novelty.
  - **⟨AMENDED 2026-08-02 — Phase-136 re-scope, owner (`136-CONTEXT.md` A-6, D-23a)⟩** The shipped
    candidacy wording is fixed on the record: toggle **"Candidates for new finds" / "מועמדים לממצאים
    חדשים"**, row badge **"Candidate new find" / "מועמד לממצא חדש"**, sub-line **"Findings you would not
    reach by searching the catalogues"**, and a help affordance carrying the checked-source list, the
    as-of date, and the explicit sentence that the identification itself is an unreviewed algorithmic
    match, so this is a candidate, not a confirmed find. "New discovery" and "likely new find" were
    **offered and DECLINED** because they stack two unearned claims on a row carrying no human review
    until Phase 137 — that the match is correct, and that it is new; a wrong match that no catalogue
    records is not a discovery. "Not found in the finding aids checked" survives as the explanatory
    sentence carried alongside the toggle/badge; "new discovery" / "unknown to scholarship" stay
    PROHIBITED. D-15a is restated: novelty filters and MAY group; it never orders and never styles a row.
    Strike the base requirement's clause "with the owner's confidence estimate stated AS an estimate" —
    replaced by the tri-state fail-closed vocabulary and the reviewed alias-aware identity key already
    named in the 2026-07-30 amendment above.

    **Reconcile the two novelty vocabularies on the record.** `.planning/ROADMAP.md` Phase 136 success
    criterion 6 names the tri-state `not_in_finding_aids` / `already_recorded` / `not_checked`;
    `136-CONTEXT.md` D-23a names the same three states `not_found` / `known` / `indeterminate`. ROADMAP
    outranks the CONTEXT body under this phase's own stated precedence order, so **the STORED enum is
    ROADMAP's: `not_in_finding_aids` / `already_recorded` / `not_checked`, defaulting to `not_checked`.**
    D-23a's `not_found` / `known` / `indeterminate` are recorded here as descriptive ALIASES of those same
    three states respectively, so no later plan invents a fourth value or a second storage vocabulary.
  - **⟨AMENDED 2026-08-02 (later the same day) — Phase-136 plan 03, owner decision E
    (`136-GATE1-DECISIONS.md` item E)⟩** The tri-state `novelty_status` (`not_in_finding_aids` /
    `already_recorded` / `not_checked`) becomes a **SEVEN-VALUE SHADE ENUM** in the SAME column:
    `confirms` / `refines_granularity` / `diverges` / `fills_gap` / `extends` / `alias_merge` /
    `not_checked` (fail-closed default, unchanged in meaning and unchanged as the default). Owner
    rationale: the tri-state collapsed two materially different findings into one bucket — a
    catalogue CONTRADICTION ("an aid ties this fragment to a DIFFERENT work") scored
    `not_in_finding_aids`, the SAME score as a genuine "previously unknown" case, and a granularity
    refinement (an aid names the parent/child work — the D-13d author-gated rule; Class 3's measured
    276-group population) scored `already_recorded` and became invisible though it is genuinely
    informative. **The public surface is UNCHANGED IN SHAPE:** "Candidates for new finds" (the
    2026-08-02 A-6 candidacy wording immediately above) selects `fills_gap` ONLY — the genuine
    "previously unknown" case. `diverges` and `extends` are explicitly EXCLUDED from the candidate
    toggle (a contradiction is not a new find; a folio-extension of an already-identified manuscript
    is unremarkable). `refines_granularity` is stored but never voiced as a candidate. No new public
    filter values, no new bilingual candidacy wording, no change to D-15/D-15a/D-16, no change to
    D-24's orthogonality-to-tier rule. `not_checked` remains the sole fail-closed default — never
    "novel by default." This amends the 2026-07-30 amendment's clause (1) above; clauses (2) (display
    wording) and (3) (reviewed novelty identity) are UNCHANGED. Implemented by plans **136-04**
    (novelty module + pinned LLM contract — the prompt must now elicit a shade, so the pinned prompt
    hash necessarily changes), **136-06** and **136-12** (schema/build wiring — the `novelty_status`
    CHECK constraint and its index in `docs/specs/discovery-sidecar-schema-v1.md` widen from three
    values to seven), and **the release verifier** (`scripts/verify_discovery_sidecar.py`'s
    frozen-enum-vocab check, extended in 136-12 and executed as part of 136-13's gate battery) — NOT
    implemented in this plan (136-03), which only records the ruling. Full rationale, the
    condition-by-shade table and the enumerated downstream-contract list are recorded in
    `136-GATE1-DECISIONS.md` § E.
  - **⟨AMENDED 2026-08-02 (same day, later dispatch) — Phase-136 plan 03, owner CORRECTION E′
    (`136-GATE1-DECISIONS.md` § E′)⟩** A correction to the immediately-preceding amendment, not a
    new ruling: decision E's `refines_granularity` row collapsed TWO opposite directions of a
    granularity relationship into one shade. The owner (verbatim): *"if we have רש"י על התורה and
    catalog has רש"י בראשית כב we are different but for worse, so no novelty at all"* — a catalogue
    that already names the chapter knows strictly MORE than a claim naming only the whole work, and
    that direction must not score the same as the opposite one (our claim being the finer one, which
    genuinely adds information). **The shade enum widens from SEVEN to EIGHT values** by splitting
    `refines_granularity` into two direction-gated shades (both still require the D-13d author-gated
    title relationship): `refines_granularity` — OUR claim is finer than the aid's (we add precision);
    `aid_more_specific` — the AID's identification is finer than ours (we add nothing; the catalogue
    already knew more). **`aid_more_specific` is the LEAST novel shade** and joins `confirms` /
    `refines_granularity` / `diverges` / `extends` as EXCLUDED from the "Candidates for new finds"
    toggle — `fills_gap` remains the ONLY shade that predicate selects, UNCHANGED by this correction.
    **Display ruling (owner): SHOW, BUT NEVER AS A CANDIDATE FIND** — `aid_more_specific` rows render
    normally with their tier, exactly like every other non-candidate shade. Three alternatives were
    offered and explicitly DECLINED: demoting these rows below the main pool, showing the aid's
    more-specific wording alongside ours, and hiding them entirely. This is a NOVELTY-ONLY fix: no
    change to the main-pool rule, no new public surface wording, no additional masking surface.
    Implemented by the SAME plans decision E already named (136-04's pinned LLM contract now elicits
    DIRECTION as well, so the pinned prompt hash changes again on this account; 136-06/136-12's
    schema CHECK and frozen-enum-vocab widen from seven to eight values; the release verifier's
    frozen-enum-vocab check) — NOT implemented in this plan (136-03), which only records the
    correction. Full rationale, the corrected two-row table and the enumerated downstream-contract
    list are recorded in `136-GATE1-DECISIONS.md` § E′.
  - **⟨AMENDED 2026-08-02 (same day, a further dispatch) — Phase-136 plan 03, owner decision F
    (`136-GATE1-DECISIONS.md` § F)⟩** Having read the actual 15 Class-6 candidate cases (not merely
    the shade's abstract definition), the owner ruled `diverges` into its own OPT-IN, HIDDEN-BY-
    DEFAULT category, split by SCOPE, with correctness recorded as a SEPARATE axis. **The shade enum
    widens from EIGHT to NINE values**, `diverges` RETIRED and replaced by `diverges_work` (the aid
    names a genuinely different work) and `diverges_part` (the aid names a different/finer part of
    the SAME work — owner: "more delicate and essentially less important"). Owner's stated reason:
    reading the real cases showed divergent rows are largely OUR false positives (worked examples:
    case 92 — we claim ילקוט שמעוני, the aid says תנחומא; case 84 — we claim משנה תורה, ספר זמנים,
    the aid says הגדה של פסח) — but silently trusting the catalogue over the claim would breach the
    catalogue-never-evidence discipline (the catalogue is a recall yardstick, never acceptance
    evidence), so **neither the catalogue nor the system adjudicates; the user does**, via a NEW
    default-visibility rule: `diverges_work`/`diverges_part` rows are HIDDEN BY DEFAULT (not merely
    excluded from the candidate toggle, as decision E already had it) behind an explicit toggle
    carrying a clear warning — reusing BAND-03's "show more possible identifications" shape for a
    different axis. A NEW sibling column, `divergence_correctness` (`catalogue_correct` /
    `claim_correct` / `unclear`, NULL outside the two divergence shades), records which side is
    actually right — an axis the shade token itself cannot carry, since the owner's own review found
    BOTH directions occur under the identical shade. Implemented by **136-04** (shade classifier +
    correctness field + a further-changed pinned prompt hash), **136-06/136-12** (schema/build
    wiring, both the nine-value CHECK and the new column), **the release verifier**, **136-05**
    (allowlist gains the new column), and — newly, not implied by decision E — **136-15/136-17**
    (panel) and **136-16/136-18** (findings page), which must build the hidden-by-default/explicit-
    warned-toggle behavior fresh. NOT implemented in this plan (136-03), which only records the
    ruling. Full rationale and the enumerated downstream-contract list are recorded in
    `136-GATE1-DECISIONS.md` § F.
  - **⟨AMENDED 2026-08-02 (same day, a further dispatch still) — Phase-136 plan 03, owner decision G
    (`136-GATE1-DECISIONS.md` § G)⟩** A boundary correction to the `confirms`/`refines_granularity`
    distinction, with a systemic consequence for the novelty check's own coverage. Worked cases: case
    83 (we claim תשובות האיי גאון against a catalogue structured entry keyed to the generic תשובות —
    but the catalogue's OWN identification TEXT already reads שאלות ותשובות מאת האי בן שרירא גאון) and
    case 87 (we claim ספר יוסיפון (ערבי) against a structured entry keyed to the generic יוסיפון — but
    the catalogue's own text reads יוסיפון בערבית). Owner, verbatim: "What you called situation 2 is
    plain and simple nothing new from our side." **Rule:** `refines_granularity` is reserved for
    cases where we genuinely add information the aid contains IN NO FORM — neither a structured field
    NOR free text; where the aid's own prose already states the identification and only the
    structured work-id keying differs, the shade is `confirms`. **This amends NOVEL-01's own checked-
    source-set clause as a REQUIREMENT, not merely a labelling nuance:** the checked-source list
    already named "catalogue... bibliography, titles, PGP, FGP, and M-source shelfmark attributions",
    but this ruling makes explicit that the check MUST read the aid's FREE-TEXT identification field
    as a genuine input to the confirms/refines/diverges decision — an id-only join against a
    structured field is NOT sufficient and would manufacture false novelty (`fills_gap`) at scale on
    precisely the rows most damaging to publish. Measured finding, recorded not fixed: this project's
    OWN Class-6 selector (`select_catalogue_divergence_candidates`) is exactly such an id-only-join
    heuristic and, per the owner's characterization in ruling F, over-fired on roughly half of the 15
    Class-6 candidates on this account — left uncorrected in this plan (136-03) deliberately, so the
    same failure mode continues to surface as the candidate pool is expanded, per the plan's own
    instruction not to quietly fix it away. Flagged explicitly to **136-04** (the pinned LLM contract
    must present the aid's full free-text identification to the judgment step and must state this rule
    directly) and **136-12** (any heuristic first pass before the model gate must check the free text
    for the claimed work's own identity, not merely test structured-field disagreement). NOT
    implemented in this plan (136-03), which only records the ruling. Full rationale recorded in
    `136-GATE1-DECISIONS.md` § G.
  - **⟨AMENDED 2026-08-02 (same day, after a READ-ONLY prior-art research pass) — Phase-136 plan 03,
    owner rulings H and I (`136-GATE1-DECISIONS.md` §§ H, I)⟩** A read-only reconciliation pass
    (`136-NOVELTY-PRIOR-ART.md`) found decisions E/E′/F/G recorded but never propagated into the
    documents that implement them, and separately surfaced two open questions the owner then ruled on.
    **H — a TENTH shade, `container_predicts`.** The prior-art pass measured a real "witness" shape
    under the OLDER five-way title-gate vocabulary — a catalogue names a broader standard-rite
    prayer-book/cycle/ceremony whose predictable content includes the claimed unit without naming it
    (1,327 of 20,410 in-scope rows, 6.5%, in that population) — which the CURRENT shade enum had no
    bucket for and would misfile as `fills_gap`. Owner: adopt the concept as a shade, under a
    NON-colliding name — "witness" already names five other distinct concepts in this project (per the
    prior-art pass's own sweep). **Chosen name: `container_predicts`**, verified to collide with nothing
    else in the codebase or specs. **The shade enum widens from NINE to TEN values**, `container_predicts`
    inserted immediately before `fills_gap` — the shade it would otherwise be misfiled into by
    elimination. Adopted DEFINITION is VERBATIM from the prior measured work; only the name changes.
    **Treatment: excluded from "Candidates for new finds" like every non-`fills_gap` shade, but — UNLIKE
    `diverges_work`/`diverges_part` — NOT hidden by default.** Ruling F's default-hidden posture was
    specifically about rows the owner has measured reason to believe are OUR false positives; that
    reasoning does not apply here, because there is no disagreement between the aid and the claim — the
    container predicts the unit, it just doesn't name it. A future implementer must NOT generalise F's
    hiding rule onto this shade. `novelty_source_label` populates on `container_predicts` exactly as it
    already does on the other aid-says-something shades. **I — re-measure the pinned gate on the
    WIDENED task before the production run.** Decision B ("run now, pinned") stands as an intention but
    is now CONDITIONED: the pinned config's validation (40/40 agreement; 99% vs 103 human grades) covers
    the FIVE-way vocabulary and the ORIGINAL one-title-string input contract ONLY — it does NOT cover the
    ten-value shade enum or ruling G's free-text input contract. Owner: run the pinned config against the
    owner-labelled evaluation set on the NEW vocabulary/contract FIRST, and only THEN authorize the
    production run. The `~$27` figure on record is a COST estimate carried forward by size-extrapolation
    — NEVER cite it as accuracy evidence for this widened task. **Consequence for the evaluation set:**
    per the prior-art pass's own §5c/§7 recommendation, a NEW Class 7 (liturgical-container
    predictability, ~12 cases) is added to the hard-case worksheet, built with the identical
    zero-model-call, script-reproducible selection discipline used for Classes 4-6 — so the model's FIRST
    encounter with the `container_predicts` question is a graded evaluation, never production. Measured
    outcome (this continuation): 12 Class 7 candidates added, total candidate pool **95** (83 + 12; every
    prior candidate unchanged). Implemented by the SAME plans decision E already named (**136-04** — the
    shade classifier widens to ten values, Task 3 gains the ruling-I re-measurement as a NEW acceptance
    criterion gating the production run; **136-06/136-12** — schema/build wiring for the ten-value CHECK;
    **the release verifier**; **136-05** — allowlist covers the ten-value set), plus — newly, per H's
    display distinction — **136-15/136-16/136-17/136-18** must render `container_predicts` NORMALLY, not
    behind ruling F's toggle. NOT implemented in this plan (136-03) beyond amending this requirement and
    extending the hard-case candidate set (Task 3) — the re-measurement itself is 136-04 Task 3's job.
    This same continuation ALSO performs the surgical reconciliation the prior-art pass found overdue:
    `136-04-PLAN.md`, `136-12-PLAN.md`, `docs/specs/discovery-sidecar-schema-v1.md` and
    `.planning/ROADMAP.md` success criterion 6 are brought into agreement with this ten-value enum. The
    CANONICAL, single-cited restatement of the full enum going forward is `docs/specs/discovery-novelty-v1.md`
    (136-04's own artifact, not yet written as of this amendment) — every document above cites it or
    `136-GATE1-DECISIONS.md` §§ E-H rather than re-deriving the list, except the SQL CHECK constraint in
    the schema doc, where a second literal restatement is unavoidable. Full rationale, the adopted
    definition and the enumerated downstream-contract list are recorded in `136-GATE1-DECISIONS.md`
    §§ H, I.
  - **⟨AMENDED 2026-08-02 (same day, a later continuation still) — Phase-136 plan 03, owner ruling J
    (`136-GATE1-DECISIONS.md` § J)⟩** Closes the open design question `136-NOVELTY-PRIOR-ART.md` § 7
    flagged and left unresolved: does the LLM arm run over ALL identifications, or only a
    heuristic-funnel residual? Owner, verbatim: *"Makes sense to use LLM only after the heuristics."*
    **The funnel runs FUNNEL-FIRST: the heuristic pass (bib/catalogue/FGP/PGP mechanical name-match)
    runs over every identification first; the pinned LLM gate (decision B) sees ONLY the rows that
    pass survive unresolved — the residual.** Rows the heuristic funnel resolves in EITHER direction
    (a genuine name-match, or a heuristic demotion) never reach the model at all. **This is a
    REQUIREMENT on the checked-source-set clause, not merely an implementation detail:** NOVEL-01's own
    "checked whether ANY available finding aid already ties THAT fragment to THAT work" now happens in
    TWO STAGES — a mechanical stage that runs over everything, and a model stage that runs only over
    what the mechanical stage could not resolve.

    **The consequence that must be stated on the record, because it is the real cost of this choice and
    is otherwise invisible:** the funnel only ever DEMOTES (discovery → known, never the reverse), so
    under a funnel-first design a heuristic FALSE-KNOWN is now PERMANENT and UNRECOVERABLE — the model
    never re-examines a row the funnel has already marked "already recorded." Codex measured this exact
    population in the reference implementation: **3,688 `published_full` false-knowns** and **2,014 PGP
    false-knowns** (942 sole-source). The error runs in the CONSERVATIVE direction (a real finding is
    silently lost, never that a fake one is manufactured and published) — the correct direction to be
    wrong given this milestone's publication posture — but it is a real, measured cost, not an assumed
    one, and must be reported as such wherever this axis's behavior is described.

    **Effects this ruling has, enumerated:** (1) **the `~$27` cost basis**
    (`reference_discovery_llm_gate_cost`; decision B) was estimated against the FULL identification
    set; under funnel-first, the true denominator is the RESIDUAL, a strict subset — the figure must be
    re-derived against the actual residual size once the heuristic funnel runs for real (136-04), not
    repeated against the full count without this caveat. (2) **Which Codex-flagged defects the model can
    ever catch**: findings 1 (`published_full` over-demotion) and 6 (PGP over-demotion) are now defects
    the model structurally CANNOT catch (a row they wrongly demote never reaches it); findings 2/3/4/5
    remain correctable, because they concern rows that DO reach the model (the residual). (3) **NOVEL-01's
    honesty wording**: a row demoted by the heuristic funnel receives NO model verdict at all — its
    stored `novelty_status` reflects a MECHANICAL judgment only, never a model's; `docs/specs/discovery-novelty-v1.md`
    (136-04's own contract doc) must state this two-stage distinction explicitly rather than implying a
    uniform pipeline.

    No enum value changes (this ruling is about PIPELINE ORDER, not a new shade or column). Implemented
    by **136-04** (the funnel runner's Task 3 action text is reconciled by this same continuation from
    "run the authorized funnel over the full identification set" to residual-only, funnel-first wording
    — a surgical edit, not a restructuring). The hard-case evaluation set (this continuation's own
    Task 3/4) is restructured into a **three-arm, source-stratified sample** built against the REAL
    bib/PGP/FGP/FJMS-catalogue sidecars (Arm 1 residual, Arm 2 heuristic-demoted, Arm 3 no-source-text)
    that replaces the former Classes 4/5/7, per `136-NOVELTY-PRIOR-ART.md` § 7's own finding that those
    classes read only `libraries.csv` column 7 and had zero representation of the bib/PGP/FGP failure
    modes Codex measured as most damaging. Class 6 (catalogue divergence) and Classes 1-3 (identity
    spot-check) are RETAINED UNCHANGED; Class 5 (generic collection works) is DROPPED (no owner ruling
    exists for any specific Class 5 case). Full rationale, the measured arm sizes and the kept/folded/
    dropped accounting are recorded in `136-GATE1-DECISIONS.md` § J.
- [ ] **NOVEL-02**: The novelty flag's **provenance** (`known_source` — which aid already had it) is masked on the public side: the boolean is publishable, but a restricted-corpus provenance value collapses to a non-identifying label (e.g. "recorded in a restricted corpus"), never the corpus name, and passes the DATA-05 masking scan on every surface that renders or exports it — including copy/clipboard output, JSON payloads, and error paths. Public surfaces therefore support "filter **and explain**" only where the explaining source is itself public; elsewhere they support "filter only". The heuristic-plus-LLM funnel's verdict cache is a build-time artifact and is never shipped in the sidecar

### Bands & Certification

- [ ] **BAND-01**: Every displayed claim carries its confidence-band label inseparably through every UI surface and every serializer/copy path shipped in v9 (v9 adds no new generalized export features)
- [ ] **BAND-02**: Band labels and precision copy are data-driven from sidecar metadata — R-A reads "expert-verified (independent audit pending)" until the audit passes; certification-status changes flip copy with no code change
- [ ] **BAND-03**: High-confidence bands are shown by default; screening bands appear only behind an explicit toggle worded as "show more possible identifications" (probability framing, not "uncertified"; HE wording fixed in the UX discuss-phase); screening rows are labeled "possible identification" with the measured screening precision reachable via the band tooltip → BAND-05 methods page
  - **⟨AMENDED 2026-07-30 — Phase-136 discuss, owner (`136-CONTEXT.md` D-06/D-06a)⟩** The clause "with the measured screening precision reachable via the band tooltip" is **STRUCK**. No precision percentage is reachable from any surface, tooltip included. Owner: *"the exact percentage may be misleading and may include sources the user will never see. What's important is the tiers, and the user can judge for themselves."* The tooltip still links to the BAND-05 methods page, which explains the tier **qualitatively** (as amended below). Everything else in BAND-03 stands: default-shown high-confidence bands, the explicit toggle, probability framing, the "possible identification" label — and per D-21 the tier names carry match framing, not asserted same-work identity.
- [ ] **BAND-04**: A recall-honesty disclaimer appears on all discovery surfaces (sense: "no identification shown ≠ none exists"; per-surface wording varies and is fixed at application time in the UX discuss-phase)
- [ ] **BAND-05**: A bilingual methods/confidence page documents each band: population, unit of measurement, sample size, strata, weighted estimate, confidence interval, measurement date, grader + audit status, immutable report identifier; band tooltips link to it
  - **⟨AMENDED 2026-07-30 — Phase-136 discuss, owner (`136-CONTEXT.md` D-06a)⟩** The methods page is **QUALITATIVE**: **weighted estimate, confidence interval and the strata table are STRUCK**; each tier is explained in words, alongside the non-percentage facts that still publish — that a graded measurement happened, the population, the unit, the sample size, the grader, the measurement date, the method, the audit state, and the immutable report identifier. The amendment is live NOW, not at 139: `web/pages/help.py:245-260` already renders estimates and intervals and must be rewritten in this phase. `docs/specs/discovery-band-labels-v1.md` §3 needs the matching dated amendment. **Still owed at Phase 139:** REL-01 and CERT-02 require tier-A to go public *with* its measured number — that clause must be amended or satisfied at the release gate; 136 publishes nothing, so nothing is violated now.
- [ ] **CERT-01**: A pre-registered stratified tier-A precision measurement runs inside the milestone with a written protocol fixing: estimand + unit (deduplicated manuscript–work claim), the frozen eligible-frame hash (frame freezes AFTER distillation stabilizes, BEFORE cards are drawn), mutually exclusive strata + weights, sampling seed, blindness procedure, treatment of existing gold, exclusion/indeterminate rules, confidence-interval method, pass/fail gates, release copy for every possible outcome, and the FAILURE ACTION: if tier-A fails its pre-registered floor, the `tier_a` band rebands to screening (moves behind the BAND-03 toggle with not-certified labeling, homepage/promotion copy adjusts per the pre-registered outcome branch) — release proceeds with the certified bands as the default view; it is not blocked
- [ ] **CERT-02**: Each band displays ONLY a number measured in that band's own unit, always paired with its status: tier-A shows no precision number until its CERT-01 certificate lands; R-A shows 0.889 with "audit pending" status; R-B/R-CANON show screening values with "not certified"; unit + status are those recorded on the BAND-05 methods page
  - **⟨CONFLICT FLAGGED 2026-07-30 — Phase-136 discuss (`136-CONTEXT.md` D-06/D-06a/D-06b); NOT amended here, resolution owed at Phase 139⟩** D-06 forbids precision numbers on every surface, so "R-A shows 0.889" and "R-B/R-CANON show screening values" cannot both hold with it; the tier-A clause is satisfied trivially (no number, ever). Phase 136 publishes nothing, so nothing is violated now — but the release gate must either amend CERT-02 + REL-01 to the no-number posture or the owner must reverse D-06. Separately (D-06b): the CERT-01 measurement is on the all-source population, and the PUBLIC projection is a structurally different one — the measured figure must never be copied into the public asset as if frame-matched, whether or not any number is displayed.

### MS Connections Panel

- [ ] **PANEL-01**: The browse page offers a "Computed identifications" / "זיהויים מחושבים" button (enrichment section on the existing staleness-guarded path) opening the manuscript's list of potential identifications, each with band label and inline voting — ✓ / ? / ✗ + optional note — wired to JUDGE-01
- [ ] **PANEL-02**: The panel shows two relation types, both with band labels + click-through: (1) "Other manuscripts of ⟨work⟩" — MS-to-MS with title info, derived from shared work–witness claims (displays the weaker band of the pair; shows each side's own relation type when they differ); (2) "Pages in other manuscripts related to this page" — direct page-to-page alignment claims presented in the context of the currently viewed page
  - **⟨AMENDED 2026-08-02 — Phase-136 re-scope, owner (`136-CONTEXT.md` A-2, A-3, D-09)⟩** Amends BOTH
    PANEL-01 and PANEL-02. A panel-level **relation filter** ("Direct match / Partial match / Shared
    text", AND-composed with every other active filter, empty set = all) is IN SCOPE for this phase; it
    was previously specified only for `/work/{id}` under D-16. D-09's amendment: the manuscript group is
    **not collapsed** — the two panes carry EVEN weight (1fr/1fr at ≥900px, stacking page-then-manuscript
    on mobile) and the manuscript pane NAMES the works; the left-to-right ordering ("On this page" first,
    then "Elsewhere in this manuscript") is unchanged. Quality is displayed as **two buckets** ("main
    pool" / "more matches"), per the rule in `main-pool-rule.md`; band labels are tooltip-only (the frozen
    strings are unchanged; only their surface changes); no human-review badge is rendered on any row
    (D-13f). Work titles render as **plain text, not links**, until Phase 136.1 ships `/work/{id}`.
- [ ] **PANEL-03**: An on-demand evidence view shows the supporting span(s) from OUR manuscript text with match stats; for MS–MS relation claims each side shows its own span; reference text is never rendered
  - **⟨AMENDED 2026-08-02 — Phase-136 re-scope, owner (`136-CONTEXT.md` A-4, D-12); forward note —
    delivered in Phase 136.1, moved out of Phase 136⟩** Strike "reference text is never rendered".
    Reference text MAY render, licence-gated PER WORK — the operative test is the acquisition manifest's
    per-work `reuse_ok` flag, NOT the source corpus: `reuse_ok='yes'` (**277** works — PD 264 · CC-BY 11 ·
    CC0 1 · CC-BY-SA 1) may render, carrying the manifest's generated `attribution_text` where the licence
    requires it; `unclear` (**46** — 42 `unknown` · 4 CC-BY-NC), `noncommercial_only` (1) and **absent**
    (**21** — every JA work) all **fail closed** — a work ABSENT from the manifest fails closed, it never
    defaults open. Two counter-intuitive facts to plan around: "public corpus" is NOT the operative
    test — being on Sefaria is not permission, and a meaningful share of Sefaria works are not clearly
    reusable — and every JA work is absent from the licence metadata, so JA text cannot render today even
    though JA rows display elsewhere in the UI. D-12's corrected mechanism, in one sentence: stored
    offsets index the normalized Hebrew-letter stream (not raw text), the result must be clipped per
    line, one renderer must emit both discovery spans and search-term marks, and the highlight is dropped
    on version change.

### Work → Witnesses

- [ ] **WORK-01**: A `/work/{id}` page lists all identified carriers in the dated snapshot grouped by codicological witness unit (DATA-10: joined fragments appear as one witness) with per-witness band labels, server-side pagination (PERF-01 page cap), deterministic sorting, visible counts, and band + library filters with defined semantics (filters compose as AND; empty filter = all currently ENABLED bands — screening rows appear on any surface only while the BAND-03 toggle is on)
  - **⟨AMENDED 2026-08-02 — Phase-136 re-scope, owner (`136-CONTEXT.md` A-3); forward note — delivered
    in Phase 136.1, moved out of Phase 136⟩** Strike "band + library filters". A tier-labelled control is
    **DELETED, not renamed** — quality is displayed as the two-bucket main-pool split (a default plus a
    "show more matches" toggle) and kind is the relation filter; a tier-labelled control would be a second
    vocabulary for an axis the rows no longer speak. Tier A stays reachable behind the screening toggle.
    Novelty and coverage filters remain; the library filter is included only if the existing web
    library-filter component drops in cheaply (D-16), otherwise deferred.
- [ ] **WORK-02**: Works are findable by neutral title with defined bilingual title normalization and alias/duplicate-title handling, linked from panel + atlas; the identification-browse surface reuses/extends the existing `/catalog` Browse-by-Identification structure (domain/author/work facets) rather than inventing a new browse paradigm (exact shape fixed in the UX discuss-phase)

### Atlas & Homepage

- [x] **ATLAS-01**: An offline-precomputed, canon-masked aggregated corpus overview ships as a static asset (layout never computed at request time); OWNER PRIORITY (2026-07-20): this is the milestone's FIRST deployable artifact — an early standalone beta page, shipped before the claim surfaces, under the REL-01 atlas-preview exception
- [ ] **ATLAS-02**: A server-bounded drill-down explorer serves capped neighborhoods (PERF-01 node/edge/byte caps, single-hop server-side expansion); the client never loads the full edge set; the graph's primary object (works vs manuscripts vs clusters) is fixed in the UX discuss-phase BEFORE sidecar/layout design
- [ ] **ATLAS-03**: Homepage promotion is a CLS-safe static band (no live suggestions/graph in v9), enabled only via the REL-01 gates. ATLAS-PREVIEW EXCEPTION (owner, 2026-07-20, Phase 133 discuss): a small claim-free homepage TEASER card linking to the Phase 133 `/atlas` beta may ship early — a CLS-safe static card with NO claim-level statements, gated by the dedicated atlas-preview flag, passing the DATA-05 masking scan + i18n/RTL basics, and `noindex` until the full REL-01 gate; the full homepage discovery band (promoting claims) still waits for Phase 139

### Leads Queue

- [ ] **LEADS-01**: A screening-leads page presents the R-B lane as the lead queue, labeled with the BAND-03 "possible identification" framing, with pagination, deterministic sort, and filters; the canon lane is separately caveated (incl. the known Targum-confusion class), not merely ranked lower
- [ ] **LEADS-02**: Reviewing a lead uses the same JUDGE-01 voting as every other surface: main verdict ✓ / ? / ✗ + note, plus an OPTIONAL advanced typed refinement drawn from a fixed vocabulary (A cites B / B cites A / both cite a common source / compilation / another language or edition / …— finalized in the UX discuss-phase, informed by the E1 reason codes)

### Community Judgments

- [ ] **JUDGE-01**: Logged-in users vote ✓ / ? / ✗ + optional note (+ optional typed refinement per LEADS-02) on a stored claim; the UX is ONE editable judgment per user per claim; storage underneath is DB-enforced append-only (changing a vote inserts a superseding event — audit trail for future certification, visible-vandalism protection, per-sidecar-version context); each judgment persists claim id, claim type, sidecar version, and the band shown at judgment time
- [ ] **JUDGE-02**: The Supabase schema ships with RLS + explicit GRANTs (2026-05-30 rule) and role-matrix tests; note free-text is private by DEFAULT (owner + moderators), implemented by the simplest safe means (e.g., RLS on the notes column/table) — not a place to spend engineering effort beyond the default posture
- [ ] **JUDGE-03**: Community judgments never affect band assignment, precision copy, ranking, or certified styling — structurally a separate display layer
- [ ] **JUDGE-04**: Aggregate judgment signals (incl. explicit disagreement counts) are visible on claims; free-text annotations are captured but NEVER publicly rendered in v9 (public rendering is FUT-06); moderation actions (admin hide, spam mark) are append-only moderation events in a separate table, and hidden/spam judgments are excluded from aggregates
- [ ] **JUDGE-05**: Judgment writes are abuse-resistant: per-user rate limits, annotation length limits, input escaping/sanitization

### Cross-cutting

- [ ] **I18N-01**: All discovery surfaces are fully translated EN/HE with HE-mode render-smoke tests
- [ ] **I18N-02**: RTL/bidi correctness across all discovery surfaces including graph/atlas labels
- [ ] **A11Y-01**: Confidence is never signaled by color alone; graph information has a textual/table equivalent; keyboard navigation and screen-reader labels on interactive discovery controls
- [ ] **A11Y-02**: Contrast checks pass on all new discovery text, controls, focus indicators, band labels/badges, atlas marks/labels, and textual equivalents; atlas transitions/loading/highlight animations honor reduced-motion preferences
- [ ] **SEO-01**: `/work/{id}` + atlas pages get canonical URLs, EN/HE hreflang, titles/descriptions, sitemap inclusion; `/leads` and uncertified-toggle states are `noindex`; JSON-LD carries neutral work metadata only (no algorithmic witness associations); all SEO outputs inside the DATA-05 masking gate; activation gated by REL-01
- [ ] **OBS-01**: Privacy-allowlisted PostHog product events (panel impressions, lead-toggle use, evidence opens, work/atlas navigation, judgment completion) with an explicit denylist: never work titles, manuscript text, shelfmarks, raw research IDs, or annotation/free text in any event property
- [ ] **OBS-02**: Operational metrics cover query timeouts, result truncation, sidecar unavailable/incompatible events, atlas payload sizes, and judgment rate-limit/moderation activity
- [ ] **REL-01**: Release order is gated: claim semantics + masked schema → validated title map + sidecar + frozen-frame artifact → certificate card draw → read surfaces (panel/work) → Supabase migration + security smoke → judgment UI → leads → bounded atlas → public promotion; the feature flag, sitemap/SEO discovery, and homepage band stay OFF until: the CERT-01 measurement is graded to completion, the BAND-05 immutable methods report is published, the CERT-02 outcome-specific copy is applied (tier-A goes public WITH its measured number), and the masking (DATA-05), RTL (I18N-02), accessibility (A11Y-01/02), performance (PERF-01), and deployment (DATA-08) checks pass. ATLAS-PREVIEW EXCEPTION (owner, 2026-07-20): the static atlas overview (ATLAS-01) may deploy EARLY as a standalone beta page, before the certificate and the claim surfaces, PROVIDED it displays no claim-level statements (no work–witness identifications, no bands, no precision numbers — cluster/shelfmark-level visualization only), region/cluster labels come only from our own catalogue titles (libraries.csv — masking-safe catalogue metadata, distinct from the DATA-04 reviewed neutral WORK titles), reviewed neutral titles, or are omitted, an asset-level masking scan passes, its PERF/i18n basics hold, and it sits behind a DEDICATED atlas-preview feature flag distinct from the main discovery flag (the main discovery flag, the full homepage discovery band, and sitemap/SEO discovery stay OFF until the full gate). The exception EXTENDS (Phase 133 discuss, owner 2026-07-20) to a claim-free homepage TEASER card linking to the /atlas beta: same claim-free + masking + i18n/RTL conditions, CLS-safe, gated by the atlas-preview flag, and noindex until the full REL-01 gate; the full homepage discovery band (promoting claims) still waits for Phase 139.

## Future Requirements (deferred)

- **FUT-01**: Text-reuse engine as an alternative/main backend for `/parallels` (desktop: composition) search (user, 2026-07-19)
- **FUT-02**: Public API endpoints for discovery (band-labeled, masked) + skill parity
- **FUT-03**: Desktop parity for the discovery module
- **FUT-04**: Refresh pipeline/cadence for the discovery snapshot
- **FUT-05**: Live-interactive full-corpus WebGL atlas (sigma.js escalation) + multi-hop exploration
- **FUT-06**: Public rendering of moderated free-text annotations
- **FUT-07**: R-B / gen-2-at-scale certification round; R-A independent audit completion (external gate)
- **FUT-08**: New generalized discovery exports (xlsx/CSV)

## Out of Scope

| Feature | Reason |
|---------|--------|
| Crowd auto-certification (votes promote claims into certified bands) | Would pollute the certified bands; judgments are a separate additive layer (Codex R1 #7, FEATURES anti-pattern) |
| Rendering reference-corpus (M-source) text anywhere | Hard provenance-masking constraint |
| Public display of judgment free-text in v9 | Moderation not yet operational; FUT-06 |
| Full-corpus force-directed client-side graph | 89% liturgical giant component: meaningless + browser-melting |
| Live recomputation of identifications | Snapshot ship; rebuild is offline |
| Stemma/variant collation views | Different product; far beyond band-labeled identification |
| Canon-lane certification (round-4 Bible stratum) | User decision 2026-07-19: low scholarly value |
| Per-row decimal probabilities in UI | False precision for lay users; bands only |

## Traceability

Every v9.0.0 requirement maps to exactly one phase. Phases continue from the previous milestone (last phase 132), starting at 133. Owner revision 2026-07-20: Phase 133 = Visual Atlas Preview (ATLAS-01, the milestone's first deployable artifact under the REL-01 atlas-preview exception); the remaining phases run 134-139.

| Requirement | Phase | Status |
|-------------|-------|--------|
| ATLAS-01 | Phase 133 | Complete |
| DATA-01 | Phase 134 | Complete |
| DATA-02 | Phase 134 | Complete |
| DATA-03 | Phase 134 | Complete |
| DATA-04 | Phase 134 | Complete |
| DATA-05 | Phase 134 | Complete |
| DATA-06 | Phase 134 | Complete |
| DATA-07 | Phase 134 | Complete |
| DATA-08 | Phase 134 | Complete |
| DATA-10 | Phase 134 | Complete |
| PERF-01 | Phase 134 | Pending |
| BAND-01 | Phase 135 | Pending |
| BAND-02 | Phase 135 | Pending |
| BAND-03 | Phase 135 | Pending |
| BAND-04 | Phase 135 | Pending |
| BAND-05 | Phase 135 | Pending |
| CERT-01 | Phase 135 | Pending |
| CERT-02 | Phase 135 | Pending |
| VIS-01 | Phase 136 | Pending |
| VIS-02 | Phase 139 | Pending |
| NOVEL-01 | Phase 136 (moved IN, owner 2026-07-30) | Pending |
| NOVEL-02 | Phase 136 (moved IN, owner 2026-07-30) | Pending |
| PANEL-01 | Phase 136 | Pending |
| PANEL-02 | Phase 136 | Pending |
| PANEL-03 | Phase 136.1 (moved out of 136, owner 2026-08-02) | Pending |
| WORK-01 | Phase 136.1 (moved out of 136, owner 2026-08-02) | Pending |
| WORK-02 | Phase 136.1 (moved out of 136, owner 2026-08-02) | Pending |
| JUDGE-01 | Phase 137 | Pending |
| JUDGE-02 | Phase 137 | Pending |
| JUDGE-03 | Phase 137 | Pending |
| JUDGE-04 | Phase 137 | Pending |
| JUDGE-05 | Phase 137 | Pending |
| LEADS-01 | Phase 138 | Pending |
| LEADS-02 | Phase 138 | Pending |
| ATLAS-02 | Phase 139 | Pending |
| ATLAS-03 | Phase 139 | Pending |
| SEO-01 | Phase 139 | Pending |
| I18N-01 | Phase 139 | Pending |
| I18N-02 | Phase 139 | Pending |
| A11Y-01 | Phase 139 | Pending |
| A11Y-02 | Phase 139 | Pending |
| OBS-01 | Phase 139 | Pending |
| OBS-02 | Phase 139 | Pending |
| REL-01 | Phase 139 | Pending |

**Coverage:** 44 / 44 v9.0.0 requirements mapped ✓ (no orphans, no duplicates). Note: the requirement set skips DATA-09 by design — the Claim Model & Data Spine block runs DATA-01..08 then DATA-10.

**Cross-cutting note:** I18N-01/02, A11Y-01/02, SEO-01, OBS-01/02, and REL-01 are homed in Phase 139 (the release-hardening capstone) because their completion criterion is a comprehensive cross-surface gate and several explicitly reference graph/atlas labels. Translations, RTL, and accessibility are nonetheless BUILT INTO every UI surface (Phases 133, 136–139) from line one per house convention — Phase 133's atlas-preview page carries its own PERF/i18n basics per the REL-01 atlas-preview exception; Phase 139 owns their final verification. Likewise PERF-01, DATA-05, and DATA-08 are delivered in Phase 134 but re-verified at the REL-01 gate.

---
*Requirements defined: 2026-07-19 (draft v5 after Codex rounds 1–4; R1 critique at .planning/research/REQUIREMENTS-CODEX-CRITIQUE.md)*
*Traceability populated: 2026-07-20 (roadmapper — Phases 133-139; owner atlas-preview revision applied same day)*
*Revised 2026-07-20 (Phase 133 discuss): ATLAS-03 + REL-01 ATLAS-PREVIEW exception widened to include a claim-free homepage teaser + a dedicated atlas-preview flag; catalogue-title region/cluster labels clarified as allowed in the preview. See .planning/phases/133-visual-atlas-preview-early-quick-win/133-CONTEXT.md (D-05, D-13, D-16).*
