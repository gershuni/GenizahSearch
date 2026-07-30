# Phase 136: Read Surfaces — Connections Panel & Work→Witnesses - Context

**Gathered:** 2026-07-30
**Status:** Ready for planning

<domain>
## Phase Boundary

The **first UI that renders discovery claims**: a browse-page "Computed identifications" panel
(PANEL-01/02/03), a `/work/{id}` witness-map page (WORK-01/02), computed identifications folded into
the existing `/catalog` Browse-by-Identification page, and a corpus-wide **findings browse** ordered
novelty-first. Everything is built **behind the discovery flag** — Phase 139 flips it on (REL-01's
plain reading; the curated-surface exception was declined by the owner on 2026-07-28).

**Owner-authorized scope expansion (2026-07-30, this discussion).** The phase now opens with ONE data
rebuild + ONE production redeploy carrying three payloads, and absorbs two requirements previously
homed after 136:

- **VIS-01** (public/private projection — already homed here)
- **NOVEL-01 + NOVEL-02** (the novelty axis + its provenance masking) — **moved INTO 136**, reversing
  the 2026-07-28 "Post-136" homing. Owner rationale: novelty is the axis that makes these surfaces
  worth using ("new — or those not obvious from the title/identification — are the most important"),
  the surfaces cannot honestly ship a novelty toggle over data where the flag is uncomputed, and the
  phase is already rebuilding + redeploying the asset once, so a second rebuild/deploy cycle is pure
  waste. `.planning/REQUIREMENTS.md` traceability + the ROADMAP Phase-136 goal/success-criteria need
  updating to match (see Deferred → bookkeeping).

**Requirements delivered:** PANEL-01, PANEL-02, PANEL-03, WORK-01, WORK-02, VIS-01, NOVEL-01, NOVEL-02.

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
  `g_launch3.db` — **not in 136.** Measured reasons, all verified against both files during this
  discussion:
  - Only **34.7%** of v2.1's 236,497 shipped claims (82,156) name a work that carries an
    owner-reviewed neutral title. DATA-04 is fail-closed (never ship a research title), so v2.1 at
    full breadth needs a NEW owner curation round over ~2,670 works — and at constant curation it
    would *shrink* the renderable surface from ~152K to ~82K claims.
  - v2.1 **strands CERT-01**: its shipped population differs, and REL-01 wants the measurement to
    describe what ships → re-registration + a fresh blind draw + owner grading time (the scarcest
    resource on this program).
  - v2.1 has **no MS-to-MS / shared_text family at all** (every claim is `direct_witness`) and
    `band` is NULL — so it cannot serve PANEL-02's second view or the propagated witness family, and
    bands would have to be re-derived. A v2.1 bake must MERGE v2's propagated/shared_text inputs
    alongside v2.1's direct evidence; that is a data phase, not a file swap.
  - Carried into that later phase with it: the **reference-granularity stage** (Bible → chapter,
    Mishnah/Tosefta → tractate+chapter, Talmud → folio+amud — actionability, measured NOT to raise
    precision) and the **witness-vs-quoter lever**.
- **D-02 — ONE rebuild + ONE production redeploy, as wave 1, before any surface is built.** Payloads:
  1. **Precision fields** onto the `band_precision` `tier_a` row (`measurement_status='measured_pass'`,
     precision 0.9382, `ci_low` 0.9084, `ci_high` 0.9644, method/strata/report id). Nothing else about
     the claim rows changes, so CERT-01 keeps describing the shipped population.
  2. **Page length / coverage**, so span-coverage can be displayed, sorted and filtered. The builder
     already computes it (it was the Lever-1 routing input); the sidecar currently stores
     `matched_letters` (37–5,847, avg 666) and `density` but **no page length**, and `router_bucket`
     is NULL throughout.
  3. **The novelty flag for ALL evidence families** + NOVEL-02 masking (D-25).
  Asset-first, human-approved, ONCE, per `docs/specs/discovery-deploy.md`.
- **D-03 — Why the rebuild must precede the UI:** in the deployed file the `tier_a` `band_precision`
  row is empty, so `is_default_eligible()` fails closed and the default panel would render **~2,660 of
  ~152K** shipped claims; novelty is flagged for **14,003** propagated rows while **all 144,294**
  shipped `track1_direct` rows sit at `is_new = 0` — a computation gap, not a finding. Building the
  default view against that data would be building against unrepresentative data (the same reason
  VIS-01 was homed here).
- **D-04 — Phase shape: ONE phase, data first, then surfaces.** Not split into two numbered phases;
  the wave ordering does the de-risking and keeps the production deploy count at one.
- **D-05 — Two hard process gates.** (a) **Real-data mockups** (frozen local asset + real HTR text
  from `Transcriptions.txt`) BEFORE the UI implementation decisions are locked; PANEL-03 is explicitly
  held open for one (D-11). (b) A **Codex adversarial pass** — owner: "many possible pitfalls."
  Owner instruction: *"do the corrections and other decisions upon the mockup+codex gate"* — the
  open items in Deferred are resolved there, not by re-running this discussion.

### Bands, numbers and honesty

- **D-06 — NO precision percentages on ANY surface, and no by-source breakdown table.** Owner
  (2026-07-30): *"the exact percentage may be misleading and may include sources the user will never
  see. What's important is the tiers, and the user can judge for themselves."* Surfaces show the tier
  label + review overlay + a link to the methods page. This supersedes the 135 carry-forward that the
  per-stratum spread (1.000 `ja` → 0.471 `msource:medium`) must surface on BAND-05, and it means the
  BAND-05 methods section shipped in 135-02 needs re-thinking into a **qualitative** per-tier
  explanation (what the tier is, how it was produced, what it does not claim).
  - **Flagged for Phase 139, not re-litigated here:** REL-01/CERT-02 currently require that tier-A
    "goes public WITH its measured number." D-06 conflicts with that clause. 139 must either amend
    REL-01/CERT-02 or publish the number then. 136 publishes nothing, so nothing is violated now.
- **D-07 — The measured numbers still go INTO the file, they are just never displayed.**
  `is_default_eligible()` reads `measurement_status` + `ci_low` from the sidecar; that is an internal
  gate, not a display path. Internal ≠ published.
- **D-08 — The span-coverage percentage IS displayed** ("Matches ⟨work⟩ · 68% of page"), sortable and
  filterable. **Do not confuse it with a precision number.** Wording must keep it a statement of how
  much of the page is accounted for, never a confidence score.

### Panel (PANEL-01/02/03)

- **D-09 — Scope: both page and manuscript.** One panel, two groups: **"On this page"** first, then
  **"Elsewhere in this manuscript"** (collapsed). Served via `page_id IN (…)` over the browse page's
  own page list — `discovery_evidence` has indexes on `claim_id`, `a_page_id`, `other_page_id` and
  **none on `sys_id`**. Per manuscript: median **2** shipped claims / **1** distinct work; max 427
  claims / 47 works; **429** manuscripts carry >50 claims → the manuscript group needs pagination.
  (Adding a `sys_id` index to the wave-1 rebuild is Claude's discretion — see below.)
- **D-10 — Nested structure, not three flat lists.** Top level = identification rows (work + tier +
  review overlay + span %). "Other manuscripts matching ⟨work⟩" is derived per work, so it nests
  under its own identification row as an on-demand expansion via
  `get_work_witnesses(work_id, anchor_sys_id=…)` (which already excludes the anchor's own unit and
  suppresses same-unit members). "Pages matching this page in other manuscripts" is a separate
  section below.
- **D-11 — Related-pages section: header + count by default, rows behind the toggle.** The 20,435
  shipped `shared_text` rows carry band `not_evaluated`, which `is_default_eligible()` rejects. The
  section therefore always shows "N pages in other manuscripts match this page" with the disclaimer,
  while the row list expands only via "Show more possible matches". **No amendment to
  `discovery-band-labels-v1.md`** — nothing unevaluated is asserted as an identification.
- **D-12 — PANEL-03 (evidence view): OPEN, decided at the mockup gate.** Owner: *"Probably 1, but
  I'll need to see a mockup (preferably with our real data) to decide."* Leading candidate: **highlight
  the matched span inside the transcription browse already renders** (reusing `web/pages/browse.py`'s
  `highlight_text` machinery) with match stats beside it, failing closed — explicit "evidence
  unavailable for this text version", no highlight — when the live page text disagrees with the stored
  `snapshot_hash`. Alternatives kept alive for the mockup: a dedicated evidence pane; match-stats-only.
  Reference text is NEVER rendered in any variant.
- **D-13 — The entry control is hidden when a manuscript has no claims.** Only **44,375** of ~255K
  sys_ids carry shipped claims (~17%), so the control would otherwise be a dead end on ~83% of browse
  pages. Accepted trade-off: the BAND-04 recall disclaimer then does not appear on empty manuscripts.

### Work page (`/work/{id}`, WORK-01)

- **D-14 — Sort is a toggle with three options, tier-first by default:** (1) strongest tier, then
  shelfmark (default, deterministic); (2) library then shelfmark; (3) span coverage. All three ship.
- **D-15 — A novelty toggle is first-class:** "show only new findings / show everything", with the
  novelty state clearly marked on rows in the second state. **Wording caveat:** NOVEL-01 PROHIBITS
  "new discovery" / "new" / "unknown to scholarship" on public surfaces; the marker must express
  "not identified in any available finding aid" / "לא מזוהה באמצעי העזר הקיימים" in a chip-sized form.
  The exact chip text is an open wording item for the mockup gate.
- **D-16 — Filters: tier + novelty + span coverage are the important ones.** Owner: the library filter
  "is not the one that's important, though it is not entirely useless" → include it ONLY if the
  existing web library-filter component (v8.4 dual-mode show-only/hide) drops in cheaply; otherwise
  defer it. Filters compose as AND; empty = all currently enabled tiers (screening rows appear only
  while the "show more" toggle is on).
- **D-17 — Plain paging with the real total visible.** "13,038 manuscripts — page 1 of 66"; no hidden
  truncation, no size-dependent behaviour change. **120** works exceed the 200-row PERF-01 cap; the
  median work has **9** carriers; 1,088 works have ≥1 shipped claim.
- Witness-unit grouping itself is already fixed by DATA-10 (joined/part-grouped fragments = ONE
  witness at the highest member tier, members visible on expansion) — not re-decided here.

### Findability (WORK-02) and the findings browse

- **D-18 — The `/catalog` Browse-by-Identification page gains computed identifications**, with BOTH
  the same-work-ish and the parallel/citation rows present, **visibly separated and separately
  worded** — but NOT under wording that asserts "is a copy of" versus "quotes". Owner: *"we are not
  sure that tier_a is same work and the next are parallels, just heuristics."* Note the integration
  problem to solve: that page's work vocabulary is the FJMS catalogue's, ours is the 1,088-work
  reviewed neutral-title set, so the mapping is real work and will not be complete.
- **D-19 — A corpus-wide findings browse ships in this phase**, ordered and filtered novelty-first
  (plus tier and span filters). This is where the owner's "maximum ability to see new findings" lives,
  and it gives **SEED-032** (surface new/uncataloged discoveries above known works) its home — closing
  the disposition that `v9-PUBLICATION-STRATEGY.md` §8.6 left open. Its detailed shape (is a row an
  identification, a work, or a manuscript? one surface or two?) is an open item for the mockup gate.
- **D-20 — Work pages are reachable from** the manuscript panel, the catalogue page, and the findings
  browse.

### Relation wording (the deferred DATA-01 display vocabulary)

- **D-21 — "Match framing", heuristic-honest.** Chosen family (owner-selected preview), with the
  Hebrew maqaf written as U+05BE `־`:
  - Row: **"Matches ⟨work⟩ · 68% of page"** / **"התאמה ל⟨חיבור⟩ · 68% מהדף"**
  - Weaker relation: **"Partial match with ⟨work⟩"** / **"התאמה חלקית ל⟨חיבור⟩"**
  - Section: **"Other manuscripts matching ⟨work⟩"** / **"כתבי־יד נוספים התואמים ל⟨חיבור⟩"**
  - Section: **"Pages matching this page in other manuscripts"** /
    **"דפים התואמים לדף זה בכתבי־יד אחרים"**
  - PROHIBITED in display: "copy of", "quotes", "witness of", and anything else that names the
    relationship the routing only guesses at. The stored vocabulary
    (`direct_witness` / `quotes_this_work` / `shared_text`) stays as-is — this is display only.
  - "Possible identification" stays RESERVED for screening-tier rows (BAND-03); it is not a general
    row label.
  - The panel's own title stays the requirement's **"Computed identifications" / "זיהויים מחושבים"**.

### Public/private projection (VIS-01) and novelty provenance (NOVEL-02)

- **D-22 — The public/private decision is made PER ROW, on the origin of the displayed claim** — not
  per work and not on `works.source_corpus` alone. Measured during this discussion: the crosswalk's
  restricted-corpus id prefix maps to **656 restricted-identity works AND 235 open (Sefaria) ones**,
  so a corpus-keyed rule mislabels in both directions. A row is public only when the evidence that
  produced it came from an open source AND the work it names has an open identity. Exclusion is
  **structural** — private rows are ABSENT from the public artifact, never filtered in the UI. Needs a
  build-time per-row provenance decision plus a test proving the public file contains no restricted row
  (the VIS-02 positive control at 139 then has something real to check).
  - Reference numbers for the projection: shipped display claims by corpus × family — sefaria/direct
    113,337; ja/direct 15,821; msource/direct 15,136; sefaria/propagated 11,604; msource/propagated
    6,564; ja/propagated 4,075. Works: 1,269 total (sefaria 507, msource 656, ja 106); 1,088 with ≥1
    shipped claim.
- **D-23 — Novelty computation: the FULL check.** The enumerable finding-aid set (FJMS + NLI catalogue
  and bibliography, titles, PGP, FGP, M-source shelfmark attributions), per `(sys_id, specific work)`,
  with the paid title judgement settling the cases a string comparison misses. The owner's own
  observation — "what's computed and not in the catalogue is new" — IS the funnel's catalogue arm; the
  title check exists because a catalogue title can name the work in different words, and because the
  handoff measured that bibliography `published_full` and bare PGP descriptions **over-demote**
  (presence ≠ naming *this* work). Handoff-supplied specifics to honour: compute at the **raw
  `ref_work`** grain, not the over-collapsed `canonical_work_id`; read the catalogue's own
  identification (`catalog.TitleHeb` / `GenizahTitleOrgTitle`), NOT `catalog_refs` (matched zero);
  keyed by `sys_id == AlmaId`. Cost ≈ **$27 one-time**, ~15 min parallelized, with
  `google/gemini-3.6-flash` + `reasoning:{effort:"low"}` — **do not downgrade the model** (a weaker
  flash model scored 62.5% agreement vs 100%); always read real `usage.cost`. Verdict cache is a
  build-time artifact and never ships in the sidecar.
- **D-24 — Novelty is structurally orthogonal to the tier.** It must never feed band assignment,
  precision copy, ranking weight, or "certified"-style styling. Absence from a finding aid is not
  evidence a claim is correct.
- **D-25 — When a row is already recorded but only in a source we may not name:** the row is simply
  not flagged novel, and the explanation degrades — name the source where nameable ("recorded in the
  catalogue"), otherwise **"recorded in another reference source"** with no corpus name anywhere,
  including copy/clipboard output, JSON payloads and error paths.

### Claude's Discretion

- **Per-surface BAND-04 disclaimer variants.** The canonical base sentence is fixed (135 D-12):
  "Not exhaustive — more identifications may exist." / "אינו ממצה — ייתכנו זיהויים נוספים." Per-surface
  tuning (panel / work page / catalogue rows / findings browse) is Claude's, subject to the mockup gate.
- **Whether the wave-1 rebuild also adds an index on `discovery_evidence.sys_id`** — it would let the
  panel query a manuscript directly instead of via `page_id IN (…)`. Cheap while the file is being
  rebuilt anyway; the planner decides against the PERF-01 budget and the D-09 query shape.
- Page sizes, per-query timeouts, LRU sizing and query shapes, within the PERF-01 caps; the
  overload / sidecar-absent copy; whether `discovery-budgets.md` needs a version bump for the new
  findings-browse and novelty-filter query shapes.
- The band-values/label module already exists (`shared/discovery_band_labels.py`); every surface MUST
  render claims through `serialize_banded_claim()` so the tier is structurally inseparable (SC#1).

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Owner strategy & milestone state (read first)
- `.planning/v9-PUBLICATION-STRATEGY.md` — open-first / visibility-gated output (§2), the ONE gate at
  the packaging boundary (§3), the novelty axis and its precise public label (§4), gen-2 posture (§5),
  the precision posture (§6), and §8's open list — items 2, 3 and 6 are answered by this CONTEXT.
- `.planning/v9-REQUIREMENTS-ADDENDUM-DRAFT.md` — the rationale record for VIS-01/02 + NOVEL-01/02 and
  for the DECLINED curated-surface exception (section C is declined-draft text — do NOT re-derive an
  early surface from it).
- `.planning/STATE.md` — the CERT-01 measurement outcome + the three findings that shape 136+.
- `.planning/REQUIREMENTS.md` — PANEL-01/02/03, WORK-01/02, VIS-01, NOVEL-01/02, BAND-01..05,
  CERT-02, DATA-03/04/06/07/10, PERF-01, REL-01.
- `.planning/ROADMAP.md` §Phase 136 — goal + success criteria (needs updating per D-03/D-19).

### GEN2 handoff (the v2.1 decision — D-01)
- `same_work_spike/probe/rsource/HANDOFF-TO-135.md` — the v2.1 evidence package, the two-surface
  coverage router, the post-G validation, and §6's four stages moved onto the milestone. **§6.1 is
  load-bearing for NOVEL-01** (grain, catalogue source, strictness, pre-built tooling).
- `same_work_spike/probe/rsource/GEN2-HANDOFF.md` — the fuller engine-track fold-up. (gitignored,
  local-only)
- `same_work_spike/probe/rsource/data/g_launch3.db` — the v2.1 evidence. **NOT consumed in 136.**

### Discovery contract specs (single sources of truth)
- `docs/specs/discovery-sidecar-schema-v1.md` — the frozen two-table model, `claim_id`/`evidence_id`
  hashing, the frozen enum vocab, the 2026-07-24 amendments (`measurement_status` CHECK,
  `discovery_routing_audit`, the v2 provenance `meta` keys).
- `docs/specs/discovery-band-labels-v1.md` — §2 EN/HE labels + review overlay, §3 precision-presentation
  rules (a band estimate is NEVER a per-row probability), §3.1 coverage bands + the 0.45 cliff, §4
  default-shown policy + the multi-register invariant + the D-18 amendment, §5 the enum-rename lockstep.
  **D-06 changes what is displayed, not this contract's rules** — any wording change here is a dated
  amendment, never a silent edit.
- `docs/specs/discovery-budgets.md` — the PERF-01 caps and the tunable-only-by-versioning discipline.
- `docs/specs/discovery-deploy.md` — asset-first deploy / rollback / reproducible rebuild (the wave-1
  redeploy follows it).
- `docs/specs/discovery-coordination.md` — session roles, the naming discipline ("v2" is reserved for
  the milestone sidecar; gen-2 outputs are never called "v2"), and the pinned-artifact handoff rule.
- `docs/specs/discovery-frames.md` — the frozen frame artifact; a rebuild needs a new dated frame doc
  with the public/private row-count reconciliation (VIS-02 input).

### Phase context
- `.planning/phases/134-discovery-data-spine/134-CONTEXT.md` — the two-table contract (C-1..C-9), band
  sources, the DATA-10 unit×work projection, the `band_precision` mechanism, the masked-title workflow.
- `.planning/phases/135-precision-certificate-confidence-bands/135-CONTEXT.md` — D-11 toggle wording,
  D-12 disclaimer base sentence, D-17 chronological demotion, D-18 default-shown sequencing.
- `.planning/phases/135-precision-certificate-confidence-bands/135-09-CERT01-MEASUREMENT.md` — the
  measured 0.9382 [0.9084, 0.9644], the strata table, and the error-concentration finding.

### Code the phase builds on
- `shared/discovery_service.py` — the async chokepoint. All four read paths exist and are UNUSED:
  `get_claims_for_page`, `get_pages_related_to_page`, `get_evidence`, `get_work_witnesses`
  (+ `get_band_precision*`, `get_band_claim_counts`). Shipped-only by default with an
  `include_review` opt-in; `_BAND_RANK_*` ordering; LIMIT/OFFSET paginates over units post-grouping.
- `web/discovery.py` — fail-open async wrappers, **zero callers today**; this phase is their first
  consumer. Also `discovery_methods_noindex()`.
- `shared/discovery_band_labels.py` — `band_label`, `review_overlay`, `band_measurement_status`,
  `is_default_eligible` (the D-18 gate), `serialize_banded_claim` (SC#1 — mandatory).
- `web/discovery_assets.py` — the fail-closed versioned loader + availability predicate.
- `web/pages/browse.py` + `web/pages/browse_enrichment.py` — the staleness-guarded enrichment path
  (`_load_generation` generation token, `enrichment_refs`, `update_enrichment_sections`) the panel
  hangs off; `highlight_text` is the PANEL-03 candidate mechanism.
- `web/pages/catalog_browse.py` — the Browse-by-Identification page to extend (domain/author/work
  facets, `_CatalogFacetWorker`, the reusable library filter).
- `web/pages/help.py` — the BAND-05 methods section + its per-band anchor registry (needs the D-06
  qualitative rework).
- `scripts/build_discovery_sidecar.py` / `scripts/verify_discovery_sidecar.py` /
  `scripts/discovery_ids.py` — the bake, the all-invariant verifier + strict masking gate, the frozen
  id/enum module (wave 1 touches all three).
- `scripts/check_atlas_masking.py` + `MASKING_SCAN_PATTERNS_FILE` — the DATA-05 gate that must pass on
  the rebuilt asset AND every new surface. **Known blind spot:** it scans git index/HEAD, never history.
- `scripts/discovery_identified_gate.py` + `scripts/title_gate_llm.py` — the pre-built NOVEL-01 funnel
  and title gate (rewire the LLM gate to `gemini-3.6-flash` + `reasoning:{effort:"low"}`).

### Discipline (memory)
- `feedback_catalogue_never_evidence` — the catalogue is a recall yardstick, NEVER acceptance
  evidence. Applies in reverse to novelty: absence from a finding aid is not evidence of correctness.
- `project_msource_codename_rule` — restricted corpora appear ONLY as M-source / R-source in any
  tracked file or product surface.
- `reference_discovery_llm_gate_cost` — the validated cheap novelty-gate configuration and its cost
  measurement discipline.
- `reference_io_bound_safe_storage_trap` — background execution loses NiceGUI context; bind
  `page_client` at render time (relevant to every async panel/browse fetch).

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **The entire read spine is already built, tested, and has zero callers** —
  `shared/discovery_service.py` + `web/discovery.py`. This phase is UI plus one data rebuild, not
  data-layer design.
- **`serialize_banded_claim()`** enforces the band-inseparability invariant; it raises rather than
  emitting a bandless presentation. Every surface goes through it.
- **`get_work_witnesses(work_id, enabled_bands, anchor_sys_id=…)`** already implements the DATA-10
  unit×work projection: one row per witness unit at its highest member band, anchor unit excluded,
  same-unit members suppressed, pagination over units (not over a truncated row set).
- **Browse's enrichment path** (generation-token staleness guard + `enrichment_refs` +
  `update_enrichment_sections`) is the established place to hang a lazily-loaded section.
- **The v8.4 dual-mode library filter** (show-only / hide) is the component to reuse if D-16's library
  filter ships.
- **The `/catalog` facet worker pattern** (`_CatalogFacetWorker`, off-UI-thread facet counts) is the
  model for the findings browse's counts.
- **`highlight_text` in `web/pages/browse.py`** — the PANEL-03 in-transcription highlight candidate.

### Established Patterns
- Content-hashed, versioned frozen artifacts; tunable-only-by-versioning contract docs; display labels
  rendered over stored keys (a surface never shows a raw key); fail-closed versioned loader; the
  masking gate over asset + repo + every surface; asset-first deploy (DBs before code).
- Fail-open everywhere: flag AND sidecar-readiness, and every service wrapper degrades to empty
  rather than raising.

### Integration Points
- New page module(s) for `/work/{id}` and the findings browse — **note the naming hazard:**
  `web/pages/discoveries.py` is the PRE-EXISTING community "Discoveries Center" and is unrelated.
- The panel attaches to `web/pages/browse_enrichment.py`; the catalogue integration modifies
  `web/pages/catalog_browse.py`; the methods rework touches `web/pages/help.py`.
- Wave 1 touches the bake/verifier/id modules and produces a new schema-versioned asset + a new dated
  frame doc; the redeploy is the one human-approved checkpoint in the phase.

### Measured facts worth planning against (from the deployed asset, 2026-07-30)
- `page_id` = `{sys_id}_{IE…}_{P00000N}_{FL…}` → maps 1:1 onto a browse page.
- 268,361 claims / 297,415 evidence rows. Shipped display claims: tier_a 134,449; `not_evaluated`
  20,435; screening_canon 6,594; screening_rb 2,399; weak 1,078; high_confidence_algorithmic 852
  (750 unreviewed + 102 human_confirmed); corroborated 730.
- 44,375 sys_ids and 1,088 works carry shipped claims. Claims per page: 141,553 pages have exactly 1,
  8,449 have 2, tail to 10.
- No index on `discovery_evidence.sys_id`; `router_bucket` is NULL throughout; no page-length column.

</code_context>

<specifics>
## Specific Ideas

- Row wording, verbatim: **"Matches ⟨work⟩ · 68% of page"** / **"התאמה ל⟨חיבור⟩ · 68% מהדף"**.
- Panel groups: **"On this page"** then **"Elsewhere in this manuscript"** (collapsed).
- Novelty toggle, owner's words: *"show only novel findings / show everything"*, the latter with a
  clear novelty mark — expressed as "not identified in any available finding aid", never "new".
- Big-work page reads: **"13,038 manuscripts — page 1 of 66"**.
- Owner's framing of the whole phase: *"It's a big new amazing feature and it should be very
  accessible, allowing for maximum ability to see new findings — new, or those that are not obvious
  from the title/identification, are the most important."*
- Owner's honesty constraint on the tiers: *"we are not sure that tier_a is same work and the next are
  parallels, just heuristics."*

</specifics>

<deferred>
## Deferred Ideas

**Resolved at the mockup + Codex gate (owner instruction), NOT by re-running this discussion:**
- **PANEL-03's final form** (D-12) — highlight-in-transcription vs dedicated pane vs stats-only.
- **The findings browse's detailed shape** — row unit (identification / work / manuscript), behaviour
  with novelty filtering off, one surface or two.
- **The BAND-05 methods-page rework** under D-06 (numbers off the table → qualitative per-tier copy).
- **The novelty chip's exact bilingual wording** under NOVEL-01's prohibited-words constraint.
- **Roadmap / requirements bookkeeping** — NOVEL-01/02 re-homing to Phase 136, the rewritten Phase-136
  goal + success criteria, and the findings-browse ↔ leads-queue relationship (Phase 138).

**Later phases:**
- **The gen-2 / discovery-v2.1 evidence refresh** as its own phase (D-01), carrying with it the
  reference-granularity stage (Bible → chapter, Mishnah/Tosefta → tractate+chapter, Talmud →
  folio+amud) and the witness-vs-quoter precision lever. Also the ~2,670-work neutral-title curation
  round and the CERT-01 re-registration that a v2.1 population change forces.
- **Phase 139:** whether REL-01/CERT-02's "tier-A goes public WITH its measured number" clause is
  amended or satisfied (the D-06 conflict); the correction/retraction policy that lost its home when
  the curated-surface exception was declined; VIS-02's positive-control release gate.
- **Community judgments (137)** — 136 ships placeholder voting controls only.
- **The leads queue (138)**.
- **The library filter on the work page** if reusing the existing component is not cheap (D-16).
- **Promotion path** (`v9-PUBLICATION-STRATEGY.md` §8.5): whether restricted-source rows ever move
  private → public later, which decides if D-22's projection is permanent or a revisable policy label.

</deferred>

---

*Phase: 136-read-surfaces-connections-panel-work-witnesses*
*Context gathered: 2026-07-30*
