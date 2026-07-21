# Phase 134: Discovery Data Spine - Context

**Gathered:** 2026-07-21
**Status:** Ready for planning

<domain>
## Phase Boundary

Build the v9.0.0 Discovery module's **data spine**: a masked, versioned `discovery.db` sidecar produced by an offline distillation, plus **one async `DiscoveryService`** chokepoint — so every downstream surface can read banded same-work claims safely, with **provenance masking, event-loop safety, and fail-open behavior all proven BEFORE any claim UI is built**. This phase opens the REL-01 main gate sequence (claim semantics + masked schema → title map + sidecar + frozen-frame).

Delivers **DATA-01, DATA-02, DATA-03, DATA-04, DATA-05, DATA-06, DATA-07, DATA-08, DATA-10, PERF-01**.

**In scope:** the offline distillation (consuming the research DB) → `discovery.db`; the claim model (both families) with deterministic ids + bands + witness-units; structural provenance masking; the permanent DATA-05 leak guard extended to the sidecar; the async `DiscoveryService` (timeouts, bounded concurrency, LRU, pagination, overload behavior); the discovery feature flag + fail-open gating; the versioned release contract + deploy/rollback/rebuild recipe; the committed `discovery-frames.md` (frozen frame) + `discovery-budgets.md` (acceptance budgets) exit artifacts; the neutral-title curation/review workflow + artifact.

**Out of scope (later phases):** band-display contract + methods page + tier-A certificate (135); connections panel + work→witnesses pages (136); community judgments + Supabase (137); leads queue (138); atlas drill-down + homepage band + full release gate (139). **NO discovery UI renders in this phase** — it proves masking + event-loop safety + fail-open only. **R-source ingest** and **M-source piyyut/documentary works** are explicitly deferred to a parallel research track / gen-2 refresh (see Decisions + Deferred).

</domain>

<decisions>
## Implementation Decisions

### Reference sources & source-extensibility (DATA-01/02/04/05/08)
- **D-01:** The v9.0 launch distillation matches on **three reference sources: Sefaria + JA + M-source**. The newly-acquired 4th corpus (**R-source**) is **deferred** — NOT ingested for launch.
- **D-02:** **R-source** is a newly-acquired 4th reference corpus (owner-held, **off-repo / gitignored**; ~6 GB, ~1,679 Hebrew text files). It is masked **exactly like M-source** — referenced in ALL committed and product material only by the codename **"R-source"**, never by its real name, path, or provenance. Its research ingest runs as a **PARALLEL track alongside the milestone** and MUST NOT block Phases 134–139.
- **D-03:** The sidecar is built **source-extensible**, so a gen-2 refresh (adding R-source, plus the deferred M-source piyyut/documentary works) is a **versioned REBUILD, not a schema migration**:
  - (a) a **masked `source_corpus` provenance field** on works/claims — internal only, **never displayed**, storing only masked codenames;
  - (b) a **cross-corpus canonical `work_id`** — one stable opaque id when the same work appears in multiple corpora (work-level dedup);
  - (c) **R-source name/aliases/sigla pre-registered in the DATA-05 masking pattern set** now (defense-in-depth, even though R-source text enters no launch surface);
  - (d) the frozen-frame + certificate are explicitly scoped to **"the reference sources present at THIS distillation,"** so gen-2 = a new versioned frame (+ a later cert round), never a retrofit of the launch frame.
- **D-04 (R-source handling policy — for the parallel research track, NOT the Phase 134 build):** R-source is ~**86% post-Genizah** (responsa / שו״ת, the Shulchan-Aruch tradition, Rishonim→Acharonim commentators, Hasidut, modern encyclopedias incl. אנציקלופדיה תלמודית) and overlaps Sefaria/M-source on the classical layer. Its ingest requires (i) a **composition-date-aware anachronism/eligibility policy** — NOT a naive container-date cutoff, since a late container can witness an early text (a Geonic responsum preserved inside a later collection) — and (ii) **cross-corpus work-level dedup + span-shadowing** (`shadowed_by IS NULL`) for nested quotes. This is genuine research, deferred; captured so the researcher/planner understand WHY R-source is out of the launch spine.

### Shown work-set (DATA-04)
- **D-05:** The launch **DISPLAYED** work-set = **Sefaria + JA (all works)** + **M-source large literary works that resemble the open corpora**. **EXCLUDE M-source piyyut and documentary works at launch** (deferred to the fast-follow / gen-2 track alongside R-source). Rationale: large literary works have obvious neutral titles + low masking sensitivity + cleaner same-work claims; piyyut (incipit-keyed micro-units) and documentary (letters/legal docs, often untitled) are both a title-curation nightmare AND the highest masking risk.
- **D-06:** "Large literary works resembling the open corpora" is a **curation policy, not yet a mechanical filter.** The researcher investigates M-source's genre/classification + size metadata to propose the candidate set (primary signal = exclude-by-genre: drop piyyut + documentary); the **owner is the final gate** via the D-08 review artifact (approve/hand-pick).

### Neutral-title curation workflow (DATA-04)
- **D-07:** Every shown work carries a **human-reviewed neutral title** (+ reviewed author/genre). **NO fallback to research titles** — **fail-closed**: an unreviewed/unapproved work is **EXCLUDED** from the shipped set, never shipped with a research title.
- **D-08:** Curation runs via a **generated review artifact** emitted by distillation: rows = **opaque `work_id`** + candidate neutral title + author + genre, with **source provenance MASKED** (no M-source/R-source names or sigla in the artifact itself). The owner edits/approves; **only approved rows distill** into the shipped work-set. **Auto-adopt open-corpus (Sefaria/JA) canonical titles** with a light spot-check (already public/neutral); concentrate **FULL manual owner review on the M-source literary subset.** Model the artifact on the existing audit-review pattern (`scripts/export_translation_audit_sample.py` + `web/components/translation_report.py`).

### Bands at launch (DATA-02)
- **D-09:** **All four bands populate the sidecar** — `expert_verified` (R-A) > `tier_a` > `screening_rb` (R-B) > `screening_canon` (R-CANON); exactly one band per claim key post-precedence. (Default-view vs behind-the-toggle presentation is a Phase 135/136 concern; the spine ships all four so the downstream surfaces have data.)
- **D-10:** The canon lane (`screening_canon`) **ships but is separately caveated** (known Targum-confusion class) per LEADS-01 — not merely ranked lower. (Canon-lane *certification* remains out of scope per REQUIREMENTS.)
- **D-11:** **Row-count posture:** trim to fit the **≤300 MB** budget (DATA-08). The planner sets concrete per-band inclusion caps against `discovery-frames.md` + `discovery-budgets.md`; the frozen-frame artifact records per-band deduped counts BEFORE any certificate cards are drawn (DATA-02 → CERT-01 dependency in Phase 135).

### Relation-vocabulary display wording (DATA-01)
- **D-12:** The `claim_type` semantic set is **FROZEN** (direct witness / quotes-this-work / textual parallel / direct text overlap) and stored in the sidecar as a **stable code**. The **bilingual EN/HE display wording is DEFERRED** to Phase 135 (band contract) / Phase 136 (panel + work pages) where it renders — the spine never blocks on wording, and the wording stays data-driven / UI-side.

### Claude's Discretion
- Exact per-query **timeouts, bounded-concurrency limits, LRU sizing, and pagination page sizes** (DATA-06) — set by the planner against the PERF-01 caps.
- The user-facing **overload copy** ("temporarily unavailable" sense) and the fail-open / sidecar-absent messaging (DATA-06/DATA-07).
- The `discovery.db` internal **table/index layout**, the deterministic `claim_id` / `unit_id` hashing implementation (algorithm frozen in DATA-01/DATA-10; implementation is the planner's), and the schema-versioned filename scheme (DATA-08).
- Whether the DATA-05 guard extension **reuses `scripts/check_atlas_masking.py` wholesale or factors a shared scanner core.**

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

> **Provenance-masking note:** the `same_work_spike/probe/**` tree and **R-source** are **gitignored / off-repo research** (on disk for the researcher/planner). Their contents must NEVER be committed and must NEVER leak M-source or R-source into any product surface. Reference the restricted sources in committed material only as **"M-source"** and **"R-source."**

### Phase / milestone docs
- `.planning/ROADMAP.md` — Phase 134 detail (goal, 5 success criteria) + the REL-01 gate sequence + milestone framing.
- `.planning/REQUIREMENTS.md` — **DATA-01..08, DATA-10, PERF-01** (this phase); **BAND-01..05 / CERT-01..02** (Phase 135, which consumes the frozen frame); **DATA-05** masking; **REL-01** gate; **FUT-04** (refresh pipeline — the gen-2 R-source home).
- `.planning/PROJECT.md` — milestone goal + the M-source masking hard constraint + the epistemic-honesty posture + the "curated subset (open-corpus works at launch)" note.
- `.planning/phases/133-visual-atlas-preview-early-quick-win/133-CONTEXT.md` — Phase 133 decisions: **D-01** primary graph object = manuscript (ATLAS-02), **D-07** the reusable masking scan (DATA-05 forerunner), byte-cap → `discovery-budgets.md`.

### Research provenance (gitignored — read-only for planning; NEVER commit; NEVER leak)
- `same_work_spike/probe/data/fullcorpus_v2.db` — the source research DB this phase distills; table **`accepted_pairs_canonmask`** (canon-masked `sys_id` page pairs + `aligned_len`/`density`/`flank_class`; no reference text). Source-of-truth for the **launch 3-source frame** (Sefaria + JA + M-source).
- `same_work_spike/probe/METHOD.md` + `same_work_spike/probe/SYNTHESIS-AND-PLAN.md` — method + the relation-type distinctions (page-pair vs manuscript-pair vs physical join vs textual overlap) the DATA-01 claim families must not conflate; **span-shadowing** (`shadowed_by IS NULL`) for nested quotes.
- `same_work_spike/probe/scripts/build_reuse_graph.py` + `same_work_spike/probe/scripts/build_atlas_draft.py` — the distillation/clustering pipeline to fork/consume.
- `same_work_spike/probe/PROBE-RESULTS.md` + the E1 certification registry — band definitions (**R-A 0.889 / R-B 0.859 / R-CANON 0.647**), strata, gold machinery (feeds CERT-01 in Phase 135).
- `.planning/seeds/SEED-029-fragment-textual-similarity-same-work-detection.md` — the research seed this milestone productizes.
- **R-source** — the newly-acquired 4th reference corpus (owner-held, off-repo/gitignored; ~6 GB, ~1,679 Hebrew text files; ~86% post-Genizah). **PARALLEL-track ingest only**; never named/committed; masked exactly as "R-source".

### Existing sidecars, services & scripts (the build model)
- `shared/fjms_service.py`, `shared/document_service.py`, `shared/nli_crossref_service.py` (+ 10 more `shared/*_service.py`) — the read-only SQLite-sidecar service pattern → new `shared/discovery_service.py`.
- `web/search_api.py` — the `run_in_executor` + `asyncio.wait` (NOT `wait_for` — `run_in_executor` threads are not cancellable) off-event-loop precedent for the DATA-06 async chokepoint (heavy queries never block the loop; timeout → overload response).
- `web/feature_flags.py::_env_enabled` — the discovery feature-flag pattern (DATA-07; distinct from the Phase 133 atlas-preview flag).
- `scripts/check_atlas_masking.py` + the `MASKING_SCAN_PATTERNS_FILE` env — the DATA-05 masking guard forerunner to **extend to the sidecar** (schema + every cell) + **register R-source tokens**. Already scans committed repo + product surfaces.
- `scripts/export_translation_audit_sample.py` + `web/components/translation_report.py` — the audit-review-artifact pattern to model the **D-08 neutral-title review file**.
- `libraries.csv` — catalogue titles + `library_code` (masking-safe; DATA-10 codicological-part grouping via Oxford part id; witness-unit labels).
- `fist_data/fjms_enrichment.db` — `domains` table + join groups (DATA-10 witness-unit merging: PGP/FJMS/user physical joins merge; "same scribe" does NOT).

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **`shared/*_service.py` (13 services)** — module-level sidecar open, indexed bounded queries, graceful-absent; `shared/discovery_service.py` follows this shape with async wrappers added for DATA-06.
- **`web/search_api.py` `run_in_executor` + `asyncio.wait`** (NOT `wait_for` — threads are not cancellable) — the exact async off-loop + per-query-timeout → overload-response pattern DATA-06 needs.
- **`scripts/check_atlas_masking.py`** — the hardened, fail-closed, multi-surface masking scanner from Phase 133 (literal + NFC/NFD + casefold + UTF-8/16/32 + URL/HTML/JS forms; scans committed repo + assets). Extend its scan set to the sidecar (schema + every cell) and add R-source patterns.
- **`web/feature_flags.py::_env_enabled`** — the discovery flag.
- **Phase 133 `atlas_data/` + `web/atlas_assets.py`** — the versioned-asset + content-hash + fail-closed availability-predicate loader; the model for the DATA-08 schema-versioned filename + startup integrity-check + reject-incompatible-sidecar loader.

### Established Patterns
- Read-only SQLite sidecars served from local files (pgp.db, fjms_enrichment.db, nli_crossref.db); Supabase reserved for community features (Phase 137, NOT this phase).
- Deploy posture (memory: scp DBs FIRST, then code) → DATA-08 **temp-upload → verify → atomic rename → code deploy**; documented rollback + reproducible rebuild recipe.
- Fail-open feature-flag gating (Phase 133 D-13): the flag is **necessary but not sufficient** — AND it with sidecar readiness so a flag-ON/sidecar-missing window hides cleanly.

### Integration Points
- New `shared/discovery_service.py` (async chokepoint) + `discovery.db` sidecar (schema-versioned filename) in the sidecar dir; discovery feature flag in `web/feature_flags.py`; DATA-05 guard extension in `scripts/check_atlas_masking.py`; a new **offline distillation script** (consumes `fullcorpus_v2.db`) emitting `discovery.db` + `discovery-frames.md` + the title-review artifact; `discovery-budgets.md` committed as an exit artifact.
- **NO UI in this phase.**

</code_context>

<specifics>
## Specific Ideas

- The distillation consumes `accepted_pairs_canonmask` in `fullcorpus_v2.db` (the launch 3-source frame); the map is ~**1.34M pairs / 62,414 MSS** (research notes) — the planner trims per band to the ≤300 MB budget.
- **Span-shadowing** (`shadowed_by IS NULL`) is the existing mechanism for nested/quoted overlaps; the claim model's within-key dedup + this shadowing keep "quotes-this-work" distinct from "direct witness" (DATA-01).
- **Both claim families ship**: work–witness (page→witness aggregation) AND MS–MS relation (child page-alignment records, **queryable by page** for PANEL-02's "pages related to this page").
- M-source is ~86% piyyut/documentary; its **launch-eligible slice is the ~14% large literary layer**. Sefaria already covers canonical Bible/Talmud/midrash, so the launch's classical coverage does not depend on M-source.

</specifics>

<deferred>
## Deferred Ideas

- **R-source ingest** (the 4th corpus) → **parallel research track → gen-2 sidecar refresh** (FUT-04). Needs a composition-date-aware anachronism/eligibility policy + cross-corpus work-level dedup + a re-frozen frame and (eventually) a re-cert round. The sidecar is built R-ready (D-03) so this is a versioned rebuild.
- **M-source piyyut + documentary works** → fast-follow / gen-2 (title-curation + masking cost; ride with R-source).
- **Relation-vocabulary bilingual EN/HE wording** → Phases 135 (band contract) / 136 (panel + work pages).
- **All downstream surfaces**: band contract + methods page + tier-A certificate (135); connections panel + work→witnesses pages (136); community judgments + Supabase (137); leads queue (138); atlas drill-down + homepage band + full REL-01 gate (139).
- **FUT-01** (text-reuse engine backing `/parallels` / desktop composition) + FUT-02, FUT-03, FUT-05..08.

*Note: the `todo.match-phase 134` cross-reference was not completed this session; Phase 133's run returned only low-relevance keyword collisions (none discovery-related), so no todos were folded.*

</deferred>

---

*Phase: 134-Discovery Data Spine*
*Context gathered: 2026-07-21*
</content>
</invoke>
