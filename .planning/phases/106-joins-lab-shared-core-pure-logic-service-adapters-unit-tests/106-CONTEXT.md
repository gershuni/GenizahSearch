# Phase 106: Joins Lab Shared Core - Context

**Gathered:** 2026-06-03
**Status:** Ready for planning

<domain>
## Phase Boundary

A new **shared, web-reusable, unit-tested Python module** that encapsulates the validated Joins
Lab *pure domain logic* — query composition, cross-side `(sys_id, page±1)` membership, candidate
dedup/compaction, text/visual merge ordering with provenance, self-match detection, and
snippet/page helpers — behind a `SearchExecutor` adapter and the existing shared services
(visual_similarity / FJMS-measurement / metadata-image).

**No PyQt. No direct `fist_data/*.db` access. No UI.** This phase delivers logic + adapters +
unit tests only; Phases 107-110 build the desktop UI on top, and a later phase adds the web UI on
the same shared core.

The six deliverable logic units (ROADMAP success criteria, all validated in the frozen Spike 002
sketch): (1) line-by-line query composition → engine line-break syntax; (2) cross-side AND/OR
membership; (3) candidate dedup/compaction; (4) text/VS merge ordering with provenance; (5)
self-match detection + snippet/page helpers; (6) a static import guard proving no PyQt / no direct
sqlite.

</domain>

<decisions>
## Implementation Decisions

### Module shape & domain model
- **D-01:** New shared module is a **single file `shared/joins_lab.py`** — all six logic units, the
  `SearchExecutor` protocol, and the dataclasses live in one module. Matches the existing
  `shared/*_service.py` single-file convention; no package, no submodule split, no `__init__` barrel.
- **D-02:** The domain model is **typed**: frozen dataclasses for `BuilderRow`, `SideQuery`,
  `Candidate` (explicit provenance fields + a canonical candidate key), and the merge result. The
  shared module owns the **single `dict → Candidate` normalizer** (one source of truth). This is the
  domain model the Codex productionize critique asks for; it makes the dedup key (SC#3) and
  provenance tags (SC#4) trustworthy and unit-testable.

### SearchExecutor adapter contract
- **D-03:** `SearchExecutor` is a **narrow Protocol over the search engine only**:
  `execute_search(...)`, `get_browse_page(...)`, `get_meta_for_id(...)`, `get_library_for_id(...)`
  (the exact signatures the sketch used — see DESKTOP-INTEGRATION-NOTES). It is the single injected
  runtime dependency; only **cross-side membership** needs it live.
- **D-04:** The adapter **returns the engine's raw result dicts**; the shared module normalizes them
  to `Candidate` (consistent with D-02). Each app's `SearchExecutor` implementation stays a **thin
  passthrough** — no per-app normalizer (avoids drift).
- **D-05:** VS look-alikes and material/measurement enrichment are **NOT in the adapter** — they are
  reached via the **existing shared services** (`shared/visual_similarity_service.py`,
  `shared/fjms_service.py`) and the existing metadata-image route, which are already web-usable and
  already own their sqlite. Honors the constraint "no direct `fist_data/*.db` — all data flows
  through shared services OR the adapter."
- **D-06:** Pure functions (compose, dedup, merge, self-match, snippet/page) take **already-fetched
  data** — no adapter, no I/O — so they unit-test with plain fixtures. A `FakeSearchExecutor`
  returning canned result dicts covers the one I/O-bound unit (cross-side membership).

### Builder depth (deferral #3)
- **D-07:** Builder input model = the **validated minimal shape**: each `BuilderRow` carries
  `{term, line_start: bool, line_end: bool, gap_to_next: int}`, plus a per-`SideQuery` global
  `variants` toggle. Composition emits the engine's line-break syntax (`|`-groups + `[|N]`
  line-gaps), with the **line-START token on the RIGHT for RTL** (Hebrew line start). Round-trips
  against `genizah_core._parse_line_break_query` (SC#1).
- **D-08:** **PLUS page-level anchors, position-constrained:** a **page-START anchor is available
  ONLY on the first row**, a **page-END anchor ONLY on the last row**. Semantics: page-START is an
  **additional, stronger constraint** — the candidate's text begins at the top (engine
  `text_position='start'` / `content_head`) — layered on top of and **independent of** that row's
  per-line line-START (which matches any line). page-END is the `content_tail` mirror on the last
  row. The page anchor adds "and it's the first/last line of the page."
- **D-09:** Dataclasses are **additive-extensible** (fields with defaults). No speculative fields
  added now; later phases can add per-row variation columns etc. without reshaping the API.
- **D-10:** **Editable raw composed-query preview = DEFERRED.** `compose()` stays one-way
  (rows → query string); the structured rows remain the single source of truth. Power users can use
  the main search for fully manual queries.

### Scope boundary (deferrals #1 / #6 / #7)
- **D-11:** **Known-joins grouping (pairwise→group BFS) is NOT in 106** — it stays in Phase 107,
  calling the existing `JoinsManager.get_connected_fragments_by_id` (`genizah_core.py:9936`)
  directly. The v8 join model is **pairwise→group** with no richer N-fragment / per-edge-evidence
  model baked into the core (deferral #1 confirmed).
- **D-12:** **All JSA / parallels logic is OUT of 106** (Phase 110). Recorded lean for the JSA-02/03
  disposition, to be **finalized at `/gsd-discuss-phase 110`**: **JSA-01 keep** (parallels seeding),
  **JSA-03 spike** (`[`/`]`-aware torn-word completion), **JSA-02 cut** (corpus-frequency
  completion).
- **D-13:** **"Other side" = adjacent image `p±1`** within the same sys_id (first→+1, last→−1,
  middle→both); cross-side membership decided by `(sys_id, page±1)` set logic. The multi-leaf /
  bifolio adjacency nuance is **deferred** (future refinement, not v8) (deferral #7 confirmed).

### Research flags (for gsd-phase-researcher / gsd-planner)
- **R-01:** Page-level `content_head`/`content_tail` (the `text_position` branch) and the line-break
  `[|N]` syntax run through **different engine paths** (`execute_search` text_position vs
  `_execute_line_break_search`). Investigate whether a page-anchored line-break query composes into
  **one** engine call or needs a **separate execution + `(sys_id, page)` intersection**. SC#1's
  round-trip MUST cover the page-anchored case (D-08).
- **R-02:** Leading tear-bracket tokens (e.g. `]שהדותא`) can defeat the engine's line_start test
  (sketch iteration E). Self-match detection (SC#5) and composition correctness should account for
  bracket stripping consistent with the main responsa search.

### Claude's Discretion
- Exact dataclass field names and module-internal helper decomposition.
- `FakeSearchExecutor` / fixture design; whether round-trip tests import the pure module-level
  `genizah_core._parse_line_break_query` (`:5811`, no heavy init) directly vs a stub.
- Snippet centering parameters (`max_lines`/`max_chars`) and highlight MARK-token internals.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Executable spec (Spike 002 — frozen at git tag `spike-002-joins-workbench`)
- `.planning/spikes/002-assisted-join-workbench/sketch/join_workbench.py.txt` — the UAT-validated
  throwaway sketch = **executable spec for all six logic units**: compose (`QueryBuilder.compose`,
  ~L560), cross-side AND/OR (`_CrossSideWorker.run`, ~L387), dedup (`_on_results`, ~L1102), merge
  ordering (`_maybe_assemble`, ~L1149), self-match (`_anchor_matched`, ~L1100), snippet/page helpers
  (`page_of`/`snippet_html`/`snippet_plain`/`_match_line`, ~L84-148). Extract the BEHAVIOR; do NOT
  copy the PyQt scaffolding.
- `.planning/spikes/002-assisted-join-workbench/CODEX-PRODUCTIONIZE-CRITIQUE.md` — "C-stricter"
  architecture verdict: `SearchExecutor` boundary, pure-logic-first + unit-test-before-UI, public
  action APIs, canonical dedup key, batch-everything perf, i18n as acceptance.
- `.planning/spikes/002-assisted-join-workbench/DESKTOP-INTEGRATION-NOTES.md` — verified reuse-map:
  exact engine/service signatures (`execute_search`, `get_browse_page`, `get_meta_for_id`,
  `get_library_for_id`, `get_vs_service().get_suggestions`, FJMS measurements), image route,
  line-break composition verified headless, the RTL start/end trap (iteration E).
- `.planning/spikes/002-assisted-join-workbench/SPIKE-FINDINGS.md` — feasibility, primitive
  inventory, VS ~50% coverage, what exists vs not (the auto-finder does NOT exist as code).

### Requirements & roadmap
- `.planning/REQUIREMENTS.md` § "Design-Critique Conclusions & Amendments (2026-06-03)" +
  § "Build constraints" + § "Deferrals / discuss-phase questions" — JWB-10/11/12 definitions and the
  7 deferrals this discussion resolved.
- `.planning/ROADMAP.md` § "Phase 106" — the 6 success criteria this module must satisfy.

### Engine integration points (read before planning)
- `genizah_core.py:5811` `_parse_line_break_query(query_str)` — pure module-level fn; SC#1
  round-trip target.
- `genizah_core.py:8001` `_execute_line_break_search(...)` — line-break exec path (first/last
  line-group component → `line_starts`/`line_ends`).
- `genizah_core.py:8298` `execute_search(..., responsa_options=, restrict_sys_ids=, text_position=,
  corpus_scope=)` — the adapter's primary method; the `text_position` branch realizes page-level
  start/end (`content_head`/`content_tail`).
- `genizah_core.py:9483` `get_browse_page(sys_id, p_num=...)` → `{text, total_pages, ...}` —
  neighbor-page fetch for cross-side membership.
- `genizah_core.py:9936` `JoinsManager.get_connected_fragments_by_id(sys_id)` — known-joins BFS.
  **Phase 107, NOT 106** (listed so the planner sees the boundary, D-11).

### Shared services (data flows through these, never raw sqlite)
- `shared/visual_similarity_service.py` — `get_vs_service().get_suggestions(sys_id, limit)`, VS
  look-alikes (~50% coverage; non-exhaustive, supplementary signal only).
- `shared/fjms_service.py` — material / measurements (`manuscript_measurements`).
- `shared/parallels_service.py` — `fetch_parallels_results(...)`; **Phase 110 (JSA-01)**, noted for
  the boundary (D-12).

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **The frozen sketch's pure helpers transplant nearly verbatim** — `page_of`, `snippet_html`,
  `snippet_plain`, `_match_line`, `QueryBuilder.compose`, the `_CrossSideWorker` AND/OR membership,
  the `_on_results` dedup, the `_maybe_assemble` merge ordering. The inner logic is already
  PyQt-free; the QThread wrappers strip away, leaving pure functions.
- **`shared/*_service.py` modules** are the convention template for `shared/joins_lab.py`
  (single-file, module-level functions + a small coordinating class, `get_*_service()` singletons).
- **`tests/` conventions** — class-based pytest (`TestX`), Hebrew-text fixtures, local mock classes
  (e.g. `MockMetadataManager`), minimal conftest. A `FakeSearchExecutor` fits this exactly.

### Established Patterns
- Two-phase search (Tantivy candidates → regex filter/highlight). Line-break path maps the first/last
  component of a line group to `line_starts`/`line_ends`; `text_position` maps start/end →
  `content_head`/`content_tail`.
- Result dict shape: `display{id(sys_id), shelfmark, title, library_code, img(page), source}`,
  `full_text`, `snippet`, `uid`, `highlight_pattern`, `score`, `scope`.
- `(sys_id, page±1)` adjacency. `AlmaId == sys_id` (the long `99000…` ids).
- Provenance badges the merge must support: ★ both / ⊙ VS#rank / ⇄ other / ⚓ self.

### Integration Points
- Desktop (Phase 107+) injects a `SearchExecutor` backed by `self.searcher` (SearchEngine) +
  `self.meta_mgr`.
- Web (later phase) injects a `SearchExecutor` backed by its own engine/API — shared core unchanged.

</code_context>

<specifics>
## Specific Ideas

- The Spike 002 sketch is the **executable spec** — extract behaviors, do not copy the PyQt
  scaffolding (Codex: "don't clean up the sketch until it feels production-ish").
- **RTL:** the line-START anchor sits on the **RIGHT** (Hebrew line start) — the start/end trap from
  sketch iteration E; get the anchor orientation right in the composition + its tests.
- Page-START is "only the attribute of first line", page-END "only of the last" (Hillel,
  this discussion) — enforce the position constraint in the model (D-08).

</specifics>

<deferred>
## Deferred Ideas

- **Per-row variation columns** (per-term variants/fuzzy) — additive later phase; global `variants`
  toggle is enough for v8.
- **Editable raw composed-query preview** (string↔rows round-trip) — inverts the source of truth;
  not v8 (D-10).
- **Richer N-fragment / per-edge evidence+confidence join model** — pairwise→group for v8
  (deferral #1, D-11).
- **JSA-02** (corpus-frequency completion) leaning **CUT**; **JSA-03** (`[`/`]`-aware torn-word
  completion) leaning **SPIKE** — finalized at `/gsd-discuss-phase 110` (deferral #6, D-12).
- **Multi-leaf / bifolio "other side"** adjacency beyond `p±1` — deferral #7 (D-13).
- **Web Joins Lab UI** — later phase on this shared core (deferral #5).

### Reviewed Todos (not folded)
- `todo.match-phase 106` surfaced 4 keyword-coincidence hits — FIST data fill, corrections-service
  migration, server-side email search, NLI MARC crawl. None touch the Joins Lab shared core; not
  folded.

</deferred>

---

*Phase: 106-joins-lab-shared-core-pure-logic-service-adapters-unit-tests*
*Context gathered: 2026-06-03*
