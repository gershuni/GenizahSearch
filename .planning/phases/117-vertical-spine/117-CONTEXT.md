# Phase 117: Vertical Spine - Context

**Gathered:** 2026-06-17
**Status:** Ready for planning

<domain>
## Phase Boundary

The riskiest **end-to-end slice** of the v8.2.0 Web Joins Lab, built first to de-risk the
milestone. A scholar opens `/joins-lab`, pins an anchor fragment (image + RTL numbered
transcription, loaded through the existing per-provider image proxies), types lines into a
**minimal** anchor-side builder, runs a search **off the event loop** through the new
`WebSearchExecutor` adapter, and sees a **deduped one-per-image candidate grid** — all
**anonymous** (no login wall), all state isolated through `web/safe_storage.py`.

The point of this phase is to prove the **`WebSearchExecutor` adapter seam** (FND-01) works
end-to-end before building the full feature surface in 118–120. This is UI composition + the
one missing web adapter, NOT search-engine work — it rides the complete, web-reusable
`shared/joins_lab.py` core (v8.0.0 Phase 106).

**In scope (Phase 117 requirements):** FND-01 (web SearchExecutor adapter, off-loop),
FND-02 (`/joins-lab` route), FND-03 (cold-start by shelfmark/sys_id), FND-06 (no login wall;
all per-user state via `safe_storage`), FND-08 (deep-link URL contract), ANC-01 (anchor image
zoom/pan + folio nav), ANC-02 (images via existing per-provider proxies + Phase-98 breaker),
ANC-03 (RTL numbered transcription), BLD-01 (anchor-side line builder), BLD-05
(compose → execute → candidates), CND-01 (dedup one-per-image), CND-02 (candidate grid).

**Explicitly NOT in this phase (locked to later phases — do not pull forward):**
- Known-joins group, "Find joins" entry from `/search`+`/browse`, other-side builder,
  per-line modifiers, global toggles (variants/JA/spacing/bidirectional) → **Phase 118**.
- Table surface, triage Y/?/N, self-match readout, candidate filters/pagination/enrichment,
  side-by-side Compare, Visual Similarity → **Phase 119**.
- Add-as-join, bulk Add-to-Puzzle, add-to-list/export, full builder/triage/filter persistence
  with re-run-on-restore, clear/reset → **Phase 120**.
- Complete i18n coverage pass + RTL audit + Hebrew-leak AST audit → **Phase 121** (but the page
  is **bilingual from line one** via `tr()` — Phase 121 is the completeness audit, not the start).

</domain>

<decisions>
## Implementation Decisions

### Page layout & responsive shape
- **D-01:** Wide-screen layout is **anchor pinned to a side, work column scrolls**. The anchor
  pane (image + RTL transcription) is **sticky / stays in view**; the line builder + candidate
  grid live in the main column that scrolls. This honors the Joins Lab's defining principle —
  "keep ONE anchor fragment in view while hunting/triaging candidates" (PROJECT.md).
- **D-02:** The anchor side is **direction-aware**: anchor on the **reading-start side**
  (left in EN LTR, right in HE RTL), flipping with app language per the existing web-app RTL
  convention. Not a fixed physical side.
- **D-03:** **Narrow screens stack** — anchor collapses to a (collapsible) strip on top, builder
  + grid below in one scroll column. (Plain responsive fallback; do not over-engineer.)
- **D-04:** The layout MUST leave structural room for what 118/119 add into the same shell:
  Phase 118 known-joins panel + other-side builder; Phase 119 table view + side-by-side Compare.
  Plan the anchor pane and work column so those additions don't force a re-layout.

### Cold-start entry & empty state
- **D-05:** Cold-start input is a **single smart box that accepts a shelfmark string OR a raw
  sys_id**. A shelfmark (e.g. `T-S 12.123`) is resolved to a sys_id via the **existing shelfmark
  normalization / search path** (do not invent a new resolver). Scholars think in shelfmarks —
  sys_id-only would make `/joins-lab` a dead end.
- **D-06:** Alongside the smart box, an **always-visible "choose from a list" button** that pulls
  from the user's **saved research lists** (`/lists`). For an **anonymous visitor it prompts
  login on click** (consistent with every other login-gated list read on web) — it is NOT hidden,
  and it is NOT deferred. After login the scholar picks a fragment from a list as the anchor.
- **D-07:** The empty state (bare `/joins-lab`, no anchor) is a **centered "pin an anchor" panel**
  containing the smart box + the "choose from list" button + a **one-line description** of what
  the Joins Lab does. Bilingual from line one.

### Spine builder (minimal — full builder is Phase 118)
- **D-08:** The Phase-117 anchor-side builder is a **single multi-line textarea**. Each non-empty
  line becomes a `BuilderRow(term=<line>)` (line treated as a phrase term; `line_start`/`line_end`
  False, `gap_to_next` 0 in the spine). The rows assemble into a `SideQuery` and flow through
  `compose()` → the FND-01 adapter → `dedup_candidates`. This deliberately exercises the **real
  multi-row `SideQuery`/`compose` data model** (de-risking Phase 118's integration) while staying a
  throwaway-cheap UI. Phase 118 swaps the textarea for the rich row widgets (OR word-boxes, ⚙
  per-line modifiers, global toggles).
- **D-09:** Spine search runs in a **fixed default = exact mode** (`SideQuery.variants=False`, no
  per-line modifiers, `page_position=None`). Mode selection + the variants/JA/spacing/bidirectional
  toggles are explicitly Phase 118 (BLD-04). Do not wire toggles in 117.

### Anchor image viewer
- **D-10:** **Reuse `/browse`'s existing image viewer** for the anchor pane — its
  `manuscriptViewer` JS (zoom 0.25–4.0, pan, rotate, zoom resets on folio change), per-provider
  proxy image resolution (NLI/Oxford/Cambridge/Manchester/JTS), folio navigation, and
  mobile/tablet zoom controls. **Extract it from `web/pages/browse.py` into a reusable form**
  (shared component / helper) rather than building a lighter throwaway viewer. This gives full
  ANC-01/ANC-02 parity from line one and **Phase 119 Compare reuses the same per-pane viewer**.
  The extraction refactor is acceptable cost because it pays off twice (anchor + Compare).
- **D-11:** All anchor (and later Compare) image fetches go through the **existing per-provider
  proxy endpoints + the Phase-98 NLI circuit breaker** — never a direct/unguarded IIIF URL (ANC-02).

### Spine persistence (full persistence is Phase 120)
- **D-12:** Define the **versioned `safe_storage` schema** now: a single namespaced key (e.g.
  `joins_lab`) holding a dict with an explicit **`schema_version` invalidation field**. ALL reads/
  writes go through `safe_user_*` helpers (zero raw `app.storage.user`).
- **D-13:** The spine **writes the current anchor sys_id** through `safe_user_*`. Returning to a
  **bare `/joins-lab` (no URL param) re-loads the last anchor**. When a `sys_id` URL param IS
  present (deep links / sharing), **the URL wins**. Full builder/triage/filter persistence +
  re-run-on-restore stays locked to Phase 120 — the spine persists only the anchor.

### Candidate grid (spine = read-only display)
- **D-14:** The Phase-117 candidate grid is **read-only display**: per dedup'd candidate, a
  thumbnail + shelfmark + library + title (CND-01 dedup `dedup_candidates`, CND-02 grid). **No
  triage Y/?/N (CND-04), no per-card actions, no table, no Compare, no VS** — those are 119/120.
  An "open in `/browse`" affordance per card is acceptable spine polish (read-only navigation),
  but is not required.

### FND-01 adapter shape (the de-risk target)
- **D-15:** `WebSearchExecutor` wraps **`state.searcher.execute_search` directly** (and
  `get_browse_page` / `get_meta_for_id` / `get_library_for_id` via the metadata manager) — **NOT
  `/api/search`** (which drops `text_position`/`corpus_scope` and caps modes — Codex BLOCKER 1).
  It mirrors the desktop `_DesktopSearchExecutor` passthrough (`desktop/join_workbench.py:1473`),
  thin, returning `[]` on failure.
- **D-16:** The call is made **off the event loop** using the same pattern as `web/pages/search.py`
  (`await run.io_bound(...)` around the core call), with **timeout, cancellation, and
  stale-generation latest-wins** handling modeled on `search.py`'s `search_generation` counter
  (`web/pages/search.py:4036`/`:4189`) + an `is_running` re-entrancy guard. The NiceGUI event loop
  is never blocked.

### Claude's Discretion
- Exact column widths / sticky offsets / breakpoint px for D-01..D-03 — choose to match the rest
  of the web app's responsive conventions (`create_layout()` + browse CSS).
- The precise `safe_storage` key name and the dict shape under it (beyond requiring
  `schema_version`) — pick something forward-compatible with Phase 120's full schema.
- Whether the spine grid card carries an "open in `/browse`" link (D-14) — include if cheap.
- The deep-link param set surfaced in 117 (minimum: `sys_id`; FND-08 names optional
  `shelfmark` / `fl_id` / `page` / `volume_ie`) — resolve `sys_id` minimally now; the full
  multi-IE/`volume_ie` resolution is exercised harder in Phase 118's "Find joins" entry. Document
  whatever subset 117 implements explicitly (FND-08 requires the contract be documented).

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Milestone requirements & roadmap (read first)
- `.planning/REQUIREMENTS.md` — the 37 v8.2.0 requirements; Phase 117 owns FND-01/02/03/06/08,
  ANC-01/02/03, BLD-01/05, CND-01/02. Has the hard cross-phase constraints (safe_storage chokepoint,
  proxy+breaker, off-loop, bilingual, no new Supabase schema).
- `.planning/ROADMAP.md` §"Phase 117: Vertical Spine" — the 6 success criteria this phase is
  verified against, plus the 5-phase milestone build order.
- `.planning/v8.2.0-REQ-CODEX-CRITIQUE.md` — the code-grounded pre-lock critique. **BLOCKER 1**
  (adapter must wrap `state.searcher`, not `/api/search`, off-loop) and **BLOCKER 2** (PST is
  server-side per-session via `safe_storage`, NOT browser localStorage) are load-bearing for 117.
  Its "Phase 117 = vertical spine" MEDIUM note is the de-risk thesis.

### The shared core this phase rides (do not re-implement search logic)
- `shared/joins_lab.py` — the v8.0.0 Phase-106 core. Key pieces for 117: the `SearchExecutor`
  Protocol (`:150`, 4 methods the web adapter must satisfy), `BuilderRow`/`SideQuery` domain model
  (`:28`–`:72`), `Candidate` + `.key` dedup tuple (`:75`–`:128`), `compose()` (composes a SideQuery
  into engine syntax; hardcodes JA/flex/bidirectional False — relevant to 118, not 117),
  `dedup_candidates`, `normalize_candidate`.

### Parity reference — the desktop Joins Lab being ported
- `desktop/join_workbench.py:1473` — `_DesktopSearchExecutor`, the exact passthrough shape the
  web `WebSearchExecutor` mirrors (thin forward to `searcher.execute_search` / `get_browse_page` /
  `meta_mgr.get_meta_for_id` / `get_library_for_id`; `[]` on failure).
- `desktop/join_workbench.py` (broadly) — desktop anchor pane (image + RTL numbered transcription),
  cold-start by shelfmark + 📋 pick-from-list, candidate grid. UAT-approved reference for parity.

### Web seams to reuse (the "how" the spine wires into)
- `web/pages/search.py:3979` (`execute_search`) + `:4036` (`search_generation` invalidation) +
  `:4160`–`:4189` (`run_core_search` inside `await run.io_bound(...)`) — the canonical off-loop +
  latest-wins + `is_running` pattern FND-01 must follow.
- `web/pages/browse.py` — the image viewer to extract (`manuscriptViewer` JS ~`:600`/`:1487`,
  zoom in/out/reset `:1388`–`:1400`, per-provider proxy resolution ~`:3624`, folio nav, responsive
  zoom controls), AND the two-panel image+transcription + line-number gutter pattern (`:116`–`:143`).
- `web/safe_storage.py` — the `safe_user_*` chokepoint (server-side `app.storage.user`, keyed by
  NiceGUI session cookie). ALL Joins Lab per-user state goes through here.
- `web/main.py` — `@ui.page` route registration + `create_layout()` (header/sidebar shell the new
  page lives inside) + sidebar nav entry.
- `web/translations.py` / `tr()` — bilingual strings from line one.

### Invariant guards (must stay green)
- `tests/test_no_raw_storage_access.py` — Phase 87 CI guard; allowlist MUST stay `[]`. A NEW CI
  test must also assert the search call is not made on the event loop (SC#3).
- `docs/guides/MULTITENANT.md` — the safe_storage / multitenant architecture reference.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **`shared/joins_lab.py` (Phase 106 core):** `SearchExecutor` Protocol, `BuilderRow`/`SideQuery`,
  `compose()`, `dedup_candidates`, `normalize_candidate`, `Candidate.key`. The spine produces
  `BuilderRow`s from textarea lines and consumes `Candidate`s — no search logic is written here.
- **`/browse` image viewer (`web/pages/browse.py`):** working `manuscriptViewer` JS with
  zoom/pan/rotate/folio-nav + per-provider proxy resolution + responsive controls. Extract into a
  reusable form for the anchor pane (and Phase 119 Compare).
- **`web/pages/search.py` off-loop pattern:** `await run.io_bound(run_core_search)` +
  `search_state.search_generation` (latest-wins) + `is_running` re-entrancy guard +
  cancellation/`is_cancelled`. FND-01 copies this discipline.
- **`web/safe_storage.py` `safe_user_*` helpers:** the only sanctioned per-user state path.
- **`create_layout()` + `tr()`:** standard page shell + i18n.

### Established Patterns
- **Page = `web/pages/<name>.py` with a `create_*_page()` + `@ui.page` in `web/main.py` + sidebar
  nav** (STRUCTURE.md "New Page"). `/joins-lab` follows this.
- **Reusable UI → `web/components/<name>.py`** exported via `web/components/__init__.py`. The
  extracted anchor viewer + candidate grid likely live here.
- **Multitenant invariant (Phase 87):** zero raw `app.storage.user`; CI-guarded.
- **Image fetches → per-provider proxy + Phase-98 circuit breaker**, never direct IIIF.

### Integration Points
- New `/joins-lab` route in `web/main.py` + sidebar nav entry.
- New `WebSearchExecutor` (probably `web/`-level) binding `state.searcher` + the metadata manager
  to the `shared/joins_lab.py` `SearchExecutor` Protocol.
- Anchor-viewer extraction touches `web/pages/browse.py` (refactor to share, keep browse working).
- `safe_storage` gains a versioned `joins_lab` namespace.
- Two CI tests: existing `tests/test_no_raw_storage_access.py` stays green (allowlist `[]`) + a NEW
  test asserting the search runs off the event loop.

</code_context>

<specifics>
## Specific Ideas

- The cold-start UX explicitly mirrors the **desktop's** shelfmark-entry + 📋 pick-from-list
  (user reaffirmed this shape verbatim): smart box **AND** a "choose from list" button.
- The anchor must **stay in view** while working candidates — this is the user's mental model of
  the Lab ("keep one anchor fragment in view"), and the reason the layout is sticky-anchor rather
  than a single scroll column.
- Parity with the **UAT-approved desktop Joins Lab** is the north star wherever a web-specific
  decision wasn't required — when in doubt, match `desktop/join_workbench.py` behavior.

</specifics>

<deferred>
## Deferred Ideas

- **Typeahead/autocomplete on the cold-start box** — considered as a richer entry; deferred (it's
  closer to a mini-search; revisit as 118+ polish if desired). Not in 117.
- **Builder modes / global toggles in the spine** — deliberately out; the spine runs a fixed exact
  mode. Variants/JA/spacing/bidirectional + per-line modifiers are Phase 118 (BLD-03/BLD-04).
- **Candidate triage / actions / table / Compare / VS in the spine** — out; the spine grid is
  read-only display. Phases 119–120.
- **Full builder/triage/filter persistence + re-run-on-restore** — out; the spine persists only the
  anchor sys_id. Phase 120 (PST-01/02/03).

### Reviewed Todos (not folded)
The `todo.match-phase 117` query surfaced 7 pending todos (scores 0.5–0.6), but **all are spurious
keyword coincidences** ("shared"/"web"/"search"/"side") — none concern porting the desktop Joins Lab
to the web. Reviewed and **not folded**: *Migrate desktop corrections to shared service*,
*Server-side search with email notification*, *NLI MARC crawl/translate*, *Unified metadata text
search*, *Reading Desk UX fixes*, *One-click scholarly citations*, *Fill missing manuscripts from
FIST*. None are in Phase 117 (or v8.2.0) scope.

</deferred>

---

*Phase: 117-vertical-spine*
*Context gathered: 2026-06-17*
