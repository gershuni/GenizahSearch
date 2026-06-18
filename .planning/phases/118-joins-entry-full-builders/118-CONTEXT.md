# Phase 118: Joins, Entry & Full Builders - Context

**Gathered:** 2026-06-18
**Status:** Ready for planning

<domain>
## Phase Boundary

The web Joins Lab gains its full query-building power and its connective tissue, built on
top of the Phase-117 vertical spine (`/joins-lab` route, `WebSearchExecutor` off-loop adapter,
sticky anchor pane + scrolling work column, versioned `safe_storage` schema). Phase 118 delivers:

- **Known-joins group (ANC-04/ANC-05):** the anchor's scientific joins (PGP + FJMS + user +
  community) shown as a connected, source-attributed group, multitenant-safe.
- **"Find joins" entry (FND-04/FND-05):** reachable from `/search` result cards, `/browse`, and
  the `/search` Quick View dialog, opening the Lab pre-loaded on that fragment.
- **Full both-side builders (BLD-02/BLD-03/BLD-04):** a web-idiomatic, line-based query builder
  for the anchor side AND the other side of the leaf, with per-line modifiers, a per-line gap
  control, a Text Position control, a mode selector, and global toggles — none silently dropped.

**In scope (Phase 118 requirements):** ANC-04, ANC-05, FND-04, FND-05, BLD-02, BLD-03, BLD-04.

**Explicitly NOT in this phase (locked elsewhere — do not pull forward):**
- Candidate triage Y/?/N, table surface, self-match readout, filters/pagination/enrichment,
  side-by-side Compare, Visual Similarity → **Phase 119**.
- Add-as-join, bulk Add-to-Puzzle, add-to-list/export, full builder/triage/filter **persistence**
  with re-run-on-restore, clear/reset → **Phase 120**. (Phase 118 builds the builder UI; persisting
  its inputs across refresh is Phase 120 — the 118 builder is transient beyond the anchor that 117
  already persists.)
- Complete i18n coverage pass + RTL audit + Hebrew-leak AST audit → **Phase 121** (but every new
  string in 118 is bilingual via `tr()` from line one).

</domain>

<decisions>
## Implementation Decisions

### Builder shape & default UI
- **D-01:** The Joins Lab builder is **web-idiomatic and free to diverge from the desktop look**.
  It is its **own widget** (a web port of the desktop `JoinQueryBuilder`) feeding
  `shared/joins_lab.py` (`BuilderRow` → `SideQuery` → `compose()` → the Phase-117 off-loop
  `WebSearchExecutor`). It is **NOT** the generic `/search` Responsa tabular query-builder dialog,
  though it shares the same Responsa modifier vocabulary and composes to the same engine syntax.
- **D-02:** **Responsa is the default** — the line builder always composes Responsa syntax
  (`responsa_mode=True`). The builder is **shown by default** (inline on the page, not behind a
  dialog). This replaces Phase 117's fixed `mode='exact'` spine default.
- **D-03:** **Line-based, rows stacked VERTICALLY** (one manuscript line above another,
  top-to-bottom), mirroring the anchor's numbered transcription. This replaces the Phase-117
  single textarea (`web/pages/joins_lab.py:333-340`; `lines_to_side_query:116-130` evolves to
  produce richer `BuilderRow`s instead of one-term-per-line).
- **D-04:** **OR-entry within a line = a per-line text field with light Responsa syntax**: space =
  word sequence on the line, `a/b` = OR-alternatives. The builder wraps the structural bits
  (Text Position, modifiers, gap) around what is typed. (Chosen over the desktop multi-box `[+or]`
  replica and over chip slots — leverages Responsa-literacy, least clutter, compact.)
- **D-05:** **No anchor-line-click seeding** — the builder is typed by hand. (The "click an anchor
  transcription line to seed a builder row" affordance was declined; keep it simple.)
- **D-06:** **Per-row modifiers live in a compact "more" affordance, not the default view**
  (no clutter): line-start ⊢ / line-end ⊣, plene/defective, prefix/suffix, wildcard, negation —
  the full desktop modifier vocabulary, hoisted into Responsa forms (`#(a/b)`, `-(a/b)`, `(a/b)*`,
  `%word`, etc.) by the **existing** `compose()` / build-side-query hoist rules. **Do not redesign
  the hoist** — parity (`desktop/join_workbench.py:1272-1347`; `compose` line/start/end at
  `shared/joins_lab.py:762-765`).
- **D-07:** **Per-line GAP control — "distance to the next line"** — surfaced **inline between the
  stacked rows** (a first-class control, NOT buried in advanced). Maps to `BuilderRow.gap_to_next`
  → composes to the `[|N]` gap marker. Lets a scholar express "this line, then N lines down, this
  line." (User-requested.)

### Text Position (join-critical)
- **D-08:** **Surface the existing `/search` "Text Position" control prominently** — the 5-way
  `ui.select` at `web/pages/search.py:646-655` (`anywhere / start / end / line_start / line_end`,
  whose code comment literally says *"for join detection"*). It is buried in `/search` advanced
  options but is **central to torn-edge joins**, so it gets a prominent place in the Joins Lab
  builder. Maps to `text_position` passed to `execute_search` (Phase 117 already carried
  `SideQuery.page_position` → `text_position`).

### Mode & toggles
- **D-09:** **Mode selector: Exact / Variants / Fuzzy** — the variant-expansion tier, **layered on
  the always-Responsa builder**. "Variants" **combines with** Responsa (adds a variant layer); it
  is **NOT** a redundant toggle. Fuzzy is the heaviest tier and rides Phase 117's off-loop +
  timeout/cancel/stale-generation path. (This adds Fuzzy beyond the desktop's binary variants
  checkbox.)
- **D-10:** **Advanced toggles = Flexible-spacing + Bidirectional only. Judeo-Arabic is DROPPED.**
  → **Documented divergence from BLD-04 (which names Judeo-Arabic) and from desktop parity** — user
  decision: JA is not needed for the web Lab. `ja` stays `False` (compose() already hardcodes
  `ja=False`; simply do not surface a JA toggle).
- **D-11:** **ONE shared toggle set applied to BOTH sides** (anchor + other side). compose()
  hardcodes `ja/flex_spacing/bidirectional` to false (`shared/joins_lab.py:741-749`), so the web
  `_merge_globals`-equivalent **MUST re-inject `flex_spacing` + `bidirectional` into BOTH composed
  queries** (`ro` and `b_ro`) — the RR-14 parity rule (`desktop/join_workbench.py:2475-2489`,
  applied to both sides at `:2493-2524` and `:2579-2580`). `variants` flows via
  `SideQuery.variants` → compose → `variant_mode`. **Diverges from desktop's per-side toggle
  state** (accepted — these tolerances are rarely wanted on only one side).
- **D-12:** **"Advanced search options" collapsible disclosure** holds: the Flex-spacing +
  Bidirectional toggles **and** the other-side builder. Default view stays clean.

### Other-side builder (BLD-02)
- **D-13:** The **other-side-of-the-leaf builder is OFF + collapsed by default** (checkbox-gated),
  with **Narrow (AND) as the default** combine mode when enabled. Same line-based builder shape as
  the anchor side, but `allow_page_position=False` on the other side (parity,
  `desktop/join_workbench.py:2251`). It drives `resolve_other_side_pages`
  (`shared/joins_lab.py:283-303`) → `cross_side_membership` (`:306-341`) → `apply_cross_side`
  (`:344-463`): **Narrow = AND = a proper subset** of the anchor-only results (keep only
  candidates whose adjacent page ±1 matches the other side); **Widen = OR = a superset** (append
  synthesized neighbor-page candidates). The **web page contract** for `resolve_other_side_pages`
  (which identifier feeds it — `p_num` vs internal index — multi-IE `volume_ie` behavior, unknown
  total-page counts, sparse/metadata-only pages) is a **researcher deliverable** — the core assumes
  numeric `page ± 1` (`shared/joins_lab.py:286`) but browse supports `volume_ie` /
  metadata-only / nearest-page fallback (`genizah_core.py:~9869`).

### Collapse behavior
- **D-14:** On search, the builder **auto-collapses to a readable summary bar**, re-expandable via
  an Edit affordance, so candidates get the room. Empty-builder search is **guarded** (show a hint,
  do not run). Exact summary wording is Claude's discretion (suggested:
  `"Variants · 5 lines · Text Position: line ends · flex-spacing on"`).

### Known-joins group (ANC-04 / ANC-05)
- **D-15:** **Placement — a collapsible section in/under the sticky anchor pane** (below the
  transcription). Joins are a property of the anchor and should stay in view while hunting (matches
  desktop, where the joins panel sits with the anchor). Uses compact source-badged member rows
  (PGP / FJMS / user / community — parity `badge_for_source`,
  `desktop/join_workbench.py:166-179`). Reuses the **pairwise→group connected-member model, no new
  schema**.
- **D-16:** **Interactions THIS phase: display + click-member-to-RE-ANCHOR + open-member-in-`/browse`.**
  Bulk Add-to-Puzzle / Add-to-List are **deferred to Phase 120** (ACT-02/03).
- **D-17:** **Multitenant safety (ANC-05) = public/confirmed-only joins in the process-global
  path** — structurally leak-proof (User A's unconfirmed/creator-only RLS-scoped joins can never
  reach User B). **Accepted trade-off:** a logged-in scholar's OWN not-yet-confirmed joins will not
  appear in the group. This fixes the `web/components/joins_panel.py:24-29` global cache (keyed by
  `doc/pgp`, **not** user/status) + the `web/supabase_client.py:1574-1623` `get_fragment_joins` RLS
  concern (Codex BLOCKER 3).

### Entry points (FND-04 / FND-05)
- **D-18:** **"Find joins" opens `/joins-lab` in a NEW browser tab.** Deep link = `sys_id`
  (+ `volume_ie` for multi-IE manuscripts); **no builder/triage state in the URL** (FND-08
  contract). Preserves the user's search/browse context in the original tab (≈ the desktop modeless
  window).
- **D-19:** **Reuse the EXISTING joins button + dialog** (`web/components/joins_panel.py`
  `create_joins_button` / `create_joins_dialog`) as the entry — do **NOT** add a separate button.
  On `/browse` the joins button is **always present on every fragment**:
  - **When known joins EXIST →** the joins dialog gains a **"Find more joins"** button under
    "View all fragments" (tooltip *"go to joins lab to find more joins"*).
  - **When NO joins →** the button shows in a **different color**, tooltip *"Find Joins"*, and
    clicking goes **straight to the Joins Lab** (new tab) — no near-empty dialog.
- **D-20:** **Same behavior in Quick View** (the `/search` result advanced dialog).
- **D-21:** **`/search` result cards show the joins icon** (chain/link). Click behavior = **same
  logic as browse everywhere**: joins exist → open joins dialog (with "Find more joins" → Lab);
  none → straight to the Lab (new tab). The icon may hint joins-present vs none. Shown on **every
  fragment**.

### Claude's Discretion
- Exact collapsed summary-bar wording (D-14) and the empty-builder hint text.
- The precise per-row "more" affordance widget (popover vs inline expand) and how the OR text /
  chips render within a row (D-04/D-06).
- The exact placement/styling of the per-line gap control between stacked rows (D-07).
- The joins icon glyph + colors for joins-present vs none on cards / the recolored browse button
  (D-19/D-21).
- The `safe_storage` builder-state shape — full builder/toggle/triage persistence is Phase 120;
  if any 118 builder state must survive, extend the `joins_lab` schema under `schema_version=1`
  (no bump unless a key is removed/retyped).

### Divergences from locked requirements / desktop parity (flagged for the planner)
1. **Judeo-Arabic dropped** — diverges from BLD-04's named toggle list and from desktop parity
   (D-10). User decision.
2. **One shared toggle set** for both sides — diverges from desktop's per-side toggle state (D-11).
   Matches BLD-04's "global ... applied to BOTH sides" wording.
3. **Mode selector adds Exact/Variants/Fuzzy** — desktop exposes only a binary variants checkbox;
   web adds an explicit tier incl. Fuzzy (D-09).

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Milestone requirements, roadmap & pre-lock critique (read first)
- `.planning/REQUIREMENTS.md` — the 37 v8.2.0 requirements; Phase 118 owns ANC-04, ANC-05, FND-04,
  FND-05, BLD-02, BLD-03, BLD-04. Carries the hard cross-phase constraints (safe_storage chokepoint,
  proxy+breaker, off-loop, bilingual, no new Supabase schema, no automated finder).
- `.planning/ROADMAP.md` §"Phase 118: Joins, Entry & Full Builders" — the 5 success criteria this
  phase is verified against; §"Hard constraints across all phases".
- `.planning/v8.2.0-REQ-CODEX-CRITIQUE.md` — code-grounded pre-lock critique. **BLOCKER 3** (ANC-04
  known-joins RLS leak → ANC-05), **HIGH** "BLD-04/05 will silently drop JA/flex/bidirectional"
  (compose() hardcodes them false; `_merge_globals` re-injects), **HIGH** "cross-side needs a web
  contract" (BLD-02 page contract), **HIGH** "FND deep-link contracts". All load-bearing for 118.
- `.planning/phases/117-vertical-spine/117-CONTEXT.md` — prior-phase decisions; esp. **D-04**
  (layout reserved structural room for 118's known-joins panel + other-side builder), D-08/D-09
  (spine builder was the throwaway textarea / fixed exact mode that 118 replaces), D-15/D-16 (the
  adapter shape this builds on).

### The shared core this phase rides (do not re-implement search logic)
- `shared/joins_lab.py` — `BuilderRow`/`SideQuery` (`:28-72`), `compose()` (`:741`; hardcodes
  ja/flex/bidirectional false at `:741-749`; line-start/end pipe injection `:762-765`),
  `resolve_other_side_pages` (`:283-303`), `cross_side_membership` (`:306-341`), `apply_cross_side`
  (`:344-463`), `dedup_candidates`, `Candidate` + `.key`.

### Parity reference — the desktop Joins Lab being ported (north star unless web required a change)
- `desktop/join_workbench.py` — **Known joins:** `build_known_join_rows` (`:368-405`),
  `dedup_join_rows` (`:189-203`), `badge_for_source` (`:166-179`), `_build_joins_panel`
  (`:4510-4580`), `_reload_known_joins`/`_on_known_joins_loaded` (`:5281-5361`), `_build_join_row`
  (`:5386-5500`). **Builder:** `JoinQueryBuilder` (`:797-1466`), per-row modifier dialog
  (`:1140-1216`), hoist rules `build_side_query` (`:1272-1347`). **Other-side:** UI (`:2226-2259`),
  `_CrossSideWorker` (`:1592-1640`), search flow `_on_results`/`_on_cross_done` (`:2552-2611`).
  **Global toggles + merge:** options dialog (`:1099-1134`), `_responsa_opts` (`:1253-1270`),
  **`_merge_globals` (`:2475-2489`)**, `do_search` (`:2493-2524`).

### Web seams to extend (Phase 117 + reuse targets)
- `web/pages/joins_lab.py` — the page to extend: `create_joins_lab_page` (`:194-666`),
  `lines_to_side_query` (`:116-130`, evolves), builder seam (`:333-340`, the textarea to replace),
  `execute_joins_search` (`:500-621`, the compose→off-loop→dedup pipeline to reuse for both sides),
  anchor/work-column containers (`:240-308`; D-04 room for the known-joins panel + other-side).
- `web/joins_executor.py` — `WebSearchExecutor` (4 Protocol methods; off-loop discipline enforced
  by `tests/test_joins_lab_off_loop.py`). `get_browse_page` (`:76-106`) drives other-side page
  resolution.
- `web/joins_lab_storage.py` — versioned `joins_lab` schema (`schema_version=1`, anchor-only today;
  `safe_user_*` helpers). Builder-state persistence is Phase 120; extend under v1 if needed.
- `web/components/joins_panel.py` — **reuse for entry points (D-19/D-20/D-21) AND as the known-joins
  data path (D-15/D-17):** `fetch_connected_fragments` (`:32-267`; global cache `:24-29`),
  `create_joins_button` (`:310-373`), `create_joins_dialog` (`:376-766`; "View all fragments"
  `:627-655`; community puzzle joins `:671-742`), `invalidate_joins_cache` (`:270-291`).
- `web/supabase_client.py` — `get_fragment_joins` (`:1574-1623`, `.or_()` filter `:1592`); the
  ANC-05 confirmed-only / RLS fix touches here or the joins_panel cache.
- `web/pages/search.py` — the **Text Position** control to surface (`:642-665`, options `:646-655`).
- `web/pages/search_results.py` — result-card render + action row (`create_result_card` `:350`;
  actions `:581-629`, Browse beside `:584-600`, Quick View `:603-606`); `sys_id` `:356`, `fl_id` /
  `volume_ie` parsed `:587-593`. Insertion point for the joins icon (D-21) + Quick View (D-20).
- `web/pages/browse.py` — per-fragment action row (`:1833-1876`: list/puzzle/share); `state.sys_id`,
  `state.current_page` (`fl_id`/`p_num`/`shelfmark`/`total_pages`); insertion point for the
  recolored joins button (D-19).
- `web/safe_storage.py` — `safe_user_*` chokepoint (Phase 87 invariant).
- `web/translations.py` / `tr()` — bilingual strings from line one.

### Invariant guards (must stay green)
- `tests/test_no_raw_storage_access.py` — Phase 87 CI guard; allowlist MUST stay `[]`.
- `tests/test_joins_lab_off_loop.py` — search call must stay off the event loop (any new search
  path 118 adds, incl. the other-side / cross-side query, must obey this).
- `docs/guides/MULTITENANT.md` — safe_storage / multitenant architecture reference (ANC-05).

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **`shared/joins_lab.py`** — full builder/compose/cross-side/dedup core. 118 writes UI that
  produces richer `BuilderRow`s (modifiers, gap, line-start/end) and a second `SideQuery` for the
  other side; the AND/OR cross-side logic and the modifier hoist already exist — **do not
  re-implement**.
- **Phase-117 `execute_joins_search` pipeline** (`web/pages/joins_lab.py:500-621`) — reuse the
  compose → `run.io_bound` → timeout/cancel/stale-generation → `dedup_candidates` flow for the
  anchor side; extend it with the other-side compose + `apply_cross_side` (also off-loop).
- **`web/components/joins_panel.py`** — both the entry-point button/dialog (D-19/20/21) and the
  known-joins fetch (D-15/17). The connected-group display already exists for `/browse`.
- **`/search` Text Position select** (`web/pages/search.py:646-655`) — lift the same 5-option
  control into the builder.
- **`_merge_globals` pattern** (`desktop/join_workbench.py:2475-2489`) — port verbatim in spirit:
  re-inject flex_spacing + bidirectional into the composed `ro` for BOTH sides (no JA).

### Established Patterns
- **Page = `web/pages/joins_lab.py`** extended in place; reusable UI → `web/components/`.
- **Off-loop search discipline** (`run.io_bound` + generation counter + `is_running` guard) — every
  new search path obeys it (CI-guarded).
- **Multitenant invariant (Phase 87):** zero raw `app.storage.user`; all per-user state via
  `safe_user_*`.
- **Image fetches → per-provider proxy + Phase-98 circuit breaker** (relevant to known-joins member
  thumbnails / re-anchor).
- **Pairwise→group join model, no new Supabase schema.**

### Integration Points
- Known-joins section added into the **sticky anchor pane** (D-15) — uses the layout room reserved
  by Phase 117 D-04.
- Other-side builder + advanced toggles added inside the **"Advanced search options" disclosure** in
  the work column (D-12/D-13).
- "Find joins" wired into `joins_panel.create_joins_button`/`create_joins_dialog` and surfaced on
  `search_results.py` cards + Quick View + `browse.py` (D-19/20/21).
- ANC-05 fix touches `joins_panel.fetch_connected_fragments` cache and/or
  `supabase_client.get_fragment_joins` (confirmed/public-only).
- Researcher to define the **web other-side page contract** for `resolve_other_side_pages` (D-13).

</code_context>

<specifics>
## Specific Ideas

- **"Take the Responsa mode as default"** and show the line builder by default — the user's framing
  of the whole builder (D-02).
- **Lines stacked one above another, not side-by-side** — mirror the manuscript / the anchor's
  numbered transcription (D-03).
- **"Show explicitly the Text Position" — it's hidden in `/search` advanced and is crucial for
  joins** (D-08). Verbatim user emphasis.
- **"Allow for all other search options without cluttering" + "compact to one line when
  searching"** — the auto-collapsing builder (D-14) and the advanced disclosure (D-12).
- **"Variants is not redundant — it combines with Responsa (adds a layer). But Judeo-Arabic is not
  needed."** — D-09 (variants via mode selector) + D-10 (JA dropped). Verbatim user correction.
- **"There should be a place to control the line distance between each line."** — the per-line gap
  control (D-07). Verbatim user request.
- **Reuse the existing Browse joins button**: when joins exist, add "Find more joins" under "View
  all fragments"; when none, recolor it + "Find Joins" → straight to the Lab; also in Quick View;
  joins icon on search cards (D-19/20/21). Verbatim user spec.
- **Parity with the UAT-approved desktop Joins Lab** is the north star wherever a web-specific
  decision wasn't required.

</specifics>

<deferred>
## Deferred Ideas

- **Bulk Add-to-Puzzle / Add-to-List from the known-joins group** → Phase 120 (ACT-02/03).
- **Full builder/triage/filter persistence + re-run-on-restore** → Phase 120 (PST-01..03). Phase
  118's builder/toggle/other-side state is transient beyond the anchor that Phase 117 persists.
- **Candidate triage / table / Compare / Visual Similarity** → Phase 119.
- **Anchor-line-click → seed builder row** — considered and declined (D-05); could be revisited as
  later polish if the typed-by-hand flow proves slow.

### Reviewed Todos (not folded)
The `todo.match-phase 118` query surfaced 7 pending todos (top score 0.9), but all are spurious
keyword coincidences ("manuscripts"/"search"/"web"/"side"/"metadata") — none concern porting the
desktop Joins Lab to the web. Reviewed and **not folded**: *Fill missing genizah manuscripts from
FIST.db*, *Server-side search with email notification*, *Unified metadata text search with
translations*, *Reading Desk UX fixes*, *One-click scholarly citations*, *Migrate desktop
corrections to shared service*, *NLI MARC crawl/translate*. None are in Phase 118 (or v8.2.0) scope.

</deferred>

---

*Phase: 118-joins-entry-full-builders*
*Context gathered: 2026-06-18*
