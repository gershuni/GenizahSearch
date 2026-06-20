# Phase 120: Actions & Persistence - Context

**Gathered:** 2026-06-20
**Status:** Ready for planning

<domain>
## Phase Boundary

The web Joins Lab gains its **research-output actions** and **working-state durability**, built on
the Phase 117–119 surface (`/joins-lab`, off-loop `WebSearchExecutor`, sticky anchor + known-joins,
full builders, candidate grid/table + triage + filters + Compare + Visual Similarity). Phase 120
delivers:

- **Actions (ACT-01/02/03):** Add-as-Join (login-gated community write), bulk Add-to-Puzzle
  (anchor + selected candidates via a NEW multi-fragment staging handoff), Add-to-List
  (login-gated cloud write) + candidate Export (CSV/XLSX).
- **Persistence (PST-01/02/03):** builder inputs + global toggles + triage + active filter + view
  mode survive refresh / navigate-away-and-back via server-side per-browser-session `safe_storage`;
  the search is **re-run from persisted inputs** on return (NOT a stored result blob); clear/reset
  escape hatch.
- **SEED-007 workbench actions (folded in):** Make-an-anchor, Browse-in-Compare, Compare
  catalog/bib info buttons, Compare image prefetch (fast flip-through), Stop-search-with-partial-
  results, and hiding the 👁 VS toggle for anchors with no VS data.
- **Lists ↔ Joins Lab integration:** wire the (currently stubbed) "choose anchor from my lists"
  authenticated picker; fix the sign-in route bug; add an "Open in Joins Lab" entry point on `/lists`.
- **Stability / close-out:** SEED-008 client-deleted crash hardening + the deferred Phase-119
  verification (test gate, `gsd-verifier`, live HUMAN-UAT R2-3/6/8/10, OPEN_ISSUES refresh).

**In scope (Phase 120 requirements):** ACT-01, ACT-02, ACT-03, PST-01, PST-02, PST-03 — plus the
folded SEED-007 / SEED-008 / Lists-integration items above (user-directed scope, 2026-06-19/20).

**Explicitly NOT in this phase:**
- Cloud sync of Joins Lab working state across devices → PST-F1 (future). Persistence is
  device-local (per-browser-session, server-side via `safe_storage`).
- Complete i18n coverage pass + RTL audit + Hebrew-leak AST audit → **Phase 121** (every new 120
  string is bilingual via `tr()` from line one).
- Admin/moderation UI for confirming or deleting *other users'* joins (only self-delete in 120).
- Showing the anchor's saved `joins.db` join-documents (SEED-007 #3 — **declined**, shared-db noise).
- True engine-streaming partial results (the existing return-partials-on-cancel mechanism is reused;
  no new streaming).

</domain>

<decisions>
## Implementation Decisions

### Add-as-Join (ACT-01)
- **D-01:** "Add as Join" writes to the **Supabase community pairwise-join** path via the EXISTING
  `web/supabase_client.py:1625 create_fragment_join` (anchor = fragment_a, candidate = fragment_b),
  login-gated. It is a scholarly claim that two fragments physically join → flows into the Phase-118
  known-joins group. The `joins.db` "saved-join document" role is covered by **Add-to-Puzzle**
  (ACT-02), so SEED-007 #4 is satisfied **without** a second write path. **Rejected:** writing to
  `joins.db` (web `joins.db` is a SHARED server-side sidecar — `web/pages/puzzle.py:2246` — so a
  per-user join proposal there is globally visible / multitenant-unsafe).
- **D-02:** **Match the observed LIVE behavior** — the user verified on production
  (`genizahsearch.com/browse?sys_id=990051753430205171…`) that a regular user's added join shows
  **immediately to everyone**, and there is **no confirm UI and no delete UI** anywhere. So the new
  join must appear in the known-joins group right after adding (satisfies ROADMAP SC#1). **Researcher
  MUST verify** the live `fragment_joins.status` default and the browse/known-joins fetch branching:
  the committed code says `status DEFAULT 'proposed'` (`supabase_setup.sql:162`) + the group fetch
  filters `status='confirmed'` (`joins_panel.py:114`), which CONTRADICTS the live behavior. Resolve
  the discrepancy and set status **explicitly** on insert so the join reliably appears (do NOT rely
  on an unclear default). This also reconciles the apparent conflict with Phase-118 D-17 (which
  assumed user joins start unconfirmed/creator-only).
- **D-03:** **Add a self-service "remove my join"** affordance on the logged-in user's OWN joins in
  the known-joins group — wire the existing `web/supabase_client.py:1658 delete_fragment_join` + the
  "users can delete own joins" RLS policy. Login-gated, self-scoped, OWN-joins-only. NO admin /
  cross-user delete (that would be its own follow-up). Motivation: the easier Lab creation path makes
  accidental joins likelier, and today there is no delete UI at all.

### Add-to-Puzzle (ACT-02)
- **D-04:** Bulk handoff sends the **anchor + the MULTI-SELECTED candidates** (the 119 D-12 checkbox
  multi-select substrate; matches ROADMAP "all selected candidates") to `/puzzle`. **The anchor is
  always included** regardless of selection. Because `/puzzle?add=` currently accepts a SINGLE
  fragment at a time (`web/pages/puzzle.py:2202` `initial_add`), this phase adds a **NEW
  multi-fragment bulk staging payload/API** so the anchor + selected candidates open together. The
  staging mechanism (server-side staging key via `safe_storage` vs batched query param) is a
  researcher/planner deliverable — must respect the multitenant + size-cap invariants.

### Add-to-List + Export (ACT-03)
- **D-05:** **Add-to-List is login-gated** (cloud write via the existing `/lists` path —
  `web/supabase_client.py:1147 add_list_item`); operates on the multi-selected candidates (D-04).
- **D-06:** **Export = a flat single-table CSV + XLSX** of the candidate set (NOT the 4-sheet
  research dossier). Columns: shelfmark, library, title, material, dimensions, **triage verdict
  (Y/?/N)**, image URL, **+ transcription TEXT of one page** — for text-matched candidates the
  **matched page** (`candidate.page`), for VS-only candidates (no text match) the **first available
  text page**. Export is a local download (no cloud write) → **anonymous-OK** (NOT login-gated). The
  per-candidate text fetch runs **off-loop, batched** with a progress indicator (bounded by the
  candidate pool ~100 / ≤500 fuzzy), and per-cell text is **capped** (reuse the API's ~4000-char
  `SEARCH_API_BROWSE_TEXT_CAP` convention). Reuse `shared/export_dossier.py` column/helper bits where
  handy but the output stays one flat sheet.

### Compare & search enhancements (SEED-007 + user additions)
- **D-07:** **Make-an-anchor** — promote any candidate (or arbitrary fragment) to the anchor slot to
  pivot the workbench in place without re-navigating. Reuses the existing set-anchor / re-anchor flow;
  **triage resets on re-anchor** (119 D-11). (SEED-007 #2.)
- **D-08:** **Browse-in-Compare** — a control INSIDE the Compare modal to open the candidate (and/or
  anchor) in the full `/browse` reader carrying the correct `sys_id` + page (reuse
  `candidate_grid.build_browse_url`; new tab). (SEED-007 #5.)
- **D-09:** **Compare info buttons** — surface FJMS catalog + PGP/bibliography metadata per pane
  inside Compare, reusing the existing Browse/ResultDialog info dialogs (do NOT re-implement metadata
  fetching). (SEED-007 #6.)
- **D-10:** **Compare image prefetch (NEW — user-requested)** — silently preload the **adjacent
  (next/prev) candidates' images** off-loop while the current pane is viewed, so flip-through
  navigation (119 D-02) is instant. Bounded pool (mirror desktop's 5-slot `_pump_images`,
  `desktop/join_workbench.py:5061`); images through the per-provider proxy + Phase-98 breaker; the
  SEED-008 client-deleted guard MUST apply to the prefetch tasks too.
- **D-11:** **Stop-search-with-partial-results (SEED-007 #1) — IS feasible, parity with `/search`.**
  `execute_search` already RETURNS the partial deduped results gathered so far when the
  `progress_callback` raises `InterruptedError` (`genizah_core.py` ~9000; `web/joins_executor.py:54-58`),
  and `/search` already exposes a "Stop" button that shows partials (`web/pages/search.py:558`,
  tooltip "Stops the search and shows partial results"). The Joins Lab currently DISCARDS partials
  via the `_should_apply_results` guard (`web/pages/joins_lab.py:36-37`). Phase 120 adds a **visible
  Stop button that APPLIES the partials on an EXPLICIT user stop**, while the existing guard keeps
  discarding partials from a **superseded** run (newer search / re-anchor). The load-bearing nuance:
  distinguish "user clicked Stop" (apply partials) from "newer search superseded this one" (discard).
- **D-12:** **Hide the 👁 Visual Similarity toggle when the anchor has NO VS data (NEW —
  user-requested).** On anchor load, probe VS availability off-loop (the local `visual_similarity.db`
  read — D-05 of Phase 119) and **hide** (not just disable) the toggle when the anchor has zero
  look-alikes. Tightens 119 D-06's "disabled / no-data" state into an actually-hidden control.

### Persistence (PST-01/02/03)
- **D-13:** **Persist (inputs only, no result blobs):** anchor (`sys_id` already persisted in 117 D-13),
  builder line text + per-line modifiers + gap + Text Position + mode (Exact/Variants/Fuzzy), the
  global toggles (flex-spacing + bidirectional; JA dropped per 118 D-10), the other-side builder
  state + cross-side combine mode, the **`sys_id`-keyed triage**, the active filter, and the view
  mode (grid/table). **NEVER** persist candidate lists / `full_text` / image blobs (size cap
  enforced) — deliberately avoids the 778 MB `search_history` payload-bloat class of bug.
- **D-14:** **Restore = AUTO re-run + a "restoring…" indicator.** On return (refresh OR
  navigate-away-and-back, same browser session) the search is **automatically re-run from the
  persisted inputs** with a brief visible "restoring your search…" indicator, and the grid
  repopulates. Matches the desktop `join_workbench` restore AND the navigation-back todo's acceptance.
  (No opt-in prompt; no defer-for-fuzzy variant — user chose the simplest zero-friction option.)
- **D-15:** After re-run, the persisted **triage / filter / view re-attach by `sys_id`** (triage is
  `sys_id`-keyed; orphan triage for sys_ids that don't reappear is harmless). View mode + filter
  restore directly.
- **D-16:** **Clear / Reset** control wipes ALL Joins Lab working state (anchor + builder + toggles +
  triage + filter + view) from `safe_storage`; a test confirms the `safe_storage` keys are empty
  after reset. State lives under the versioned `joins_lab` namespace (117 D-12, `schema_version`;
  bump only if a key is removed/retyped) via `safe_user_*` (Phase 87 chokepoint, zero raw
  `app.storage.user`).

### Lists ↔ Joins Lab integration (user-directed, 2026-06-20)
- **D-17:** **Wire the "Choose anchor from my lists" authenticated picker.** The button exists
  (`web/pages/joins_lab.py:855`) and the anonymous login-prompt works, but for a LOGGED-IN user the
  dialog only shows a "Go to Lists" button that navigates to `/lists` (`:1561`) — it never lets the
  user pick a fragment and load it as the anchor. Wire the authenticated path to a list/fragment
  picker (parity with the desktop 📋 pick-from-list) that loads the chosen fragment as the anchor.
  Login-gated list read (117 D-06 stays the locked decision).
- **D-18:** **Fix the sign-in route bug.** The anonymous login dialog's "Sign in" button does
  `ui.navigate.to('/settings')` (`web/pages/joins_lab.py:1573`) instead of invoking the real sign-in
  flow. Route it to the canonical sign-in (pointer: `web/pages/corrections.py:53` `create_login_view`
  / `open_login`; completion `web/main.py:2227 complete_login`) — researcher to confirm the exact
  invocation.
- **D-19:** **Add an "Open in Joins Lab" entry point on `/lists`.** Insert a `link`-icon button in
  each list-item action row **between** Browse (`menu_book`, `web/pages/lists.py:694`) and
  Add-to-Puzzle (`extension`, `:701`) → `/joins-lab?sys_id={sys_id}` (FND-08 deep-link contract). A
  third Lab entry point alongside the FND-04/05 entries from `/search` and `/browse`.

### Stability / close-out
- **D-20:** **SEED-008 — harden fire-and-forget tasks against client/tab deletion.** Wrap post-await
  UI mutations in `except RuntimeError: return` at `_load_known_joins` (`web/pages/joins_lab.py`
  ~1122–1184, dispatched ~1309) and `_do_vs_fetch_and_update` (~1474, dispatched ~1257/~1515), and
  audit any other `asyncio.ensure_future(...)` on the page that mutates UI after an `await` (incl.
  the new D-10 prefetch tasks). Precedent: `web/pages/joins_lab.py:2122`,
  `web/components/joins_panel.py:512`. Add a render-smoke test that simulates client disconnect
  mid-fetch.
- **D-21:** **Run the deferred Phase-119 verification** during 120 close-out: the post-merge test
  gate (`119-DEFERRED-VERIFICATION.md` item 1 command), the `gsd-verifier` goal-check (CMP-01/02/03,
  CND-04/06/08, VSM-02), the live HUMAN-UAT re-run of R2-3 / R2-6 / R2-8 / R2-10 (real corpus data),
  and refresh `docs/OPEN_ISSUES.md` with R2 status + SEED-008.

### Claude's Discretion
- ACT-02 bulk staging mechanism (server-side staging key vs batched query param) — within the
  multitenant + size-cap invariants.
- Export per-cell text cap value + exact column ordering + the CSV/XLSX UI affordance (D-06).
- The "choose from my lists" picker layout (flat recent-items list vs list→fragments drill-down) (D-17).
- The Stop button placement/styling and the "restoring…" indicator presentation (D-11/D-14).
- The `link` glyph + tooltip for the `/lists` "Open in Joins Lab" button (D-19).
- Compare info-button placement + whether prefetch also warms transcription text (D-09/D-10).
- Exact `joins_lab` schema additions under `schema_version` (extend v1; bump only on remove/retype).

### Folded Todos
- **`2026-06-18-joins-lab-search-results-survive-navigation.md`** (`resolves_phase: 120`, `area: web`)
  — "search results should survive navigation away and back." Restore brings back the anchor but not
  the candidate grid today. Folded directly into **PST-01 / D-14** (re-run from persisted inputs on
  return; NO result snapshots persisted — preserves the Phase-87 `safe_user_*` chokepoint). This is
  the candidate-grid half of persistence; the anchor half already works (117 D-13).

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Milestone requirements, roadmap & pre-lock critique (read first)
- `.planning/REQUIREMENTS.md` — the 37 v8.2.0 requirements; Phase 120 owns **ACT-01, ACT-02, ACT-03,
  PST-01, PST-02, PST-03** (lines 63–73). Carries the hard cross-phase constraints (safe_storage
  chokepoint, proxy+breaker, off-loop, bilingual, no new Supabase schema). PST deferral table: cloud
  sync = PST-F1.
- `.planning/ROADMAP.md` §"Phase 120: Actions & Persistence" — the 5 success criteria this phase is
  verified against; §"Hard constraints across all phases".
- `.planning/v8.2.0-REQ-CODEX-CRITIQUE.md` — code-grounded pre-lock critique. **BLOCKER 2** (PST is
  server-side per-session via `safe_storage`, NOT browser localStorage) is load-bearing for PST.
- `.planning/phases/117-vertical-spine/117-CONTEXT.md` — **D-06** (Choose-from-list login gate, now
  wired by D-17), **D-12/D-13** (versioned `joins_lab` schema + anchor persistence this phase
  extends), **D-10/D-11** (extracted browse viewer + proxy/breaker).
- `.planning/phases/118-joins-entry-full-builders/118-CONTEXT.md` — **D-17** (ANC-05 confirmed-only
  known-joins group — reconcile with ACT-01 D-02), **D-10** (JA dropped — not persisted), **D-19/20/21**
  (existing joins button/dialog entry points; D-19 of THIS phase adds the /lists entry).
- `.planning/phases/119-candidates-compare-visual-similarity/119-CONTEXT.md` — **D-11** (sys_id-keyed
  triage, in-memory in 119, persisted here), **D-12** (multi-select substrate the bulk actions ride),
  **D-02** (Compare flip-through that prefetch D-10 accelerates), **D-06** (VS no-data state D-12
  tightens). Also `119-DEFERRED-VERIFICATION.md` (the D-21 close-out checklist).

### Seeds routed into this phase
- `.planning/seeds/SEED-007-…workbench-actions.md` — the six workbench actions (Make-an-anchor,
  Browse-in-Compare, Compare info buttons, Add-as-Join, etc.). Items #1/#2/#5/#6 + Add-as-Join folded;
  #3 (show saved joins.db joins) declined.
- `.planning/seeds/SEED-008-joins-lab-client-deleted-crash.md` — the fire-and-forget client-deleted
  crash + the exact fix pattern and dispatch sites (D-20).

### The shared core this phase rides (do not re-implement)
- `shared/joins_lab.py` — `Candidate` (`.key == (sys_id, page)`, provenance flags), `dedup_candidates`,
  `merge_candidates`, `badge_and_tooltip`. Actions consume `Candidate`s; persistence stores the
  `SideQuery`/`BuilderRow` inputs, not candidates.
- `shared/puzzle_service.py` / `joins.db` — the saved join-document/puzzle write path that ACT-02
  (Add-to-Puzzle) targets (NOT ACT-01). Web `joins.db` is SHARED across users
  (`web/pages/puzzle.py:2246`).
- `shared/export_dossier.py` — column/helper bits reusable for the flat candidate export (D-06).
- `shared/visual_similarity_service.py` — `get_suggestions` (the VS-availability probe for D-12).

### Web seams to extend / reuse
- `web/pages/joins_lab.py` — the page: candidate surface + Compare + VS toggle + builder; the
  `_should_apply_results` partial-discard guard (D-11), `_load_known_joins` / `_do_vs_fetch_and_update`
  fire-and-forget tasks (D-20), the choose-from-list dialog (`:853`/`:1545`-`:1576`, D-17/D-18).
- `web/joins_executor.py` — `WebSearchExecutor` (off-loop; already forwards `progress_callback` and
  returns partials on `InterruptedError` — D-11). CI-guarded by `tests/test_joins_lab_off_loop.py`.
- `web/joins_lab_storage.py` — the versioned `joins_lab` `safe_storage` schema to extend for PST
  (D-13/D-16). `safe_user_*` helpers only.
- `web/supabase_client.py` — `create_fragment_join` (`:1625`, ACT-01/D-01), `delete_fragment_join`
  (`:1658`, D-03), `get_fragment_joins` (`:1574`, status semantics for D-02), `add_list_item`
  (`:1147`, ACT-03/D-05).
- `web/components/joins_panel.py` — the known-joins group (where the added join must appear, D-02,
  and where "remove my join" lives, D-03).
- `web/components/candidate_grid.py` / `web/components/compare_modal.py` / `web/components/anchor_viewer.py`
  — the multi-select + Compare surfaces the actions, prefetch (D-10), Browse-in-Compare (D-08), and
  info buttons (D-09) attach to.
- `web/pages/puzzle.py` — `create_puzzle_page(initial_add=…)` (`:2202`) + `_add_fragment_by_sys_id`
  (`:2110`); the single-fragment entry ACT-02's bulk staging extends.
- `web/pages/lists.py` — the list-item action row (Browse `:694` / Add-to-Puzzle `:701`); insertion
  point for the "Open in Joins Lab" button (D-19).
- `web/pages/search.py` — the `/search` Stop button + partial-results path to mirror (`:558`, `:2355`,
  `progress_cb` `:4055`) for D-11.
- `web/pages/corrections.py:53` (`create_login_view`/`open_login`) + `web/main.py:2227`
  (`complete_login`) — the real sign-in flow for D-18.
- `web/safe_storage.py` — `safe_user_*` chokepoint (Phase 87). `web/translations.py` / `tr()`
  — bilingual from line one.

### Invariant guards (must stay green)
- `tests/test_no_raw_storage_access.py` — Phase 87 CI guard; allowlist MUST stay `[]`. PST writes go
  through `safe_user_*` only.
- `tests/test_joins_lab_off_loop.py` — the search/VS/enrichment/prefetch calls must stay off the event
  loop (`run.io_bound`); the D-10 prefetch + D-12 VS-availability probe must obey it.
- `tests/test_no_server_side_stop_propagation.py` — server-side `e.stop_propagation()` is forbidden.
- `docs/guides/MULTITENANT.md` — safe_storage / multitenant reference.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **`create_fragment_join` / `delete_fragment_join`** (`web/supabase_client.py:1625`/`:1658`) — the
  community-join write + the unused-but-RLS-backed delete that D-01/D-03 wire to UI.
- **`/search` Stop + partial-results machinery** (`web/pages/search.py:558`/`:2355`/`:4055`) — the
  exact pattern D-11 mirrors; `execute_search` already returns partials on `InterruptedError`.
- **`web/joins_lab_storage.py` versioned `joins_lab` schema** — extend for the full PST payload (D-13).
- **Desktop `_pump_images` bounded loader pool** (`desktop/join_workbench.py:5061`) — the prefetch
  model for D-10.
- **`shared/export_dossier.py` helpers** + the `/api` `text_cap` convention — for the flat export (D-06).
- **`web/components/joins_panel.py` known-joins group** — where added joins surface (D-02) + the
  remove affordance lives (D-03).

### Established Patterns
- **Off-loop discipline** (`run.io_bound` + generation counter + `is_running`, CI-guarded) — every
  new search/VS/enrichment/prefetch path obeys it.
- **All per-user state via `safe_user_*`** (Phase 87, allowlist `[]`); versioned `joins_lab` namespace.
- **Persistence = inputs + triage, re-run on restore, no result blobs** (mirrors desktop
  `join_workbench`; avoids the search_history bloat class of bug).
- **Image fetches → per-provider proxy + Phase-98 breaker**; local SQLite reads (VS, FJMS) off-loop
  but no breaker.
- **Fire-and-forget UI mutation after `await` → guard `except RuntimeError`** (D-20 precedents in-repo).
- **Web-idiomatic UI, desktop behavior = parity north star** (118 D-01).

### Integration Points
- Actions (Add-as-Join / Add-to-Puzzle / Add-to-List / Export / Make-an-anchor) attach to the
  candidate surface multi-select + the known-joins group + Compare.
- ACT-02 introduces a NEW multi-fragment staging handoff between `/joins-lab` and `/puzzle`.
- PST extends `web/joins_lab_storage.py`; restore wires an auto-re-run on page load using the
  Phase-117 off-loop search pipeline.
- Lists integration touches `web/pages/lists.py` (new button) + `web/pages/joins_lab.py` (picker +
  sign-in fix).
- SEED-008 hardening touches the fire-and-forget tasks in `web/pages/joins_lab.py`.

</code_context>

<specifics>
## Specific Ideas

- **"Users' joins are auto-confirmed — I confirmed it live."** The user added a join on production as
  a regular user and it showed immediately to everyone; there is **no confirm UI and no delete UI**
  (not for the creator, not for admins, except raw Supabase). ACT-01 matches this live behavior;
  D-03 adds the missing self-delete. (Verbatim user finding, 2026-06-19.)
- **Export "has to include text of one page"** — the matched-term page for text hits, or the first
  text page in VS-only cases. (Verbatim user requirement, D-06.)
- **"Silently loading images in bg when navigating in Compare, allowing for fast navigation"** — the
  user-added prefetch requirement (D-10).
- **"Don't show the Visual Similarity switch for fragments without VS"** (D-12). Verbatim.
- **"We didn't wire Choose Anchor from list (also the 'sign in' there goes to settings instead of
  sign in). Also … in the lists page a button for Open in Joins Lab (link icon between Browse and
  Puzzle)."** (Verbatim, D-17/D-18/D-19.)
- **"Currently the web search allows for stop and show partial results — isn't it the same search
  behavior?"** — the user corrected an over-cautious infeasibility read; D-11 reuses the existing
  `/search` partial-on-stop mechanism.
- **Parity with the UAT-approved desktop Joins Lab** is the north star wherever a web-specific
  decision wasn't required.

</specifics>

<deferred>
## Deferred Ideas

- **Cloud sync of Joins Lab working state across devices** → PST-F1 (future). 120 is device-local.
- **Admin / cross-user join moderation + delete** — 120 adds only self-delete (D-03); an admin
  confirm/delete UI (and a real moderation workflow) is a separate follow-up. There is currently NO
  confirm-join or delete-join UI anywhere in the app — worth an OPEN_ISSUES note.
- **Show the anchor's saved `joins.db` join-documents** (SEED-007 #3) — **declined** this phase: web
  `joins.db` is shared across all users, so it would surface everyone's saved join-docs (noise).
  Could be revisited if a per-user saved-joins model ever lands.
- **True engine-streaming partial results** — not needed; D-11 reuses the existing return-partials-
  on-cancel mechanism. A genuine incremental-streaming search would be a much larger, separate effort.
- **Full i18n / RTL / Hebrew-leak audit** → Phase 121 (120 strings are bilingual from line one).

### Reviewed Todos (not folded)
The `todo.match-phase 120` query surfaced 4 matches. **One folded** (the navigation-restore todo —
see Folded Todos). The other 3 are spurious keyword coincidences, already reviewed-and-rejected in
Phases 117–119 and **not folded**: *Migrate desktop corrections fetch to shared corrections_service*
(area: desktop), *Fill missing genizah manuscripts from FIST.db* (area: data), *Reading Desk UX
fixes* (area: desktop). None concern the Web Joins Lab.

</deferred>

---

*Phase: 120-actions-persistence*
*Context gathered: 2026-06-20*
