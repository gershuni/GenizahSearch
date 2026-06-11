# Phase 107: Desktop Join Workbench — Anchor, Entry Points, Actions & Join Model - Context

**Gathered:** 2026-06-04
**Status:** Ready for planning

<domain>
## Phase Boundary

The **desktop Join Workbench SHELL**: a dedicated surface that opens with a fragment **pinned as
anchor** (image + numbered transcription, zoom + folio nav, brief metadata — dark-mode/RTL safe),
shows the anchor's **already-known joins as a connected group** (pairwise→group BFS), and wires the
**four public actions** (Browse / Puzzle / Add-to-List / Add-as-Join) including persisting a new
pairwise join and refreshing the group.

**In scope (JWB-01, 02, 03, 04, 09 + build constraints):** the window/host, the three entry points,
the anchor pane, the known-joins group display, the public action APIs, the join-model wiring, and
i18n from the first line.

**Explicitly OUT of 107:** no candidate search / query builders (Phase 108), no visual-similarity
source or combined view or VS-dialog soft-retire (Phase 109), no JSA/parallels (Phase 110), no web
Join Workbench UI (later phase). 107 builds the shell on top of the shared core
`shared/joins_lab.py` (Phase 106).

</domain>

<decisions>
## Implementation Decisions

### Workbench shell & lifecycle
- **D-01:** Host = a **dedicated modeless window** opened FOR a specific anchor — NOT a new
  main-window tab and NOT a dock/overlay. Matches the UAT-validated sketch (`desktop/join_workbench.py`)
  and the "open FOR this fragment" entry model. JWB-01's "tab/page" is read as app-agnostic intent
  ("a dedicated place"); the **web** Join Workbench will be a page in a later phase.
- **D-02:** **Single reusable instance.** A second "Find joins" (different fragment) **re-anchors the
  existing open workbench** (swap anchor, replace hunt context) rather than opening a second window.
  Matches the sketch's single `self._join_workbench` ref. No multi-window management in v8.

### Entry points (JWB-02)
- **D-03:** Three entry points, all proven in the sketch:
  1. 🔗 "Find joins" on the **ResultDialog** action row — anchors the *live page* state
     (`current_sys_id` / `p_num` / `page_text` / `uid`); the ResultDialog **closes after launch**.
  2. 🔗 "Find joins" on the **Browse** tab's `ext_info_row` — builds an anchor from
     `current_browse_sid` / `p` + browse text.
  3. **Cold start by shelfmark** — resolve via `meta.resolve_system_by_shelfmark(q)` (options picker
     on ambiguity, Claude's discretion on that UX). The original result-row 🔗 button is optional
     (detail/Browse triggers are primary).

### Anchor pane (JWB-03)
- **D-04:** Anchor pane = **image + line-numbered transcription + zoom ± + folio prev/next + brief
  metadata** (library · img · title), **dark-mode/RTL safe**.
- **D-05:** Image via the **PROVEN route only**: `meta_mgr.enrich_metadata(sys_id)` → `images_nli`
  (else `images_ext`), `iiif_full` URL = base + `/full/2000,/0/default.jpg`, loaded through
  `desktop.image_loader.ImageLoaderThread` (Referer + Rosetta fallback + disk cache). **NOT**
  FL-substituted thumbnail URLs (they hit NLI's forbidden placeholder — spike iteration D root
  cause). Works for non-NLI (Oxford/Cambridge/etc.). No-image → "(no image)" placeholder.
- **D-06:** Text via `desktop.widgets.line_number_text_edit.apply_line_numbered_text(browser, html,
  source_text=raw_text, is_html=True)` (RTL gutter, `pages=` per-page restart); reuse the existing
  highlight wrapping where applicable.
- **D-07:** **Folio prev/next pages the SAME fragment** — it navigates the anchor sys_id's image
  list by index and refetches per-page text via `get_browse_page(sid, idx+1)`. The **anchor identity
  stays the sys_id**; the known-joins group does **NOT** reload per page (it is a fragment-level
  lookup). The current page is recorded for later (108's "other side = p±1") but in 107 folio nav is
  **viewer-only**.

### Known-joins group (JWB-04)
- **D-08:** Source = `JoinsManager.get_connected_fragments_by_id(sys_id)` (genizah_core.py:9936) —
  the transitive-closure GROUP over PGP `document_fragments` + FJMS scholarly joins + user pairwise
  joins + community puzzle joins (no new schema — REQUIREMENTS join-model decision).
- **D-09:** Presentation = a **flat list** of connected fragments, each carrying a **per-row source
  badge** (PGP / FJMS / user / community). See **R-01** — if the transitive closure does not expose
  per-member/per-edge provenance, the badge **degrades to a generic "known join" tag**; do NOT
  fabricate a source.
- **D-10:** Each row = **thumbnail + shelfmark + title**. Thumbnails via the existing
  `meta_mgr.get_thumbnail` route, fetched **batched** (groups are small, but honor the batch-perf
  build constraint — no per-row serial fetch).
- **D-11:** **Empty state = nothing shows.** The known-joins panel is **hidden entirely** when the
  anchor has no known joins (the common case). No empty-state prompt. **Consequence:** Add-as-Join
  must NOT be gated behind this panel (see D-13).

### Actions & join model (JWB-09, public APIs)
- **D-12:** The four actions are exposed via **public, named methods** — **no `_vs_*` private calls
  remain on the workbench path** (SC#5). Reuse existing app methods, promoting/renaming the `_vs_*`
  ones to public wrappers: Browse = `app.open_result_in_browse_from_table(res)`; Add to List =
  `app.show_add_to_list_menu([{sys_id,fl_id,img}], source=…, anchor_widget=…)`; Puzzle = public form
  of `app._vs_add_to_puzzle(sid)`; Add as Join = public form of `app._vs_open_joins_with_partner(…)`.
- **D-13:** Action homes = an **always-visible anchor action-row** (Browse / Puzzle / Add-to-List the
  anchor itself + **Add as Join**) **PLUS per known-join-member row** (Browse / Puzzle / Add-to-List
  + "⚓ make anchor" re-anchor — D-15). Add-as-Join sits on the **anchor row** so it is reachable even
  when the joins panel is hidden (D-11).
- **D-14:** **Add as Join** opens the existing **`JoinsDialog`** (`corrections_ui.py:3278`)
  **pre-filled with the anchor as fragment A**; the scholar enters partner B there. Persistence via
  the existing path `corrections_client.create_join` / `JoinsManager.create_join_local` → Supabase +
  `joins_cache.pkl`. On success the known-joins group **refreshes** to include the new join (SC#4).
  See **R-02** (confirm free partner-B entry).
- **D-15:** **Re-anchor** — each known-join row has an explicit **"⚓ make anchor"** action that
  re-anchors the workbench to that fragment and reloads ITS known-joins group (walk the connected
  component). **Explicit action, not single-click** (avoid losing hunt context). Reuses the D-02
  re-anchor machinery.

### Architecture / build constraints (locked — carried forward, not re-discussed)
- **D-16:** **i18n from line one** — every new string wrapped in `tr()`; the Workbench renders fully
  under `lang=he` with **no hardcoded English** (SC#6 — acceptance criterion, not cleanup).
- **D-17:** **No candidate search and no VS source in 107.** Query builders + candidate surface =
  Phase 108; VS source + combined view + VS-dialog soft-retire = Phase 109.
- **D-18:** **Desktop-first.** The shell consumes the shared core `shared/joins_lab.py` (Phase 106)
  where applicable; the web Join Workbench is a later phase on the same core. Batch per-row calls.

### Claude's Discretion
- Cold-start ambiguous-shelfmark resolution UX (reuse `resolve_system_by_shelfmark`'s options).
- No-image fallback rendering ("(no image)" placeholder, already proven).
- Exact zoom step, metadata-line composition, window sizing/title, and close/cleanup (cancel
  in-flight `ImageLoaderThread`s on close).
- Whether the workbench is a `QDialog` vs `QMainWindow`; whether the optional result-row 🔗 trigger
  is retained alongside ResultDialog/Browse.
- Internal helper decomposition; how much of the frozen sketch's anchor/helper code transplants.

### Research flags (for gsd-phase-researcher / gsd-planner)
- **R-01:** Does `get_connected_fragments_by_id` (or the underlying join records) expose **per-member
  / per-edge source provenance** (PGP vs FJMS vs user vs community)? D-09's per-row badge depends on
  it; if not recoverable, degrade to a generic "known join" tag — do not fabricate.
- **R-02:** Does the existing `JoinsDialog` support **free partner-B entry** (anchor pre-filled as A,
  scholar types/looks-up B) WITHOUT a pre-supplied candidate? `_vs_open_joins_with_partner` passes
  both fragments; 107 has only the anchor. If the dialog requires a supplied partner, add a minimal
  by-shelfmark resolve (`resolve_system_by_shelfmark`) before opening it.
- **R-03:** Which `_vs_*` methods (`_vs_add_to_puzzle`, `_vs_open_joins_with_partner`) need **public
  wrappers**, and are their bodies **safe to call from the workbench window / its threads**? The
  sketch (iteration F) warns `_enrich_vs_suggestions` touches fjms sqlite on the wrong thread — 107
  does not load VS, but Add-to-List / Puzzle / Browse public methods must be thread-checked.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Executable spec (Spike 002 — frozen at git tag `spike-002-joins-workbench`)
- `.planning/spikes/002-assisted-join-workbench/sketch/join_workbench.py.txt` — the UAT-validated
  throwaway desktop sketch = **executable spec for the workbench shell**: anchor pane (image route,
  zoom, folio nav via `_AnchorLoadWorker` / `_PageTextWorker`), entry points, the four actions, and
  re-anchor. Extract the BEHAVIOR; do NOT copy the PyQt scaffolding.
- `.planning/spikes/002-assisted-join-workbench/DESKTOP-INTEGRATION-NOTES.md` — **the primary 107
  reference**: verified desktop reuse-map (engine/meta/joins signatures, the proven `enrich_metadata`
  → `images_nli/ext` + `iiif_full` 2000px image route [iteration D], `apply_line_numbered_text`,
  entry-point hooks on ResultDialog + Browse, `JoinsManager` known-joins, `JoinsDialog` reuse,
  the RTL start/end trap [iteration E], the four-action wiring [iteration F]).
- `.planning/spikes/002-assisted-join-workbench/CODEX-PRODUCTIONIZE-CRITIQUE.md` — "C-stricter"
  architecture: public action APIs (no `_vs_*`), shared-services-not-sqlite, batch-everything,
  i18n as acceptance.
- `.planning/spikes/002-assisted-join-workbench/SPIKE-FINDINGS.md` — feasibility + primitive
  inventory; what exists vs not.
- `.planning/spikes/002-assisted-join-workbench/REVERT.md` — `JOINS-SKETCH` hook markers + revert
  recipe (the sketch's production hooks are reversible/additive).

### Requirements & roadmap
- `.planning/REQUIREMENTS.md` § "Component A — Join Workbench (JWB)" (JWB-01..04, 09) +
  § "Design-Critique Conclusions & Amendments (2026-06-03)" (join-model decision; validated shape) +
  § "Build constraints" (public APIs, i18n, desktop-first) + § "Deferrals" (#1 N-fragment richness,
  #4 VS-dialog timing, #7 other-side adjacency).
- `.planning/ROADMAP.md` § "Phase 107" — goal + the 6 success criteria this shell must satisfy.

### Prior phase context (shared core)
- `.planning/phases/106-…/106-CONTEXT.md` — the `shared/joins_lab.py` domain model + `SearchExecutor`
  adapter this shell will eventually inject (candidate logic lands in 108, but the join model /
  identity helpers and the pairwise→group decision [106 D-11/D-13] are shared).

### Engine / desktop integration points (read before planning)
- `genizah_core.py:9936` `JoinsManager.get_connected_fragments_by_id(sys_id)` — known-joins BFS (D-08).
- `genizah_core.py:9483` `get_browse_page(sys_id, p_num=…)` → `{text, total_pages, …}` — per-folio
  text for anchor nav (D-07).
- `genizah_core` `MetadataManager.enrich_metadata(sys_id)` / `get_thumbnail(sys_id, size)` /
  `resolve_system_by_shelfmark(q)` — anchor image route (D-05), known-join thumbnails (D-10),
  cold-start (D-03).
- `desktop/image_loader.py::ImageLoaderThread(url)` — anchor image load (D-05).
- `desktop/widgets/line_number_text_edit.py::apply_line_numbered_text(...)` — numbered transcription (D-06).
- `desktop/result_dialog.py` (ResultDialog action row) + `genizah_app.py` Browse `ext_info_row` —
  entry-point hosts (D-03); `GenizahGUI` main refs at `genizah_app.py:3087` / set in
  `on_startup_finished` (~3257): `self.searcher / self.meta_mgr / self.joins_mgr`.
- `corrections_ui.py:3278` `JoinsDialog` + `corrections_client.create_join` /
  `JoinsManager.create_join_local` — Add-as-Join persist path (D-14).
- Public-action targets to wrap: `app.open_result_in_browse_from_table`, `app.show_add_to_list_menu`,
  `app._vs_add_to_puzzle`, `app._vs_open_joins_with_partner` (D-12, R-03).

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **The frozen sketch's anchor/shell code transplants** — anchor pane (image + zoom + folio nav),
  `htmlify`/snippet helpers (now also in `shared/joins_lab.py`), the entry-point hooks, and the
  four-action wiring are all proven; strip the QThread/PyQt scaffolding, keep the behavior.
- **`ResultDialog(parent, all_results, current_index, meta_mgr, searcher)`** — prev/next nav,
  image + line-numbered text, action buttons; its `text_src` sub-pane is the pattern for pinning the
  anchor. The sketch's own **`CompareDialog`** (two equal image+text panes) is reserved for 108.
- **`JoinsDialog` (`corrections_ui.py:3278`)** — view/create joins; the real Supabase +
  `joins_cache.pkl` persist path. Reused pre-filled for Add-as-Join (D-14).
- **`ImageLoaderThread`**, **`apply_line_numbered_text`**, **`JoinsManager`**, `get_thumbnail` /
  `enrich_metadata` / `resolve_system_by_shelfmark` — all verified in DESKTOP-INTEGRATION-NOTES.

### Established Patterns
- Result dict shape: `display{ id(sys_id), shelfmark, title, library_code, img(page), source }`,
  `full_text`, `snippet`, `uid`, `highlight_pattern`, `score`, `scope`.
- `AlmaId == sys_id` (the long `99000…` ids). Joins are keyed by **sys_id** (fragment-level).
- Entry-point pattern: `self._create_action_button(label, tooltip, callback)` +
  `actions_widget.add_btn(btn, always_visible=False)`.
- Workers guard QLabel writes with `try/except RuntimeError` (deleted-widget safety).

### Integration Points
- Desktop injects `self.searcher` (SearchEngine) + `self.meta_mgr` + `self.joins_mgr` into the
  workbench (the 106 `SearchExecutor` adapter is wired for the candidate work in 108).
- Entry hooks live on ResultDialog + Browse `ext_info_row` (`JOINS-SKETCH`-tagged in the sketch).

</code_context>

<specifics>
## Specific Ideas

- The Spike 002 sketch + DESKTOP-INTEGRATION-NOTES are the **executable spec** — extract behaviors,
  do not copy the PyQt scaffolding.
- **RTL start/end trap (iteration E):** Hebrew lines START on the right — keep the anchor/labels
  oriented correctly (matters more for 108's builder, but the anchor pane is RTL too).
- **FL-substituted thumbnail trap (iteration D):** the anchor image MUST use the
  `enrich_metadata`→`iiif_full` route, never the FL-substituted thumbnail URL (NLI forbidden
  placeholder). Candidate/known-join *thumbnails* may use `get_thumbnail` (no substitution).
- **ResultDialog closes after launching the workbench** (iteration B) — preserve that.
- **Known-joins panel hidden when empty** (this discussion) — Add-as-Join therefore lives on the
  anchor action-row, not inside the joins panel.

</specifics>

<deferred>
## Deferred Ideas

- **Candidate search / line-by-line query builders + cross-side AND/OR + candidate grid/table +
  triage + self-match readout + side-by-side Compare** — Phase 108 (JWB-05/06/07/08/10/11, JWB-12
  text surface). The sketch's `QueryBuilder` / `_CrossSideWorker` / `CompareDialog` belong there.
- **Visual-similarity source + combined view + provenance ★/⊙/✎ ordering + VS-dialog soft-retire** —
  Phase 109 (JWB-12 VS surface).
- **JSA / parallels seeding** — Phase 110.
- **Web Join Workbench UI** — later phase on the same shared core.
- **Multiple concurrent workbench windows** — chose single reusable instance (D-02); revisit only if
  power users ask.
- **Dock panel / Join Mode overlay host** — rejected for v8 (D-01); dedicated window only.
- **Richer N-fragment join model / per-edge evidence+confidence/notes** — pairwise→group for v8
  (REQUIREMENTS deferral #1).
- **Multi-leaf / bifolio "other side" adjacency** beyond `p±1` — deferral #7 (matters in 108).

### Reviewed Todos (not folded)
- `todo.match-phase 107` surfaced 4 keyword-coincidence hits — desktop corrections-fetch migration,
  Reading-Desk UX fixes, FIST.db manuscript fill, server-side email search. All matched on the
  generic "desktop"/"shared" area, none touch the Join Workbench shell. Not folded.

</deferred>

---

*Phase: 107-desktop-join-workbench-anchor-entry-points-actions-join-model*
*Context gathered: 2026-06-04*
