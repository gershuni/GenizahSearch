# Join Workbench — desktop integration notes (survives /compact)

> Handoff so a fresh/compacted session can continue WITHOUT re-discovering anything.
> Written 2026-06-02. Companion to `CODEX-CRITIQUE.md` (evidence + design decisions).

## Current state of the sketch (what exists on disk)

- **`desktop/join_workbench.py`** — self-contained `JoinWorkbenchDialog(parent, app, anchor_result)`
  + `CompareDialog`. Triage funnel: anchor pane (image + line-numbered text + material) | query
  with Line START/END + paginated 20/page thumbnail grid (material/VS/score + Y/?/N) | enlarge
  one-by-one compare beside anchor. Helpers `htmlify / bigger / material_for / vs_score /
  ThumbResolver / CandidateCard`. Image label writes are guarded with `try/except RuntimeError`
  (deleted-QLabel safety).
- **`genizah_app.py` hooks (only 2, easy to revert):**
  1. Result-row button — in the `actions_widget.add_btn(...)` block (~line 17066): a 🔗 button
     `self._create_action_button("\U0001f517", tr("Find joins"), lambda _, r=res: self.open_joins_workbench(r))`.
  2. Method `open_joins_workbench(self, res)` — inserted right before `def show_full_text` (~18774);
     imports `JoinWorkbenchDialog` lazily, opens it modeless, keeps ref `self._join_workbench`.
- **`_tmp/join_triage.py`** — the OLD standalone NiceGUI prototype (local engine + prod images).
  SUPERSEDED by the desktop sketch; keep only as reference.
- Verified headless: both files compile; helpers + highlight + DB lookups pass (VS=1.639 on the
  Or.1081 join pair). NOT GUI-tested here (no display). User runs `python genizah_app.py`.

## Verified desktop reuse-map (DO NOT re-derive — these are checked against source)

- **Index location:** `Config.INDEX_DIR` auto-resolves to the LEGACY path
  `C:\Users\gersh\Genizah_Tantivy_Index\` on this machine (portable + AppData absent). `tantivy_db`
  is there; the local engine works. Engine load ~9s, a line search ~6s, first `get_browse_page` ~9s.
- **Engine, headless:** `from genizah_core import MetadataManager, VariantManager, SearchEngine`;
  `meta=MetadataManager(); meta._load_csv_bank(); var=VariantManager(); eng=SearchEngine(meta,var)`.
- **Search:** `eng.execute_search(query, mode, gap, text_position=None|'line_start'|'line_end',
  corpus_scope='genizah'|'all'|'local')`. Modes: `variants / literal / fuzzy / Phrase / Regex`.
  Returns a **list of dicts**, each: `display{ id(sys_id), shelfmark, title, library_code, img(page), source }`,
  `full_text`, `snippet`, `uid`, `raw_header`, `highlight_pattern`, `score`, `scope`.
- **Search thread (UI):** `gui_threads.SearchThread(searcher, query, mode, gap, exclude_words=None,
  responsa_options=None, restrict_sys_ids=None, text_position=None, corpus_scope="all")` →
  signals `results_signal(list)`, `error_signal(str)`, `progress_signal(int,int)`; `cancel_flag` attr.
- **Anchor by shelfmark:** `meta.resolve_system_by_shelfmark(q)` → `{sys_id, options[{sys_id,shelfmark,title}], selected_shelfmark}`;
  then `eng.get_browse_page(sys_id, p_num=None)` → `{uid, p_num, full_header, text, total_pages,
  current_idx, sys_id, volume_ie}` (NO shelfmark/library/image in this dict).
- **Images:** `desktop.image_loader.ImageLoaderThread(url)` — takes a **URL string**; signals
  `image_loaded(QImage)`, `load_failed()`; method `cancel()`. Disk-caches by FL id / URL hash.
  URL source: `meta_mgr.get_thumbnail(sys_id, size=320)` → NLI 400px IIIF URL
  (`{NLI_IIIF_BASE}/FL{digits}/full/400,/0/default.jpg`); does a MARC fetch per uncached id
  (circuit-breaker protected); returns **None for non-NLI** (Oxford/Cambridge/Manchester/JTS).
  Enlarge: swap width with regex `/full/\d+,/` → `/full/{W},/`. For non-NLI images use
  `meta_mgr.enrich_metadata(sys_id)` → `images_ext[i]['url']` (+ `/full/{W},/0/default.jpg`) and
  `_detect_external_provider` (desktop/viewers.py:817). (NOT yet wired in the sketch.)
- **Text rendering:** `from desktop.widgets.line_number_text_edit import apply_line_numbered_text`;
  `apply_line_numbered_text(qtextbrowser, html, source_text=raw_text, is_html=True)` (RTL gutter,
  `pages=` for per-page restart). Highlight: wrap `highlight_pattern` regex matches, escape, bold.
- **Entry-point pattern:** `self._create_action_button(label, tooltip, callback=None, parent=None)`
  + `actions_widget.add_btn(btn, always_visible=False)`. Row result dict is on
  `results_table.item(row, COL_SYS_ID).data(Qt.ItemDataRole.UserRole)`. Double-click →
  `show_full_text_for_result(res)`. Context menu builder: `_show_results_context_menu` (~5651).
- **Main-window refs:** `GenizahGUI` (genizah_app.py:3087). `self.searcher / self.meta_mgr /
  self.joins_mgr / self.var_mgr / self.lab_engine` set in `on_startup_finished` (~3257).
- **Known joins (JWB-04/09, NOT yet wired):** `JoinsManager` (genizah_core.py:9936) —
  `get_connected_fragments_by_id(sys_id)` / `get_connected_fragments(shelfmark)`. Desktop
  `JoinsDialog` (corrections_ui.py:3278) reusable for view/create. FJMS scholarly joins:
  `shared.fjms_service.get_fjms_service().get_join_group(sys_id)`. PGP: `shared.document_service`.
- **Material + VS sidecars:** `fist_data/fjms_enrichment.db` (`manuscript_measurements`:
  AlmaId, catalog_width_cm, catalog_height_cm, material, avg_num_lines, size_category — ~90% of
  catalog, 99.8% of physical-join members) and `fist_data/visual_similarity.db`
  (`visual_suggestions(alma_id_a, alma_id_b, svm_score)`, ~50% coverage). AlmaId == sys_id (99000…).
  Desktop also has `shared.visual_similarity_service.get_vs_service()`.
- **Public API (used by the old NiceGUI proto; does NOT accept `text_position`):**
  `POST /api/search {query, search_mode, limit}`; `GET /api/browse?sys_id=&uid=`;
  `POST /api/parallels {text, chunk_size, mode}`. Bot-UA blocked → send a browser User-Agent.

## Evidence already established (full detail in CODEX-CRITIQUE.md)

- Corpus = `Transcriptions.txt` (948,549 page records / 216,911 sys_ids; MiDRASH is the bulk).
- Tear markers pervasive: 72% of pages have edge brackets, 42.9% clear the ≥15% bar.
- Side mapping (verified, vs JWB-05's spec which is INVERTED): **start-`]` = left half,
  end-`[` = right half** (8.2:1 and 3.35:1 in the corpus).
- Per-page lean (excl. whole-line lacunae): clean-left 5.8%, clean-right 11.8%, both-edges 13.0%,
  neither 69.5%. On 2,178 known PHYSICAL joins, a clean complementary L+R read fires on only **2.5%**
  (55% "involves both"; contradictory only 6%) → JWB-05 is a minor conservative assist, not a headline.
- 85.4% of known size-2 joins have BOTH sides transcribed → seeding is fine; **discrimination**
  (right text ≠ right fragment; 89% of joins are literary) is the real constraint.
- Probe scripts/reports: `_tmp/joins_probe*.py`, `_tmp/corpus_*probe*.py`, `_tmp/codex_probes.py`
  (+ `*_report.txt`).

## User's product direction (as of last turn — about to change "significantly")

High-throughput VISUAL TRIAGE: anchor (image+text) on one side; fan out search/browse results as
thumbnails (~20/page, paginated) on the other; quick yes/no/maybe; then enlarge & confirm one-by-one
beside the anchor. Material info + visual similarity = real helpers (not the headline). Wants FAST
ad-hoc iteration, NOT a pre-planned GSD process. Build inside the desktop app.

## Update 2026-06-02 (this session) — starting point moved + results upgraded

- **Trigger moved off the result row, onto the detail views** (per user): added a 🔗 "Find joins"
  button to **ResultDialog** action row (`desktop/result_dialog.py::_open_join_workbench`, anchors
  the *live page* state — current_sys_id/p_num/page_text/uid) and to the **Browse** tab's
  `ext_info_row` (`genizah_app.py::_browse_open_join_workbench`, builds an anchor from
  current_browse_sid/p + browse_original_text). The original result-row button is still wired too.
- **Results pane upgrades** in `desktop/join_workbench.py`:
  - **responsa mode** added to the mode dropdown → sends `mode='exact'` +
    `responsa_options={responsa_mode:True, variants:True, …}` (verified against
    `genizah_app.py:16800`). Position combo still feeds `text_position` line_start/line_end.
  - **Grid ⇄ Table view toggle** (`view_btn`/`toggle_view`/`render_table`): the table is the
    responsa-style, text-forward, scannable view — cols `· | Shelfmark | Library | Title | Text |
    Material | VS | Score`; double-click a row → CompareDialog. Grid (thumbnails) unchanged.
  - Text already shown on cards (6 lines) + table Text column.
- **Reusability finding (corrects the user's lead):** the Composition "two panes" are 1-line
  preview cells in the comp *tree* (`genizah_app.py:22079`) — NOT reusable. The reusable artifacts
  are (a) the standard **`ResultDialog(parent, all_results, current_index, meta_mgr, searcher)`** —
  prev/next nav, image+line-numbered text, a source-context sub-pane (`text_src`) that could pin the
  anchor, and every action button; and (b) our own **`CompareDialog`** (two equal panes, both
  image+text, prev/next). Split: navigate-many in ResultDialog/table, compare-side-by-side in
  CompareDialog. Host layout (the "5 options" — in-dialog split / Join Mode / dedicated window /
  dock / new Joins tab) still UNDECIDED; current sketch = dedicated window (Option 3).
- **Reversibility:** all 8 production-file hooks tagged `JOINS-SKETCH`; full revert recipe in
  `REVERT.md`. Today it's one `git restore` + delete (uncommitted, additive).

**NEXT:** GUI smoke by Hillel (`python genizah_app.py` → open a result / Browse a shelfmark →
🔗 Find joins → try responsa mode + Table view). Then decide the host layout among the 5 options.

## Update 2026-06-02 (iteration B) — UAT batch 1 (13 items)

`desktop/join_workbench.py` rewritten (clean, still untracked/throwaway). Done: (1) ResultDialog
`self.close()` after launching workbench; (2) anchor image zoom ±  + folio prev/next via
`_AnchorPageWorker` (get_browse_page text + per-page image by substituting the FL id into the
get_thumbnail base URL — NLI only, degrades to "(no image)"); (3) Start/End combo border-only
(dark-mode safe); (4) 5-way position combo Anywhere/Text START/Text END/Line START/Line END →
engine `[None,start,end,line_start,line_end]`; (6) removed the query auto-seed (we hunt what's
MISSING); (7) compact one-row-per-ms-image dedupe (by uid, then sid|img); (8) refine bar with
material dropdown + has-dimensions + dark-mode-safe metadata colors; (9) table rows resizable
(Interactive vertical header + wordwrap + resizeRowsToContents) + global text filter; (10) VS
tooltip explaining SVM-pair score + ~50% coverage; (11) Score column renamed "Relevance" + tooltip
(Tantivy text relevance, NOT join confidence); (12) "filter / search within results" box; (13a)
highlighted snippet centered on the match (`snippet_html`/`snippet_plain`); (13b) brief anchor
metadata line (`meta_brief`: library · img · title). `meas_for`/`vs_score` now cached.
Filtered-list plumbing: `self.results` (deduped) → `self.filtered` (refine view); grid/table/
CompareDialog all index `self.filtered`.

**Item 5 — BUILT (2026-06-02 iteration C).** Multi-row responsa-style builder (each row = term +
"gap N" → composes `t1 [g1] t2 …`; 1 row honors the mode dropdown, 2+ rows force responsa
proximity; global variants toggle; this-side position combo). Cross-side constraint via
`_CrossSideWorker`: "other side" = adjacent image **p±1** (Hillel confirmed A; first→+1, last→−1,
middle→both; total_pages via get_browse_page, cached). **AND** = post-filter base candidates,
keep only those whose neighbor page matches query B (CAP 600, progress, capped-note). **OR** = keep
all this-side + union pages whose other side matches B (runs B via `searcher.execute_search`
directly in the worker, maps each B-hit page q→its neighbors, builds neighbor result dicts via
meta_mgr.get_meta_for_id/get_library_for_id; CAP 600). B-position: anywhere / Text START / Text END
/ "line #" (1-based, brackets stripped before match; substring all-words test). Verified headless:
page_of, snippet centering, and the line-N / anywhere B-test all correct. NOT GUI-tested.
**Still deferred (fast-follow):** line-GAP semantics (`[|N]` + `|` line-break syntax) as an
alternative to word-gaps; per-row variation columns; an editable composed-query preview field.

## Update 2026-06-02 (iteration D) — UAT batch 2 (image route + line-gaps + symmetric builder)

- **Item 1 — anchor images FIXED.** Root cause: FL-substituted thumbnail URL hit NLI's forbidden
  placeholder. Switched the anchor to the PROVEN route Browse/ResultDialog use:
  `meta_mgr.enrich_metadata(sys_id)` → `images_nli` (else `images_ext`), each `{url(base),label,fl_id}`;
  loadable URL = `base + "/full/2000,/0/default.jpg"` (helper `iiif_full`), loaded via
  `ImageLoaderThread` (Referer + Rosetta fallback + cache). Folio prev/next now navigates the
  images list by index; per-folio text via `get_browse_page(sid, idx+1)`. Works for non-NLI too.
  (`_AnchorPageWorker` → `_AnchorLoadWorker`; added `_PageTextWorker`.) Candidate thumbnails still
  use `get_thumbnail` (works; no substitution).
- **Item 2 — gap is now by LINES.** Builder rows compose the responsa **line-break** syntax
  (`|`-groups + `[|N]` line-gaps) instead of word-gaps `[N]`. Verified vs `gc._parse_line_break_query`:
  `|aaa bbb [|2] ccc` → 2 groups, gap [2], group0 line_start. Each row = a manuscript line with a
  "↓ N ln" gap to the next.
- **Item 3 — symmetric builder.** New reusable `QueryBuilder` widget (rows: `⌞start | word(s) |
  end⌝ | ↓N ln | ×`, + variants toggle) used for BOTH this-side and the other-side. The global
  position combo + mode combo were REMOVED — per-row line START/END now carries positioning
  (line-level). (NOTE for Hillel: page-level "Text START/END" from item 4 is gone, folded into
  per-row line anchors — say the word if you want it back as a per-builder toggle.)
- **Cross-side worker rewritten** to run query B through the real engine
  (`searcher.execute_search(bq, "exact", 0, responsa_options=bro, corpus_scope="genizah")` — so B's
  line-gaps/anchors/variants all work) and match by **(sys_id, page±1) set membership**: AND keeps
  base candidates whose neighbour ∈ B-set (no per-page fetch — fast); OR keeps all base + adds
  neighbours of B-matches (fetched for display, bounded by total_pages). CAP 4000.
- Always-responsa now (mode combo dropped); `MODES`/`POSITIONS` constants removed. Compiles; image
  URL builder + line-break composition verified headless. NOT GUI-tested.

## Update 2026-06-02 (iteration E) — UAT: "can't find the anchor itself"

Hillel searched the anchor's own line-beginnings and got no self-match. Two causes + fixes:
- **Anchor silently excluded.** `_on_results` drops `sys_id == anchor` (correct for join-hunting,
  but defeats self-verification with zero feedback). Fix: (a) always compute `_anchor_matched =
  any(r_sid==anchor in raw results)` and show it in the status — "⚓ anchor matches this query ✓"
  / "✗ does NOT match"; (b) new **"include anchor itself"** checkbox (default off, applies on next
  search) → keeps the anchor, marked `_is_anchor_self` (teal border + "⚓ self" badge, no VS).
- **RTL start/end trap.** Row had ⌞start on the LEFT and end⌝ on the RIGHT, but Hebrew lines START
  on the right — so the checkbox nearest the text's beginning was labelled "end". Hillel ticked
  "end" for line-beginnings. Fix: reordered each builder row to `[ends line ⊣] [field] [⊢ starts
  line] [↓gap] [×]` so "starts line" sits on the right (Hebrew start edge); clearer labels +
  tooltips ("right edge in Hebrew" / "left edge"). compose() keys unchanged.
- WATCH: if a correct "starts line" query still reports "✗ anchor does NOT match", suspect leading
  tear-bracket tokens (`]שהדותא`) defeating the engine's line_start test — would need bracket
  handling in the line-break match (engine-side; shared with the main responsa search).

## Update 2026-06-02 (iteration F) — MERGED Visual Similarity into the workbench

Hillel: the VS dialog is already a join-triage surface ("Add as Join" → JoinsDialog → joins_cache.pkl
+ Supabase). Decision: full merge — one surface, candidates from text / visual / combined, with the
four real actions on every candidate. Built:
- **Three source buttons**: `Search` (text line-builder), `Visual similarities`
  (`_VsLoadWorker` → `shared.visual_similarity_service.get_vs_service().get_suggestions(anchor, limit=80)`,
  csv_bank-enriched in-thread (NOT `_enrich_vs_suggestions` — that touches fjms sqlite on the wrong
  thread), per-candidate first-page text via get_browse_page), `Search + visual` (combine).
- **Source state machine** (`_run_sources`/`_set_text_cands`/`_set_vs_cands`/`_maybe_assemble`): waits
  for all requested sources, then merges. Combine = annotate text candidates also in the VS set
  (`_via_vs`+`vs_rank`), append VS-only, sort **both-first → text-only → VS-only**. Provenance badges
  on cards: **★ both** / **⊙ VS#rank** / **⇄ other** / **⚓ self**. Verified merge ordering headless.
- **Four actions on every candidate card + in the Compare pane** — reuse existing app methods:
  Browse = `app.open_result_in_browse_from_table(res)`; Puzzle = `app._vs_add_to_puzzle(sid)`;
  Add to List = `app.show_add_to_list_menu([{sys_id,fl_id,img}], source=…, anchor_widget=btn)`;
  **Add as Join** = `app._vs_open_joins_with_partner(anchor_sid, anchor_shelf, cand_sid, cand_shelf)`
  (opens JoinsDialog pre-filled, anchor=fragment A → the REAL persist+Supabase path). Compare pane
  also has Re-anchor. Card now: triage Y/?/N + ⤢compare + ⚓re-anchor row, then 📖🧩📋🔗 row.
- Anchor-self-match readout reset on no-text runs. Compiles; merge logic verified; NOT GUI-tested.
- **VS dialog NOT yet retired** (still opens from ResultDialog + has JoinsDialog pick-mode hook).
  Phase 2 (reroute its entry to the workbench) deferred until Hillel is happy with the merged UX.

Original design (kept for reference):
- **Multi-line this-side builder:** N rows (term + gap-to-next) compose a responsa query string
  (`t1 [2] t2 …`) run in responsa mode; global variants toggle; per-query position.
- **Cross-side constraint:** a query B on the **other side of the leaf** + AND/OR.
  - "Other side" = the recto/verso pair = adjacent image (p±1) in the same sys_id. r/v isn't
    reliably labelled, so check both neighbors. (CONFIRM with Hillel: adjacent-image vs
    any-other-page-of-fragment.)
  - **AND** = post-filter A's candidates: per candidate fetch the other-side page
    (`get_browse_page(sys_id, p±1)`) and test B (regex; optional "line N contains X" — evaluated
    by us, not the engine). Worker + progress + explicit cap (no silent truncation).
  - **OR** = union A's candidates with pages whose other side matches B (run B, map hits to their
    neighbor pages). Confirm whether OR is needed for v1 or AND-only first.
  - Use case: AND narrows when A floods; OR widens when A is sparse. Example:
    page starts `אמר רבי` (this side, Line START) AND other side line 3 has `שלמה`.
