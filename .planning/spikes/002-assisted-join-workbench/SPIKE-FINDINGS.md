# Spike 002 — Assisted Join Workbench (Feasibility)

> **Status:** Spike complete (read-only investigation, no production code changed)
> **Date:** 2026-06-01
> **Verdict:** FEASIBLE as a composition of existing building blocks. The MVP is mostly UI glue around tools that already ship in both apps. The hard part (the v7/v8 "join finder" algorithm) does NOT exist as code and is NOT required for the MVP — the workbench is a *human-in-the-loop* surface that drives the existing line-start/line-end and parallels search over the whole corpus, exactly as the user described.

---

## 0. The proposed feature (recap)

A scholar pins ONE anchor manuscript (image + transcription + line numbers) on screen and, while it stays in view, uses the app's EXISTING search tools to hunt for the OTHER fragments that physically JOIN it:
- **End-of-line / start-of-line search**: take the END text of an anchor line and search for fragments whose LINE STARTS with the continuation (and the mirror).
- **Free-text + parallels (composition) search**: shared distinctive phrases.
- **Visual similarity = SUPPLEMENTARY only** (it covers only part of the corpus — quantified below).
Candidates surface ranked; the scholar inspects side-by-side and opens a confirmed pair in the Fragment Puzzle.

---

## 1. The JOIN_FINDER_REPORT — summary and "does the code exist?"

Source: `docs/archive/JOIN_FINDER_REPORT.md` (842 lines, dated March 2026, "Active research — POC/experimental").

### The approaches (v1→v8)
- **v1–v3 (MSBERT)**: predict the masked missing word(s) on a torn line via a Hebrew-manuscript BERT, search predictions in `line_starts`. Abandoned — predictions are generic function words (של/את/על), zero discriminative power. Best rank ≈ #181.
- **v4–v6 (direct phrase/TF-IDF/long-phrase)**: tried matching the two halves directly. Proved the core difficulty: two halves of a *vertical tear* share almost **zero complete words** (every line is split mid-word) — v6 found **0 shared 4-word phrases** between the Or.1081 halves; v5 TF-IDF ranked the true partner #834.
- **v7 — "Two-Hop via parallels" (the breakthrough)**: don't match LEFT↔RIGHT directly; use a **bridge**. (a) take the last ~2 words of each torn (`]`) line on the LEFT half, phrase-search them in `content` to find PARALLEL manuscripts that contain the full un-torn text; (b) in each parallel, read the ~8 words that FOLLOW the phrase (the continuation); (c) search those continuation words across `content`; (d) score `n_matched_lines*100 + n_matched_words*50 + idf_sum`, with a **physical complementarity filter** (a LEFT/`]` source requires the candidate to have ≥15% RIGHT/`[` lines). Ranked the two benchmark cases (Or.1081, PGPID 3433) at **#1**.
- **v8 — v7 + FIST visual**: re-rank v7 candidates with Friedberg's SVM image-similarity pairs (`Image_BestMarkForJoin`); +1500 boost when a textual candidate also appears in SVM, and a separate "visual-only" bucket scored `svm*800`. FJMS scholar-confirmed `joins` are used **only as ground truth**, never to score.
- **Sequential join finder**: end-of-A → start-of-B continuation with orthographic-variant generation; finds the *text* but cannot isolate the specific partner among many copies of the same liturgical work.

### How line-continuation / line_starts / line_ends are used
The whole family relies on the Tantivy index fields `line_starts` (first word of each line, with `L{n}:word` positional tokens) and `line_ends` (last word of each line). The cross-tear signal is: *end of line X on the LEFT flows into the start of line X+1 on the RIGHT* — so continuation words are matched against `line_starts`. The report's own "Recommended App-Oriented Implementation Plan" (lines 717–800) notes the scripts actually search broad `content` rather than these positional fields, and recommends switching to `line_ends`/`line_starts` + `scope="system"` dedup.

### Infrastructure it relied on
Tantivy index (`content`, `line_starts`, `line_ends`, `unique_id`, `shelfmark`); MSBERT local server on `localhost:5000` (abandoned); `fjms_enrichment.db` `joins` (ground truth) and FIST.db `Image_BestMarkForJoin` (35.9M SVM pairs) via the `AlmaId→InventoryId→FGP→DocumentId` mapping chain.

### Batch evaluation results (stated)
15 PGP Hebrew 2-fragment vertical-tear cases:

| Metric | v7 (text only) | v8 (text + SVM) |
|---|---|---|
| Recall@1 | 6.7% | 20.0% |
| Recall@10 | 20.0% | 40.0% |
| Recall@50 | 33.3% | 46.7% |
| MRR | 0.113 | 0.251 |

**6 of 15 cases (40%) had NO textual parallels at all** → fundamentally unsolvable by this approach. Runtime: **~83–101 seconds per fragment** (Phase-3 continuation-word fan-out dominates) — too slow for interactive use.

### ⚠️ Does any of this exist as runnable CODE in the repo? — **NO.**
- `scripts/join_finder_*.py` (`poc`, `v2`–`v8`, `sequential`, `eval`): **do not exist** in the working tree (`ls scripts/join_finder*` → no such file) and **never existed in git history** (`git log --all -- 'scripts/join_finder*.py'` → empty).
- Every grep hit for `join_finder` / `find_joins` / `two_hop` / `two-hop` / `sequential_join` is in **documentation or `.claude/worktrees/` copies only** — no `.py` source:
  - `docs/archive/JOIN_FINDER_REPORT.md` (the report)
  - `docs/plans/JOIN_FINDER_IMPLEMENTATION_PLAN.md` (**Status: "Planning"** — never executed)
  - `docs/FEATURE_IDEAS.md`, `docs/OPEN_ISSUES.md`, `_tmp/codex-v8-wild-ideas.md`
- **Conclusion:** the v7/v8/sequential join-finder algorithm is **research/prototype-only and not embedded anywhere in the shipping product.** The *primitives it would consume* (line_starts/line_ends fields, phrase search, parallels) ARE in production. The proposed Workbench deliberately side-steps the unbuilt algorithm by putting the scholar in the loop.

---

## 2. Line-start / line-end search — EXISTS and is driveable with arbitrary text

The "search by start/end of line" feature is **live in both apps today**.

**Index fields** — `genizah_core.py`:
- `Indexer._extract_position_fields` (`genizah_core.py:5266`) builds `line_starts` / `line_ends` from each line's first/last word, plus `L{n}:word` positional tokens (`genizah_core.py:5283-5286`).
- Schema declares both as whitespace-tokenized text fields: `genizah_core.py:5454-5455`.

**Query path** — `genizah_core.py`:
- A `text_position` argument selects the search field via `position_field_map = {'start':'content_head','end':'content_tail','line_start':'line_starts','line_end':'line_ends'}` (`genizah_core.py:8564-8570`), then `self.index.parse_query(t_query_str, [search_field])` (`genizah_core.py:8580`). If the index predates these fields it raises a "rebuild the index" error (`genizah_core.py:8583-8586`).
- A separate per-component path (`|`-syntax line-break search) maps the first/last component of a line group to `line_starts`/`line_ends`: `_execute_line_break_search` at `genizah_core.py:8001`, field selection at `genizah_core.py:8026-8031`.
- Exact-position correctness is enforced post-Tantivy by `_validate_position_match` (`genizah_core.py:5294`).

**Web UI wiring** — `web/pages/search.py`:
- A `text_position` dropdown with options including `'line_start': tr('Line starts')` / `'line_end': tr('Line ends')` (`web/pages/search.py:646-654`); value flows into the search call as `text_position=...` (`web/pages/search.py:4169-4178`, `:4368`).
- Per-component checkboxes "Start of line |_" / "End of line _|", visible in Lines scope (`web/pages/search.py:2828-2853`).

**Desktop UI wiring** — `genizah_app.py`:
- `self.chk_line_start` / `self.chk_line_end` checkboxes ("Start of line |_" / "End of line _|"), Lines-scope only (`genizah_app.py:1655-1663`, visibility `:1973-1974`, modifier map `:1998`, `:2025`).

**Can a scholar run it today, with an arbitrary phrase?** **Yes.** The user-facing feature is exposed in both apps, and programmatically the engine accepts any query string with `text_position='line_start'`/`'line_end'`. So feeding the anchor's line-END text into a `line_start` search (and vice-versa) is a direct call to an existing code path — no new search engine work.

---

## 3. Parallels / composition search — EXISTS, accepts arbitrary text

- Service: `shared/parallels_service.py`. Entry: **`fetch_parallels_results(*, text, chunk_size, mode, max_freq=None, boundary_mode='full', restrict_sys_ids=None)`** (`shared/parallels_service.py:151`). It chunks an arbitrary `text` snippet and calls `SearchEngine.search_composition_logic(full_text=text, ...)` (`shared/parallels_service.py:203-218`), returning ranked, sys_id-grouped `main_results` (capped at 200 groups) + `filtered_results`.
- Public HTTP path: `POST /api/parallels` — body model at `web/search_api.py:298`, handler at `web/search_api.py:1186`. Takes `text` (not a sys_id), so any snippet works.
- **Implication:** the anchor's transcription (or a selected passage) can be fed straight into parallels to surface shared-phrase candidates. No new code needed to *call* it; only UI to seed it from the anchor and to merge its candidates into the workbench list.

---

## 4. Visual similarity — COVERAGE QUANTIFIED (validates "VS is partial")

- Service: `shared/visual_similarity_service.py` → `get_vs_service()` singleton; key methods `get_suggestions(sys_id, limit)` (`:97`), `has_suggestions` (`:130`), `get_suggestion_partners(sys_ids, mode)` (`:211`).
- Backing data: **`fist_data/visual_similarity.db`** (1.3 GB), table **`visual_suggestions(alma_id_a INTEGER, alma_id_b INTEGER, svm_score REAL)`**, plus `vs_metadata`. Server-only; desktop fetches per-MS via `/api/visual_suggestions/{sys_id}`.

### Live SQLite queries (read-only, this machine)
`vs_metadata`: `version=1.0.0`, `import_date=2026-03-30`, `source=FIST.db Image_BestMarkForJoin`, `pair_count=13,647,365`, `manuscript_count=129,456`.

| Measure | Value |
|---|---|
| Total SVM pairs | **13,647,365** |
| Distinct source manuscripts with VS data (`alma_id_a`) | **129,456** |
| Distinct partners (`alma_id_b`) | 129,335 |
| Distinct manuscripts touched (union a∪b) | 129,456 |
| Full catalog size (distinct `system_number` in `libraries.csv`) | **255,723** |
| VS source ids that ARE present in the catalog | 129,456 (100% — id formats match exactly, the long `99000…` AlmaIds) |
| **VS coverage / full catalog (255,723)** | **50.62%** |
| **VS coverage / transcribed-searchable corpus (~217K, report's index figure)** | **≈59.7%** |

**Verdict on the user's claim:** CONFIRMED. Visual similarity touches at most ~129K of ~256K manuscripts — **~50% of the catalog (≈60% of the transcribed corpus)**. It is structurally non-exhaustive and cannot be the backbone of a join hunt; it is a legitimate supplementary signal only. (No image → no VS row; ~half the catalog has no SVM data at all.)

---

## 5. Anchor view + open-in-Puzzle surfaces — building blocks present

- **Anchor viewer (image + transcription + line numbers):** the Browse / Reading Desk manuscript viewer already renders image + transcription with a line-number gutter (line-numbering shipped v7.12, per CHANGELOG; web `web/pages/browse.py`, desktop ResultDialog/Browse). The workbench reuses this as the pinned anchor pane.
- **Joins panel:** `web/components/joins_panel.py` — `fetch_connected_fragments(shelfmark, document_id, pgpid)` (`:32`) merges user pairwise joins + PGP `document_fragments`; renders a "Community Puzzle Joins" section that opens the puzzle via `ui.navigate.to(f'/puzzle?doc={pj_id}')` (`:716-719`).
- **Fragment Puzzle:** `web/pages/puzzle.py::create_puzzle_page(initial_add=None, initial_doc=None)` (`:2202`); `initial_add` is a `/puzzle?add=sys_id,shelfmark` query param (`:2218-2220`, handled `:3779-3792`), and `_add_fragment_by_sys_id` (`:2110`) places a fragment on the Fabric.js canvas. Backing CRUD in `shared/puzzle_service.py` (joins.db sidecar) and image fetch in `shared/puzzle_image_service.py`.
- **Existing precedent for the exact handoff we need:** the **Visual Similarity dialog already opens a candidate in the puzzle** with `ui.navigate.to(f'/puzzle?add={aid}')` (`web/components/visual_similarity_dialog.py:609`); also used from search results (`web/pages/search_results.py:1458`) and lists (`web/pages/lists.py:702`).
- **Hand-off for a confirmed PAIR:** navigate to `/puzzle?add=` with anchor then candidate (two adds), or create a join document up front. The single-`add` param exists today; a two-fragment seed is a tiny extension (loop `_add_fragment_by_sys_id` twice, or extend the query param to accept a comma-list).

---

## 6. Feasibility verdict + MVP scope

### Verdict
**FEASIBLE — MVP is largely UI composition, not new search/algorithm work.** Every retrieval primitive the user described is already in production and already callable with arbitrary text:
1. line-end → line-start search (`text_position` path, `genizah_core.py:8564`),
2. parallels over a snippet (`fetch_parallels_results`, `shared/parallels_service.py:151`),
3. VS as a supplementary bucket (`get_vs_service().get_suggestions`, ~50% coverage),
4. anchor viewer + open-in-Puzzle (`/puzzle?add=...`, precedent at `visual_similarity_dialog.py:609`).
The unbuilt, slow, research-only v7/v8 *automated* join algorithm is explicitly NOT on the MVP path — the scholar is the ranker/disambiguator, which is exactly what makes this shippable.

### Ready to compose (no new core work)
- line_starts/line_ends Tantivy fields + query path (web + desktop)
- parallels service + `/api/parallels`
- visual_similarity_service + sidecar DB
- Browse/Reading-Desk anchor viewer with line numbers
- Puzzle `?add=` handoff + joins_panel

### Needs building (MVP)
- A **two-pane "Join Workbench" layout**: left = pinned anchor (image + numbered transcription); right = search + candidate list. (New page/panel; reuses existing viewer + search widgets.)
- A **"Find continuations" action**: select an anchor line (or click its end) → auto-run a `line_start` search seeded with that line's END words (and a mirror `line_end` ← line START). Pre-fill the existing search call; render hits in the right pane. (Glue + a small "seed query from selected line" helper.)
- A **candidate list** that merges line-search hits + (optional) parallels hits + (supplementary) VS partners, **kept in separate evidence buckets** (text-line / parallels / visual), each linking to side-by-side compare and **"Open pair in Puzzle"**.
- **Two-fragment puzzle seed** (extend `?add=` to seed anchor+candidate, or sequential adds).

### Effort
- **MVP (anchor pane + "find continuations" line-search + bucketed candidate list + open-in-Puzzle), web-first:** **M** (~1 focused phase). No new index, no new algorithm, no new DB — it is wiring existing engine calls into a new two-pane surface. Risk is mostly UX (selecting a line, RTL layout, async result streaming) not capability.
- **Fuller workbench** (parallels seeding from selection, VS bucket, bidirectional mirror search, persisted candidate shortlist, desktop parity, in-pane side-by-side image compare, evidence/explanation chips): **L**.
- **Automated v7/v8-style ranked join finder** (the report's algorithm, refactored to `line_ends`/`line_starts` + offline precompute to beat the ~90s latency): **XL** and explicitly OUT of MVP scope — only ~33–47% Recall@50 and 40% of cases have no parallels, so it would be a "suggestions" assist, never the backbone.

### Key risks
- **No built index on this dev machine** (`Genizah_Index/` absent) — line/position search requires the index to carry `line_starts`/`line_ends`; older indexes raise the rebuild error (`genizah_core.py:8583`). Confirm prod index has these fields before building UI.
- **VS half-coverage** (50.62%) — must be framed in UI as "partial/supplementary", never "no visual match ⇒ no join".
- **False positives**: line-start/line-end matches on common words; parallels finds the *text* not the *partner* (esp. multi-copy liturgy). Needs honest "evidence, not proof" framing and disambiguation by the scholar.
- **Latency** if parallels/whole-corpus fan-out is run eagerly — keep it on-demand per user action, not auto-fired on every line.
- **Web vs desktop**: line-search + viewer + puzzle all exist on web; desktop has line-search + viewer but the Puzzle is web-only — MVP should be **web-first**.

### Top open questions for /gsd-discuss-phase
1. **Ranking & evidence display** — do we merge buckets into one ranked list or keep text-line / parallels / visual strictly separate (the implementation plan recommends separate buckets)? What evidence chips per candidate (matched line #, shared phrase, edge complementarity %, SVM score)?
2. **False-positive framing & disambiguation** — how to present "this is the right *text* but maybe not the right *fragment*" for multi-copy works; do we surface the physical-complementarity (`]`/`[` ≥15%) signal from the report as a filter?
3. **Web vs desktop first, and precompute vs on-demand** — confirm web-first; line/parallels search per user action (on-demand) vs any precomputed candidate pools (the plan's offline cache) — MVP almost certainly on-demand, no precompute.
4. **Pair hand-off semantics** — open a *transient* puzzle with both fragments (`?add=` ×2) vs create a persistent join document immediately; and how/whether to write back a confirmed join to the joins_panel / `fragment_joins`.

---

## Appendix — commands run (reproducibility)
- `git -C . log --oneline --all -- 'scripts/join_finder*.py'` → empty (never committed).
- `ls scripts/join_finder*` → no such file.
- VS coverage via `sqlite3` (`file:fist_data/visual_similarity.db?mode=ro`): distinct `alma_id_a` = 129,456; `vs_metadata.manuscript_count` = 129,456; `pair_count` = 13,647,365.
- Corpus size via `csv` over `libraries.csv` col 0: 255,723 distinct `system_number` (255,726 rows incl. header).
- Cross-check: all 129,456 VS ids are integer-parseable AlmaIds present in `libraries.csv` (0 outside corpus).
