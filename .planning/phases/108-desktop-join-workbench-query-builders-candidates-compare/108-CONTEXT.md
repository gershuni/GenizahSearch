# Phase 108: Desktop Join Workbench — Query Builders, Candidates & Compare - Context

**Gathered:** 2026-06-04
**Status:** Ready for planning

<domain>
## Phase Boundary

The **scholar-driven candidate hunt** in the desktop Join Workbench's right pane, built on the
Phase 107 anchor shell and the Phase 106 pure logic (`shared/joins_lab.py`). This phase is largely
**UI composition + a few interaction decisions** — the composition / cross-side membership / dedup /
merge / self-match logic already exists and is unit-tested.

**In scope (JWB-06 reframed, 07, 08, 10, 11 + text/combined surface of JWB-12):**
- A **line-by-line query builder** for the anchor side and an **identical builder for the OTHER side
  of the leaf** (adjacent image p±1), cross-side **AND/OR**, running the **existing** engine.
- Candidates returned **deduped one-per-image** in a **grid + table** with material, **highlighted
  snippet**, and **Y/?/N triage**; a refine/filter bar (text / material / dimensions / triage).
- A **self-match readout** (✓/✗ anchor satisfies the query) + an **"include anchor itself"** toggle.
- **Side-by-side anchor↔candidate compare** for eyeball confirmation, with the four actions.
- The **JWB-12 text/combined surface scaffolding** (source selector + provenance badges + both-first
  merge plumbing), with the visual-similarity source **stubbed/disabled** (wired in Phase 109).

**Descoped from 108 (changed this discussion):**
- **JWB-05 tear-side assist → DEFERRED.** 108 is the *manual* finder; the `[`/`]` side-reading
  earns its keep in a more *algorithmic* approach (Component B / JSA-03 Phase 110 / post-v8). This
  **drops roadmap SC#6** from Phase 108 and reassigns JWB-05. **ROADMAP.md + REQUIREMENTS.md mapping
  must be updated** to reflect this. (Bracket-aware MATCHING correctness — 106 R-02 — stays; only
  the side-verdict UI is deferred.)

**Explicitly OUT of 108:** visual-similarity *source population* + combined-view ordering +
VS-dialog soft-retire (Phase 109); JSA / parallels seeding (Phase 110); web Join Workbench UI
(later phase); any auto-ranked join finder (out of v8 entirely).

</domain>

<decisions>
## Implementation Decisions

### Query builders & cross-side (JWB-06 reframed, JWB-10, JWB-11)
- **D-01:** **Layout = stacked, other-side collapsed.** The anchor-side builder is always visible;
  the **other-side builder is a collapsible section below it, collapsed by default**. Single-side
  hunts (the common case) stay uncluttered; the scholar expands the other side to narrow a flood.
- **D-02:** **Builder starts BLANK** (no pre-seed of the anchor's own line text). The scholar hunts
  the **MISSING continuation** (JWB-06 reframe). An optional "copy selected anchor text → row"
  affordance MAY lift a partial torn word/phrase from the anchor transcription when useful (nicety,
  not auto-seed).
- **D-03:** **New dedicated builder widget, adapted from the existing `TabularQueryBuilderDialog`
  (`genizah_app.py:1543`)** — borrow its look + reuse its query-composition helpers, but keep it a
  **separate widget decoupled from the stable dialog** (lower risk; some duplication accepted). It
  serves both the anchor-side and other-side builders.
- **D-04:** **Adaptation specifics** (Hillel's sketch):
  - **Scope defaults to "Lines"** (not Within-Document / Word-Range).
  - **Orientation flipped vs today's dialog:** each **line is a horizontal row** of word-boxes
    (`[ ] or [ ] or [ ]`), and **lines stack vertically**; per-row **"Lines gap: N"** spinner;
    **+Add Line**. (Maps onto the tabular model: Component = line, Distance(lines) = gap,
    Word1/2/… = OR-alternatives.)
  - **"Start of text" / "End of text" options** (like the main-search selector) — first term pinned
    to start-of-text, last term to end-of-text. **This realizes 106 D-08** (page-START available on
    the first row only, page-END on the last row only).
  - Carry the **full existing modifier row** (Start/End of line `|_`/`_|`, Negation, Defective,
    Wildcards, Prefixes/Suffixes, Bidirectional, Flex Spacing, Judeo-Arabic, Variants) + Search
    Options + a **(read-only) Preview** field.
  - **⚠ REVISED 2026-06-05 (Codex round-2 HIGH, user decision = WIRE not trim):** the per-word
    modifiers (**Negation, Defective/Plene, Wildcard start/end, Prefixes, Suffixes**) must actually
    affect the composed query — they are **per-word, applied to the ACTIVE (focused) OR-box**, exactly
    mirroring `TabularQueryBuilderDialog`'s `_active_word` mechanism (`genizah_app.py:1559`,
    `:1731-1789` per-word `{'text','mods'}` + focus event filter + `mod_ind` indicator below each box).
    Each OR-box stores its own `mods` dict; focusing a box reflects/edits that box's mods via the
    modifier row; `build_side_query()` **decorates each box's token with its mods BEFORE OR-joining**,
    using the EXACT order in `genizah_core.py:6014-6027`: plene `%`→prefix `#`→suffix `…#`→wildcard
    `*…`/`…*`, and negation `-…` (prepend). A decorated box yields e.g. `#שלום`, `שלום*`, `-עץ`; an
    OR row of decorated boxes yields `(#שלום/%שלומות)`. The two GLOBAL Search-Options toggles
    (**Variants, Judeo-Arabic, Flex Spacing, Bidirectional**) stay in `_responsa_opts()` as today
    (they ARE global). Transplant the existing dialog's `_on_modifier_changed` / `_on_word_text_changed`
    / `_update_preview` / per-word decoration logic rather than re-inventing it. The headless builder
    test must assert a decorated box round-trips: e.g. a box `שלום` with `prefix` mod composes a term
    parseable by `parse_responsa_query` as a grammatical-prefix component (`#שלום`).
- **D-05:** **Per-row semantics:** each row = one manuscript line; the `[ ] or [ ] or [ ]` boxes are
  **OR-alternatives** for that line (spelling variants / synonyms). **⚠ REVISED 2026-06-05 (Codex
  review HIGH #1, code-verified):** OR composes to the engine's **real slash-group syntax
  `(w1/w2/w3)`**, NOT a `|` group. A bare `w1|w2` is parsed as a SINGLE word and regex-escaped
  (`genizah_core.py:5727` documents `(עץ/אילן)->['עץ','אילן']`; `:6219`/`:7593` confirm the pipe
  fall-through). **OR boxes are SINGLE-TOKEN** (user decision 2026-06-05): each box holds one word;
  `build_side_query()` joins a row's non-empty single-token boxes into `BuilderRow.term =
  "(" + "/".join(tokens) + ")"` (a single-box row stays a **bare term** — additive, no regression;
  whitespace inside a lone box stays as proximity). Phrase-level OR (multi-word boxes) is **NOT**
  offered in v8 (see Deferred Ideas). "Lines gap: N" = the blank-line gap **down** to the next row.
  (**R-01 RESOLVED**: the `|`-join hypothesis was wrong; the slash-group is the correct round-trip
  through `_parse_line_break_query`/`parse_responsa_query`. The headless test must assert the
  composed query parses as an OR group via the **parser/engine**, NOT merely that `compose()`
  contains a separator — a `compose()`-only `|` test would lock in the bug.)
- **D-06:** **RTL content / LTR chrome.** In the EN UI the **query-content area** (rows + word
  boxes) is **RTL** (first word on the right — Hebrew line start, the 107/106 start-on-right
  invariant); the **surrounding chrome** (modifier checkboxes, scope selector, gap labels, buttons)
  is **LTR**. In the HE UI everything is RTL. **Also fix the existing `TabularQueryBuilderDialog`'s
  mirrored/clipped chrome** (separate fix, both surfaces). See **R-04** (size / split risk).
- **D-07:** **Cross-side combine defaults to AND** (you add the other side to *narrow* a flood), a
  simple toggle flips to **OR** (widen a poor yield). "Other side" = adjacent image **p±1** within
  the same sys_id (first→+1, last→−1, middle→both), membership via `(sys_id, page±1)` set logic
  (locked 106 D-13). Multi-leaf/bifolio adjacency deferred.
  **⚠ REVISED 2026-06-05 (Codex review MEDIUM #5, user decision):** the OTHER-side builder
  instance **omits the page-position (start/end-of-text) control** — Phase-106 `apply_cross_side()`
  (`shared/joins_lab.py:344`) runs query B WITHOUT `text_position`, so exposing the control there
  would be a silent no-op. Phase 106 stays **frozen** (no `apply_cross_side` signature change in
  108). The `JoinQueryBuilder` widget therefore takes a flag (e.g. `allow_page_position: bool`,
  default True) so the anchor-side instance keeps the control and the other-side instance hides it.

### Candidate surface (JWB-07, JWB-12 text/combined surface)
- **D-08:** **Grid is the default view, 20/page**, with a **table toggle**. Grid **cards show
  thumbnail + material + score + a HIGHLIGHTED matched-text snippet + Y/?/N triage**; table columns =
  Shelf | Lib | Material | Score | snippet. **Deduped one-per-image** (106 `dedup_candidates`).
- **D-09:** **Default ordering = by engine score, best first.**
- **D-10:** **Triage Y/?/N persists PER ANCHOR across re-runs and filter changes** (keyed by a stable
  per-candidate key — see **R-05**); **cleared on re-anchor**. **NOT** persisted to disk across app
  sessions (a confirmed join is persisted as a real join anyway).
- **D-11:** **Y = the collected-candidates list** (the keepers, JWB-07); **? = maybe; N =
  dismissed/hidden**. The four actions (Browse / Puzzle / Add-to-List / Add-as-Join) operate on the
  **current selection or any candidate**, not only Y's. One concept, less UI.
- **D-12:** **Refine/filter bar** filters by **text / material / has-dimensions / triage**.
- **D-13:** **Dimensions = evidence, never an automatic cull.** Manuscript dimensions show as
  evidence on each candidate, plus a **soft "size mismatch" hint** when anchor & candidate differ a
  lot — they **never auto-exclude**. **PLUS an opt-in explicit min/max size filter, OFF by default**
  (power-user cull, user-invoked). SC#7's "never an *automatic* hard filter" is preserved; the
  explicit cull is deliberate and off by default.
- **D-14:** **JWB-12 108↔109 seam — build the scaffolding now, VS stubbed.** 108 lays in the
  **source selector (Text / Visual / Combined)** + the **provenance-badge + both-first merge
  plumbing** (via `shared/joins_lab.merge_candidates`), but only the **Text** source is wired;
  **Visual/Combined are disabled / "arrives in 109."** Phase 109 plugs in the VS service. Cleaner
  seam, less 109 rework. (Text-only badge in 108 = ✎text.)

### Self-match readout (JWB-12 verification)
- **D-15:** The **self-match readout stays** (✓/✗ "the anchor satisfies the current query") with an
  **"include anchor itself"** toggle **defaulting to OFF (exclude the anchor)** — you hunt OTHER
  fragments, and the readout tells you whether your query is even self-consistent (fixes the
  "can't find the fragment itself" trap). **Placement = Claude's discretion** (lean: a small readout
  by the candidate-list header).

### Compare (JWB-08)
- **D-16:** **Compare opens in a separate two-pane `CompareDialog`** (spike-validated): anchor |
  candidate, **equal image+text panes**, prev/next. Navigate-many stays in the grid/table; deep
  compare is the focused modal. (The 107 pinned anchor is NOT reused as a compare pane — the modal
  carries its own anchor pane so the workbench stays usable behind it.)
- **D-17:** **All four actions + Y/?/N triage are reachable inside CompareDialog**, so a join can be
  confirmed without going back. **Add-as-Join here pre-fills anchor = fragment A, this candidate =
  fragment B** (then the existing `JoinsDialog` persist path from 107 D-14).
  **⚠ RESOLVED 2026-06-05 (Codex review HIGH #3, code-verified):** the public
  `open_anchor_as_join(anchor_sys_id, anchor_shelfmark)` (`genizah_app.py:15443`) leaves B EMPTY;
  the only A+B prefill path today is the **private** `_vs_open_joins_with_partner()`
  (`genizah_app.py:5242`, sets `dialog.frag_b_input.setText(partner_shelfmark)`), which **D-20
  forbids** the workbench from calling. **Fix (locked):** EXTEND the public method to
  `open_anchor_as_join(anchor_sys_id, anchor_shelfmark, partner_sys_id=None, partner_shelfmark=None)`
  — when `partner_shelfmark` is given it sets `dialog.frag_b_input.setText(partner_shelfmark)` before
  `dialog.exec()` (lifting the one extra line from `_vs_open_joins_with_partner` into the public
  method). This keeps D-20 intact (workbench calls the PUBLIC method only) and satisfies D-17.
  **`genizah_app.py` must therefore appear in `files_modified` of the plan that wires Add-as-Join.**
- **D-18:** **Compare nav:** prev/next steps through the **(filtered) candidate list**; each
  candidate **opens to the page that matched** (incl. the **cross-side p±1 page** when matched via
  the other-side builder — see **R-06**), with folio nav on the candidate side; the **anchor pane
  stays put**.

### Architecture / build constraints (carried forward — locked)
- **D-19:** **i18n from line one** — every new string `tr()`-wrapped; the builder + candidate surface
  + CompareDialog render fully under `lang=he` with no hardcoded English.
- **D-20:** **No `_vs_*` private calls on the workbench path** — actions go through the public,
  named methods established in 107 D-12.
- **D-21:** **Candidate enrichment is BATCHED, not per-candidate serial** (material / dimensions /
  thumbnail / snippet / cross-side membership). See **R-03** (Codex perf risk; same risk as 109's
  ~80-candidate load, applies here for large text result sets too).
- **D-22:** **Desktop-first**, consuming the shared core `shared/joins_lab.py` via a desktop
  `SearchExecutor` adapter backed by `self.searcher` + `self.meta_mgr`. Web Join Workbench is a later
  phase on the same core.

### Claude's Discretion
- Self-match readout exact placement (D-15) — lean: candidate-list header.
- Builder collapse/expand affordance styling; grid card layout details; table column widths/resize.
- Whether the optional "copy selected anchor text → row" affordance (D-02) ships in 108 or defers.
- CompareDialog sizing / pane split ratio; thumbnail sizes; snippet `max_lines`/`max_chars`.
- Internal helper decomposition; how much of the frozen sketch's `QueryBuilder` / `CompareDialog` /
  `_CrossSideWorker` / `_on_results` / `_maybe_assemble` code transplants.

### Research flags (for gsd-phase-researcher / gsd-planner)
- **R-01:** Does `shared/joins_lab.compose()` / `BuilderRow` support **OR-alternatives within a line
  row** (`[ ]or[ ]` → `|` group) and the **page-anchored line-break case** (106 R-01: one engine
  call via `text_position` vs separate `_execute_line_break_search` + `(sys_id, page)`
  intersection)? Confirm the round-trip against `genizah_core._parse_line_break_query` (`:5811`) and
  whether an **additive `BuilderRow` field** is needed or the UI joins boxes into `term` with `|`.
- **R-02:** Identify the **reusable composition/modifier logic inside `TabularQueryBuilderDialog`
  (`genizah_app.py:1543`)** that can be borrowed into the new decoupled builder widget vs the
  dialog-bound code that must be re-implemented. Map each modifier toggle to its
  responsa-options/query effect.
- **R-03:** **Batch** all per-candidate enrichment (material/dimensions via `shared/fjms_service.py`,
  thumbnails via `meta_mgr.get_thumbnail`, browse text via `get_browse_page`, cross-side membership)
  — verify on a large text result set; reuse existing batch paths, no per-row serial fetch.
- **R-04:** Size the **existing `TabularQueryBuilderDialog` RTL-chrome fix** (D-06). If it balloons
  beyond a small layout fix, **split to `/gsd-quick`** rather than bloating this phase.
- **R-05:** Triage persistence (D-10) needs a **stable per-candidate key surviving re-runs** — `uid`
  may change per search; prefer `shared/joins_lab.normalize_candidate`'s canonical key (sys_id+page).
- **R-06:** Compare "open the matched page" (D-18) — when a candidate matched via the **other-side
  builder (query B on p±1)**, determine which page (the A-side or the B-side) the CompareDialog
  should open to, and whether to label which side matched.

### Cross-AI Review Resolutions (2026-06-05 — Codex code-verified, fold into the replan)
These are **binding directives for the replan** (`/gsd-review --codex` → `108-REVIEWS.md`). All were
verified against the live source by file:line. The two that change locked decisions (HIGH #1 → D-05,
HIGH #3 → D-17, MEDIUM #5 → D-07) are already revised inline above; the rest are implementation fixes
the planner MUST encode in the plans:

- **RR-1 (HIGH #1 — OR syntax):** see revised **D-05**. Serialize OR boxes as `(w1/w2/w3)`
  slash-groups, single-token boxes; single-box row = bare term. **Replace the `compose()`-only `|`
  test** (Plan 01 Task 3 + Plan 02 Task 1 currently assert `term == "פירוש|פירש"`) with a
  **parser/engine-level OR regression**: assert `(w1/w2)` composes and `parse_responsa_query()` /
  `_parse_line_break_query()` yields an OR group with `words=[w1, w2]` (mirror `genizah_core.py:5727`).
  A `|`-asserting test would lock in the bug.
- **RR-2 (HIGH #2 — Candidate vs dict):** `dedup_candidates()`/`apply_cross_side()` return **`Candidate`
  dataclasses**; `merge_candidates()` returns a **plain list** (NOT a `MergeResult` — Plan 03
  `_maybe_assemble` uses `result.candidates`, which fails). **Decision: use `Candidate` as the UI
  model throughout Plans 03/04** — read `candidate.sys_id` / `.page` / `.full_text` /
  `.highlight_pattern` / `.shelfmark` / `.score`, NOT `r_sid(c)` / `r_text(c)` / `c.get(...)`. Add a
  thin **`candidate_to_result_dict(c) -> dict`** adapter (in `desktop/join_workbench.py` or
  `shared/joins_lab.py`) ONLY where a Phase-107 host method requires a raw result dict
  (`open_result_in_browse_from_table(res)`, `show_add_to_list_menu([{...}])`, `_enqueue_image_for_pane`).
  Fix `_maybe_assemble` to use the returned list directly (`self.results = list(merge_candidates(...))`).
  **Key per-page enrichment/snippets by `(sys_id, page)` (or list index), NOT `sys_id`** — the same
  fragment on two pages must not overwrite each other's snippet/highlight. (Measurement *lookup* may
  stay `sys_id`-keyed; triage stays `sys_id`-keyed per R-05.)
- **RR-3 (HIGH #3 — Add-as-Join public path):** see revised **D-17**. Extend public
  `open_anchor_as_join(..., partner_sys_id=None, partner_shelfmark=None)` in `genizah_app.py`; add
  `genizah_app.py` to the wiring plan's `files_modified`. No `_vs_*` call from the workbench (D-20).
- **RR-4 (HIGH #4 — i18n guard):** `tests/test_join_workbench_i18n.py::test_all_tr_keys_in_translations`
  fails unless every new `tr()` key in `desktop/join_workbench.py` exists in
  `genizah_translations.TRANSLATIONS`. **Add `genizah_translations.py` to `files_modified` in EVERY
  plan that introduces new `tr()` keys (Plans 02, 03, 04)** and add the keys (EN+HE) via
  `TRANSLATIONS.update({...})`.
- **RR-5 (MEDIUM #5 — other-side page-position):** see revised **D-07**. Drop the page-position
  control on the other-side builder; Phase-106 `apply_cross_side()` stays frozen.
- **RR-6 (MEDIUM #6 — batch measurements):** a batch API **already exists**:
  `FjmsService.get_measurement_summaries_batch()` (`shared/fjms_service.py:3005`), returning
  `{AlmaId: {width_cm, height_cm, avg_num_lines, avg_text_density, avg_line_height_mm, material}}` via
  `COALESCE(catalog_width_cm, max_computed_width_cm)` (so it catches computed-only measurements that
  raw catalog columns miss). **Decision: REUSE/EXTEND it rather than add a parallel
  `get_measurements_batch`.** It lacks only `size_category` — add `size_category` to its SELECT +
  returned dict (additive, keep the existing keys/tests green), and have `_EnrichWorker` consume the
  existing key names (`width_cm`/`height_cm`/`material`/`avg_num_lines`/`size_category`). Update Plan
  01 accordingly (no `manuscript_measurements WHERE AlmaId IN` net-new method premise). Adjust the
  Plan 01 pytest selector from the broad `-k measurements_batch` to a precise node id so unrelated
  tests aren't deselected (LOW #9).
- **RR-7 (MEDIUM #7 — page-specific images):** matched-page / compare images must use **per-page**
  resolution (`desktop/join_workbench.py::_image_url_for_idx` `:189`, as the Phase-107 anchor loader
  does at `:365`) + enriched image lists — NOT manuscript-level `meta_mgr.get_thumbnail()` (which is
  one image per manuscript). Grid *thumbnails* may stay manuscript-level, but the CompareDialog panes
  and any "open to the matched page" image MUST resolve the specific `page_of(res)` image.
- **RR-8 (LOW #8 — imports):** Plan 02 (or the first plan to touch them) must add the missing
  `desktop/join_workbench.py` widget imports: `QFrame`, `QSpinBox`, `QGridLayout`, `QTableWidget`,
  `QTableWidgetItem` (PyQt6.QtWidgets) and `from gui_threads import SearchThread`. The current import
  block (`:315`) lacks them. **⚠ See RR-10 — split the imports across waves to avoid an F401 lint
  failure.**

### Round-2 Review Resolutions (2026-06-05 — Codex code-verified re-review, fold into the replan)
Round 1's RR-1..RR-8 all landed (Codex CONFIRMED). The round-2 re-review (`108-REVIEWS.md`) found 4
NEW code-verified issues. Bindings:

- **RR-9 (HIGH — modifier-row no-op → WIRE per-word).** See revised **D-04**. User decision: WIRE
  (not trim) the per-word modifiers. Plan 02 must give each OR-box a `mods` dict, edit the active
  (focused) box's mods via the modifier row (transplant the existing `_active_word` /
  `_on_modifier_changed` / `mod_ind` mechanism from `genizah_app.py:1559`/`:1731-1789`), and have
  `build_side_query()` decorate each box's token (`%`/`#`/`#`-append/`*`-pre/`*`-append, `-`-prepend
  per `genizah_core.py:6014-6027`) BEFORE the `/`-OR-join. Search-Options globals
  (Variants/JA/Flex/Bidir) stay in `_responsa_opts()`. Plan 01's builder test (and/or Plan 02's) must
  add a parser-level assertion that a decorated box round-trips (e.g. `prefix` mod on `שלום` →
  `parse_responsa_query("#שלום")` yields a grammatical-prefix component). Update Plan 02 Task 1
  `<behavior>`/`<action>`/`acceptance_criteria`/`must_haves`, and the Preview must reflect decoration.
- **RR-10 (MEDIUM/blocker — Plan 02 Task 0 ruff F401).** `ruff.toml:14-20` selects `F401` (unused
  imports). Plan 02 Task 0 currently front-loads Plan-03-only imports AND runs `ruff check
  desktop/join_workbench.py` in the SAME task → guaranteed lint failure. **Fix:** keep only the imports
  Plan 02 actually USES in Plan 02 (`QFrame`, `QSpinBox`); MOVE `QGridLayout`, `QTableWidget`,
  `QTableWidgetItem`, and `from gui_threads import SearchThread` into **Plan 03** (the wave that first
  uses them — the grid/table render + SearchThread launch). Do NOT use blanket `# noqa: F401` to mask
  it. Update Plan 02 + Plan 03 import tasks + their acceptance greps accordingly.
- **RR-11 (MEDIUM/blocker — `size_category` missing-column guard).** Plan 01 must NOT add
  `size_category` as an unconditional SELECT column — an old sidecar lacking the column would make the
  whole SELECT fail and return an empty batch (`shared/fjms_service.py:3035-3060`). **Mirror the
  existing `avg_line_height_mm` guard (`:3017-3029`):** `has_size_category = "size_category" in cols`,
  build a conditional `sc_col = ", size_category" if has_size_category else ""`, and return
  `size_category` as `None` when absent. Resolve Plan 01's internal contradiction (plain column vs
  degrade-to-None) in favor of the guarded form. Add a test asserting the method still works (returns
  `size_category: None`) against a table WITHOUT the column.
- **RR-12 (LOW — per-page image `page is None` guard).** `Candidate.page` is `Optional[int]`
  (`shared/joins_lab.py:104`; VS-only / None-page rows exist, `tests/test_joins_lab.py:121-124`).
  `_enqueue_image_for_pane` (and any `_image_url_for_idx(images, page-1, …)` call) must NOT do
  arithmetic on a `None` page — guard `if page is None: treat as page 1` (or render the "no image"
  placeholder) before `page-1`. Update Plan 03 (`_enqueue_image_for_pane`) and Plan 04
  (`CompareDialog._fill_*` page handling).

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Executable spec (Spike 002 — frozen at git tag `spike-002-joins-workbench`)
- `.planning/spikes/002-assisted-join-workbench/sketch/join_workbench.py.txt` — UAT-validated
  throwaway sketch = **executable spec for 108's surfaces**: `QueryBuilder.compose` (~L560), the
  symmetric/other-side builder + `_CrossSideWorker.run` AND/OR membership (~L387), `_on_results`
  dedup (~L1102), `_maybe_assemble` merge ordering (~L1149), `_anchor_matched` self-match (~L1100),
  the grid/table toggle (`view_btn`/`toggle_view`/`render_table`), the Y/?/N triage cards, and the
  two-pane **`CompareDialog`** (~L111-113). Extract the BEHAVIOR; do NOT copy the PyQt scaffolding.
- `.planning/spikes/002-assisted-join-workbench/DESKTOP-INTEGRATION-NOTES.md` — verified reuse-map:
  the triage funnel (anchor | builder + thumbnail grid 20/page + Y/?/N | CompareDialog), the
  Grid⇄Table toggle + columns, "other side = p±1" cross-side worker, the self-match fix (Hillel got
  no self-match on his own line-beginnings — bracket-token + line_start cause), the four-action
  wiring, dimensions/material dropdown filter, resizable table rows.
- `.planning/spikes/002-assisted-join-workbench/CODEX-PRODUCTIONIZE-CRITIQUE.md` — `SearchExecutor`
  boundary, **batch-everything perf**, public action APIs, canonical dedup key, i18n as acceptance.
- `.planning/spikes/002-assisted-join-workbench/SPIKE-FINDINGS.md` — feasibility + primitive
  inventory (VS ~50% coverage; auto-finder does NOT exist).

### Requirements & roadmap
- `.planning/REQUIREMENTS.md` § "Component A — Join Workbench (JWB)" (JWB-06/07/08), § "New
  requirements" (JWB-10/11/12), § "Amendments" (JWB-05 amended **[now deferred from 108]**, JWB-06
  reframed), § "Deferrals / discuss-phase questions" (#2 dimensions-as-evidence, #4 VS-dialog timing,
  #7 other-side adjacency).
  **⚠ Update needed:** the JWB-05→108 mapping and Phase 108 SC#6 (tear-side assist) must be amended
  to reflect the 108 deferral decided this discussion.
- `.planning/ROADMAP.md` § "Phase 108" — goal + success criteria (SC#6 tear-side assist now
  descoped; SC#7 dimensions confirmed as evidence + opt-in cull).

### Prior phase context
- `.planning/phases/106-…/106-CONTEXT.md` — the `shared/joins_lab.py` domain model + `SearchExecutor`
  + builder model (D-07 BuilderRow, D-08 page anchors), cross-side (D-13), dedup/merge/self-match.
- `.planning/phases/107-…/107-CONTEXT.md` — the workbench shell this phase extends: anchor pane,
  known-joins group, public actions, `JoinsDialog` Add-as-Join path (107 D-14), re-anchor (107 D-15).

### Code to extend / reuse (read before planning)
- `desktop/join_workbench.py` — the Phase 107 shell (anchor pane + known-joins + actions) the right
  pane attaches to.
- `shared/joins_lab.py` — `compose`, `cross_side_membership` / `apply_cross_side`,
  `dedup_candidates`, `merge_candidates`, `detect_self_match`, `snippet_html`/`snippet_plain`/
  `htmlify`, `normalize_candidate`/`page_of`, `Candidate`/`SideQuery`/`BuilderRow`/`MergeResult`,
  `SearchExecutor` Protocol.
- `genizah_app.py:1543` `TabularQueryBuilderDialog` — builder visual + compose reference (D-03/D-04)
  AND the RTL-chrome fix target (D-06).
- `genizah_core.py:5811` `_parse_line_break_query`, `:8001` `_execute_line_break_search`, `:8298`
  `execute_search(..., text_position=, restrict_sys_ids=, corpus_scope=)`, `:9483`
  `get_browse_page(sys_id, p_num=…)` — the engine paths the builder + cross-side + compare use.
- `shared/fjms_service.py` — material / `manuscript_measurements` for candidate dimension evidence
  (D-13), batched (R-03).
- `desktop/image_loader.py::ImageLoaderThread`, `desktop/widgets/line_number_text_edit.py::
  apply_line_numbered_text` — CompareDialog candidate pane (image + numbered text).

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **The frozen sketch's `QueryBuilder`, `CompareDialog`, `_CrossSideWorker`, `_on_results` dedup, and
  `_maybe_assemble` merge** transplant nearly verbatim once the QThread/PyQt scaffolding is stripped —
  the inner logic already lives (pure) in `shared/joins_lab.py` (Phase 106).
- **`TabularQueryBuilderDialog` (`genizah_app.py:1543`)** is the visual + compose template for the
  new builder widget (D-03): scope radios, component/word boxes, the modifier checkbox row, the
  Preview field. Borrow look + compose; keep decoupled.
- **`shared/joins_lab.py`** (Phase 106) provides every pure logic unit 108 needs — 108 is the desktop
  `SearchExecutor` adapter + the Qt UI on top.
- **Phase 107 public actions** (`open_result_in_browse_from_table`, `show_add_to_list_menu`, public
  Puzzle + Add-as-Join wrappers) are reused for the candidate/compare action rows (D-17, D-20).

### Established Patterns
- Result dict shape: `display{id(sys_id), shelfmark, title, library_code, img(page), source}`,
  `full_text`, `snippet`, `uid`, `highlight_pattern`, `score`, `scope`.
- Two-phase search (Tantivy → regex); line-break path maps first/last line-group to
  `line_starts`/`line_ends`; `text_position` maps start/end → `content_head`/`content_tail`.
- `(sys_id, page±1)` adjacency; `AlmaId == sys_id`; provenance badges ★both / ⊙VS / ✎text / ⚓self.
- Workers guard QLabel/QWidget writes with `try/except RuntimeError` (deleted-widget safety).

### Integration Points
- Desktop injects a `SearchExecutor` backed by `self.searcher` + `self.meta_mgr` into the workbench
  (106 adapter seam) for the builder runs + cross-side membership + candidate enrichment.
- The right-pane builder + candidate surface + CompareDialog attach to the existing
  `desktop/join_workbench.py` shell; the anchor pane (107) is the left/pinned pane.

</code_context>

<specifics>
## Specific Ideas

- **Builder = adapted Responsa Tabular Search** (Hillel's sketch): Lines scope default; line = a
  horizontal row of `[ ] or [ ] or [ ]` OR-boxes; lines stack vertically; per-row "Lines gap: N";
  +Add Line; start-of-text/end-of-text options; modifier row + Search Options + Preview.
- **RTL/LTR:** content area RTL (first word on right), chrome LTR in EN — and fix the existing
  Tabular Search dialog's mirrored/clipped chrome at the same time.
- **Manual finder framing:** 108 is the *manual* scholar-driven hunt. The tear-side `[`/`]` assist
  belongs to a *later algorithmic* approach — deferred deliberately, not forgotten.
- **Self-match:** default EXCLUDE the anchor; the ✓/✗ readout exists to confirm query
  self-consistency (the "couldn't find my own fragment" trap from the spike).
- **Compare = separate two-pane CompareDialog** (navigate-many in the grid/table; deep compare in the
  modal) with all four actions + triage inside.

</specifics>

<deferred>
## Deferred Ideas

- **Tear-side assist (JWB-05 amended)** — DEFERRED out of 108 to a more *algorithmic* approach
  (Component B / JSA-03 Phase 110 / post-v8). Drops Phase 108 SC#6; reassigns JWB-05.
  **ROADMAP.md + REQUIREMENTS.md mapping should be updated** to match.
- **Visual-similarity source population + combined-view ordering + VS-dialog soft-retire** — Phase
  109 (108 builds the disabled selector + merge plumbing only, D-14).
- **JSA / parallels seeding** — Phase 110.
- **"Open in main search" escape hatch** from the builder — not chosen (the read-only Preview + full
  modifier row suffice); revisit if power users want fully-manual regex from the builder.
- **Editable raw composed-query preview** (string↔rows round-trip) — Preview stays read-only
  (106 D-10).
- **Alternative "plain search-bar mode" for the builder** (a single search field with the usual
  exact / variants / fuzzy / responsa / regex mode options, as a toggle alongside the line-by-line
  builder) — raised by Hillel during the 2026-06-05 review touch-up ("we may also add … but not
  necessarily now"). **Deferred** — 108 ships the line-builder with single-token `(w1/w2)` OR
  (revised D-05); revisit a free-form search-bar mode in a later iteration if useful.
- **Phrase-level / multi-word OR alternatives** (OR-ing whole phrases, not single tokens) — the
  engine's `(…/…)` is word-level only; would need a new `BuilderRow` representation or N unioned
  searches. Deferred out of v8 (decided 2026-06-05, revised D-05).
- **Triage persisted to disk across app sessions** — rejected for v8 (D-10); a confirmed join is
  persisted as a real join.
- **Per-row per-term variants columns** — global variants modifier suffices (106 deferred).
- **Multi-leaf / bifolio "other side"** adjacency beyond `p±1` — deferral #7.
- **Web Join Workbench UI** — later phase on the same shared core.

### Reviewed Todos (not folded)
- `todo.match-phase 108` surfaced 6 keyword-coincidence hits (Reading-Desk UX, desktop corrections
  migration, server-side email search, unified metadata text search, scholarly citations, FIST.db
  fill). All matched on the generic "desktop"/"search"/"results" area; none touch the query-builder /
  candidate / compare surface. Not folded.

</deferred>

---

*Phase: 108-desktop-join-workbench-query-builders-candidates-compare*
*Context gathered: 2026-06-04*
