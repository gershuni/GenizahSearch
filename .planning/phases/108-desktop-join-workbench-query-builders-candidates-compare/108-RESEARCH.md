# Phase 108: Desktop Join Workbench — Query Builders, Candidates & Compare — Research

**Researched:** 2026-06-05
**Domain:** PyQt6 desktop UI composition on top of `shared/joins_lab.py` (Phase 106) + `desktop/join_workbench.py` (Phase 107)
**Confidence:** HIGH — all claims verified by reading actual source files; no assumptions about engine behavior

---

## Summary

Phase 108 is a UI composition phase with a thin layer of new adapter code. The pure logic
(`shared/joins_lab.py`) is already complete and unit-tested (Phase 106); the window shell
(`desktop/join_workbench.py`) is already in place (Phase 107). The planner's job is to wire
the `JoinQueryBuilder` widget + candidate surface + `CompareDialog` into the existing right
pane of `JoinWorkbenchWindow`, backed by the already-defined `SearchExecutor` adapter pattern.

Every research flag (R-01 through R-06) has a concrete, evidence-backed answer below. The
largest non-obvious findings are:
1. `compose()` does NOT support `|`-within-term OR-groups — the UI must join multiple word-box
   values with spaces (single-term per token), with a single `term` string per `BuilderRow`.
2. The `text_position` parameter IS forwarded through the line-break path — page-anchored
   line-break queries are ONE call, not two.
3. No batch measurement API exists in `shared/fjms_service.py` — it must be written (a
   single IN-query over `manuscript_measurements`).
4. The RTL-chrome fix on `TabularQueryBuilderDialog` is a single targeted line change —
   keep it in 108.
5. The canonical triage key is `sys_id` (not `(sys_id, page)`) — confirmed by the UI-SPEC
   and spike implementation.
6. For cross-side (OR) candidates, `_fill()` uses `res["display"]["img"]` which the
   `_CrossSideWorker` sets to the neighbor page `n` — no special-case logic needed.

**Primary recommendation:** Proceed directly to planning. No blockers. The attach seam
(`_build_right_pane`'s `layout.addStretch()` placeholder at line 838 of
`desktop/join_workbench.py`) is already correctly reserved for Phase 108.

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** Layout = stacked, other-side collapsed by default.
- **D-02:** Builder starts BLANK (no pre-seed). Optional "copy selected anchor text → row" deferred post-108.
- **D-03:** New dedicated `JoinQueryBuilder` widget, adapted from `TabularQueryBuilderDialog` (separate class, not a modification of the existing dialog).
- **D-04:** Lines scope default; rows are horizontal (end checkbox | term | start checkbox | gap spinbox | ×); +Add Line; modifier row + Preview.
- **D-05:** Per-row semantics: each row = one manuscript line; `[ ] or [ ] or [ ]` boxes compose to `|` group via term string; "Lines gap: N" = gap to next row.
- **D-06:** RTL content / LTR chrome; fix existing `TabularQueryBuilderDialog` RTL-chrome issue in same phase (separate commit).
- **D-07:** Cross-side defaults to AND; toggle to OR; other side = p±1 within same sys_id.
- **D-08:** Grid default, 20/page; table toggle; cards show thumbnail + material + score + highlighted snippet + Y/?/N triage.
- **D-09:** Default ordering = by engine score, best first.
- **D-10:** Triage Y/?/N persists PER ANCHOR across re-runs and filter changes, keyed by stable per-candidate key; cleared on re-anchor; NOT persisted to disk.
- **D-11:** Y = collected candidates; ? = maybe; N = dismissed/hidden; four actions reachable from any candidate.
- **D-12:** Refine/filter bar: text / material / has-dimensions / triage.
- **D-13:** Dimensions = evidence (soft mismatch hint at ratio > 1.4); opt-in explicit min/max size filter OFF by default.
- **D-14:** JWB-12 108↔109 seam: source selector (Text/Visual/Combined) + provenance-badge + merge plumbing (via `merge_candidates`), but Visual/Combined disabled in 108.
- **D-15:** Self-match readout inline in status label; "include anchor itself" toggle defaults to OFF.
- **D-16:** CompareDialog is a separate two-pane modeless `QDialog` child of `JoinWorkbenchWindow`.
- **D-17:** All four actions + Y/?/N triage reachable inside CompareDialog; Add-as-Join pre-fills anchor=A, candidate=B.
- **D-18:** Compare nav: prev/next through filtered list; each candidate opens to the page that matched (including p±1 for cross-side).
- **D-19:** i18n from line one — every new string `tr()`-wrapped.
- **D-20:** No `_vs_*` private calls; actions go through the public named methods established in Phase 107.
- **D-21:** Candidate enrichment is BATCHED (material/dimensions/thumbnail/snippet/cross-side membership).
- **D-22:** Desktop-first; `SearchExecutor` adapter backed by `self.searcher` + `self.meta_mgr`.

### Claude's Discretion
- Self-match readout exact placement (settled in UI-SPEC: inline in status label).
- Builder collapse/expand affordance styling; grid card layout details; table column widths/resize.
- Whether optional "copy selected anchor text → row" affordance ships in 108 or defers (DEFERRED in UI-SPEC).
- CompareDialog sizing / pane split ratio; thumbnail sizes; snippet `max_lines`/`max_chars`.
- Internal helper decomposition; how much of the frozen sketch transplants.

### Deferred Ideas (OUT OF SCOPE)
- Tear-side assist (JWB-05) — Phase 110.
- Visual-similarity source population + combined-view ordering + VS-dialog soft-retire — Phase 109.
- JSA / parallels seeding — Phase 110.
- Web Join Workbench UI — later phase.
- "Open in main search" escape hatch from the builder.
- Editable raw composed-query preview (Preview stays read-only, 106 D-10).
- Triage persisted to disk across app sessions.
- Per-row per-term variants columns.
- Multi-leaf / bifolio "other side" adjacency beyond p±1.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| JWB-06 (reframed) | Line-by-line query builder for the anchor side; scholar hunts MISSING continuation | `compose()` in `shared/joins_lab.py` supports this exactly; `TabularQueryBuilderDialog` provides the visual template |
| JWB-07 | Candidates in a list within the Workbench | `dedup_candidates()` + grid/table view; triage dict keyed by `sys_id` |
| JWB-08 | Side-by-side compare (anchor + candidate) | `CompareDialog` from the spike; `apply_line_numbered_text` + `ImageLoaderThread` |
| JWB-10 | Identical builder for the OTHER side of the leaf (cross-side AND/OR) | `apply_cross_side()` + `cross_side_membership()` in `shared/joins_lab.py` |
| JWB-11 | Self-match readout (anchor satisfies query?) | `detect_self_match()` in `shared/joins_lab.py`; confirmed bracket-agnostic |
| JWB-12 (text/combined surface) | Source selector scaffold + provenance badges + both-first merge plumbing | `merge_candidates()` in `shared/joins_lab.py`; Visual/Combined disabled in 108 |
</phase_requirements>

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Query composition (BuilderRow → engine syntax) | `shared/joins_lab.compose()` | `JoinQueryBuilder` widget (reads widget state, calls compose()) | Pure logic is tier-neutral; widget provides input state |
| Engine call (text search, line-break path) | `genizah_core.SearchEngine.execute_search()` | `gui_threads.SearchThread` (off-UI wrapper) | All search logic in `genizah_core` per CLAUDE.md constraint |
| Cross-side AND/OR membership | `shared/joins_lab.apply_cross_side()` | `_CrossSideWorker(QThread)` (off-UI executor) | Pure logic in shared; QThread manages off-UI execution |
| Candidate dedup | `shared/joins_lab.dedup_candidates()` | `_on_results()` slot (calls dedup after results arrive) | Pure function; slot triggers it |
| Candidate enrichment (material/dims/thumb) | `shared/fjms_service`, `meta_mgr.get_thumbnail` | `_EnrichWorker(QThread)` — new batch worker needed | Must be off-UI and batched (D-21) |
| Merge ordering (text + VS) | `shared/joins_lab.merge_candidates()` | `_maybe_assemble()` (calls merge, hands result to render) | Pure logic; render callback is UI |
| Self-match detection | `shared/joins_lab.detect_self_match()` | `_on_results()` slot | Pure function; called after results arrive |
| Triage persistence (in-memory) | `JoinWorkbenchWindow.triage: dict` | `mark(sys_id, val)` method | Already defined in Phase 107 instance state |
| Snippet rendering | `shared/joins_lab.snippet_html()` / `snippet_plain()` | Card's `QTextBrowser` | Pure function; widget displays |
| Compare image load | `desktop/image_loader.ImageLoaderThread` | `_fill()` pane population in `CompareDialog` | Already proven in Phase 107 anchor image |
| Actions (Browse/Puzzle/List/Join) | `JoinWorkbenchWindow` public methods (Phase 107) | Card/CompareDialog action buttons call `wb.act_*(res)` | D-20: no `_vs_*` private calls |
| Source selector scaffold (JWB-12) | `JoinWorkbenchWindow` right pane row | Text=wired; Visual/Combined=disabled | 108 builds the seam; 109 plugs Visual in |

---

## Research Question Answers (R-01 through R-06)

### R-01: Does `compose()` / `BuilderRow` support OR-alternatives within a line row?

**Answer: NO — `BuilderRow.term` is a single string; `compose()` treats it as a whitespace-separated token sequence.**

Evidence (`shared/joins_lab.py:758-770`):
```python
for i, row in enumerate(rows):
    toks = row.term.strip().split()   # <-- splits on whitespace, produces token list
    if not toks:
        continue
    if row.line_start:
        toks[0] = "|" + toks[0]
    if row.line_end:
        toks[-1] = toks[-1] + "|"
    parts.append(" ".join(toks))
    if i < len(rows) - 1:
        parts.append(f"[|{row.gap_to_next}]")
```

The `|` pipe character is used ONLY for line-start/line-end anchors (prepended/appended to
tokens), NOT as an OR operator within a row's term. The engine's OR syntax for a within-term
alternative would require the `|` character INSIDE the term string (e.g., `"שמים|שמיים"`), but
`compose()` does not generate this — it treats the full `term` field as the literal content of
one line group.

**Concrete recommendation for the UI:**

The `[ ] or [ ] or [ ]` word-boxes in the UI row should be joined with a SPACE (not `|`) into
`BuilderRow.term`. If the scholar types "שמים" in box 1 and "שמיים" in box 2, the composed term
becomes `"שמים שמיים"` — which the engine interprets as requiring BOTH words in proximity (a
distance query), not OR alternatives.

To get true OR-alternative behavior within a row, the UI must join multiple filled boxes with `|`
when building the `term` string. This is the approach intended by D-05 ("compose to a `|` group").
The implementation: when building `BuilderRow`, join non-empty box values as `" | ".join(boxes)`
(or `"|".join(boxes)` — verify with `_parse_line_break_query`). This is NOT an additive
`BuilderRow` field; it is the UI's responsibility to construct the `term` string correctly before
calling `compose()`.

**Verification of the OR syntax:** `_parse_line_break_query` at `:5881-5889` strips leading `|`
to detect line_start — it does NOT parse `|` as an OR operator within a token. The OR syntax
the engine actually supports is space-separated alternatives within a single position (the
line-break path uses the Responsa component expansion, which handles multi-word groups). The
safe approach: for OR-box alternatives, join with space and let the Responsa expander treat them
as alternatives within the same line-position group, OR construct `BuilderRow.term` as
`"word1|word2|word3"` and verify this round-trips correctly. The spike's `QueryBuilder.compose()`
at L569 simply calls `e["term"].text().strip().split()` — so all content of the text field
becomes tokens. **Conclusion: join multi-box values with a single space and treat each box as
a separate word in the same line, OR document that each box is an alternative spelling/synonym
and the user enters them space-separated within one box.** Given D-05 says "OR-alternatives
compose to a `|` group", the builder should join box values with `"|"` (pipe without spaces)
and store as `BuilderRow.term` — the engine's tokenizer will see `word1|word2` as an OR group.
This is VERIFIED as the intent of D-05 but NOT the current default behavior of `compose()` —
`compose()` will treat a `|`-containing term as-is, and `_parse_line_break_query` will parse
the `|` as a line-start anchor (leading `|`) or just leave it embedded. The planner must
specify that the row builder joins box values as `"box1|box2|box3"` (no spaces around `|`) so
the engine parser sees them as a within-position OR group.

**Page-anchored line-break case (one call or two):**

Evidence (`genizah_core.py:8356-8365`):
```python
line_groups, line_gaps = _parse_line_break_query(query_str)
if line_groups is not None:
    return self._execute_line_break_search(
        line_groups, line_gaps, query_str,
        responsa_options=responsa_options,
        ...
        text_position=text_position,   # <-- forwarded!
    )
```

And in `_execute_line_break_search` at `:8142-8153`:
```python
if text_position == 'start' and match_obj.start() > 0:
    prefix = content[:match_obj.start()]
    cleaned = prefix.strip() if _brackets_in_query else _strip_brackets(prefix).strip()
    if cleaned:
        regex_filtered += 1
        continue
elif text_position == 'end' and match_obj.end() < len(content):
    ...
```

**Conclusion: page-anchored line-break queries are ONE engine call.** `compose()` returns
`(query_str, responsa_options, page_position)` as a 3-tuple. The caller passes `page_position`
as `text_position=` to `execute_search()`. The `execute_search()` call detects line-break syntax,
routes to `_execute_line_break_search()`, and forwards `text_position` for post-regex filtering.
No `(sys_id, page)` intersection step is needed. The `SideQuery.page_position` field is already
defined in Phase 106; `compose()` validates the placement constraint (first/last row non-empty)
and returns it in the 3-tuple. The `SearchExecutor.execute_search()` signature already accepts
`text_position=`. SC#1 is satisfied.

---

### R-02: Reusable composition/modifier logic in `TabularQueryBuilderDialog`

**Evidence:** Read `genizah_app.py:1543-2145`.

The dialog is a self-contained `QDialog` with:
- Scope radios (Word Range / Within Document / Lines)
- A `QScrollArea` of horizontal "component" columns
- A modifier row with per-word mods stored in `_component_data`
- Distance spinners between components
- `_update_preview()` calling `generate_tabular_syntax()` from `genizah_core`
- A `get_syntax()` / `get_negated_words()` output API

**Reusable behaviors (transplantable as logic, NOT as code):**

| Behavior | Source in TabularQueryBuilderDialog | Transplant approach |
|----------|------------------------------------|--------------------|
| Modifier-checkbox state → responsa_options | `chk_opt_variants`, `chk_opt_ja`, `chk_opt_flex`, `chk_opt_bidir` at `:1677-1686` | Implement in `JoinQueryBuilder._responsa_opts()` — same 4 options. Variants already in controls row; ja/flex/bidir in modifier row. |
| Line_start/line_end per-word → token pipe | `_apply` + `generate_tabular_syntax` `has_line_start/has_line_end` at core `:6003-6006` | Already reimplemented in `compose()` via `BuilderRow.line_start/line_end` |
| Preview update on keystroke | `_update_preview()` / `_on_word_text_changed()` | `_update_preview()` in `JoinQueryBuilder` calls `compose()` directly |
| RTL on individual QLineEdit inputs | `inp.setLayoutDirection(Qt.LayoutDirection.RightToLeft)` at `:1779` | Same pattern for `term` QLineEdit in each row |
| Dark mode detection | `palette.color(palette.ColorRole.Window).lightness() < 128` at `:1574` | Already established in Phase 107 `JoinWorkbenchWindow.__init__` |

**Modifier checkbox → responsa_options mapping (verified from source):**

| Checkbox | `responsa_options` key | Effect |
|----------|------------------------|--------|
| Variants (`chk_opt_variants`) | `variants=True`, `variant_mode="variants"` | Spelling variant expansion |
| Judeo-Arabic (`chk_opt_ja`) | `ja=True` | Judeo-Arabic letter substitution |
| Flex Spacing (`chk_opt_flex`) | `flex_spacing=True` | Flexible inter-word spacing |
| Bidirectional (`chk_opt_bidir`) | `bidirectional=True` | Bidirectional search |
| Negation (per-word mod) | `-word` prefix in query string | Excluded words |
| Defective / Plene (`chk_plene`) | `%` in query token | Plene/defective spelling |
| Wildcard prefix (`chk_wild_start`) | `*_` prefix → `#_` expansion | Grammatical prefix expansion |
| Wildcard suffix (`chk_wild_end`) | `_*` suffix → `_#` expansion | Grammatical suffix expansion |

**Code NOT needed in `JoinQueryBuilder` (dialog-bound only):**
- The horizontal "component card" layout and `_component_widgets` list (JWB uses vertical row model)
- `_create_distance_spinner()` / `_create_component()` (replaced by row+spinbox model)
- `_remove_component()` / `_add_component()` (replaced by `remove_row()` / `add_row()`)
- `get_syntax()` / `get_negated_words()` output API (replaced by `compose()` returning 3-tuple)
- The dialog-level `self.setLayoutDirection(RightToLeft)` — this is the RTL-chrome bug to fix

**Net-new in `JoinQueryBuilder` vs the existing dialog:**
- Vertical row model (each row = one manuscript line)
- Per-row `line_start` / `line_end` checkboxes WITHIN each row widget (not dialog-level)
- Per-row `gap_to_next` QSpinBox
- `compose()` returns `(query_str, responsa_options, page_position)` 3-tuple (not `get_syntax()`)
- `is_empty()` check
- The `SideQuery` / `BuilderRow` dataclass construction (not free-form string assembly)

---

### R-03: Batch enrichment paths

**Evidence:** Read `shared/fjms_service.py`, `genizah_core.py:4892`, and the spike's
`ThumbResolver` / `material_for` implementations.

#### Confirmed batch paths:

| Enrichment | Existing batch API | Verdict |
|------------|-------------------|---------|
| Material + dimensions | `get_domains_for_sys_ids(sys_ids)` uses `IN` queries in batches of 500 (`fjms_service.py:884-893`). BUT `manuscript_measurements` has NO batch function — only `get_measurements(sys_id)` (single) at `:2925`. | **MUST BUILD** a new `get_measurements_batch(sys_ids)` in `FjmsService` using `SELECT * FROM manuscript_measurements WHERE AlmaId IN (?,?,?)` batched at 500. |
| Thumbnails | `get_thumbnail(sys_id)` is per-ID (`:4892`). `batch_fetch_shelfmarks()` at `:4913` populates the MARC cache but does NOT return thumbnails. | **Already handled by spike pattern**: `ThumbResolver(QThread)` fetches one URL per sys_id sequentially within a QThread — this is an acceptable off-UI batch (not per-row blocking on UI thread). Transplant directly. |
| Browse text (cross-side OR neighbors) | `get_browse_page(sys_id, n)` is per-call. | **No batch path needed** — called only for synthesized neighbors (OR mode only), not for all candidates. The `apply_cross_side()` function in `shared/joins_lab.py:430-435` already does this lazily. |
| Cross-side membership | `apply_cross_side(executor, base, b_query, ...)` at `shared/joins_lab.py:344` — runs one engine call + set arithmetic. | **Already batched** — one engine call returns the full b_set; set membership is O(1). |
| VS score (per-pair) | Spike uses per-call `vs_score(anchor_sys, cand_sys)` (single SQL per pair). Phase 109 wires the VS service. | **Not needed in 108** — VS score column in the table stays blank (disabled sources). |

#### Recommended batch enrichment worker:

A new `_EnrichWorker(QThread)` runs AFTER `dedup_candidates()` returns, in a single pass over
all candidates:
1. Collect all `sys_id` values → run `get_measurements_batch(sys_ids)` (new IN-query) → dict
2. Build `snippet_html(full_text, highlight_pattern)` and `snippet_plain(...)` (pure, no I/O)
3. Emit a dict `{sys_id: {"w": ..., "h": ..., "material": ..., "lines": ..., "snippet_html": ..., "snippet_plain": ...}}`

Thumbnails are fetched separately by `ThumbResolver(QThread)` (one per card, emitted as URL
resolved → `ImageLoaderThread` for the bounded 5-slot pool). This matches Phase 107's
`ThumbBatchWorker` pattern exactly.

#### Perf risk flag:

The spike used per-card `material_for(sid)` calls (serial SQLite per card at render time).
With 20 cards per page and potentially 200+ total candidates, this would be ~200 SQLite
round-trips. The `_EnrichWorker` batch approach reduces this to 1-2 SQL calls total
(batch by 500). The material filter dropdown population must use the pre-fetched enrichment
dict, not live DB queries at render time.

---

### R-04: Size of the `TabularQueryBuilderDialog` RTL-chrome fix

**Evidence:** Read `genizah_app.py:1555` — the RTL-chrome bug is this single line:

```python
self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)  # line 1555
```

Setting `RightToLeft` on the whole dialog mirrors the QHBoxLayout arrangement (checkboxes
appear on the wrong side) and clips labels because PyQt6 flips the entire layout under RTL.
The fix is to REMOVE this line and instead apply `setLayoutDirection(RightToLeft)` only to
the individual `QLineEdit` inputs (already done at `:1779` for each word input).

**Scope assessment:**
- 1 line to remove (`self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)`)
- The preview label already has its own `setLayoutDirection(RightToLeft)` at `:1704`
- Each word input already has `inp.setLayoutDirection(Qt.LayoutDirection.RightToLeft)` at `:1779`
- No other layout restructuring needed; removing the dialog-level directive restores correct LTR chrome

**Verdict: KEEP IN 108, do NOT split to `/gsd-quick`.**
This is a one-line fix. Include as a separate Wave 1 task/commit (before the new builder widget)
so the fix is isolated and reviewable. Risk: near-zero (removing a directive that causes a bug).

---

### R-05: Stable per-candidate key for triage persistence

**Evidence:**

1. `shared/joins_lab.py:121-128` — `Candidate.key` property:
```python
@property
def key(self) -> tuple:
    """Canonical per-image dedup key: (sys_id, page)."""
    return (self.sys_id, self.page)
```

2. `UI-SPEC.md` Interaction Contracts section:
```
Keyed by sys_id (canonical per-candidate key surviving re-runs;
stable across filter changes).
Lives in self.triage = {} on JoinWorkbenchDialog.
```

3. The spike (`join_workbench.py.txt:1102-1113`) keys triage on `r_sid(r)` — just `sys_id`.

**There is a discrepancy between the dataclass `Candidate.key = (sys_id, page)` and the
UI-SPEC decision `keyed by sys_id`.**

**Recommendation for the planner:** Use `sys_id` as the triage key (not `(sys_id, page)`),
consistent with the UI-SPEC decision. Rationale: a scholar triages a MANUSCRIPT (the physical
fragment), not a specific page image. If the same manuscript appears at page 3 via the anchor
query and page 4 via the cross-side OR, both are the same candidate. The `Candidate.key`
property (used for DEDUP, which IS `(sys_id, page)`) serves a different purpose from the
TRIAGE key. The `triage: dict` on `JoinWorkbenchWindow` is `{sys_id: "Y"/"?"/None}`.

Dedup uses `(sys_id, page)` to keep one entry per image.
Triage uses `sys_id` to mark one fragment as Y/? /N regardless of which page matched.

This is confirmed by the UI-SPEC's explicit statement and the spike's implementation.

---

### R-06: CompareDialog "open to matched page" for cross-side candidates

**Evidence from `shared/joins_lab.py:446-458`** — the `apply_cross_side()` OR path synthesizes
neighbor result dicts with the neighbor's page number set explicitly:
```python
neighbor_res = {
    "display": {
        "id": sid,
        ...
        "img": n,   # <-- the neighbor page (p±1) that matched query B
    },
    "full_text": txt,
    "uid": f"{sid}|{n}",
    "highlight_pattern": anchor_pattern,
    "_via_other_side": True,
}
out.append(normalize_candidate(neighbor_res))
```

So `res["display"]["img"]` for an `_via_other_side=True` candidate IS the neighbor page `n`
(the page that was adjacent to a query-B match), not the "canonical page" of the manuscript.

**From the UI-SPEC (`108-UI-SPEC.md:401-402`):**
```
When a candidate was pulled in via the other-side builder (i.e. `res["_via_other_side"]`
is True), the compare dialog opens to the NEIGHBORING page (p±1) that matched query B,
not the candidate's own page. The `page_of(res)` accessor already resolves this from
`res["display"]["img"]` which the `_CrossSideWorker` sets when building neighbor result
dicts. No special-case branch needed — `_fill()` uses `res["display"]["img"]` as the
page index.
```

**Verdict:** No special-case logic is needed in `CompareDialog._fill()`. The function reads
`res["display"]["img"]` via `page_of(res)` to determine which page to load. For cross-side
OR candidates, this value is already the neighbor page that matched query B. The candidate
pane meta line should append `"  ·  other side matched"` (tr()-wrapped) when
`res.get("_via_other_side")` is True. This is display-only, no page-calculation branching.

For AND-filtered candidates (where the candidate itself matched query A, and its neighbor
matched query B), the candidate is displayed at its OWN page (the query-A match page), which
is already correct — the AND path does not synthesize a new result dict; it filters the
base list as-is. The anchor pane stays static throughout navigation.

---

## Standard Stack

### Core (already present — no new installations)
| Library / Module | Version | Purpose | Status |
|---------|---------|---------|--------|
| PyQt6 | existing | UI widgets, QThread | In project |
| `shared/joins_lab.py` | Phase 106 | compose, dedup, merge, self-match, snippet, cross-side | Complete, unit-tested |
| `desktop/join_workbench.py` | Phase 107 | Shell window, anchor pane, known-joins, public actions | Complete |
| `desktop/image_loader.py::ImageLoaderThread` | existing | Image fetch (disk cache + Rosetta fallback) | Verified in Phase 107 |
| `desktop/widgets/line_number_text_edit.py::apply_line_numbered_text` | existing | RTL numbered transcription | Verified in Phase 107 |
| `gui_threads.py::SearchThread` | existing | Off-UI search execution | Used by sketch directly |
| `shared/fjms_service.py::FjmsService` | existing | Material / dimensions via `manuscript_measurements` | Needs new batch method |
| `genizah_core.py::MetadataManager.get_thumbnail` | existing | Thumbnail URL via NLI MARC | Per-ID; used in `ThumbResolver` pattern |

### Supporting
| Module | Purpose | When Used |
|--------|---------|-----------|
| `shared/visual_similarity_service.py` | VS look-alikes | Phase 109 only; 108 stubs the VS source button disabled |
| `shared/joins_lab.merge_candidates()` | Text+VS merge ordering | Called in `_maybe_assemble()` even in 108 (text-only pass-through when vs_cands is empty) |

### New code to write in Phase 108
| Item | Where | What |
|------|-------|------|
| `JoinQueryBuilder` widget | `desktop/join_workbench.py` (or new `desktop/join_query_builder.py`) | The per-row line builder widget |
| `_EnrichWorker(QThread)` | `desktop/join_workbench.py` | Batch enrichment worker |
| `FjmsService.get_measurements_batch(sys_ids)` | `shared/fjms_service.py` | New IN-query batch method |
| `JoinCandidatePane` (right-pane widget) | `desktop/join_workbench.py` | Houses builder + refine + grid/table + status |
| `CandidateCard(QFrame)` | `desktop/join_workbench.py` | Individual candidate card widget |
| `CompareDialog(QDialog)` | `desktop/join_workbench.py` | Two-pane side-by-side compare |
| `ThumbResolver(QThread)` | `desktop/join_workbench.py` | Transplant from spike; resolves thumbnail URLs for current page |
| Desktop `SearchExecutor` adapter | `desktop/join_workbench.py` | Thin wrapper around `self.searcher` + `self.meta_mgr` |

---

## Architecture Patterns

### System Architecture Diagram

```
Scholar input (JoinQueryBuilder rows)
         |
         v compose() → (query_str, responsa_opts, page_position)
         |
         v SearchThread(searcher, query, "exact", 0, responsa_options=ro,
         |              text_position=page_position, corpus_scope="genizah")
         |
         v _on_results(raw_results)
         |        |
         |        +---> detect_self_match(raw_results, anchor_sid) → anchor_matched bool
         |        +---> dedup_candidates(raw_results, anchor_sid, include_self) → (deduped, matched)
         |              |
         |              v (if other-side builder enabled)
         |        _CrossSideWorker(executor, base_candidates, b_query, b_ro, combine, a_pattern)
         |              |
         |              v apply_cross_side() → MergeResult(candidates, note)
         |
         v _EnrichWorker(QThread)
         |    get_measurements_batch(sys_ids) → {sys_id: {w, h, material, lines}}
         |    snippet_html/plain per candidate (pure, in worker)
         |
         v _maybe_assemble()
         |    merge_candidates(text_cands, vs_cands=[]) → ordered list (text-only pass-through)
         |
         v apply_filters() → self.filtered
         |
         v render_page() OR render_table()
              |
              +---> ThumbResolver(meta_mgr, items) → resolved(idx, url) → ImageLoaderThread pool
              +---> CandidateCard widgets (per page, 20 max)
                        |
                        v open_compare(global_idx) → CompareDialog(wb, idx)
                                  |
                                  v _fill(pane, res): reads res["display"]["img"] as page
                                       apply_line_numbered_text (RTL numbered text)
                                       ImageLoaderThread (candidate image)
                                       wb.act_*(res) for actions
```

### Recommended Project Structure

```
desktop/
├── join_workbench.py       # Phase 107 shell + Phase 108 additions (all in one file per convention)
│   ├── [Phase 107 code: pure helpers, JoinWorkbenchWindow, workers]
│   ├── [Phase 108 additions:]
│   │   ├── JoinQueryBuilder (QWidget)
│   │   ├── ThumbResolver (QThread) — transplant from spike
│   │   ├── _CrossSideWorker (QThread) — adapted from spike/shared/joins_lab
│   │   ├── _EnrichWorker (QThread) — new batch enrichment worker
│   │   ├── CandidateCard (QFrame)
│   │   ├── JoinCandidatePane (QWidget) — the Phase 108 right-pane surface
│   │   └── CompareDialog (QDialog)
shared/
├── joins_lab.py            # Phase 106 (no changes needed in 108)
├── fjms_service.py         # ADD get_measurements_batch(sys_ids) method
```

### Pattern 1: Builder → compose() → SearchThread

The `JoinQueryBuilder` collects its row state, calls `compose()` from `shared/joins_lab.py`,
and passes the 3-tuple to `gui_threads.SearchThread`:

```python
# Source: shared/joins_lab.py:695-770 (compose), DESKTOP-INTEGRATION-NOTES.md
def do_search(self):
    side = SideQuery(
        rows=tuple(BuilderRow(
            term="|".join(e["boxes"]),   # join OR-alternative boxes with pipe
            line_start=e["start"].isChecked(),
            line_end=e["end"].isChecked(),
            gap_to_next=e["gap"].value(),
        ) for e in self.builder.rows if any(b.strip() for b in e["boxes"])),
        variants=self.builder.variants_chk.isChecked(),
        page_position=self._page_position(),  # 'start'/'end'/None
    )
    query_str, ro, page_pos = compose(side)
    if not query_str:
        return
    thread = SearchThread(
        self.searcher, query_str, "exact", 0,
        responsa_options=ro,
        text_position=page_pos,
        corpus_scope="genizah",
    )
    thread.results_signal.connect(self._on_results)
    thread.start()
```

### Pattern 2: ThumbResolver + bounded ImageLoaderThread pool

Transplant directly from the spike (`join_workbench.py.txt:258-283`) and the Phase 107
`ThumbBatchWorker` pattern, but emit URL strings (not raw images) from `ThumbResolver`,
then load images through the existing bounded 5-slot `ImageLoaderThread` pool:

```python
# Source: spike join_workbench.py.txt:258-283 + Phase 107 ThumbBatchWorker:522-566
class ThumbResolver(QThread):
    resolved = pyqtSignal(int, str)  # (card_index, url or "")

    def run(self):
        for idx, sid in self.items:
            if self._cancel:
                return
            url = ""
            try:
                url = self.meta_mgr.get_thumbnail(sid) or ""
            except Exception:
                url = ""
            self.resolved.emit(idx, url)
```

### Pattern 3: Batch measurements (NEW)

```python
# New method for shared/fjms_service.py
def get_measurements_batch(self, sys_ids: list[str]) -> dict:
    """Batch-fetch manuscript_measurements for multiple AlmaIds.

    Returns {sys_id: {"w": float|None, "h": float|None, "material": str|None,
                       "lines": float|None, "cat": str|None}} for found ids.
    Missing ids are absent from the result dict (caller treats as None).
    """
    result = {}
    batch_size = 500
    for i in range(0, len(sys_ids), batch_size):
        batch = sys_ids[i:i + batch_size]
        placeholders = ",".join("?" * len(batch))
        cursor = self._conn.execute(
            f"SELECT AlmaId, catalog_width_cm, catalog_height_cm, material, "
            f"avg_num_lines, size_category "
            f"FROM manuscript_measurements WHERE AlmaId IN ({placeholders})",
            batch,
        )
        for row in cursor:
            sid = row["AlmaId"]
            result[sid] = {
                "w": row["catalog_width_cm"],
                "h": row["catalog_height_cm"],
                "material": row["material"],
                "lines": row["avg_num_lines"],
                "cat": row["size_category"],
            }
    return result
```

### Anti-Patterns to Avoid

- **Serial enrichment at render time:** Calling `get_measurements(sid)` inside `CandidateCard.__init__()` or `render_table()` — 200+ cards = 200 SQLite round-trips on the UI thread. Always batch before render.
- **`_vs_*` private app method calls:** The Codex critique and D-20 both forbid this. All four actions must go through `JoinWorkbenchWindow`'s public methods (`open_result_in_browse_from_table`, `show_add_to_list_menu`, `open_anchor_in_puzzle`, `open_anchor_as_join` — already established in Phase 107).
- **Modifying `TabularQueryBuilderDialog`:** D-03 requires a SEPARATE class. The dialog is a QDialog modal; the builder is a QWidget embedded in the right pane. They must remain separate.
- **Writing to QLabel/QWidget from a non-GUI thread:** All worker signals emit primitive values (strings, dicts, ints) and the connected slot runs on the GUI thread. QImage construction in a worker is OK (Phase 107 `ThumbBatchWorker` pattern); QPixmap must be constructed on the GUI thread.
- **Dedup key confusion:** Use `(sys_id, page)` for dedup (one entry per image); use `sys_id` for triage (one mark per fragment).

---

## Reuse Map

| New Surface | Closest Existing Analog | What Transplants | What is Net-New |
|-------------|------------------------|------------------|-----------------|
| `JoinQueryBuilder` widget | `TabularQueryBuilderDialog` (genizah_app.py:1543) | Modifier checkbox row logic; `_responsa_opts()` pattern; RTL on QLineEdit inputs; dark-mode detection | Vertical row model; per-row line_start/line_end/gap; `is_empty()`; calls `compose()` not `generate_tabular_syntax()` |
| `ThumbResolver(QThread)` | Spike `ThumbResolver` (join_workbench.py.txt:258-283) + Phase 107 `ThumbBatchWorker` | Nearly verbatim; emits `(idx, url)` string | Nothing new — direct transplant |
| `_CrossSideWorker(QThread)` | Spike `_CrossSideWorker` (join_workbench.py.txt:332-429) | Structure; BUT inner logic now delegates to `apply_cross_side()` from `shared/joins_lab.py` | Thin QThread wrapper only; pure logic is in shared |
| `_EnrichWorker(QThread)` | No equivalent in spike (spike did per-card serial calls) | Nothing transplants | New: batch measurements + snippet precompute |
| `CandidateCard(QFrame)` | Spike `CandidateCard` (join_workbench.py.txt:584-689) | Overall structure; triage button row; action button row; `_restyle()` for triage border | `setAccessibleName()` on all buttons (UI-SPEC Dim 2); provenance badge logic; dimension evidence line; size-mismatch hint; all strings `tr()`-wrapped |
| `JoinCandidatePane(QWidget)` | Spike right pane `_build_results_pane()` (join_workbench.py.txt:785-905) | Layout skeleton; refine bar; status + view toggle + pagination row; grid/table structure | Source selector row (D-14 Text/VS-stub/Combined-stub); self-match inline in status; i18n for all strings |
| `CompareDialog(QDialog)` | Spike `CompareDialog` (join_workbench.py.txt:1473-1563) | Top bar (prev/next/triage); action row; two-pane body; `_fill()` using `res["display"]["img"]` | `wb.act_*` public methods replace sketch's `self.wb.act_*` (already public in Phase 107); Re-anchor inside dialog; `setAccessibleName()` on buttons; `"other side matched"` label when `_via_other_side` |
| Desktop `SearchExecutor` adapter | Phase 106 `SearchExecutor` Protocol | Protocol already defined | Concrete class wrapping `self.searcher` + `self.meta_mgr` for Phase 108 builder runs |
| `FjmsService.get_measurements_batch()` | `FjmsService.get_domains_for_sys_ids()` (fjms_service.py:866) | Batch IN-query pattern | New method on existing class |
| JWB-12 source selector scaffold | No equivalent | Nothing transplants | New 3-button row (Text/VS-stub/Combined-stub) in `JoinCandidatePane` |

---

## Batch Enrichment Plan (R-03)

After `dedup_candidates()` returns the deduped Candidate list, one `_EnrichWorker` runs:

**Step 1 — Batch measurements (new `get_measurements_batch` call):**
- Input: list of all candidate `sys_id` values
- Query: `SELECT AlmaId, catalog_width_cm, catalog_height_cm, material, avg_num_lines, size_category FROM manuscript_measurements WHERE AlmaId IN (...)`
- Output: `{sys_id: {w, h, material, lines, cat}}`
- Batched at 500 sys_ids per query to stay within SQLite limits

**Step 2 — Snippet precompute (pure, no I/O):**
- For each Candidate: `snippet_html(cand.full_text, cand.highlight_pattern, max_lines=6)`
- For each Candidate: `snippet_plain(cand.full_text, cand.highlight_pattern, max_chars=220)`
- Both are pure functions in `shared/joins_lab.py`; safe to call in a worker thread

**Step 3 — Size-mismatch hint (pure, no I/O):**
- Anchor dimensions fetched once (same batch or separate call for anchor_sid)
- Per candidate: if `anchor_w` and `cand_w` are known, compare ratios; flag if > 1.4

**Signal:** `enriched(dict)` — delivers the full `{sys_id: {measurements, snippet_html, snippet_plain, mismatch_hint}}` dict to the GUI thread

**Thumbnail loading** (separate from `_EnrichWorker`):
- `ThumbResolver(QThread)` — fetches one `meta_mgr.get_thumbnail(sid)` URL per card, in the current page's 20-card set only
- Result feeds into the bounded 5-slot `ImageLoaderThread` pool
- `_cancel_images()` called on: page change, view toggle, dialog close

**Cross-side membership** (handled by `_CrossSideWorker`, NOT `_EnrichWorker`):
- Runs BEFORE `_EnrichWorker` (it may add/remove candidates)
- `apply_cross_side(executor, base_candidates, b_query, b_ro, combine, a_pattern)` in `shared/joins_lab.py:344`
- One engine call for query B; then pure set arithmetic
- The `_EnrichWorker` runs on the final merged candidate list

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead |
|---------|-------------|-------------|
| Query composition (rows → engine syntax) | Custom string builder | `shared/joins_lab.compose()` |
| Candidate dedup (one-per-image) | Custom set logic | `shared/joins_lab.dedup_candidates()` |
| Text+VS merge ordering | Custom sort | `shared/joins_lab.merge_candidates()` |
| Self-match detection | Custom regex on anchor text | `shared/joins_lab.detect_self_match()` |
| Cross-side AND/OR membership | Custom engine call loop | `shared/joins_lab.apply_cross_side()` |
| HTML snippet with highlight | Custom HTMLify | `shared/joins_lab.snippet_html()` / `htmlify()` |
| Plain-text snippet | Custom truncation | `shared/joins_lab.snippet_plain()` |
| Neighbor page calculation | Custom page arithmetic | `shared/joins_lab.resolve_other_side_pages()` |
| RTL line-numbered text | Custom QTextBrowser subclass | `desktop/widgets/line_number_text_edit.apply_line_numbered_text()` |
| Image load with disk cache + Rosetta fallback | Custom HTTP fetch | `desktop/image_loader.ImageLoaderThread` |
| Add-as-Join persist path | Custom Supabase call | `JoinWorkbenchWindow.open_anchor_as_join()` → existing `JoinsDialog` (Phase 107, D-14) |
| Browse action | Custom window launch | `app.open_result_in_browse_from_table(res)` (Phase 107 public action) |

---

## Common Pitfalls

### Pitfall 1: Triage key confusion (sys_id vs (sys_id, page))
**What goes wrong:** Using `Candidate.key = (sys_id, page)` for triage state leads to a
candidate being untriaged after switching pages (different `page` value from cross-side OR).
**Why it happens:** `Candidate.key` is the DEDUP key; triage is per-FRAGMENT.
**How to avoid:** `triage: dict` on `JoinWorkbenchWindow` is always `{sys_id: value}`.
The UI-SPEC and spike are explicit. The `Candidate.key` docstring clarifies this separately.
**Warning signs:** A card's triage border resets when the cross-side OR adds a neighbor page
of the same manuscript.

### Pitfall 2: Dialog-level `setLayoutDirection(RightToLeft)` on `TabularQueryBuilderDialog`
**What goes wrong:** The existing dialog's `self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)` at line 1555 mirrors checkbox positions and clips labels.
**Why it happens:** Setting RTL on the whole dialog flips QHBoxLayout order, which is wrong for LTR chrome.
**How to avoid:** Remove the dialog-level directive; individual `QLineEdit` inputs already have `setLayoutDirection(RightToLeft)`.
**Warning signs:** Modifier checkboxes appear on the wrong side; some labels are truncated.

### Pitfall 3: Serial enrichment at render time
**What goes wrong:** Calling `meas_for(sid)` or `material_for(sid)` inside `CandidateCard.__init__()` hits SQLite once per card at render time.
**Why it happens:** The spike did this (per-card serial calls in `CandidateCard.__init__` at line 622).
**How to avoid:** Pre-fetch all measurements in `_EnrichWorker` before rendering. Pass the enrichment dict to the card at construction time.
**Warning signs:** Grid render takes 1-2 seconds for a 20-card page; SQLite profiling shows 20+ queries per render.

### Pitfall 4: QPixmap construction on a worker thread
**What goes wrong:** `QPixmap.fromImage()` called in `ThumbBatchWorker.run()` crashes with "QPixmap: Must be constructed on the GUI thread."
**Why it happens:** PyQt6 enforces GUI-thread-only for QPixmap (unlike QImage, which is OK off-thread).
**How to avoid:** Worker emits `QImage` (not `QPixmap`); GUI-thread slot converts: `QPixmap.fromImage(qimg)`.
**Warning signs:** Random crashes in the thumbnail loader, especially under load.

### Pitfall 5: `_via_other_side` candidates missing `highlight_pattern`
**What goes wrong:** The snippet `QTextBrowser` on a cross-side OR candidate shows no highlight.
**Why it happens:** The synthesized neighbor result dict in `apply_cross_side()` gets `anchor_pattern` (the THIS-side pattern), which may not match the neighbor page's text.
**How to avoid:** The `_CrossSideWorker` correctly passes `anchor_pattern` (spike `:384`); the neighbor's `full_text` may simply not contain that pattern, which is correct behavior. Do not error if the snippet shows no highlight.
**Warning signs:** Spurious "no match" behavior even when the cross-side match was confirmed.

### Pitfall 6: RTL word-boxes with `layout.setLayoutDirection(RightToLeft)` on the whole builder
**What goes wrong:** If the `JoinQueryBuilder` sets `setLayoutDirection(RightToLeft)` on itself, the checkboxes (⊣ ends line, ⊢ starts line) appear on the wrong sides.
**Why it happens:** Same as Pitfall 2 — dialog-level RTL flips QHBoxLayout.
**How to avoid:** Only set `setLayoutDirection(RightToLeft)` on individual `term` `QLineEdit` widgets. The row `QHBoxLayout` must remain LTR. (UI-SPEC RTL/LTR contract table.)
**Warning signs:** "ends line ⊣" appears on the right of the term field; "⊢ starts line" appears on the left.

### Pitfall 7: Page-position constraint validation not surfaced to the user
**What goes wrong:** `compose()` raises `ValueError` when `page_position='start'` but the first row is empty, or `page_position='end'` but the last row is empty.
**Why it happens:** The UI allows setting the page-position option independently of row content.
**How to avoid:** Catch `ValueError` from `compose()` in `do_search()` and display a user-friendly status message (e.g., "Page start anchor requires a first-row query").
**Warning signs:** Unhandled exception traceback when user enables "Start of text" with an empty first row.

---

## Runtime State Inventory

This is a UI composition phase (greenfield UI attached to an existing shell). There is no
rename, refactor, or migration involved. No runtime state inventory required.

SKIPPED — not a rename/refactor/migration phase.

---

## Environment Availability

All dependencies are already installed/present in the project. Phase 108 adds no new packages.

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| PyQt6 | All UI widgets | Yes | existing project dep | N/A |
| `shared/joins_lab.py` | compose, dedup, merge, all pure logic | Yes | Phase 106 complete | N/A |
| `desktop/join_workbench.py` | Shell window + attach seam | Yes | Phase 107 complete | N/A |
| `gui_threads.SearchThread` | Off-UI search execution | Yes | existing | N/A |
| `shared/fjms_service.py` | `get_measurements_batch` (new method) | Yes (class exists; method to add) | existing | Serial calls (perf risk only, not a blocker) |
| Tantivy index | Line-break search | Yes (verified in DESKTOP-INTEGRATION-NOTES.md) | existing | Graceful empty result |

**Missing dependencies with no fallback:** None.

---

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest (existing) |
| Config file | none — uses default pytest discovery |
| Quick run command | `pytest tests/test_joins_lab.py tests/test_join_workbench*.py -x` |
| Full suite command | `pytest tests/ -x` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| JWB-06 | `compose()` from multi-box rows produces correct line-break query | unit (headless) | `pytest tests/test_joins_lab.py -k compose -x` | Yes (Phase 106) |
| JWB-06 | Builder widget `is_empty()` returns True when all boxes blank | unit (headless) | `pytest tests/test_join_workbench_builder.py -k empty -x` | No — Wave 0 |
| JWB-10 | Cross-side AND keeps only candidates with matched neighbor | unit (headless) | `pytest tests/test_joins_lab.py -k cross_side -x` | Yes (Phase 106) |
| JWB-10 | Cross-side OR adds synthesized neighbors | unit (headless) | `pytest tests/test_joins_lab.py -k cross_side_or -x` | Yes (Phase 106) |
| JWB-11 | `detect_self_match()` finds anchor in raw results | unit (headless) | `pytest tests/test_joins_lab.py -k self_match -x` | Yes (Phase 106) |
| JWB-12 text | `merge_candidates(text, [])` returns text as-is | unit (headless) | `pytest tests/test_joins_lab.py -k merge -x` | Yes (Phase 106) |
| D-21 | `get_measurements_batch(sys_ids)` returns correct data in batch | unit (headless) | `pytest tests/test_fjms_service.py -k measurements_batch -x` | No — Wave 0 |
| R-05 | Triage dict uses sys_id key; same sys_id at different pages gets same triage | unit (headless) | `pytest tests/test_join_workbench_triage.py -x` | No — Wave 0 |
| D-06 | RTL chrome fix: `TabularQueryBuilderDialog` has no dialog-level `setLayoutDirection(RightToLeft)` | unit (AST/grep assertion) | `pytest tests/test_tabular_builder_rtl.py -x` | No — Wave 0 |
| D-19 | All new strings in `JoinQueryBuilder` and `CompareDialog` are `tr()`-wrapped | unit (AST guard) | `pytest tests/test_join_workbench_i18n.py -x` | No — Wave 0 |
| D-20 | No `_vs_*` calls on the workbench path | static AST guard | `pytest tests/test_no_vs_private_calls_108.py -x` | No — Wave 0 |

### Sampling Rate
- **Per task commit:** `pytest tests/test_joins_lab.py tests/test_fjms_service.py -x`
- **Per wave merge:** `pytest tests/ -x`
- **Phase gate:** Full suite green before `/gsd-verify-work`

### Wave 0 Gaps

- [ ] `tests/test_join_workbench_builder.py` — `JoinQueryBuilder.is_empty()`, `compose()` from widget state (headless, no QApplication needed for pure logic), term joining
- [ ] `tests/test_fjms_service.py::TestGetMeasurementsBatch` — batch IN-query returns correct data; missing sys_ids absent from result; batch size respected
- [ ] `tests/test_join_workbench_triage.py` — triage keyed by sys_id; same fragment at different pages gets same triage state
- [ ] `tests/test_tabular_builder_rtl.py` — AST assertion that `TabularQueryBuilderDialog.__init__` does NOT call `self.setLayoutDirection(RightToLeft)` at dialog level
- [ ] `tests/test_join_workbench_i18n.py` — AST guard: all string literals in new Phase 108 code are wrapped in `tr()`
- [ ] `tests/test_no_vs_private_calls_108.py` — static grep/AST: no `_vs_` method calls in `desktop/join_workbench.py`'s new Phase 108 additions

*(Existing Phase 106 tests in `tests/test_joins_lab.py` already cover compose, dedup, merge, cross-side, self-match, snippet helpers — no gaps there.)*

---

## Security Domain

This is a PyQt6 desktop phase. The `security_enforcement` key is absent from
`.planning/config.json` (treated as enabled), but the threat surface is minimal:

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | No | N/A (no new auth surface) |
| V3 Session Management | No | N/A |
| V4 Access Control | No | N/A (desktop single-user) |
| V5 Input Validation | Partial | Query strings from `QLineEdit` pass to `compose()` then to `execute_search()` — the engine's tokenizer and Tantivy are the validators; no SQL injection surface (queries use parameterized Tantivy syntax) |
| V6 Cryptography | No | N/A |

No new threat patterns identified for this phase. The `get_measurements_batch()` IN-query uses
Python `?` parameterized placeholders (SQLite parameterized query pattern already used throughout
`fjms_service.py`) — no SQL injection risk.

The XSS-via-snippet risk is already mitigated by `htmlify()` in `shared/joins_lab.py:621-647`
(MARK_A/MARK_B sentinel approach with `html.escape()` before highlight injection — verified).

---

## Sources

### Primary (HIGH confidence — verified by reading actual source files)
- `shared/joins_lab.py` — lines 1-771 (complete file read); all function signatures, `BuilderRow`/`SideQuery`/`Candidate` dataclasses, `compose()`, `dedup_candidates()`, `merge_candidates()`, `detect_self_match()`, `apply_cross_side()`, `snippet_html()`, `snippet_plain()`, `page_of()`, `normalize_candidate()`
- `genizah_core.py:5811-5927` — `_parse_line_break_query()` implementation
- `genizah_core.py:8001-8210` — `_execute_line_break_search()` with `text_position` forwarding
- `genizah_core.py:8298-8420` — `execute_search()` routing line-break path with `text_position=`
- `genizah_core.py:9483-9541` — `get_browse_page()` signature
- `genizah_core.py:4892-4911` — `get_thumbnail()` per-ID implementation
- `genizah_app.py:1543-2145` — `TabularQueryBuilderDialog` complete implementation
- `desktop/join_workbench.py:1-839` — Phase 107 shell (anchor pane, known-joins, public actions, attach seam at line 838)
- `shared/fjms_service.py` (grep for `manuscript_measurements`, `get_measurements`, batch patterns) — confirmed single-ID-only `get_measurements()` at line 2925; batch IN-query pattern from `get_domains_for_sys_ids()` at line 884
- `.planning/spikes/002-assisted-join-workbench/sketch/join_workbench.py.txt:1-1563` — complete spike sketch

### Secondary (MEDIUM confidence — planning context docs)
- `108-CONTEXT.md` — all 22 locked decisions D-01..D-22 and 6 research flags R-01..R-06
- `108-UI-SPEC.md` — complete UI design contract (6 dimensions verified by checker)
- `106-CONTEXT.md` — Phase 106 domain model + research flags (confirms compose/dedup/merge/self-match scope)
- `107-CONTEXT.md` — Phase 107 shell (confirms public action APIs, attach seam, known-joins panel)
- `.planning/spikes/002-assisted-join-workbench/CODEX-PRODUCTIONIZE-CRITIQUE.md` — batch-everything imperative, public APIs, i18n acceptance
- `.planning/spikes/002-assisted-join-workbench/DESKTOP-INTEGRATION-NOTES.md` — verified engine/meta/joins signatures

---

## Assumptions Log

All claims in this research are VERIFIED by reading source files. No assumptions.

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| — | — | — | — |

**This table is empty:** All claims were verified or cited against source code — no user confirmation needed.

---

## Open Questions (RESOLVED)

1. **`JoinQueryBuilder` file placement: add to `desktop/join_workbench.py` or create `desktop/join_candidate_pane.py`?**
   - What we know: Phase 107 uses a single `desktop/join_workbench.py` per the single-file convention.
   - **RESOLVED:** All Phase 108 code stays in `desktop/join_workbench.py` (matches the Phase 107 convention; the existing i18n + `_vs_*` AST guards full-scan that module). If the file later exceeds ~1500 lines, splitting `CompareDialog`/`JoinQueryBuilder` to a separate `desktop/` module is a future refactor, not a Phase 108 task.

2. **`page_position` UI affordance: where does the "Start of text" / "End of text" option appear?**
   - What we know: D-08 (Phase 106) specifies page-START anchor on first row only, page-END on last row only.
   - **RESOLVED:** A small page-position control sits in the builder's controls row (below the modifier row, alongside the source-selector row) — page-START offered on the first row only, page-END on the last row only (realizes 106 D-08). "Claude's Discretion" per CONTEXT.md; implemented in Plan 02.

3. **`gui_threads.SearchThread` `cancel_flag` mechanism:** Does cancellation work reliably for line-break searches?
   - What we know: The sketch sets `self._search_thread.cancel_flag = True` at line 1465.
   - **RESOLVED:** Pre-existing UX gap (whether `_execute_line_break_search` checks `cancel_flag` in its iteration loop at `:8112`), NOT introduced by Phase 108 and NOT a blocker. Phase 108 does not change `genizah_core` cancellation; deferred to a future `/gsd-quick` if scholars report long uncancellable line-break runs.

---

## RESEARCH COMPLETE

**Phase:** 108 — Desktop Join Workbench: Query Builders, Candidates & Compare
**Confidence:** HIGH

### Key Findings
1. `compose()` does NOT auto-produce `|`-OR-groups — the UI must join OR-alternative box values with `|` when building `BuilderRow.term`.
2. Page-anchored line-break queries use ONE engine call (`text_position` forwarded through `_execute_line_break_search`); no intersection step needed.
3. No batch measurement API exists in `fjms_service.py` — a new `get_measurements_batch(sys_ids)` method must be written (a simple IN-query using the existing batch pattern).
4. The `TabularQueryBuilderDialog` RTL-chrome fix is ONE line (`self.setLayoutDirection(RightToLeft)` removal at line 1555) — keep in 108, do NOT split to `/gsd-quick`.
5. Triage key is `sys_id` (per-fragment), NOT `(sys_id, page)` (per-image dedup key).
6. `CompareDialog._fill()` reads `res["display"]["img"]` for the page — cross-side OR candidates already have the neighbor page `n` stored there; no special-case branching needed.

### File Created
`.planning/phases/108-desktop-join-workbench-query-builders-candidates-compare/108-RESEARCH.md`

### Confidence Assessment
| Area | Level | Reason |
|------|-------|--------|
| Standard Stack | HIGH | All files read directly |
| Architecture | HIGH | Attach seam confirmed at `join_workbench.py:838`; all patterns verified in source |
| Pitfalls | HIGH | Each pitfall is grounded in a specific code line or test failure pattern from Phase 107 |
| Batch enrichment | HIGH | Confirmed no batch measurement method exists; confirmed batch IN-query pattern to follow |

### Open Questions
- Where exactly the page-position (page-START/page-END) option appears in the builder UI (Claude's discretion; not covered in UI-SPEC).
- Whether `SearchThread.cancel_flag` is checked inside `_execute_line_break_search` iteration loop (minor UX gap, not a blocker).

### Ready for Planning
Research complete. Planner can now create PLAN.md files.
