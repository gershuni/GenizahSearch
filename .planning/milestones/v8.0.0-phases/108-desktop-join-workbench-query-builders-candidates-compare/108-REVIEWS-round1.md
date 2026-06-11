---
phase: 108
reviewers: [codex]
reviewed_at: 2026-06-05T05:26:31Z
plans_reviewed: [108-01-PLAN.md, 108-02-PLAN.md, 108-03-PLAN.md, 108-04-PLAN.md]
model: codex-cli 0.136.0 (default model)
note: >
  First Codex run was blocked by a broken Windows sandbox (`spawn setup refresh`) and
  could not read the codebase; re-run with --dangerously-bypass-approvals-and-sandbox so
  Codex could verify plan claims against the actual source. The review below is the
  code-verified run. One distinct nuance from the sandbox-blocked run (per-page snippet
  overwrite when enrichment is keyed by sys_id) is folded into Concern 2 below.
---

# Cross-AI Plan Review — Phase 108

## Codex Review

**Summary**

Overall, the plans are well-structured by wave and correctly identify several real seams, but
there are major plan-code drift issues that should be fixed before execution. The two biggest are
load-bearing: embedded `|` does **not** round-trip as an OR group in the real search parser, and
Phase-106 candidate APIs return `Candidate` dataclasses, while Plans 03/04 treat them as raw result
dicts. As written, Phase 108 would produce wrong OR searches and later UI/action code would fail at
runtime.

**Strengths**

- The RTL fix target is accurate: `TabularQueryBuilderDialog.__init__` still has the dialog-level
  RTL call at `genizah_app.py:1555`, while preview/input RTL remain at `genizah_app.py:1704` and
  `genizah_app.py:1779`.
- The right-pane attach seam is exactly as claimed at `desktop/join_workbench.py:838`.
- `SearchThread` does accept and forward `text_position` and `corpus_scope` at `gui_threads.py:86`
  and `gui_threads.py:103`.
- `execute_search()` forwards `text_position` into `_execute_line_break_search()` at
  `genizah_core.py:8355`, and the line-break path enforces start/end text position at
  `genizah_core.py:8142`.
- The `manuscript_measurements` columns named in Plan 01 exist in the import schema at
  `scripts/import_measurements.py:495`. `FjmsService` also uses `sqlite3.Row` via direct and
  thread-local connections at `shared/fjms_service.py:747` and `shared/thread_local_db.py:93`.

**Concerns**

- **HIGH — Embedded `|` is not an OR operator in the real engine.** `compose()` preserves
  `BuilderRow.term` and only splits on whitespace at `shared/joins_lab.py:758`. The engine's OR
  syntax is parenthesized slash groups, documented and parsed as `(word1/word2)` at
  `genizah_core.py:5727` and `genizah_core.py:6139`. Bare `word1|word2` falls through as a single
  word at `genizah_core.py:6219`, then gets quoted in Tantivy and escaped in regex at
  `genizah_core.py:7439` and `genizah_core.py:7593`. In line-break mode, embedded pipes are also not
  split; only leading/trailing/standalone pipes are line syntax at `genizah_core.py:5875`. **This
  invalidates the current OR-box plan** (Plan 01 Task 3, Plan 02 Task 1, D-05, R-01). The research
  itself hedged on this; the code confirms the `|`-join is wrong.

- **HIGH — Plans 03/04 confuse `Candidate` objects with result dicts.** `dedup_candidates()` returns
  `Candidate` objects at `shared/joins_lab.py:498`. `apply_cross_side()` returns
  `MergeResult(candidates=tuple(Candidate...))` and normalizes synthesized neighbor dicts immediately
  at `shared/joins_lab.py:446`. `merge_candidates()` returns a plain list, **not** a `MergeResult`, at
  `shared/joins_lab.py:511`. Therefore calls like `r_sid(c)`, `r_text(c)`, `c.get(...)`,
  `page_of(res)`, and `result.candidates` in Plans 03/04 will break unless a Candidate→result adapter
  is added. (Sub-nuance: even if enrichment were keyed by `sys_id`, page-specific snippet/highlight
  evidence can be overwritten when the same fragment appears on multiple pages — enrichment/snippets
  should be keyed by the candidate key `(sys_id, page)` or list index, while measurement lookup may
  stay `sys_id`-keyed.)

- **HIGH — Add-as-Join A+B prefill is not available through the public API.** The public
  `open_anchor_as_join()` only accepts `anchor_sys_id, anchor_shelfmark` and explicitly leaves B empty
  at `genizah_app.py:15443`. The existing A+B prefill path is private `_vs_open_joins_with_partner()`
  at `genizah_app.py:5242`, which the no-private guard (D-20) forbids. Plans 03/04 cannot satisfy D-17
  without adding/extending a public method.

- **HIGH — New `tr()` keys will fail the existing i18n guard unless translations are added.**
  `test_join_workbench_i18n.py` requires every `tr()` key in `desktop/join_workbench.py` to exist in
  `TRANSLATIONS` at `tests/test_join_workbench_i18n.py:55`. Plans 02-04 add many new keys but do not
  include `genizah_translations.py` in `files_modified`.

- **MEDIUM — Other-side builder page position is silently dropped.** `apply_cross_side()` accepts only
  `b_query` and `b_responsa_options`, then calls `executor.execute_search()` without `text_position`
  at `shared/joins_lab.py:344`. If the same `JoinQueryBuilder` exposes page-position for the other
  side, the UI will imply a constraint the API cannot enforce.

- **MEDIUM — R-03 is outdated.** A batch measurement API already exists:
  `get_measurement_summaries_batch()` at `shared/fjms_service.py:3005`. It uses
  `COALESCE(catalog_width_cm, max_computed_width_cm)` at `shared/fjms_service.py:3037`. Plan 01's
  proposed new method may still be useful for `size_category`, but the "no batch API exists" premise
  is false and raw catalog-only width/height will miss computed-only measurements.

- **MEDIUM — Page-specific compare images need a different helper than thumbnails.**
  `meta_mgr.get_thumbnail()` is manuscript-level at `genizah_core.py:4892`. Matched-page image loading
  should use enriched image lists and `_image_url_for_idx()` at `desktop/join_workbench.py:189`, like
  the anchor loader does at `desktop/join_workbench.py:365`.

- **LOW — Import and Qt-guard details are missing.** `desktop/join_workbench.py` currently imports
  only a subset of widgets at `desktop/join_workbench.py:315`. Plans use `QFrame`, `QSpinBox`,
  `QGridLayout`, `QTableWidget`, `QTableWidgetItem`, and `SearchThread`, which are not imported there.

**Suggestions**

- Replace OR-box serialization from `word1|word2` to the actual Responsa syntax, likely
  `(word1/word2)` for single-token alternatives. If boxes may contain multi-word phrases, the current
  `BuilderRow.term: str` model cannot represent phrase-level OR safely; either restrict OR boxes to
  one token or extend the shared model deliberately.
- Add a real parser-level regression test for OR boxes using `_parse_line_break_query()` /
  `parse_responsa_query()` or an engine-level search fixture. A test that only checks `compose()`
  contains `|` will lock in the wrong behavior.
- Redesign Plans 03/04 around `Candidate` as the UI model. Use `candidate.sys_id`, `candidate.page`,
  `candidate.full_text`, etc., and add a small `candidate_to_result_dict()` only for host methods that
  require raw result dicts.
- Change `merge_candidates(self._text_cands, [])` handling to use the returned list directly, not
  `.candidates`.
- Add a public host method or extend
  `open_anchor_as_join(..., partner_sys_id=None, partner_shelfmark=None)` so Add-as-Join can prefill B
  without `_vs_*`.
- Include `genizah_translations.py` in Plans 02-04 or add a dedicated i18n wave; otherwise the
  existing guard will fail.
- Either remove page-position from the other-side builder in 108 or extend `apply_cross_side()` to
  accept and forward B-side `text_position`, with Phase-106 tests.
- Prefer adapting `get_measurement_summaries_batch()` or adding a thin shape adapter over creating a
  parallel batch method with overlapping semantics.

**Risk Assessment**

**HIGH.** The wave ordering is sensible, and several seams are accurately identified, but the current
plans rely on two false API assumptions: embedded `|` as OR, and result dicts after Phase-106
dedup/merge. Those are core to candidate generation and rendering. Add the public Add-as-Join API gap
and missing translation updates, and execution as written is likely to fail tests and produce
incorrect search behavior.

---

## Consensus Summary

Only one external reviewer was requested (`--codex`), so this is a synthesis/prioritization of
Codex's code-verified findings rather than a cross-reviewer consensus. Codex ran against the live
repository and cited `file:line` for every claim — these are plan↔code drift findings, not style
opinions, and several are blocking.

### Must-fix before execution (HIGH, code-verified)

1. **OR-box `|` serialization is wrong (load-bearing).** The real engine uses `(word1/word2)` slash
   groups (`genizah_core.py:5727/:6139`); a bare `word1|word2` term is treated as a single word
   (`:6219`) and escaped in regex (`:7593`). Plan 01 Task 3, Plan 02 Task 1, D-05 and R-01 all encode
   the wrong syntax. **Fix the serialization AND replace the `compose()`-only headless test with a
   real parser/engine-level OR regression** — otherwise the test locks in incorrect behavior.
2. **`Candidate`-vs-dict confusion across Plans 03/04.** `dedup_candidates()`/`apply_cross_side()`
   return `Candidate` dataclasses; `merge_candidates()` returns a plain list (not a `MergeResult`).
   The pane/card/compare code assumes raw result dicts (`r_sid(c)`, `r_text(c)`, `c.get(...)`,
   `page_of(res)`, `result.candidates`) and will fail at runtime. Decide the UI model (`Candidate`
   throughout + a thin `candidate_to_result_dict()` only where host methods require dicts) and rewrite
   the affected sections. Key per-page enrichment by `(sys_id, page)`, not `sys_id`.
3. **D-17 Add-as-Join A+B prefill has no public path.** `open_anchor_as_join()` leaves B empty
   (`genizah_app.py:15443`); the A+B path is the forbidden private `_vs_open_joins_with_partner()`
   (`:5242`). A new/extended public host method is required, or D-17 is unachievable under D-20.
4. **i18n guard will fail.** New `tr()` keys must land in `TRANSLATIONS`
   (`tests/test_join_workbench_i18n.py:55`), but `genizah_translations.py` is absent from
   `files_modified` in Plans 02-04.

### Should-resolve (MEDIUM)

5. Other-side page-position is silently dropped — `apply_cross_side()` doesn't forward `text_position`
   (`shared/joins_lab.py:344`). Either drop page-position from the other-side builder in 108 or extend
   the Phase-106 API.
6. R-03 premise is stale — `get_measurement_summaries_batch()` already exists
   (`shared/fjms_service.py:3005`) and uses `COALESCE` over computed widths; raw catalog-only columns
   miss computed-only measurements. Prefer adapting it over adding an overlapping method.
7. Compare/matched-page images should use `_image_url_for_idx()` (`desktop/join_workbench.py:189`),
   not manuscript-level `get_thumbnail()`.

### Minor (LOW)

8. Missing widget/`SearchThread` imports in `desktop/join_workbench.py:315`.
9. (From the sandbox-blocked run) Plan 01's combined `-k measurements_batch` invocation can deselect
   unrelated new tests; Plan 04 `files_modified` omits `108-VALIDATION.md`. Worth a quick cleanup.

### Agreed Strengths

- Wave ordering (non-`join_workbench.py` scaffolds first, then forced-sequential single-file UI waves)
  is sound. Reuse of Phase-106 pure logic over new engine work is the right architecture. The RTL fix
  target, attach seam, and `text_position`/`corpus_scope` forwarding are all accurately identified and
  code-confirmed.

### Recommended next step

Several findings (especially #1, #2, #3) require revisiting CONTEXT decisions (D-05 OR semantics, D-17
public-API path) and the Phase-106 API surface — not just plan edits. Route through
`/gsd-plan-phase 108 --reviews` to fold these in, and likely a short discuss-phase touch-up on the
OR-syntax and Add-as-Join-public-method decisions before re-planning.

### Divergent Views

None — single reviewer.
