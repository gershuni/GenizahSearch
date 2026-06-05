---
phase: 108
round: 2
reviewers: [codex]
reviewed_at: 2026-06-05T09:22:38Z
plans_reviewed: [108-01-PLAN.md, 108-02-PLAN.md, 108-03-PLAN.md, 108-04-PLAN.md]
model: codex-cli 0.136.0 (default model)
note: >
  ROUND 2 — re-review of the plans AFTER the round-1 findings were folded in via
  `/gsd-plan-phase 108 --reviews`. Run code-verified with
  --dangerously-bypass-approvals-and-sandbox so Codex could check plan claims against the
  live source (the Windows sandbox blocks file reads otherwise). Round 1 is preserved at
  108-REVIEWS-round1.md. Verdict: all 8 round-1 resolutions (RR-1..RR-8) CONFIRMED landed
  (RR-8 partial), but the replan left/introduced 3 NEW execution risks (2 release blockers).
---

# Cross-AI Plan Review — Phase 108 (Round 2)

## Codex Review

**Summary**

Round 2 fixed the main Round-1 plan drift: OR is now slash-group/parser-tested, Plans 03/04 now treat
candidates as `Candidate` dataclasses, Add-as-Join is routed through an extended public API,
translations are included, and matched-page images use the per-page image path. I would not execute
yet: the replan introduced or left three execution risks, two of which are release blockers.

**Round-1 Fix Verification**

| RR | Status | Evidence |
|----|--------|----------|
| RR-1 OR syntax | CONFIRMED | Context now says slash-group `(w1/w2/w3)`, not `\|`, at `108-CONTEXT.md:66-75`. Plan 01 requires parser-level `parse_responsa_query` assertions at `108-01-PLAN.md:264-272`. Plan 02 assembles `"/".join(tokens)` at `108-02-PLAN.md:257-267`. Live parser supports `(עץ/אילן)` at `genizah_core.py:5727` and splits OR at `genizah_core.py:6139-6146`. |
| RR-2 Candidate vs dict | CONFIRMED | Plan 03 uses `Candidate` attributes, not `r_sid(c)`/`c.get()`/`page_of(c)`, at `108-03-PLAN.md:66-70`; adds `candidate_to_result_dict` at `108-03-PLAN.md:188-193`; uses the `merge_candidates` list directly at `108-03-PLAN.md:362-364`. Live: `Candidate` at `shared/joins_lab.py:76`; `dedup_candidates` returns candidates at `:498-505`; `merge_candidates` returns a list at `:511`/`:526-527`/`:558-559`. |
| RR-3 Add-as-Join public path | CONFIRMED | Plan 03 includes `genizah_app.py` and extends `open_anchor_as_join(..., partner_sys_id=None, partner_shelfmark=None)` at `108-03-PLAN.md:129-156`; workbench call uses `partner_shelfmark=` at `108-03-PLAN.md:238-239`. Live public method leaves B empty at `genizah_app.py:15443-15464`; private prefill line at `genizah_app.py:5242-5261` — the plan targets the right gap. |
| RR-4 i18n guard | CONFIRMED | Plans 02/03/04 include `genizah_translations.py` in `files_modified` (`108-02:7-9`, `108-03:8-10`, `108-04:7-9`) and require registering new `tr()` keys (`108-02:275-283`, `108-03:255`, `108-04:166`). Live guard at `tests/test_join_workbench_i18n.py:56-70`. |
| RR-5 Other-side page-position | CONFIRMED | D-07 revised at `108-CONTEXT.md:83-91`. Plan 02 adds `allow_page_position` and hides the combo when false (`108-02:204-210`, `:246-250`). Plan 03 instantiates the other builder with `allow_page_position=False` (`108-03:321-327`). Live `apply_cross_side()` does not pass `text_position` at `shared/joins_lab.py:369-375`. |
| RR-6 Batch measurements | CONFIRMED (with new concern below) | Plan 01 extends existing `get_measurement_summaries_batch`, not a new method (`108-01:141-156`); Plan 03 consumes existing keys (`108-03:210-219`). Live method preserves COALESCE width/height at `shared/fjms_service.py:3005-3060`. |
| RR-7 Page-specific images | CONFIRMED | Plan 03 requires `enrich_metadata(...).get("images")` + `_image_url_for_idx(images, page-1, width)` (`108-03:246-250`); Plan 04 uses `_enqueue_image_for_pane(..., c.page)` (`108-04:150-152`). Live helper is per-index at `desktop/join_workbench.py:189-197`; thumbnail is manuscript-level at `genizah_core.py:4892`. |
| RR-8 Missing imports | PARTIAL | Plan 02 adds the imports (`108-02:150-154`) but also runs `ruff check desktop/join_workbench.py` immediately (`108-02:165`). `ruff.toml:15-18` selects `F401`, and Plan 02 front-loads Plan-03-only `QGridLayout`/`QTableWidget`/`QTableWidgetItem`/`SearchThread` — so Task 0 likely fails lint before those names are used. |

**New Concerns**

- **HIGH — Modifier row controls are planned as visible but mostly no-op.** CONTEXT/UI require Negation,
  Defective, Wildcards, Prefixes, Suffixes (`108-CONTEXT.md:61-62`, `108-UI-SPEC.md:176-178`). The live
  existing builder applies those as per-word token syntax at `genizah_core.py:6008-6027` (`-word`,
  `%word`, `#word`, `word#`, `*word`, `word*`). But Plan 02 only stores/uses `ja`, `flex`, `bidir`,
  `variants` in `_responsa_opts()` and builds terms from raw box text at `108-02-PLAN.md:241-267`.
  Selecting Prefix/Negation/etc. would change the preview but not the query semantics.

- **MEDIUM (blocker) — Plan 02 Task 0 likely fails ruff (F401).** The import block lacks the names at
  `desktop/join_workbench.py:315-320`; Plan 02 adds ALL names before Plan 03 uses several (`108-02:150-154`);
  ruff enforces unused imports (`ruff.toml:15-18`). Move Plan-03 imports to Plan 03, or `# noqa: F401`
  every intentionally deferred import (not only `SearchThread`).

- **MEDIUM — `size_category` old-sidecar robustness is internally contradictory.** Plan 01 adds it as a
  plain selected column (`108-01:154-156`) but also says a missing `size_category` should degrade to
  `None` (`108-01:160-162`). The live method guards the optional `avg_line_height_mm` before adding it to
  the SELECT (`shared/fjms_service.py:3017-3029`); an unguarded missing column makes the SELECT fail and
  returns an empty batch (`:3035-3060`).

- **LOW — Per-page image helper needs a `page is None` guard.** `Candidate.page` is optional
  (`shared/joins_lab.py:104`); tests cover `(sys_id, None)` (`tests/test_joins_lab.py:121-124`). Plan 03
  subtracts `page-1` (`108-03:246-250`); Plan 04 passes `c.page` directly (`108-04:142-152`).

**Suggestions**

- Either implement token decoration for the full modifier row, or remove the unsupported per-word
  controls from Phase 108 (scholars can still type `#`/`%`/`*`/`-` directly — the engine parses them).
- Move deferred imports into the wave that uses them, or explicitly `noqa` every intentionally unused
  import.
- Add `has_size_category = "size_category" in cols` and build a `sc_col` fragment like the existing
  `lh_col` guard.
- Make `_enqueue_image_for_pane` treat a missing page as page 1 (or "No image") without arithmetic on
  `None`.

**Risk Assessment**

Overall risk: **HIGH** until the modifier no-op and the Plan 02 lint issue are fixed. The main Round-1
blockers are mostly resolved, but execution as written can still fail lint and silently run wrong
searches when modifier controls are used.

**RELEASE BLOCKERS:** modifier-row no-op semantics; Plan 02 unused-import ruff failure; `size_category`
missing-column guard (if old-sidecar compatibility is required).

---

## Consensus Summary

Single reviewer (`--codex`), code-verified against the live repo with `file:line` citations — these are
plan↔code drift findings, not style. This round is a **re-review after the round-1 fold-in**, so the
headline is two-part: (1) the round-1 work landed, and (2) a small set of new, fixable issues remain.

### Round-1 resolutions — all CONFIRMED
RR-1 (slash-group OR + parser-level test), RR-2 (Candidate model + adapter + list-not-MergeResult),
RR-3 (public Add-as-Join), RR-4 (i18n files), RR-5 (other-side page-position dropped, 106 frozen),
RR-6 (reuse existing batch method), RR-7 (per-page images) all verified landed. RR-8 (imports) landed
but collides with the lint gate (see below).

### Must-fix before execution (new this round)
1. **Modifier row no-op (HIGH).** Plan 02 builds Negation/Defective/Wildcards/Prefixes/Suffixes
   checkboxes (locked in D-04) but `_responsa_opts()` + raw-text `build_side_query` never apply them —
   the engine wants per-word token decoration (`-`/`%`/`#`/`*`, `genizah_core.py:6008-6027`). **This is a
   CONTEXT-level decision (touches the locked D-04 modifier row): TRIM the per-word controls from 108
   (keep variants/JA/flex/bidir; scholars type `#`/`%`/`*`/`-` directly — the engine parses them), OR
   WIRE per-word token decoration into `build_side_query`.** Recommend TRIM for 108 (lower risk; the raw
   syntax still works), wire later if scholars ask.
2. **Plan 02 Task 0 ruff F401 (blocker).** Front-loading Plan-03-only imports + an immediate
   `ruff check` self-fails. Move `QGridLayout`/`QTableWidget`/`QTableWidgetItem`/`SearchThread` to the
   Plan 03 wave (the first plan that USES them), keeping only `QFrame`/`QSpinBox` in Plan 02.
3. **`size_category` missing-column guard (MEDIUM→blocker if old sidecars matter).** Mirror the existing
   `avg_line_height_mm` guard: `has_size_category = "size_category" in cols`, conditional `sc_col`,
   and `None` when absent. Resolves Plan 01's internal contradiction.

### Should-resolve
4. **Per-page image `page is None` guard (LOW).** `_enqueue_image_for_pane` / `_image_url_for_idx`
   must not do `page-1` arithmetic on a `None` page (VS-only/None-page candidates) — treat as page 1
   or "No image."

### Recommended next step
Route these through `/gsd-plan-phase 108 --reviews` again. Finding #1 needs a one-line CONTEXT decision
(trim vs wire the per-word modifier controls) — the replan will surface it; #2/#3/#4 are mechanical
plan edits. After that, the plans should be execution-ready (round-1 blockers are gone).

### Divergent Views
None — single reviewer.
