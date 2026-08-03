---
phase: 136-read-surfaces-connections-panel-work-witnesses
plan: 10
subsystem: ui
tags: [i18n, rtl, hebrew, css, nicegui, discovery, honesty-gate, wcag]

# Dependency graph
requires:
  - phase: 136-03
    provides: the gate-1 ratified decisions (D-13e keeps the third disclosure level; D-16 keeps the relation filter off the findings page) plus owner rulings E/E'/F/G/H/M/N/O
  - phase: 136-07
    provides: shared/discovery_main_pool.py -- bucket_label and main_pool_sentence, delegated to rather than redefined
  - phase: 135-01
    provides: shared/discovery_band_labels.py -- BAND_LABELS/band_label (tooltip-only), SHOW_MORE_TOGGLE, RECALL_DISCLAIMER
  - phase: 136-02
    provides: tests/render_smoke/discovery_honesty_gate.py -- the ONE shared honesty gate the sweep runs through
provides:
  - shared/discovery_display_strings.py -- the whole bilingual claim vocabulary both discovery surfaces render
  - the discovery page-chrome tr() entries (nav label, page meta, mode strip, Show-as, sort, result bar, availability card)
  - the one scoped discovery CSS block in web/static/common.css serving the panel and the findings page
  - a sweep that runs every public display function, in both languages, through the shared honesty gate
affects: [136-15, 136-16, 136-17, 136-18, 136-21, 137, 138]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "One display-vocabulary module per surface family, composed from the existing frozen tables rather than retyping them"
    - "The relation filter travels as short codes with NO code->stored-key direction at all"
    - "CSS class-name scoping under a single root class when a sketch's class names are generic"
    - "A sweep over every public callable plus a registry-coverage test, instead of a per-function assertion list"

key-files:
  created:
    - shared/discovery_display_strings.py
    - tests/test_discovery_display_strings.py
  modified:
    - genizah_translations.py
    - web/static/common.css

key-decisions:
  - "The relation filter has NO reverse code->stored-key function; the reverse direction returns the reader-facing label and a filtered query uses matches_filter_codes(relation_kind, codes), so the stored key appears in neither direction's output"
  - "row_headline keeps D-21's owner-selected 'Matches <work> - 68% of page' verbatim and APPENDS '(matched letters)', because the shared honesty gate anchors the one permitted percentage on 'of page'/'מהדף' while the plan requires the matched-letter qualifier adjacent to the figure"
  - "row_headline gained an optional evidence_source argument: a propagated row can still carry claim_type=direct_witness (5,604 not_evaluated claims do), so the relation kind alone cannot enforce D-08a's direct-family-only coverage"
  - "Every CSS rule is scoped under .gs-discovery; the sketch class names (.row/.chip/.mode/.c) would otherwise collide with Quasar globals across the whole app"
  - "Three sketch CSS values were corrected because they fail WCAG AA in at least one theme (the amber tag's white foreground, .nov.unknown on --text-muted, .chip.gated at opacity .7)"
  - "The sweep runs through tests/render_smoke/discovery_honesty_gate.py rather than a second copy of the rule"
  - "This module's own prose never spells out the three prohibited relation words, so a phrase in a docstring can never be one copy away from a surface"

patterns-established:
  - "tr() owns page chrome; shared/discovery_display_strings.py owns claim vocabulary; a string lives in exactly one, enforced by a test"
  - "Unknown enum inputs raise rather than rendering blank -- a silent empty chip hides what kind of match a reader is looking at"
  - "Contrast is measured per theme for every new text-on-background pair and the ratio is recorded beside the rule"

requirements-completed: [PANEL-01, PANEL-02, NOVEL-01]

# Metrics
duration: 95min
completed: 2026-08-03
---

# Phase 136 Plan 10: Shared Display Strings, Nav Chrome and CSS Summary

**One bilingual claim-vocabulary module (703 lines, 26 tests) plus the discovery page-chrome `tr()` entries and one `.gs-discovery`-scoped CSS block — so the panel track and the findings track can now be built concurrently without either touching a shared file.**

## Performance

- **Duration:** ~95 min
- **Started:** 2026-08-03T04:25Z
- **Completed:** 2026-08-03T06:00Z
- **Tasks:** 3
- **Files modified:** 4 (2 created, 2 modified)

## Accomplishments

- `shared/discovery_display_strings.py` — every claim-level word either surface renders: relation chips, tooltips, match-framing row headlines, the matched-letter coverage label, four section headers with the Hebrew maqaf at U+05BE, three disclosure toggles, the related-pages label, the novelty candidacy set, service-state copy, and the filter short codes.
- A **sweep** that enumerates every public callable, calls it in both languages over a registry of representative inputs, and runs each returned string through the **shared** `discovery_honesty_gate` — 58 strings per language — plus a registry-coverage test so a new function without sweep inputs fails the suite, and four positive controls proving the sweep can go red.
- The `tr()` page-chrome block, with the split documented in a comment and enforced by a test that fails if any string is defined in both places.
- One scoped CSS block serving both surfaces, with three WCAG AA corrections to the validated sketch and contrast measured in all three themes.

## Task Commits

1. **Task 1 (RED): failing honesty contract** — `9ae98f59` (test)
2. **Task 1 (GREEN): the display-vocabulary module** — `37345e0a` (feat)
3. **Task 2: page-chrome strings in `genizah_translations.py`** — `2a40ec4b` (feat)
4. **Task 3: the shared discovery CSS block** — `aa75ecf2` (feat)

## Files Created/Modified

- `shared/discovery_display_strings.py` (created, 703 lines) — the claim vocabulary. Imports `scripts.discovery_ids` for the frozen relation enum, `shared.discovery_band_labels` for tooltips/toggle/disclaimer, `shared.discovery_main_pool` for bucket names and the rule sentence. Never imports `web`.
- `tests/test_discovery_display_strings.py` (created, 26 tests) — seven behaviour tests, the two-language sweep, the registry-coverage test, four positive controls, the module-level invariants, the three `tr()` split tests, and the four CSS guards.
- `genizah_translations.py` (modified, +60 lines appended) — one new `TRANSLATIONS.update({...})` block. No pre-existing entry touched.
- `web/static/common.css` (modified, +264 lines appended, 0 deletions) — one new block. No existing selector changed.

## The public API downstream plans consume

Named here because 136-15/16/17/18 read this module rather than the plan text:

| Function | Returns |
|---|---|
| `relation_chip(relation_kind, lang)` | "Direct match" / "Partial match" / "Shared text" + HE |
| `relation_tooltip(evidence_source, confidence_band, lang)` | the frozen band label, unmodified |
| `row_headline(work_title, coverage_ppm, relation_kind, lang, evidence_source=None)` | match framing; coverage only on the direct family |
| `coverage_label(lang)` / `low_coverage_note(lang)` | the qualified coverage name / the short-match note |
| `granularity_subline(other_work_title, lang)` | the `↳` D-13d collapse sub-line |
| `missing_title(lang)` / `not_an_identification_note(lang)` | the unresolved-title marker / the D-13e italic qualifier |
| `section_header(section_key, lang, work_title)` | the four section headers (`SECTION_*` constants) |
| `disclosure_toggle(toggle_key, lang)` | the three toggles (`TOGGLE_*` constants) |
| `divergence_warning(lang)` | ruling F's warning beside the divergence toggle |
| `related_pages_label(lang)` / `related_pages_count_line(count, lang)` | "unevaluated candidate alignments", with singular agreement |
| `bucket_name(in_main_pool, lang)` / `rule_sentence(lang)` / `recall_disclaimer(lang)` | delegated, never redefined |
| `novelty_strings(lang)` | `{toggle, badge, subline, help}` |
| `novelty_unknown_badge(lang)` | the fail-closed `not_checked` badge |
| `filter_codes()` / `filter_code(kind)` / `filter_label(code, lang)` / `is_filter_code(code)` / `matches_filter_codes(kind, codes)` | the relation filter, both directions |
| `service_state_message(state, lang)` / `retry_label(lang)` | the four envelope statuses + retry |

**There is deliberately no `code -> stored key` function.** The plan asked for "a reverse lookup [that] maps a short code back — the stored key never appears in either direction's output." A function returning the stored key would have made that sentence self-contradictory and would have failed the sweep. The reverse returns the reader-facing label, and a filtered query calls `matches_filter_codes(row.claim_type, selected_codes)` — the row already carries its own stored key, so nothing ever needs to turn a code back into an internal classification. **136-16/136-17/136-18 must filter this way**; do not add a code→key map.

**Every CSS rule is scoped under `.gs-discovery`.** The panel root and the findings-page root must carry that class or none of the block applies.

## Decisions Made

1. **`row_headline` keeps D-21 verbatim and appends the qualifier.** The shared honesty gate (136-02) recognises exactly one legitimate percentage on a discovery surface and anchors it on the literal `of page` / `מהדף` within 32 characters after the figure. The plan additionally requires the percentage be "adjacent to the matched-letter qualifier". Both are satisfied by `Matches ⟨work⟩ · 68% of page (matched letters)` / `התאמה ל⟨חיבור⟩ · 68% מהדף (אותיות תואמות)` — D-21's owner-selected phrase intact, the qualifier immediately after it.
2. **`row_headline` gained an optional `evidence_source`.** The plan's behaviour list requires both "coverage ONLY for the direct family" and "a propagated row's headline contains no percentage at all", and those are two different axes: a propagated row can carry `claim_type='direct_witness'` (5,604 `not_evaluated` claims do, per `findings-page.md`'s data quirks). The argument is keyword-optional and appended after `lang`, so the plan's positional signature is unchanged; omitting it gates on the relation kind alone.
3. **Unknown enum inputs raise.** `relation_chip`, `row_headline`, `section_header`, `disclosure_toggle`, `filter_code`, `filter_label` and `service_state_message` all raise `ValueError` rather than returning an empty string. A blank chip on a real row silently hides what kind of match the reader is looking at.
4. **The module's own prose never spells out the three prohibited relation words.** The first draft of the docstring quoted them while explaining the rule, and the suite caught it — which is the same failure class as the findings sketch's negated caveat, and a good argument that the source-literal check earns its place.
5. **CSS scoping.** See the deviations section.
6. **The sweep reuses the shared gate.** Each returned string is wrapped in a minimal scoped HTML fragment and passed to `assert_discovery_honesty`, so the percentage rule, the interval rule, the review-badge rule, the negation-proof word gate and the raw-vocab-key rule all come from one implementation. Writing a second rule here is exactly the `confOf()` mistake this phase keeps citing.

## Contrast checked — all three themes

Measured with the WCAG 2.x relative-luminance formula against the token values in `common.css` (`:root`, `[data-theme="parchment"]`, `[data-theme="dark"]`).

| Rule | Foreground / background | light | parchment | dark |
|---|---|---|---|---|
| `.disc.notid > summary`, `.phead .caveat` | `--text-primary` on `--bg-tertiary` | 13.35 | 13.28 | 9.45 |
| `.disc.notid .dnote`, `.dnote` | `--text-secondary` on `--bg-primary` | 7.58 | 8.80 | 12.02 |
| `.rel` (relation chip), `.chip` base | `--text-secondary` on `--bg-secondary` | 7.24 | 8.53 | 9.85 |
| `.chip.here` | `--text-primary` on `--bg-active` | 13.89 | 13.09 | 8.87 |
| `.chip.gated` (opacity .85, blended) | `--text-secondary` on `--bg-secondary` | 4.97 | 5.86 | 7.54 |
| `.nov` | `--primary-700` on `--bg-active` | 5.21 | 6.37 | 6.38 |
| `.nov.unknown` | `--text-secondary` on `--bg-tertiary` | 6.92 | 8.27 | 6.97 |
| `.needs` (amber tag) | `#1e293b` on `--accent-amber` | 6.81 | 6.81 | 6.81 |
| `.dnode .c` | `--text-secondary` on `--bg-primary` | 7.58 | 8.80 | 12.02 |

**Deliberately below AA, and correctly so:** `.fg.blocked` and `.mode.future` at `opacity: .55` measure 2.59 / 2.87 / 4.45. Both are **inactive user-interface components**, which WCAG 1.4.3 exempts from the contrast minimum, and reading as unavailable is the entire point of the treatment. Their meaning does not depend on the dimming alone — `.fg.blocked` carries the amber `needs` tag and `.mode.future` carries its phase tag.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Every CSS rule scoped under `.gs-discovery`**
- **Found during:** Task 3
- **Issue:** The validated sketches use very generic class names — `.row`, `.chip`, `.mode`, `.c`, `.needs`. `web/static/common.css` is a **global** stylesheet loaded beside Quasar on every page, and Quasar ships `.row` as a core flex utility. An unscoped `@media (max-width: 700px) { .row { flex-direction: column; } }` would have restyled every NiceGUI page in the app at phone width — a site-wide layout regression invisible in any test this plan runs.
- **Fix:** Every selector in the block is prefixed with `.gs-discovery`. The sketch class names are kept exactly as the reference docs record them, so those docs stay accurate; only the scope is added. The requirement is stated at the top of the block and in this summary.
- **Files modified:** `web/static/common.css`
- **Verification:** `test_discovery_css_block_is_scoped_and_carries_the_required_treatments` walks every selector line in the block and fails on any that lacks the scope. `git diff --numstat` shows 264 insertions and 0 deletions.
- **Committed in:** `aa75ecf2`

**2. [Rule 1 - Bug] Three WCAG AA contrast failures in the sketch CSS, corrected**
- **Found during:** Task 3 (the plan's own instruction to verify contrast in all three themes)
- **Issue:** (a) `.needs` set `color: var(--text-inverse)` on `background: var(--accent-amber)`. `--accent-amber` is defined only in `:root` and is therefore theme-invariant, while `--text-inverse` is white in light **and** parchment — 2.15:1, a clear failure on 9px uppercase text. (b) `.nov.unknown` used `--text-muted` on `--bg-tertiary` — 4.49 (parchment) and 4.04 (dark), both under the 4.5 floor. (c) `.chip.gated` used `opacity: .7`, dropping a real, readable work title to 3.50 (light) / 4.03 (parchment).
- **Fix:** (a) fixed dark foreground `#1e293b` on the theme-invariant amber — 6.81 in all three themes; a token cannot work here precisely because the background does not vary. (b) `--text-secondary`, with italic + normal weight carrying the "muted" reading instead of a colour that fails. (c) opacity raised to `.85` (4.97 / 5.86 / 7.54); the dashed border and recessed fill still carry the gated distinction.
- **Files modified:** `web/static/common.css`
- **Verification:** ratios recomputed and tabulated above; each correction is annotated at its own rule in the stylesheet.
- **Committed in:** `aa75ecf2`

**3. [Rule 2 - Missing Critical] Five display strings the plan's action list did not enumerate**
- **Found during:** Tasks 1 and 3
- **Issue:** This plan exists so the four surface plans (136-15/16/17/18) never have to touch a shared file. Five strings those plans are explicitly required to render had no home: (a) **ruling F's divergence toggle and warning** — §F names 136-15/16/17/18 as the implementers of the default-hidden, explicitly-warned opt-in for `diverges_work`/`diverges_part`, and none of them may add a shared string; (b) the `↳` **granularity sub-line** in 136-17's documented row anatomy; (c) the **missing-title marker** 136-15 emits; (d) the **`.nov.unknown`** badge text, whose CSS this plan ships; (e) the **"not an identification"** italic qualifier D-13e requires on the middle bucket.
- **Fix:** Added `disclosure_toggle(TOGGLE_DIVERGENCE)`, `divergence_warning`, `granularity_subline`, `missing_title`, `novelty_unknown_badge` and `not_an_identification_note`. The divergence wording states **that** the aid and the claim disagree and says explicitly that **neither side has been adjudicated** — it never asserts which is right, because ruling F's whole point is that the system surfaces the disagreement and the reader decides (`feedback_catalogue_never_evidence` is preserved: catalogue disagreement is never used as evidence of wrongness).
- **Files modified:** `shared/discovery_display_strings.py`, `tests/test_discovery_display_strings.py`
- **Verification:** all six are in the sweep registry and pass the shared honesty gate in both languages.
- **Committed in:** `37345e0a`

**4. [Rule 1 - Bug] The module docstring quoted the prohibited relation words**
- **Found during:** Task 1
- **Issue:** The first draft explained rule 2 by quoting the words D-21 prohibits, and quoted `review_overlay` while explaining rule 4. `test_no_display_string_contains_a_prohibited_relation_word` and `test_module_defines_no_human_review_badge_string` both went red. The phrases were in prose, not data — the exact failure class `findings-page.md` records.
- **Fix:** The rules are now stated by reference (`136-CONTEXT.md` D-21; the shared honesty gate) with an explicit note that this file never spells them out, "because a phrase sitting in a docstring is one careless copy away from a surface."
- **Files modified:** `shared/discovery_display_strings.py`
- **Verification:** both tests green.
- **Committed in:** `37345e0a`

---

**Total deviations:** 4 auto-fixed (2 missing-critical, 2 bugs)
**Impact on plan:** All four are corrections the plan's own instructions require (contrast in three themes; "neither surface track has to touch a shared file"; the honesty rules). No scope creep — nothing was added that a named downstream plan does not render.

## Issues Encountered

- The plan's CSS verify snippet does `s.index('/* discovery')` guarded by a condition that also accepts a capitalised `'/* Discovery'`; if the block had been capitalised the snippet would have raised `ValueError` rather than failing an assertion. The block opens with the exact lowercase literal, so the snippet works as written. Worth knowing if the block header is ever reworded.
- The sweep's "checked enough strings to mean anything" floor was initially set above the real count (58 per language) and was lowered to a floor the current API clears with headroom. Every gate assertion passed on the first run; only the arbitrary threshold was wrong.

## Verification run

| Check | Result |
|---|---|
| `pytest tests/test_discovery_display_strings.py -q` | **26 passed** |
| `pytest tests/ -k "translation or i18n" -q` | **110 passed, 4 skipped** |
| CSS logical-property + no-confidence-chip snippet (plan verbatim) | **OK** |
| `pytest tests/ -k "discovery" -q` | **643 passed, 8 skipped** |
| Standing guards (`no_raw_storage_access`, `no_await_sync_function`, `no_back_edges_*`, `genizah_core_facade`) | **90 passed** |
| `ruff check` on all four files | **clean** |
| `git diff --numstat web/static/common.css` | **264 / 0** (additions only) |

## Known Stubs

None. `service_state_message('ok')` returns an empty string by design (there is nothing to say when the service is fine) and is documented as such; every other status returns copy naming a temporary condition, so an outage can never render as an authoritative zero.

## Threat Flags

None. This plan adds no network endpoint, no auth path, no file access and no schema change — it is bilingual strings and CSS. The threat register's five entries are all mitigated and tested:

| Threat | Mitigation | Test |
|---|---|---|
| T-136-10-01 stored key in markup | filter uses short codes; no code→key function exists | `test_filter_codes_round_trip_without_exposing_a_stored_key`, sweep |
| T-136-10-02 prohibited word via hand-written string | sweep through the shared negation-proof gate, both languages, plus a source-literal check | `test_every_public_function_passes_the_shared_honesty_gate`, `test_no_display_string_contains_a_prohibited_relation_word` |
| T-136-10-03 per-kind coloured chip | no relation-keyed selector in the block | `test_relation_chip_css_is_not_keyed_on_a_relation_kind` |
| T-136-10-04 missing Hebrew value | every new `tr()` key asserted to have a non-identical Hebrew value | `test_new_translation_keys_have_hebrew_values` |
| T-136-10-05 review badge reappears | the string is defined nowhere in the module | `test_module_defines_no_human_review_badge_string` |

**D-25 (masking):** no string added by this plan names a restricted corpus. The novelty help text names the checked sources as "…titles, PGP, FGP, shelfmark attributions" — the masked member appears only as "shelfmark attributions", in both languages. The repo-wide masking sweep belongs to plans 136-17/18/19 and was **not** run here: this worktree carries no gitignored `.masking_patterns`, so the scan would fail closed for a missing pattern file, which is not a gate result.

## User Setup Required

None.

## Next Phase Readiness

- **Ready:** 136-15/136-16/136-17/136-18 can now be built concurrently. Every claim string, every chrome string and every CSS token they need exists; none of them needs to touch `shared/discovery_display_strings.py`, `genizah_translations.py` or `web/static/common.css`.
- **Two contracts those plans must honour**, both stated above and in the source: (1) the panel root and findings-page root carry `class="gs-discovery"`, or no discovery CSS applies; (2) relation filtering goes through `matches_filter_codes(row.claim_type, selected_codes)` — there is no code→stored-key map and one must not be added.
- **Not in scope here and still open:** the findings page gets **no** relation filter (D-16, ratified — the codes exist for the panel's filter and for any future consumer, not because the findings page will use them). The divergence toggle's *behaviour* (default-hidden rows, ruling F) is unbuilt; only its wording ships here.
- **No blockers.**

## State files deliberately NOT updated here — for the orchestrator on main

`STATE.md`, `ROADMAP.md` and `REQUIREMENTS.md` were **left untouched**, on purpose. Three reasons,
all specific rather than procedural:

1. **`state advance-plan` would corrupt the position in a parallel wave.** `STATE.md` reads
   `Plan: 5 of 21`; this is wave-3 plan 10, not the next sequential plan. Advancing the counter from a
   worktree executing out of sequence writes a number nobody can trust.
2. **`roadmap update-plan-progress 136` was run and REVERTED.** It recalculates from summaries on
   disk, and it ticked **136-09** to `[x]` as a side effect — because 136-09 wrote a SUMMARY when it
   **HALTED** at the owner-ruling gate. `STATE.md` says that plan is "NOT release-eligible" and
   23 decisions / 29 held work-domain rows still await the owner. Shipping a `[x]` for it would have
   been a false statement in a tracking document, made by a plan that has nothing to do with it. The
   change was reverted with `git checkout -- .planning/ROADMAP.md`; nothing was committed.
   **The orchestrator should tick 136-10 by hand rather than re-running the recalculation**, or fix
   136-09's disk state first.
3. **`REQUIREMENTS.md` was not marked.** PANEL-01 / PANEL-02 / NOVEL-01 are the requirements this plan
   *contributes to*, not requirements it *completes* — the surfaces those IDs describe are built in
   136-15 through 136-18. Phase 136's own convention agrees: 136-07, which carries the same
   PANEL-01/PANEL-02 pair, did not touch `REQUIREMENTS.md` either.

`STATE.md` was rewritten on the primary checkout at 05:15Z today; a concurrent narrative edit from a
worktree is a merge conflict waiting to happen and adds nothing this summary does not already record.

## Self-Check: PASSED

Files claimed, verified present on disk:

- `shared/discovery_display_strings.py` — FOUND
- `tests/test_discovery_display_strings.py` — FOUND
- `genizah_translations.py` — FOUND (modified)
- `web/static/common.css` — FOUND (modified)
- `.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-10-SUMMARY.md` — FOUND

Commits claimed, verified in `git log --oneline --all`:

- `9ae98f59` — FOUND
- `37345e0a` — FOUND
- `2a40ec4b` — FOUND
- `aa75ecf2` — FOUND

---
*Phase: 136-read-surfaces-connections-panel-work-witnesses*
*Completed: 2026-08-03*
