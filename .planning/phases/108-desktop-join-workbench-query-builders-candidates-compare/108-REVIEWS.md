---
phase: 108
round: 3
reviewers: [codex]
reviewed_at: 2026-06-05T10:07:43Z
plans_reviewed: [108-01-PLAN.md, 108-02-PLAN.md, 108-03-PLAN.md, 108-04-PLAN.md]
model: codex-cli 0.136.0 (default model)
note: >
  ROUND 3 — re-review of the round-2 fold-in (RR-9..RR-12), code-verified with
  --dangerously-bypass-approvals-and-sandbox. Rounds 1 and 2 preserved at
  108-REVIEWS-round1.md / 108-REVIEWS-round2.md. Verdict: RR-11 + RR-12 CONFIRMED; RR-9 PARTIAL
  (the per-word modifier wiring hits a real parser limitation); RR-10 PARTIAL (Plan 02 Task 0
  still self-fails ruff); plus a NEW pre-existing finding (compose() drops JA/Flex/Bidir).
  Orchestrator cross-check ADDED below the Codex output: the `(#a/%b)` form is NOT a Phase-108-new
  bug — the shipping `generate_tabular_syntax` produces the identical form — which reframes RR-9 as
  a product decision, not a regression.
---

# Cross-AI Plan Review — Phase 108 (Round 3)

## Codex Review

**Summary**

The round-2 fold-in is improved but **not safe to execute yet**. RR-11 and RR-12 landed correctly. RR-9
is only partial: the plan wires per-box modifiers, but live parser semantics do **not** support
per-alternative modifiers inside `(a/b)` slash groups, and negation inside an OR group is especially
wrong. RR-10 is also partial: Plan 03 imports were moved correctly, but Plan 02 Task 0 still runs Ruff
immediately after adding `QFrame`/`QSpinBox`/`QEvent`, before Task 1 uses them.

**Round-2 Fix Verification**

| RR | Status | Evidence |
|----|--------|----------|
| RR-9 | **PARTIAL** | Plan 02 adds per-box `mods`, active-box focus, `_on_modifier_changed`, `_decorate`, and decorated slash-join (`108-02-PLAN.md:258-262`, `:317-330`, `:342-350`); decoration order matches `genizah_core.py:6008-6027`. Active-word mechanism correctly identified (`genizah_app.py:1559`, `:1735`, `:1779-1784`). **But** the live parser strips `-/%/#` from the whole token then `inner.split('/')` takes raw words (`genizah_core.py:6121-6163`): `(#word/%alt)` yields OR words containing literal `#`/`%`, not modifier flags. Plan 01's decorated-OR test is too weak (`108-01-PLAN.md:332-336`). |
| RR-10 | **PARTIAL** | Plan 02 correctly excludes Plan-03-only imports (`108-02-PLAN.md:181-188`); Plan 03 moves `QGridLayout`/`QTableWidget`/`QTableWidgetItem`/`SearchThread` to Wave 3 (`108-03-PLAN.md:151-157`). But Plan 02 Task 0 runs `ruff check desktop/join_workbench.py` (`108-02-PLAN.md:193-201`) right after adding `QFrame`/`QSpinBox`/`QEvent`, BEFORE Task 1 uses them; `ruff.toml:14-20` selects F401, so Task 0 self-fails. |
| RR-11 | **CONFIRMED** | Plan 01 mirrors the existing line-height guard: `has_size_category`, `sc_col`, conditional `col_names`, `None` fallback (`108-01-PLAN.md:170-182`) + absent-column test (`:199-202`). Live guard pattern at `shared/fjms_service.py:3017-3029`; SELECT-failure path at `:3035-3060`. |
| RR-12 | **CONFIRMED** | Plan 03 guards before `page-1` in `_enqueue_image_for_pane` (`108-03-PLAN.md:272-280`); Plan 04 passes `c.page` through without arithmetic (`108-04-PLAN.md:156-159`). Live `Candidate.page` optional (`shared/joins_lab.py:103-104`); None-page fixture (`tests/test_joins_lab.py:121-124`); helper `_image_url_for_idx` (`desktop/join_workbench.py:189-197`). |

**RR-1..RR-8 Regression Check**

RR-1 slash-group OR, RR-2 Candidate model, RR-3 public Add-as-Join, RR-4 i18n registration, RR-5
other-side page-position omission, RR-6 batch reuse, RR-7 per-page images remain intact; RR-8's import
split is conceptually intact but still blocked by the RR-10 Task-0 ruff ordering bug.

**New Concerns**

- **HIGH — Decorated OR groups are not parser-supported.** Plan serializes mixed per-box modifiers as
  `(#שלום/%שלומות)` (`108-CONTEXT.md:70-73`, `108-02-PLAN.md:346-350`), but the live parser only
  supports the modifier OUTSIDE the OR token, e.g. documented `#(שלום/שלומות)` (`genizah_core.py:5727-5728`);
  inside OR, alternatives are raw strings from `inner.split('/')` (`genizah_core.py:6139-6163`). This
  silently turns modifiers into literal search text.
- **HIGH — Negation inside slash-OR is semantically wrong.** Plan `_decorate` returns `"-" + text`
  (`108-02-PLAN.md:342-343`), then slash-joins (`:346-350`). Live negation is recognized only before OR
  parsing (`genizah_core.py:6115-6119`); `(-עץ/שלום)` is not a negated component.
- **HIGH — JA/Flex/Bidir global toggles still appear no-op on the planned search path.** Plan 02 defines
  `_responsa_opts()` with JA/flex/bidir (`108-02-PLAN.md:337-341`), but `SideQuery` only stores
  `variants` and `page_position` (`shared/joins_lab.py:47-65`); `compose()` HARDCODES `ja`,
  `flex_spacing`, `bidirectional` = False (`shared/joins_lab.py:741-748`); Plan 03 passes the `ro` from
  `compose(side)` directly to `SearchThread` (`108-03-PLAN.md:377-381`). The builder's `_responsa_opts()`
  is never consumed.
- **LOW — Active-box removal cleanup is underspecified.** Focus tracking by object identity is coherent
  (`108-02-PLAN.md:317-330`), but added OR boxes are removable (`:286-288`) and the plan does not clear
  `_active_box` if the focused box is deleted. The existing dialog clears stale active-word state on
  removal (`genizah_app.py:1932-1934`).

**Suggestions**

1. For RR-9: either add parser support for per-alternative modifiers inside OR groups (with tests
   asserting flags for `(#x/%y)` and `(-x/y)`), or constrain the UI so slash-OR rows cannot mix per-box
   modifiers. Hoisting only works when the same modifier applies to the whole group, e.g. `#(a/b)`.
2. Move Plan 02 import edits into the same task that adds `JoinQueryBuilder`, or remove ruff from Task 0
   and run it after Task 1.
3. In Plan 03, do not rely on `compose()` for JA/flex/bidir. After composing, merge
   `self.builder._responsa_opts()` into `ro`; do the same for the other-side builder before
   `apply_cross_side`.

**Risk Assessment**

Overall risk: **HIGH**. **RELEASE BLOCKERS:** RR-9 decorated-OR / negation semantics; RR-10 Plan 02
Task-0 ruff failure; global JA/Flex/Bidir toggles still no-op on the planned execution path.

---

## Consensus Summary

Single reviewer (`--codex`), code-verified. Round 3 confirms RR-11 + RR-12 landed and RR-1..RR-8 did not
regress, but surfaces three execution blockers. Two are clean mechanical fixes; one (RR-9) is a genuine
product/parser decision. **Orchestrator cross-check added below** — it changes how finding #1 should be
read.

### Orchestrator cross-check on the RR-9 "decorated-OR" finding (NOT a Phase-108-new bug)
The shipping `TabularQueryBuilderDialog` serializes its query through
`genizah_core.generate_tabular_syntax()` (`genizah_app.py:2070-2073`). That function (`genizah_core.py:6014-6033`)
does the IDENTICAL thing the round-2 plan does: it decorates each word (`%`→`#`→`#`-append→`*`→`*`,
`genizah_core.py:6014-6027`) and then, for a multi-alternative component, emits
`f"({'/'.join(words_with_mods)})"` — i.e. `(#a/%b)`. So:
- The `(#a/%b)` form the plan produces is **consistent with the production dialog**, NOT a new defect.
- The parser limitation Codex found is **pre-existing and shared**: in BOTH the shipping dialog and the
  planned builder, per-alternative modifiers inside a multi-box OR group degrade to literal characters
  (`genizah_core.py:6139-6163`). Single-box modifiers (`#שלום`, `שלום*`, `-עץ`) work correctly in both.
- Therefore RR-9 is best treated as a PRODUCT DECISION about a known engine limitation, not a
  regression to "fix before any execution." The honest options:

  - **(A) Match the existing dialog** — reuse the proven `generate_tabular_syntax` decoration (or keep
    the equivalent `_decorate`). Single-box mods work; multi-box-OR mods are literal-inside-`(…)` exactly
    like the shipping Tabular Search. Lowest risk, consistent app-wide. Document the limitation; a future
    engine phase can lift it.
  - **(B) Hoist row-level modifiers outside the group → `#(a/b)`** — parser-correct, and it makes the
    COMMON case work (the OR-alternatives are usually spelling variants of one word, so applying the same
    `#`/`%` to all of them is what you actually want). Cost: modifiers become PER-ROW (not per-box); you
    cannot give two alternatives different modifiers. Diverges from the existing dialog's serialization.
  - **(C) Extend the engine parser** to apply per-alternative modifiers inside OR groups (`(#a/%b)`).
    Correct in full generality but an engine change with blast radius into the existing dialog + Responsa
    search — out of Phase-108 scope; a separate engine phase.

  Recommendation: **(B) hoist row-level `#(a/b)`** for 108 — it is parser-correct AND fixes the common
  spelling-variant case (which (A) leaves silently broken), without an engine change. Keep negation
  row-level too: `-(a/b)` is parsed as a negated component (the `-` is stripped before the OR check at
  `genizah_core.py:6115-6119`, so `-(a/b)` DOES negate the whole group — verify in the replan). This
  makes modifiers a PER-ROW control rather than per-box — a small UI-model change from round 2.

### Must-fix before execution
1. **RR-9 OR-modifier semantics (HIGH).** Decide (A)/(B)/(C) above — this is a CONTEXT-level decision
   (touches revised D-04). The replan must update the serialization AND strengthen Plan 01's test to
   assert PARSER flags (e.g. `#(a/b)` → both alternatives carry `grammatical_prefixes`; `(#a/b)` → does
   NOT), so the test can't pass on a literal-`#` string.
2. **JA/Flex/Bidir no-op (HIGH).** `compose()` hardcodes them False and `SideQuery` can't carry them
   (`shared/joins_lab.py:741-748`). Fix in the DESKTOP pane (keep Phase 106 frozen): after
   `query_str, ro, page_pos = compose(side)`, merge the builder's `_responsa_opts()` JA/flex/bidir into
   `ro` before `SearchThread`; merge the other-side builder's opts into `b_ro` before `apply_cross_side`.
   Add a test asserting the merged `ro` carries ja/flex/bidir when toggled.
3. **Plan 02 Task 0 ruff F401 (blocker).** Don't run `ruff check` in Task 0 when the just-added
   `QFrame`/`QSpinBox`/`QEvent` are still unused — either fold the import edit into Task 1 (the builder
   that uses them) or move the ruff gate to the end of Task 1. (RR-10's Plan-02↔Plan-03 split is right;
   only the intra-Plan-02 task ordering is wrong.)

### Should-resolve
4. **Active-box cleanup (LOW).** Clear `_active_box` when the focused OR-box is removed (mirror
   `genizah_app.py:1932-1934`), so a deleted box can't receive modifier writes.

### Confirmed landed
RR-11 (size_category column guard), RR-12 (page-None guard), and RR-1..RR-8 (no regression).

### Recommended next step
Route through `/gsd-plan-phase 108 --reviews` once more. Finding #1 needs a one-line CONTEXT decision
(A/B/C — recommend B, hoist `#(a/b)` per-row); #2/#3/#4 are mechanical. After this the plans should be
execution-ready — the loop has now driven 3 rounds with decreasing severity (round 1: 4 HIGH; round 2:
1 HIGH + 2 blockers; round 3: 2 real blockers + 1 product decision, the rest pre-existing/known).

### Divergent Views
None — single reviewer (orchestrator cross-check supplements, does not contradict).
