---
phase: 121
reviewers: [codex]
reviewed_at: 2026-06-21T12:17:24Z
plans_reviewed: [121-01-PLAN.md, 121-02-PLAN.md, 121-03-PLAN.md]
verdict: CHANGES REQUESTED
risk: MEDIUM
---

# Cross-AI Plan Review — Phase 121 (i18n Polish)

Reviewer: **Codex** (codex-cli 0.139.0, gpt-5.x, read-only repo access). Model: default.
Brief: `_tmp/codex-121-plan-review-brief.md`. Raw output: `_tmp/codex-121-plan-review-output.md`.

## Codex Review

**Summary**

The plans are mostly repo-grounded: the cited reload behavior, shared translation gate, 17 missing
exact keys, badge strings, guard template, smoke fixture, and RTL `flex-row-reverse` sites all check
out. Not execute-as-is, though: there is at least one additional visible Joins Lab glossary drift the
plans miss, and a couple of guard/test details can false-pass the intended acceptance.

**Strengths (all repo-verified by Codex)**
- Live-switch research correct: `web/main.py:1004-1009` `set_language()` + `ui.navigate.reload()`; no Joins-Lab-only live-DOM switch needed.
- `web/translations.py:41-44` `tr()` is language-gated; adding HE keys won't leak Hebrew to EN.
- Drift value as claimed: `Open in Joins Lab` = `פתח במעבדת ההצטרפות` at `genizah_translations.py:2538`.
- XLSX sheet name raw at `web/pages/joins_lab.py:2252`; `tr` imported at `:103`.
- `badge_and_tooltip()` returns exactly the 3 strings at `shared/joins_lab.py:653-659`.
- Smoke fixture is `joins_lab_smoke_runner` at `tests/render_smoke/conftest.py:324`.
- RTL sites real: `_pg_dir` `candidate_grid.py:1363`, `_nav_dir_class` `compare_modal.py:792`.
- Desktop guard template helpers exist as cited in `tests/test_join_workbench_i18n.py`.

**Concerns**
- **HIGH** — The "one glossary drift" assumption is false. Visible reset UI still uses `מעבדת החיבורים`:
  `Clear all Joins Lab state...` → `...מעבדת החיבורים...` at `genizah_translations.py:4257-4258`, and
  `Clear Joins Lab` → `נקה מעבדת החיבורים` at `:4259`. Both are user-facing in the reset dialog/tooltip at
  `web/pages/joins_lab.py:1318` and `:1342`. Plan 01 must fix these too (or justify the exclusion).
- **MEDIUM** — Plan 01's `Filter by shelfmark…` HE (`סינון לפי סימן מדף…`) is inconsistent with the
  established term: `Shelfmark` = `מספר מדף` everywhere, and `Filter by shelfmark` = `סנן לפי מספר מדף`
  at `:1801`. Use `סנן לפי מספר מדף…`.
- **MEDIUM** — Plan 02 Compare RTL assertion can false-pass: the pagination row also carries
  `flex-row-reverse`. Identify the Compare nav row by class signature (`justify-between px-4 py-2 flex-wrap`
  at `compare_modal.py:793-794`) vs pagination (`justify-center mt-2` at `candidate_grid.py:1364-1365`),
  not "any visible row that isn't pagination."
- **MEDIUM** — Plan 01 coverage verify only asserts key PRESENCE, not that the value is Hebrew/non-empty/
  ≠ the English key. A key accidentally mapped to English would pass. Assert each value matches
  `[֐-׿]` and `T[k] != k`.
- **LOW** — Second badge call site `compare_modal.py:470-471` also wraps `tr(tooltip_text)`; `BADGE_STRINGS`
  covers it functionally, but the plan should mention both sites.
- **LOW** — `tests/render_smoke/conftest.py:26-33` has a stale docstring referencing `joins_lab_user_runner`;
  the real fixture (`:324`) is correct. Executors should ignore the stale docstring.
- **LOW** — Plan 03 verify is phrase-fragile: it requires the literal `anchor transcription`, but a natural
  checklist could say "Anchor pane" / "numbered transcription lines" and miss the substring.

**Suggestions**
- Plan 01: fix reset-control drift — `Clear Joins Lab` → `נקה את מעבדת הצירופים`; `Clear all Joins Lab state...`
  → use `מעבדת הצירופים`, not `מעבדת החיבורים`.
- Plan 01: `Filter by shelfmark…` → `סנן לפי מספר מדף…`.
- Plan 01: consider `Add anchor + this candidate...` → `הוסף את העוגן ואת המועמד הזה לפאזל הקטעים`
  (more natural than carrying `+` into RTL Hebrew).
- Plan 01: strengthen the 17-key verify — assert each value contains `[֐-׿]` and `T[k] != k`.
- Plan 02: assert the Compare RTL row by class signature (or a marker), not "any row with flex-row-reverse".

**Risk Assessment**

Overall **MEDIUM**. Directionally sound and most cited code facts accurate, but the missed visible glossary
drift means the phase could declare D-06/consistency complete while shipping inconsistent Hebrew; the
render-smoke assertion needs tightening to avoid a misleading green.

Verdict: **CHANGES REQUESTED.** Must fix the additional `מעבדת החיבורים` drift and the shelfmark term before
execution; tighten the Compare RTL assertion before relying on Plan 02 as a guard.

---

## Consensus Summary (single reviewer — Codex)

### Agreed Concerns (orchestrator-verified against source before recording)

| # | Severity | Finding | Source-verified? | Target plan / fix |
|---|----------|---------|------------------|-------------------|
| 1 | HIGH | 2nd + 3rd glossary drift `מעבדת החיבורים` (reset dialog) | ✅ confirmed at `genizah_translations.py:4257-4259`; used at `joins_lab.py:1318,1342` | **Plan 01** — add `Clear Joins Lab` → `נקה את מעבדת הצירופים` and `Clear all Joins Lab state: anchor, builder, triage, filters` → `נקה את כל מצב מעבדת הצירופים: עוגן, בונה, מיון וסננים`; add both keys to the `test_entry_point_keys` / consistency check in Plan 02 |
| 2 | MEDIUM | `Filter by shelfmark…` HE inconsistent (`סימן מדף`/`סינון`) | ✅ confirmed `Shelfmark`=`מספר מדף` ×6; `Filter by shelfmark`=`סנן לפי מספר מדף` @:1801 | **Plan 01** — change proposed HE to `סנן לפי מספר מדף…` (keep U+2026) |
| 3 | MEDIUM | Compare RTL assertion false-pass (pagination also has flex-row-reverse) | ✅ class signatures confirmed (`justify-between px-4 py-2 flex-wrap` vs `justify-center mt-2`) | **Plan 02** — match the Compare nav row by its class signature, not "not-pagination" |
| 4 | MEDIUM | Coverage verify doesn't assert value is Hebrew | ✅ Plan 01 verify only checks `k in T` | **Plan 01** — assert each new value `[֐-׿]` and `T[k] != k` |
| 5 | LOW | 2nd badge site `compare_modal.py:470-471` unmentioned | ✅ confirmed | **Plan 02** — note both badge call sites (BADGE_STRINGS already covers) |
| 6 | LOW | Plan 03 verify substring `anchor transcription` fragile | n/a (plan-text) | **Plan 03** — loosen the verify keyword set |

### Notes
- All HIGH/MEDIUM code-fact findings were re-checked against the live repo by the orchestrator and
  confirmed real — they are not speculative. The replan should treat #1 and #2 as locked corrections.
- Codex confirmed the bulk of the plans (the 17-key list, badge strings, fixture name, RTL sites, reload
  verdict) are accurate — only the items above need change.

### Resolution (2026-06-21)
All 6 findings were applied directly to the plans (user chose direct edits over a `--reviews` re-spawn)
and committed as `39a19471`:
- #1 HIGH — Plan 01 fixes both reset-dialog drifts (`Clear Joins Lab`, `Clear all Joins Lab state...`) to `מעבדת הצירופים`.
- #2 MED — Plan 01 shelfmark HE → `סנן לפי מספר מדף…`.
- #3 MED — Plan 02 Compare-RTL assertion now matches by class signature (`flex-row-reverse` + `justify-between` + `flex-wrap`).
- #4 MED — Plan 01 verify asserts each new value is Hebrew and `!= key`.
- #5 LOW — Plan 02 notes the 2nd badge site (`compare_modal.py:470-471`).
- #6 LOW — Plan 03 verify substring loosened.

The plan-checker was NOT re-run (direct-edit path). Edits were textual (action/acceptance/verify) plus one
added `must_haves.truth`; all 3 plans still parse with valid frontmatter and `requirements: [FND-07]`.
