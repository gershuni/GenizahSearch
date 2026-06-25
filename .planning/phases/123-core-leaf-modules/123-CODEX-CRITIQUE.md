# Phase 123 — Codex Plan Review (pre-execution)

**Date:** 2026-06-25
**Reviewer:** Codex CLI (codex-cli 0.139.0, sandbox read-only)
**Target:** `123-01-PLAN.md` + `123-RESEARCH.md` + `123-CONTEXT.md`, against live `genizah_core.py`
**Verdict:** REQUEST CHANGES → all 5 findings verified against live code and resolved in-plan.

The responsa boundary (the highest-risk decision) was independently confirmed sound:
`_SOFIT_TO_NORMAL`, `_has_line_break_syntax`, `LineGroup`, `_parse_line_break_query`,
`_apply_explosion_guard` are all movable if shimmed; no missed `shared/`→`genizah_core` back-edge
beyond the D-01 set.

## Findings & Resolutions

| # | Sev | Finding | Verified against live code | Resolution |
|---|-----|---------|----------------------------|------------|
| F1 | HIGH | `get_library_display` falsy-`lang` behavior change: RESEARCH used `if effective_lang is None`, but live `genizah_core.py:2228` is `effective_lang = lang if lang else CURRENT_LANG`. `lang=""` would wrongly fall back to English. | CONFIRMED — line 2228 uses `lang if lang else`. | RESEARCH.md + PLAN Task 1 now mandate `if not effective_lang` (falsy), preserving exact semantics. |
| F2 | HIGH | Prescribed imports would trip the plan's own ruff F401 gate: `Optional` (browse_map_utils), `get_top_pairs` (variants), module-level `time` (lists_manager) are all unused. | CONFIRMED — no moved browse fn uses `Optional`; `get_top_pairs` only in the import/fallback (never by VariantManager, nowhere else in core); `time` is imported inside ListsManager methods (10 sites). | PLAN Tasks 1/3/7: import only what the copied code uses; drop `Optional`/`get_top_pairs`/module-level `time`; remove the now-dead `unified_variants` block from genizah_core after VariantManager moves (grep external importers first). |
| F3 | HIGH | SC#5 full-suite gate missing from Task 1 & Task 7 `<verify><automated>` (the prior internal checker only flagged Tasks 2–6, which I'd fixed). | CONFIRMED by inspection. | Appended `&& GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/ -q` to Task 1 and Task 7 — now wired into all 7 cluster boundaries. |
| F4 | MED | Plan said "4 `tr()` call sites" in ListsManager; live count is ~10–11. | CONFIRMED — 11 `tr(` occurrences from class start to EOF. | PLAN Task 7: "grep ALL in-method `tr(` (~11) and replace every one with `_tr(`"; acceptance gate already "no in-method `tr(` remains". |
| F5 | LOW | Re-exporting `_LIBRARY_PREFIX_ALIASES` (a mutable global that starts `None`, reassigned in `_get_library_prefix_aliases`) via shim strands `genizah_core`'s binding at `None`. | CONFIRMED — reassigned at 2268; NO external importer of it from genizah_core. | PLAN Task 1: dropped `_LIBRARY_PREFIX_ALIASES` from the shim re-export (private, no external user, both helpers move together); no identity test for it. |

## Commits (Round 1)
- Plan/research review fixes committed: `5d040e4a docs(123): apply Codex plan review fixes`.

---

# Round 2 (re-review after Round-1 fixes)

**Verdict:** REQUEST CHANGES. Prior: F1/F3/F5 RESOLVED; F2/F4 only PARTIAL (plan fixed, RESEARCH +
Task-7 read_first/done still stale). Plus 5 new findings — all verified against live code and resolved.

| # | Sev | Finding | Verified | Resolution |
|---|-----|---------|----------|------------|
| F2 | — | RESEARCH still prescribed `Optional`/`get_top_pairs`/`time`; plan-only fix | CONFIRMED | RESEARCH Module 2/3/7 import blocks corrected + correction notes. |
| F4 | — | Task 7 read_first/done + RESEARCH still said "4 tr()"; live = 11 | CONFIRMED | All synced to "ALL (~11)". |
| N1 | BLOCKER | `shared/variants.py` needs `from typing import Mapping` (`generate_variants(mapping: Mapping[...])` is a runtime annotation, no `from __future__`); `List` unused | CONFIRMED — `Mapping` at line ~3036, no `List` use in class | Plan Task 3 + RESEARCH Module 3: import `Mapping`, drop `List`. |
| N2 | HIGH | `shared/codicological.py` omits `import re` (class uses `re.` 11×) → NameError | CONFIRMED — 11 `re.` calls | Plan Task 5 + RESEARCH Module 4: add `import re`. |
| N3 | HIGH | `shared/joins_manager.py` omits `import time` (3 module-scope `time.time()`, no local import) → NameError | CONFIRMED — 3 uses, 0 local imports (opposite of ListsManager) | Plan Task 6 + RESEARCH Module 6: add module-level `import time`. |
| N4 | HIGH | genizah_core's OWN imports go dead after moves: `itertools`+`Mapping` (Task 3), `dataclass`+`field` (Task 4) → F401 | CONFIRMED — itertools/Mapping VariantManager-only; the only 2 `@dataclass` (ResponsaComponent, LineGroup) + only `field()` use all move. **Codex erred on `List`** — it has 17 uses, must STAY (also Optional 9×) | Plan Tasks 3/4 + RESEARCH: remove the dead imports (ruff F401 = ground truth); explicitly KEEP List/Optional. Added "derive imports from actual code; smoke-test + ruff = two-sided gate" principle. |
| N5 | MED | RESEARCH Q1 "engine-side (NOT moved)" table still listed `_has_line_break_syntax`/`LineGroup`/`_parse_line_break_query`, contradicting Q2 + plan (which MOVE them) | CONFIRMED | RESEARCH Q1 table corrected with a CORRECTION note pointing to Q2/plan as authoritative. |

## Commits (Round 2)
- Committed: see git log `docs(123): apply Codex round-2 review fixes`.
