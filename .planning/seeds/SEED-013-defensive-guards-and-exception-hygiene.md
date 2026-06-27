---
id: SEED-013
status: shipped
planted: 2026-06-23
planted_during: 2026-06-23 product-quality fan-out audit (6 agents + Codex verification). Register: .planning/audit-2026-06-23-product-quality/MASTER.md
trigger_when: CLOUD-AUTO. No product decision needed — all items are confirmed, self-contained, testable. SHARES genizah_core.py + genizah_app.py with SEED-018's core slice → run the two as ONE sequential cloud session (NOT parallel). Disjoint from SEED-014 (web/pages) and SEED-016 (shared services).
scope: small (defensive guards + targeted logging at ~6 confirmed swallow/index sites; dual-app where genizah_core.py is shared)
---

# SEED-013: Defensive guards & exception hygiene

> From the 2026-06-23 audit. All items Codex-verified CONFIRMED (verbatim evidence:
> `_tmp/codex-audit-output.md`). These are cheap correctness/observability fixes, not behavior changes.
> Rule: replace broad `pass`/swallow with **targeted logging + safe fallback**; never silence a data path.

## Findings (file:line + fix direction)

### #12 — LOCAL-filter cycle `ValueError` on corrupt state (MED · 1LINE) [desktop]
`genizah_app.py:19023, 19032, 19041` — `states.index(self._local_filter_state_*)` raises if a restored
session value isn't in `['all','only_local','no_local']`.
**Fix:** one helper that normalizes unknown → default: `idx = states.index(v) if v in states else 0`.

### #13 — `text_position_combo` negative index → wrong value (MED · 1LINE) [desktop]
`genizah_app.py:17832, 18394` — `[None,'start','end','line_start','line_end'][combo.currentIndex()]`;
`currentIndex()==-1` silently selects `'line_end'` instead of `None`.
**Fix:** guard `return opts[i] if 0 <= i < len(opts) else None`.

### #7 — Silent exception swallowing on data paths (MED · EASY) [both/core]
- `genizah_core.py:722-727` corrupt dynamic-weights file `except Exception: pass` → user's custom ranking
  silently ignored. Add `logger.warning(..., exc_info=True)`.
- `genizah_core.py:1723` chunk-hit dedup `except (KeyError,IndexError,TypeError): pass` → silently drops
  search matches if result shape drifts. Log before continue.
- `genizah_core.py:1862-1865` LOCAL-LAB scan failure is logged but **not surfaced** → user thinks LOCAL
  search succeeded. Return a `degraded`/partial flag callers can show. (Codex: this one is logged, so it's
  the "surface to UI" half that's missing.)
- `desktop/result_dialog.py:2419-2426` `except re.error: pass` → highlight silently off. Log the bad pattern.

### #37 — `resolve_external_images` warning lacks `exc_info` (LOW · 1LINE) [web]
`web/components/image_resolution.py:292-297` → use `logger.exception(...)` or add `exc_info=True`.

### #39 — Sleep-prevention bare `except: pass` (LOW · EASY) [desktop]
`gui_threads.py:14-32` `_prevent_sleep`/`_allow_sleep` → `logger.debug(..., exc_info=True)` (best-effort,
debug level to avoid noise).

## Out of scope here (moved to avoid file collisions)
- #22 (`except NameError` in `web/pages/search.py`) → **SEED-014** (web-pages owner).
- #23, #36 (`web/api.py` NLI snapshot lock / image-fetch status logging) → **SEED-015** (owns web/api.py
  image paths).

## Tests required
- `tests/test_audit_2026_06_23_guards.py`: corrupt-state filter cycle returns default not raises (#12);
  combo `-1`/out-of-range → `None` (#13); weights-corrupt path logs + falls back to defaults (#7a);
  dedup drop path logs (#7b); LOCAL-LAB failure sets the degraded flag (#7c). Pure-unit, no Tantivy/Qt.
- ⚠ `genizah_core.py` is imported by many tests — after editing, `grep -rl` the touched helpers in `tests/`
  and run those (per the "test shared-function callers before push" lesson).

## Done when
All sites guarded/logged, new tests green, `ruff check` clean on changed files, no behavior change
beyond added observability + safe fallbacks (EXCEPT the LOCAL-LAB degraded contract — see corrections).

---

## Codex review corrections (2026-06-23) — apply before execution
- **#12 state names:** the live fields are `_local_filter_state_search` / `_local_filter_state_composition`
  / `_local_filter_state_parallels` (NOT lab/main/no). The `*` helper covers all three — name them explicitly.
- **#7 sibling swallow sites MISSED — add them:**
  - `genizah_core.py:1860-1861` — LOCAL-LAB dedup has the SAME silent-drop pattern as `:1723`; fix both.
  - `desktop/result_dialog.py:1957-1958` and `:2202-2203` — additional regex-highlight `except re.error: pass`
    sites beyond `:2426`. Fix together (or scope #7 by function name and justify leaving them).
- **#7 LOCAL-LAB degraded flag IS a behavior/contract change** (not pure logging) → ⚠ DECISION: define the
  contract before coding — a structured `degraded`/warning field in the search-result envelope (or thread
  result) + a UI surface. Tests must assert BOTH the flag and the displayed-warning path. (If we don't want
  to touch the contract now, split this sub-item into SEED-019/015 and keep #7 here as logging-only.)
- **#39 wording:** code catches `Exception` and passes (not a bare `except:`) at `gui_threads.py:21,31`.
- **Assignment:** #22 belongs to SEED-014 (web/pages owner) ✓ already moved; #23/#36/#37/#38 resolved in
  **SEED-021** (see MASTER). #37 (`image_resolution.py`) MOVED OUT of this seed to SEED-021 so that file has
  one owner (was colliding with #38).
- **Tests:** add `caplog` assertions for every logging site; add a LOCAL-LAB-branch regression (not just the
  main branch); add a caller/UI test for the degraded flag. After edits run `tests/test_image_resolution.py`,
  result-dialog unit tests, and local/lab composition tests around `dynamic_rank_map`.
