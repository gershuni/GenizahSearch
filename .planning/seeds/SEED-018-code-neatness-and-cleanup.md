---
id: SEED-018
status: shipped
planted: 2026-06-23
planted_during: 2026-06-23 product-quality fan-out audit (6 agents + Codex). Register: .planning/audit-2026-06-23-product-quality/MASTER.md
trigger_when: CLOUD-AUTO, mechanical. SPLIT BY FILE-CONFLICT — the "core slice" (#32/#34/#40 in genizah_core.py + #33's genizah_app.py status dicts) SHARES genizah_core.py/genizah_app.py with SEED-013 → run together as ONE sequential session. The "non-core slice" (#31 .gitignore, #44/M5 shared_export_utils move, #33's candidate_grid colors) is PARALLEL-SAFE with round-1.
scope: small-medium (constants extraction + cleanup + one module move; no behavior change)
---

# SEED-018: Code-neatness & cleanup

> From the 2026-06-23 audit. All Codex-CONFIRMED (one PARTIAL noted). Zero behavior change; these are
> maintainability + parity-enabling refactors. NOTE #35 (print in telemetry self-test) is BY DESIGN — skip.

## NON-CORE slice (parallel-safe with round-1)

### #31 — `_tmp/` only partially gitignored (LOW · EASY)
`.gitignore` has no broad `_tmp/` rule; `git ls-files _tmp` shows tracked `.md` probes.
**Fix:** decide scratch vs notes. If scratch: add `_tmp/` to `.gitignore` + `git rm --cached` the tracked
files (keep them on disk) or move intentional docs into `.planning/`. (Confirm with Hillel which `_tmp/*.md`
are keepers before untracking.)

### #44 + M5 — `shared_export_utils.py` at repo root blurs ownership (LOW · MED)
Root-level, imported by `genizah_app.py`, `web/export_service.py`, `shared/docx_export.py`; packaged at
`build_app.bat:27`. **Fix:** move impl to `shared/export_utils.py`, leave a root shim re-exporting it,
update imports + `build_app.bat` + the `.spec`. M5 = same pattern for any other root-level shared modules
(survey, list, fix the worst first).

### #33 (web part) — Triage/status colors duplicated (LOW-MED · MED)
`web/components/candidate_grid.py:363` (glyph map) + `:508` (literal colors near `TRIAGE_ICONS`).
**Fix (web side):** route all triage display through the shared `TRIAGE_ICONS` tokens
(`shared/joins_lab.py:632`); remove literal duplicates.

## CORE slice (sequence with SEED-013 — shares genizah_core.py / genizah_app.py)

### #32 — RRF `k=60` hardcoded (LOW · EASY)
`genizah_core.py:9380` + tests (`test_local_post_dedup_merge.py:87`, `test_side_index_merge.py:50…127`).
**Fix:** `RRF_K = 60` constant near the merge code; import it in tests.

### #34 — Magic timeout literals alongside named consts (LOW · EASY)
`genizah_core.py:4604, 4747` (`timeout=15`), `4904` (`timeout=5`). **Fix:** named per-operation timeout
constants (NOT one global); keep NLI/Rosetta-specific values explicit. (Desktop image-loader timeouts
`image_loader.py:127`, `join_workbench.py:734` are owned by SEED-015 — don't touch here.)

### #40 — Commented-out code + stale TODO (LOW · EASY · PARTIAL)
`genizah_core.py:2287, 2308` commented-out `@staticmethod` → delete (git remembers).
`shared/local_indexer.py:169, 249` `tantivy >= 0.26` TODOs → re-check against the pinned tantivy version,
remove or convert to a tracked upgrade condition. (Codex: the cited `search_tokenizer.py:43` is NOT a stale
TODO — leave it.)

### #33 (desktop part) — duplicate status-icon dicts (LOW-MED · MED)
`genizah_app.py:15510, 15548` duplicate pending/approved/rejected/draft icon dicts; `_TRIAGE_GLYPH`
`desktop/join_workbench.py:434`. **Fix:** centralize display tokens in one shared non-UI module; Qt + web
adapters translate tokens to icons/styles. (Coordinate with the web part above so both consume one source.)

## Tests required
- `RRF_K` referenced by merge code + tests (no magic 60 left in those test files) — `grep` assertion ok.
- Import shim test: `import shared_export_utils` and `from shared.export_utils import *` both resolve to the
  same symbols; build_app.bat/.spec updated.
- Centralized-token test: web + desktop triage/status glyphs derive from the shared token source.
- ⚠ Core slice shares files with SEED-013 → run that session's tests too; ruff clean.

## Done when
Constants extracted, module moved (with shim), duplicate tokens centralized, tests green, ruff clean,
zero behavior change. (`_tmp/` #31 is split OUT — see corrections.)

---

## Codex review corrections (2026-06-23) — SEED-018 was NOT-READY; re-scoped here
**#31 is NOT cloud-auto** — "confirm which `_tmp/*.md` are keepers" is a human repo-policy decision, and
`git rm --cached _tmp/118-fragment-joins-schema-probe.md` must not run without confirmation. **Split #31 out
as decision-gated** (one question for Hillel: is `_tmp/` scratch? then add the ignore + untrack). The
autonomous part of this seed = #32, #33, #34, #40, #44/M5 only.

**#44/M5 caller list was badly under-enumerated.** Real `shared_export_utils` references (enumerate via `rg`,
don't trust this list as exhaustive): `filter_text_dialog.py:13`, `genizah_app.py:80-81` (+ local imports),
`shared/docx_export.py:90,95`, `shared/search_serializer.py:324,891`, `web/export_service.py:44,566`,
`web/pages/parallels.py:39`, `desktop/telemetry.py:959`, multiple `tests/`, `build_app.bat:27`,
`GenizahSearchPro.spec:9`. **Leave a root shim**; add a test that the shim and `shared.export_utils` expose
the same symbols (representative: `build_rich_snippet_cell`, `sanitize_text_for_excel`,
`remove_highlight_markers`, `sanitize_cache_filename`, `coerce_img_page_cell`, `build_expanded_context`).
Add packaging/source checks for `build_app.bat` + `.spec`.

**#32 missed the default arg:** also `genizah_core.py:7667` (`def _rrf_merge(..., k: int = 60, ...)`) — replace
BOTH the default and the call at `:9380` with `RRF_K`.

**#33 corrections:** (a) `candidate_grid.py:508` is STALE as a "literal colors" claim — it already derives
`_TRIAGE_COLORS` from `shared.joins_lab.TRIAGE_ICONS`; only the glyph map at `:363` is the dup. (b) Correction
statuses (`genizah_app.py:15510,15548`) are a DIFFERENT concept from joins triage — use SEPARATE shared token
constants, not one blended map. (c) `tests/test_join_workbench_vs.py:1061` asserts the literal `_TRIAGE_GLYPH`
map → that test must change with the implementation.

**#40:** Tantivy is pinned `0.25.1` (`requirements.txt`/`requirements-lock.txt`) — the `tantivy >= 0.26` TODO
at `local_indexer.py:169,249` is NOT satisfied; keep it as an upgrade-gated TODO (or move to a tracked issue).
Only delete the commented-out `@staticmethod` at `genizah_core.py:2287,2308`.

**#34 scope question:** also `genizah_app.py:5947` (`urlopen(..., timeout=15)`) — decide explicitly in/out.

**Conflict note:** the CORE slice (#32, #34, #40, #33-desktop) shares `genizah_core.py`/`genizah_app.py` with
SEED-013 → ONE sequential session. Non-core slice = #44/M5 move + #33-web (candidate_grid glyph map) → these
will later collide with SEED-017 on `candidate_grid.py`; run before 017.
