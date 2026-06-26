# Phase 125 Pre-Flight — Codex Regression Audit (2026-06-26)

**Trigger:** user said "Ask Codex" when asked whether to proceed to Phase 125 or audit first,
after the Phase-123 BOM regression (genizah_core.py) surfaced late. Codex was given a written
brief ([[feedback_codex_during_discuss_phase]]) and ran read-only checks itself.

## Codex verdict: **BOUNDED AUDIT FIRST**

Codex found a concrete defect the first BOM fix missed and recommended a bounded audit before
committing to the hardest phase, plus a standing process gate.

### Defect found + fixed
- **`shared/responsa.py` still carried a UTF-8 BOM at HEAD.** Same root cause as genizah_core.py:
  Phase-123 commit `674d16b5` BOM'd both files; the first fix (`29d51f4a`) only stripped
  genizah_core.py. Traced: no BOM at the `57023501` extraction → BOM at `674d16b5` → persisted to
  HEAD. **Fixed** (`<this phase>`): byte-level strip; zero behavior change.

### Checks run — all CLEAN after the responsa fix
| Check | Result |
|-------|--------|
| BOM scan, ALL tracked `.py` | **0 / 611** files carry a BOM (both BOMs gone, none elsewhere) |
| Runtime facade identity (`genizah_core.X is shared.Y.X`) | **12 / 12** moved names — same object, no aliasing drift |
| AST method-completeness, 7 moved classes (base `08c43bea` vs HEAD) | **0 dropped, 0 added** — VariantManager 17, CodicologicalManager 18, JoinsManager 22, ListsManager 53, MetadataManager 46, Indexer 7, _BoundedLRUCache 13 |
| Codex line-level body spot-check (lists_manager, responsa) | matched base modulo expected `tr`→`_tr` + section-comment noise |
| Codex facade-shim symbol scan (50 Phase-122–124 shim imports) | 0 missing target symbols |
| `git diff --check` (whitespace) | cosmetic trailing-WS in genizah_core.py (~48) + EOF blank lines in 4 shared modules — **accepted** (pre-existing god-file style; behavior-neutral; byte-hygiene gate covers NEW code going forward) |
| ruff (responsa.py, genizah_core.py) | clean |

**Conclusion:** the 122–124 extractions are faithful — no body drift, no dropped methods, facade
identity intact. The only real defect (the two BOMs) is resolved. **CLEAR TO PROCEED to Phase 125.**

## Adopted process gate (Codex's #1 recommendation) — applies to 125–127

**Source-integrity gate — run BEFORE pytest/review classification on every extraction commit:**
1. **No BOM / strict UTF-8 / LF** on every touched `.py` (`head -c3 | xxd`; the 611-file scan).
2. **`git diff --check`** clean on the NEW changes (`git diff --check HEAD`).
3. **Runtime facade identity** — `genizah_core.X is shared.Y.X` for every moved name (Python is
   available locally; Codex's sandbox is not — so the ORCHESTRATOR must run this, not Codex).
4. **AST method/symbol-completeness** — moved classes' method sets and modules' function sets must
   match the milestone base `08c43bea` (catches silent non-verbatim drops in untested paths).
5. Keep the existing **base-vs-HEAD NAME-level test comparison + facade-name diff** (Phase-124 lesson).

Rationale: the BOM was a whole-file artifact that Python tolerates on import, so it passed pytest +
per-phase Codex review + the verifier, yet silently red'd a CLASS of AST/source-scan tests. A cheap
byte-integrity + completeness gate catches that class deterministically; do not rely on the test
suite alone for a "pure mechanical move."
