---
phase: 87
plan: 07
type: execute
wave: 3
depends_on: [87-03, 87-04, 87-05, 87-06]
files_modified:
  - .planning/phase87_storage_allowlist.yaml
  - tests/test_no_raw_storage_access.py
autonomous: true
requirements:
  - FOUND-02
  - FOUND-03
  - FOUND-04
tags:
  - phase87
  - lint
  - allowlist
  - acceptance-gate
must_haves:
  truths:
    - "tests/test_no_raw_storage_access.py::test_no_raw_storage_access_outside_allowlist PASSES (zero unallowlisted violations in web/)"
    - "tests/test_no_raw_storage_access.py::test_allowlist_counts_exact PASSES (every allowlist pattern matches its expected_count exactly — H1)"
    - "Allowlist contains exactly the bootstrap sites that MUST remain raw for downstream phases (auth_state.py + main.py OAuth + supabase_client.py:111 + export_state.py)"
    - "All 6 lint scanner tests in tests/test_no_raw_storage_access.py pass"
    - "All Plan 02 + Plan 03-06 tests still pass"
  artifacts:
    - path: ".planning/phase87_storage_allowlist.yaml"
      provides: "Finalized allowlist with H1 schema (source + expected_count per pattern); no spurious entries; each justified"
      contains: "expected_count"
    - path: "tests/test_no_raw_storage_access.py"
      provides: "Lint scanner test fully GREEN on production code; all 6 tests pass"
      contains: "test_no_raw_storage_access_outside_allowlist"
  key_links:
    - from: "tests/test_no_raw_storage_access.py"
      to: ".planning/phase87_storage_allowlist.yaml"
      via: "yaml.safe_load + substring pattern matching + H1 expected_count enforcement"
      pattern: "yaml\\.safe_load"
    - from: "Lint scanner"
      to: "ALL web/**.py files except web/safe_storage.py"
      via: "AST traversal + alias resolution + parent tracking (B2)"
      pattern: "WEB_DIR\\.rglob\\('\\*\\.py'\\)"
---

<objective>
Finalize the Phase 87 lint scanner: verify that after Plans 03-06 migrations, ALL raw `app.storage.user` access in `web/` either (a) is gone, or (b) appears in the allowlist with the correct expected_count. Adjust the allowlist if any genuine bootstrap site was missed; adjust the lint scanner if any false-positive emerges; then the lint scanner becomes the permanent CI guard against regression (FOUND-04).

**REVISION (H1, M1, M4 from 87-REVIEWS.md):**
- **H1:** The new `test_allowlist_counts_exact` test (introduced in Plan 01) must PASS after Plans 03-06 migrate. If counts drift (because the actual codebase has a different number of OAuth lines than the allowlist expects, etc.), this plan adjusts `expected_count` per pattern with full justification.
- **M1:** All acceptance gates use `pytest tests/test_no_raw_storage_access.py` invocations, not grep.
- **M4:** Windows-safe Python one-liners throughout.

Purpose: This is the acceptance gate. After Plans 03-06, the lint scanner tests `test_no_raw_storage_access_outside_allowlist` AND `test_allowlist_counts_exact` (which have been failing since Plan 01) finally go GREEN. If they don't, this plan investigates and either: (1) migrates the missed site, (2) adjusts the allowlist with justification, or (3) refines the lint scanner to skip a legitimate false-positive (rare; allowlist is preferred).

Output: Lint scanner GREEN; allowlist contains only justified bootstrap sites; lint test is wired into CI via `pytest tests/` (no additional CI config needed).
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-RESEARCH.md
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-REVIEWS.md
@.planning/phase87_storage_allowlist.yaml
@tests/test_no_raw_storage_access.py
@tests/test_safe_storage.py
@tests/test_session_uuid.py

<interfaces>
<!-- Existing allowlist (from Plan 01) covers 4 entries: -->
- web/auth_state.py (8 patterns covering 8 of 9 historical sites)
- web/main.py (3 OAuth callback patterns; expected_count=1 each)
- web/supabase_client.py (1 pattern for line 111 captured-handle)
- web/export_state.py (1 pattern for line 48 _TEST_BACKEND fallthrough)

<!-- Lint scanner tests (6 total): -->
1. test_allowlist_well_formed — H1 schema check (source + expected_count required)
2. test_lint_rejects_synthetic_violation — B2 correctness against synthetic input
3. test_lint_handles_aliased_imports — alias resolution (nicegui_app, _app)
4. test_lint_does_not_double_report_nested_nodes — B2 parent-tracking regression guard
5. test_allowlist_counts_exact — H1 expected_count enforcement
6. test_no_raw_storage_access_outside_allowlist — the big gate

<!-- Expected post-Plan-06 state: -->
- ~13 allowlisted raw accesses total: 8 (auth_state) + 3 (main OAuth) + 1 (supabase_client:111) + 1 (export_state:48)
- 0 unallowlisted raw accesses
- expected_count per pattern matches actual AST node count (H1)
</interfaces>
</context>

<tasks>

<task type="auto">
  <name>Task 1: Run lint scanner against migrated codebase; diagnose any failures; adjust allowlist or migrate missed sites</name>
  <read_first>
    - tests/test_no_raw_storage_access.py (FULL FILE — Plan 01 wrote it with B2/H1 fixes; you may need to refine the scanner if it produces false positives)
    - .planning/phase87_storage_allowlist.yaml (current allowlist — may need adjustment if missed sites surface or expected_count drifts)
    - The SUMMARY files from Plans 03-06 to confirm what was migrated
  </read_first>
  <files>tests/test_no_raw_storage_access.py, .planning/phase87_storage_allowlist.yaml</files>
  <action>
**Step 1: Run the lint scanner (Windows-safe).**

```
python -m pytest tests/test_no_raw_storage_access.py -v
```

Expected outcome categories:
- **A. All 6 tests pass:** skip directly to Task 2 (no adjustments needed).
- **B. test_no_raw_storage_access_outside_allowlist FAILS with N violations:** read the violations carefully; investigate each.
- **C. test_allowlist_counts_exact FAILS:** an allowlisted pattern's actual count differs from expected_count — adjust the allowlist OR migrate the extra site(s).
- **D. test_allowlist_well_formed FAILS:** allowlist YAML has structural issues; fix the YAML.
- **E. Other standalone tests FAIL:** scanner logic broken — DEBUG (should be impossible if Plans 01-06 didn't tamper with the scanner).

**Step 2: If outcome B — diagnose each unallowlisted violation.**

For each violation in the failure output, the format is `path:line: source-segment`. For each violation:

**Decision tree:**

Q1: **Is this site genuinely a bootstrap/atomic site that MUST stay raw?**
- Examples: an additional GlobalAuthState method that didn't exist when Plan 01 wrote the allowlist; a third OAuth-callback variant added since planning; a code path that pre-dates session existence
- If YES → ADD to allowlist with full justification + expected_count citing the downstream phase that will migrate it.

Q2: **Is this a site Plans 03-06 SHOULD have migrated but missed?**
- Examples: a new code path added between research (2026-05-13) and execute date; an inline access inside a callback that Plans 03-06 didn't enumerate
- If YES → migrate it now (extending Plan 03-06's pattern; minimal diff; document as a Plan 07 fix in the SUMMARY)

Q3: **Is this a lint scanner false positive?**
- Examples: a string literal inside a docstring that happens to contain `"app.storage.user.get("` — but the AST scanner doesn't match string literals, so this should NOT happen
- If YES → REFINE the scanner. Avoid if possible — investigate carefully.

**Step 3: If outcome C — diagnose expected_count drift.**

The test fails with messages like:
```
web/main.py: pattern "app.storage.user[GlobalAuthState.USER_KEY]" expected_count=1 but found 2 matching AST nodes
```

For each mismatch:

- **If actual > expected:** a new raw access was introduced (or an existing one substring-matches multiple AST nodes). Decide: migrate the extras or bump expected_count with justification.
- **If actual < expected:** the allowlisted pattern over-counts. Lower expected_count to match actual. This is benign (probably a Plan 04 migration unexpectedly cleaned up a site that the allowlist had reserved).
- **If actual == 0 and expected ≥ 1:** the allowlisted pattern doesn't match anything. Either the line was migrated (delete the pattern) or the source segment changed (refresh the `source` field).

**Step 4: Apply fixes to the allowlist YAML.**

Use the Edit tool on `.planning/phase87_storage_allowlist.yaml`. Maintain the H1 schema:

```yaml
patterns:
  - source: "exact source segment"
    expected_count: <int>
    enclosing: "<optional function/scope name>"
```

EVERY new or modified pattern requires `expected_count` AND the entry's `justification` (multi-line text). If the parent entry's existing justification doesn't cover the new pattern, EXTEND the justification text.

**Step 5: Apply fixes to production code (if outcome Q2 applies).**

Apply the standard Plan 03-06 pattern (add safe_storage import; replace `app.storage.user.X` with `safe_user_X`). Document the missed site in Plan 07's SUMMARY. Note the file in `files_modified` of THIS plan's frontmatter implicitly (we don't update frontmatter retroactively, but the SUMMARY notes it).

**Step 6: Re-run until green.**

```
python -m pytest tests/test_no_raw_storage_access.py -v
```

Repeat the diagnose-fix-rerun loop until all 6 tests pass.

**Step 7: Sanity-check the allowlist hasn't grown unjustifiably (Windows-safe).**

```
python -c "
import yaml, pathlib
data = yaml.safe_load(pathlib.Path('.planning/phase87_storage_allowlist.yaml').read_text(encoding='utf-8'))
entries = data['allowed_raw_access']
print(f'Total allowlist entries: {len(entries)}')
print('Files:', [e['file'] for e in entries])
total_patterns = sum(len(e.get('patterns', [])) for e in entries)
print(f'Total allowlisted patterns: {total_patterns}')
total_expected_count = sum(p['expected_count'] for e in entries for p in e['patterns'] if isinstance(p, dict))
print(f'Total expected raw-access nodes: {total_expected_count}')
"
```

Expected: 4-5 entries (the original 4 from Plan 01, plus at most 1 added during diagnosis). Total patterns: ~13-16. Total expected_count: ~13-16.

If the allowlist has grown to 8+ entries OR total expected_count > 25, this is a signal that something is wrong — either Plans 03-06 missed many sites (escalate to user for milestone re-scoping) or the allowlist is being abused to bypass migration work (revert and migrate instead).
  </action>
  <verify>
    <automated>python -m pytest tests/test_no_raw_storage_access.py -v</automated>
  </verify>
  <acceptance_criteria>
    - `pytest tests/test_no_raw_storage_access.py::test_allowlist_well_formed -x` exits 0
    - `pytest tests/test_no_raw_storage_access.py::test_lint_rejects_synthetic_violation -x` exits 0
    - `pytest tests/test_no_raw_storage_access.py::test_lint_handles_aliased_imports -x` exits 0
    - `pytest tests/test_no_raw_storage_access.py::test_lint_does_not_double_report_nested_nodes -x` exits 0
    - **`pytest tests/test_no_raw_storage_access.py::test_allowlist_counts_exact -x` exits 0** (H1 enforcement passing)
    - **`pytest tests/test_no_raw_storage_access.py::test_no_raw_storage_access_outside_allowlist -x` exits 0** (THE BIG GATE — zero unallowlisted violations)
    - Allowlist entry count: 4-6 (Python check: `python -c "import yaml, pathlib; data = yaml.safe_load(pathlib.Path('.planning/phase87_storage_allowlist.yaml').read_text(encoding='utf-8')); n = len(data['allowed_raw_access']); assert 4 <= n <= 6, f'allowlist has {n} entries — investigate'; print(f'OK: {n} entries')"` prints `OK: <N> entries`)
    - Every allowlist entry has non-empty `justification` (enforced by test_allowlist_well_formed)
    - Every allowlist pattern is a dict with `source` + `expected_count` (H1 schema enforced by test_allowlist_well_formed)
    - All Plans 03-06 migrations preserved (no rollback): verify by re-running per-file scans against the 12 migrated production files — each should still report 0 violations (or for main.py + supabase_client.py, all violations allowlisted)
  </acceptance_criteria>
  <done>All 6 lint scanner tests pass; allowlist finalized with H1 schema; production code has zero unallowlisted raw accesses.</done>
</task>

<task type="auto">
  <name>Task 2: Run full test suite to verify zero regressions</name>
  <read_first>
    - No new file reads; this task is a verification gate
  </read_first>
  <files>(read-only verification — no file edits)</files>
  <action>
**Step 1: Run the full pytest suite (Windows-safe).**

```
python -m pytest tests/ -x --tb=short
```

Expected: ALL tests pass. The v7.11.1 baseline was ~1862 tests; Plans 01 + 02 added ~16 tests (10 in test_session_uuid.py + 6 in test_no_raw_storage_access.py); total expected ~1878 tests.

If any test fails:

**Categorize the failure:**
- **A. test_safe_storage.py failure:** FOUND-05 invariant broken — investigate; Plan 02 added new functions without modifying existing ones, so existing tests should pass. If a test_safe_storage.py test fails, it's a regression that was missed.
- **B. test_session_uuid.py failure:** FOUND-01 broken — investigate Plan 02's implementation.
- **C. test_no_raw_storage_access.py failure:** Task 1 above didn't fully address.
- **D. test_browse_state.py / test_search_state.py failure:** B3 fix in Plan 05/06 incomplete — re-check monkeypatch targets.
- **E. Other test failure:** Plans 03-06 broke a semantic that an existing test was guarding. Read the test; understand what it asserts; verify migration preserved that semantic. Common culprits: M2 short-circuit, M3 wrapper collapse, missing key migration.

For category E, the most likely culprits are tests that mock `app.storage.user` in a way that depends on raw access patterns. After migration, the test may need to mock `web.safe_storage.app.storage.user` instead. Plans 05/06 already addressed this for browse_state/search_state. If a different test surfaces the same issue, apply the same fix pattern.

**Step 2: If failures exist, fix or escalate.**

- If the fix is local to a single test file (e.g., update mock target from `'web.pages.X.app'` to `'web.safe_storage.app'`), apply the fix. Document in SUMMARY.
- If the fix requires reverting part of a Plan 03-06 migration, that's a sign that Plan 03-06 was over-aggressive — investigate, possibly add the site to the allowlist with justification.

**Step 3: Run ruff + check_docs on the full project.**

```
ruff check .
python scripts/check_docs.py
```

Both should exit 0.

**Step 4: Phase 87 final gate.**

```
# All 21 Phase 87 tests (6 safe_storage + 10 session_uuid + 6 no_raw_storage_access — but note: Plan 01 added 6, Plan 02 extended session_uuid to 10)
python -m pytest tests/test_safe_storage.py tests/test_session_uuid.py tests/test_no_raw_storage_access.py -v
```

Expected: 22 tests total (6 + 10 + 6). All pass.

**Step 5: Sanity-check counts of allowlisted raw accesses (Windows-safe).**

```
python -c "
import sys, pathlib
sys.path.insert(0, '.')
from tests.test_no_raw_storage_access import _scan_file, _load_allowlist, _find_app_aliases
import ast
allowed = {e['file']: e for e in _load_allowlist().get('allowed_raw_access', [])}
totals = {}
for f in pathlib.Path('web').rglob('*.py'):
    if f.name == 'safe_storage.py':
        continue
    src = f.read_text(encoding='utf-8')
    try:
        v = _scan_file(f, src)
    except SyntaxError:
        continue
    rel = f.relative_to(pathlib.Path('.')).as_posix()
    if v:
        totals[rel] = len(v)
for rel, count in sorted(totals.items()):
    is_allowed = rel in allowed
    print(f'{rel}: {count} raw access{\" (ALLOWLISTED)\" if is_allowed else \" (NEEDS MIGRATION)\"}')
print(f'\\nTotal files with raw access: {len(totals)}')
print(f'Sum of raw accesses: {sum(totals.values())}')
print(f'Allowlist files: {sorted(allowed.keys())}')
"
```

Expected output should show only the 4 allowlisted files (with raw counts: auth_state.py=9, main.py=3, supabase_client.py=1, export_state.py=1 — total = 14). All 4 should be marked `(ALLOWLISTED)`. The scanner test ensures zero `(NEEDS MIGRATION)` entries.
  </action>
  <verify>
    <automated>python -m pytest tests/ -x --tb=line</automated>
  </verify>
  <acceptance_criteria>
    - `python -m pytest tests/ -x` exits 0 (full suite green)
    - `ruff check .` exits 0 (no lint regressions)
    - `python scripts/check_docs.py` exits 0 (docs health)
    - `python -m pytest tests/test_safe_storage.py tests/test_session_uuid.py tests/test_no_raw_storage_access.py -v` exits 0 (all 22 Phase 87 tests pass: 6 + 10 + 6)
    - `tests/test_safe_storage.py` file SHA-256 unchanged from Plan 01 baseline (FOUND-05 invariant): `python -c "import subprocess; r = subprocess.run(['git', 'diff', '--stat', 'tests/test_safe_storage.py'], capture_output=True, text=True); assert not r.stdout.strip(), r.stdout; print('OK')"` prints `OK`
    - `tests/test_session_uuid.py` byte-stable from end of Plan 02 (Plan 07 should NOT modify this file): same git-diff check pattern
    - All 5 Phase 87 ROADMAP Success Criteria satisfied:
      - **SC1:** 100 sessions never share UUID — verified by `pytest tests/test_session_uuid.py::test_session_uuid_unique_across_100_sessions`
      - **SC2:** Static scan of `web/` returns only allowlisted entries — verified by `pytest tests/test_no_raw_storage_access.py::test_no_raw_storage_access_outside_allowlist`
      - **SC3:** Allowlist file has per-entry justification AND H1 expected_count — verified by `pytest tests/test_no_raw_storage_access.py::test_allowlist_well_formed`
      - **SC4:** Lint rejects synthetic violation; passes production code — verified by `pytest tests/test_no_raw_storage_access.py::test_lint_rejects_synthetic_violation` and `::test_no_raw_storage_access_outside_allowlist`
      - **SC5:** All 6 existing safe_storage tests pass unchanged — verified by `pytest tests/test_safe_storage.py` + git diff check
  </acceptance_criteria>
  <done>Full test suite green; ruff + check_docs green; all 5 Phase 87 success criteria satisfied via automated test commands.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| CI pipeline -> lint scanner | The lint scanner runs in `pytest tests/` job on Ubuntu + Windows matrix; integration with CI requires zero config changes |
| Allowlist -> future code reviews | Every allowlist addition requires explicit justification AND expected_count in code review; both enforced at test time |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-87-04 | Tampering | Allowlist drift — future contributor adds an entry without justification | mitigate | `test_allowlist_well_formed` asserts every entry has non-empty `justification` AND every pattern has expected_count (H1). Test runs in CI. Code review is the second layer. |
| T-87-04b | Tampering | Allowlist over-expansion — future contributor adds a site to skip migration work | mitigate | `test_allowlist_counts_exact` (H1) enforces that no NEW raw access can substring-match an existing allowlisted pattern. Adding such a site requires bumping expected_count, which is visible in the YAML diff and triggers human review. |
| T-87-05 | Information disclosure | Lint scanner missing an alias variant | mitigate | `test_lint_handles_aliased_imports` covers the 3 known aliases (`app`, `nicegui_app`, `_app`); any new alias introduced in future code would need an explicit `from nicegui import app as X` import, which the AST scanner detects. |
| -- | Denial of Service | Lint scanner slowness on large web/ | accept | ~500ms scan time per R-03 timing; well within CI budget |

Block on: T-87-04 (MEDIUM) — every allowlist entry verifiably has a justification AND expected_count. Verified continuously by `test_allowlist_well_formed` + `test_allowlist_counts_exact`.
</threat_model>

<verification>
After both tasks (Windows-safe):

```
echo SC1: 100-session uniqueness
python -m pytest tests/test_session_uuid.py::test_session_uuid_unique_across_100_sessions -x

echo SC2 + SC4: Lint scanner against production code
python -m pytest tests/test_no_raw_storage_access.py::test_no_raw_storage_access_outside_allowlist -x
python -m pytest tests/test_no_raw_storage_access.py::test_allowlist_counts_exact -x

echo SC3: Every allowlist entry well-formed
python -m pytest tests/test_no_raw_storage_access.py::test_allowlist_well_formed -x

echo SC4 (scanner correctness): Synthetic rejection + alias resolution + parent tracking
python -m pytest tests/test_no_raw_storage_access.py::test_lint_rejects_synthetic_violation tests/test_no_raw_storage_access.py::test_lint_handles_aliased_imports tests/test_no_raw_storage_access.py::test_lint_does_not_double_report_nested_nodes -x

echo SC5: 6 existing safe_storage tests unchanged
python -m pytest tests/test_safe_storage.py -x
python -c "import subprocess; r = subprocess.run(['git', 'diff', '--stat', 'tests/test_safe_storage.py'], capture_output=True, text=True); assert not r.stdout.strip(); print('byte-unchanged')"

echo Regression: full suite
python -m pytest tests/ -x --tb=short

echo Lint clean
ruff check .

echo Docs healthy
python scripts/check_docs.py

echo Allowlist sanity
python -c "
import yaml, pathlib
data = yaml.safe_load(pathlib.Path('.planning/phase87_storage_allowlist.yaml').read_text(encoding='utf-8'))
entries = data['allowed_raw_access']
print(f'Allowlist: {len(entries)} files')
for e in entries:
    patterns = e.get('patterns', [])
    counts = sum(p['expected_count'] for p in patterns if isinstance(p, dict))
    print(f\"  {e['file']}: {len(patterns)} patterns, sum expected_count={counts}\")
"
```

Expected: all checks pass; full suite ~1878 tests passed; lint clean; check_docs green; allowlist has 4-6 entries with ~13-16 total expected_count nodes.
</verification>

<success_criteria>
1. All 6 tests in `tests/test_no_raw_storage_access.py` PASS (FOUND-04): well_formed, synthetic, aliased, no_double_report, counts_exact, no_raw_storage_access
2. All 10 tests in `tests/test_session_uuid.py` PASS (FOUND-01 + M5 + B1) — preserved from Plan 02
3. All 6 tests in `tests/test_safe_storage.py` PASS, file byte-identical to baseline (FOUND-05)
4. Allowlist YAML has 4-6 entries; every entry justified; every pattern has expected_count; every justification cites a downstream phase or bootstrap rationale (FOUND-03)
5. `test_no_raw_storage_access_outside_allowlist` PASSES (zero unallowlisted raw access in `web/`) — THE BIG GATE
6. `test_allowlist_counts_exact` PASSES (H1 enforcement: no silent over-allowlisting)
7. Full pytest suite passes (~1878 tests including 22 new Phase 87 tests)
8. `ruff check .` and `python scripts/check_docs.py` both exit 0
9. All 5 ROADMAP Phase 87 Success Criteria verifiable via the test commands above
</success_criteria>

<output>
After completion, create `.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-07-SUMMARY.md` summarizing:
- All 6 lint scanner tests passing
- Final allowlist composition (file count, pattern count, expected_count sum, downstream-phase citations)
- Total Phase 87 sites migrated (132 from Plans 03-06; allowlist preserves ~13-16 bootstrap sites)
- Full test suite count (passed/skipped)
- ruff + check_docs results
- Any sites that required migration in Plan 07 (Plans 03-06 missed) — should be 0 if planning was accurate
- Any allowlist entries added beyond Plan 01's 4 (should be 0-2; document each with justification)
- The 5 ROADMAP success criteria with their test commands as evidence
</output>
