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
    - "Allowlist contains exactly the bootstrap sites that MUST remain raw for downstream phases (auth_state.py + main.py OAuth + supabase_client.py:111 + export_state.py)"
    - "All 4 lint scanner tests in tests/test_no_raw_storage_access.py pass"
    - "All Plan 02 + Plan 03-06 tests still pass"
  artifacts:
    - path: ".planning/phase87_storage_allowlist.yaml"
      provides: "Finalized allowlist with verified entries; no spurious entries; each justified"
      contains: "allowed_raw_access"
    - path: "tests/test_no_raw_storage_access.py"
      provides: "Lint scanner test fully GREEN on production code"
      contains: "test_no_raw_storage_access_outside_allowlist"
  key_links:
    - from: "tests/test_no_raw_storage_access.py"
      to: ".planning/phase87_storage_allowlist.yaml"
      via: "yaml.safe_load + substring pattern matching"
      pattern: "yaml\\.safe_load"
    - from: "Lint scanner"
      to: "ALL web/**.py files except web/safe_storage.py"
      via: "AST traversal + alias resolution"
      pattern: "WEB_DIR\\.rglob\\('\\*\\.py'\\)"
---

<objective>
Finalize the Phase 87 lint scanner: verify that after Plans 03-06 migrations, ALL raw `app.storage.user` access in `web/` either (a) is gone, or (b) appears in the allowlist. Adjust the allowlist if any genuine bootstrap site was missed; adjust the lint scanner if any false-positive emerges; then the lint scanner becomes the permanent CI guard against regression (FOUND-04).

Purpose: This is the acceptance gate. After Plans 03-06, the lint scanner test `test_no_raw_storage_access_outside_allowlist` (which has been failing since Plan 01) finally goes GREEN. If it doesn't, this plan investigates and either: (1) migrates the missed site, (2) adjusts the allowlist with justification, or (3) refines the lint scanner to skip a legitimate false-positive (rare; allowlist is preferred).

Output: Lint scanner GREEN; allowlist contains only justified bootstrap sites; the lint test is wired into CI via `pytest tests/` (no additional CI config needed — pytest already runs it).
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-RESEARCH.md
@.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-PATTERNS.md
@.planning/phase87_storage_allowlist.yaml
@tests/test_no_raw_storage_access.py
@tests/test_safe_storage.py
@tests/test_session_uuid.py

<interfaces>
<!-- Existing allowlist (from Plan 01) covers 4 entries: -->
- web/auth_state.py (9 sites — Phase 91 migrates)
- web/main.py (3 OAuth callback sites — Phase 91 migrates)
- web/supabase_client.py (line 111 captured-handle — Phase 90 deletes)
- web/export_state.py (line 48 _TEST_BACKEND fallthrough — Phase 88 deletes)

<!-- Lint scanner contract: -->
```python
def test_no_raw_storage_access_outside_allowlist():
    """For every .py file in web/, find all <alias>.storage.user.* AST nodes.
    For each, check if the source segment matches an allowlist pattern.
    Fail with full violation list if any unallowlisted site remains."""
```

<!-- Expected post-migration state: -->
- ~13 allowlisted raw accesses total: 9 (auth_state) + 3 (main OAuth) + 1 (supabase_client:111) + 1 (export_state:48 — this matches once via the bare `app.storage.user` access; the function definitions for _backend may also include it in docstrings which AST won't catch)
- 0 unallowlisted raw accesses
</interfaces>
</context>

<tasks>

<task type="auto">
  <name>Task 1: Run lint scanner against migrated codebase; diagnose any failures</name>
  <read_first>
    - tests/test_no_raw_storage_access.py (FULL FILE — Plan 01 wrote it; you may need to refine the scanner if it produces false positives)
    - .planning/phase87_storage_allowlist.yaml (current allowlist — may need adjustment if missed sites surface)
    - The summary files from Plans 03-06 to confirm what was migrated
  </read_first>
  <files>tests/test_no_raw_storage_access.py, .planning/phase87_storage_allowlist.yaml</files>
  <action>
**Step 1: Run the lint scanner.**

```bash
pytest tests/test_no_raw_storage_access.py -v 2>&1 | tee /tmp/lint_output.txt
```

Expected outcome categories:
- **A. All 4 tests pass**: skip directly to Task 2 (no adjustments needed).
- **B. test_no_raw_storage_access_outside_allowlist FAILS with N violations**: read the violations carefully; investigate each.
- **C. test_allowlist_well_formed FAILS**: allowlist YAML has structural issues; fix the YAML.
- **D. test_lint_rejects_synthetic_violation FAILS**: scanner logic broken; debug the AST visitor.

**Step 2: If outcome B — diagnose each violation.**

For each violation in the failure output, the path:line:source-segment will be listed. For each:

**Decision tree:**
- **Q1: Is this site genuinely a bootstrap/atomic site that MUST stay raw?**
  - Examples: an additional GlobalAuthState method, a third OAuth-callback variant, a code path that pre-dates session existence
  - If YES → ADD to allowlist with full justification citing the downstream phase that will migrate it (or "no migration planned; intrinsic bootstrap site")
- **Q2: Is this a site Plans 03-06 SHOULD have migrated but missed?**
  - Examples: a new code path added between research date (2026-05-13) and execute date, an inline access inside a callback Plans 03-06 didn't enumerate
  - If YES → migrate it now (extending Plan 03-06's pattern; minimal diff; document as a Plan 07 fix in the SUMMARY)
- **Q3: Is this a lint scanner false positive?**
  - Examples: a string literal inside a docstring that happens to contain "app.storage.user.get(" — but AST scanner shouldn't match string literals, so this should NOT happen with the current scanner
  - If YES → REFINE the scanner. But this is unlikely given the AST-based implementation; investigate carefully.

**Step 3: Apply fixes.**

For each violation, apply the appropriate fix:

- **Allowlist additions**: Edit `.planning/phase87_storage_allowlist.yaml`. Add a new entry OR extend an existing entry's `patterns` list. EVERY new pattern requires a `justification` (or extension to existing justification text). Format:

```yaml
  - file: web/example.py
    patterns:
      - "exact source segment from the violation report"
    justification: |
      [Detailed explanation of why this site cannot be migrated in Phase 87,
       which phase will migrate it, and why the bootstrap timing requires
       raw access here. At minimum 2 sentences.]
```

- **Migration fixes**: Apply the standard Plan 03-06 pattern (add import; replace `app.storage.user.X` with `safe_user_X`). Document the missed site in Plan 07's SUMMARY.

- **Scanner refinement** (LAST RESORT — avoid if possible): Edit `tests/test_no_raw_storage_access.py`. Common refinements:
  - Add a string-literal skip (`isinstance(node.parent, ast.Constant)` — unlikely needed because the scanner already restricts to Call/Subscript/Attribute)
  - Add a comment-line skip (already implicit because AST does not parse comments)

**Step 4: Re-run until green.**

```bash
pytest tests/test_no_raw_storage_access.py -v
```

Repeat the diagnose-fix-rerun loop until all 4 tests pass.

**Step 5: Verify the allowlist hasn't grown unjustifiably.**

```bash
python -c "
import yaml
data = yaml.safe_load(open('.planning/phase87_storage_allowlist.yaml'))
entries = data['allowed_raw_access']
print(f'Total allowlist entries: {len(entries)}')
print('Files:', [e['file'] for e in entries])
total_patterns = sum(len(e.get('patterns', [])) for e in entries)
print(f'Total allowlisted patterns: {total_patterns}')
"
```

Expected: 4-5 entries (the original 4 from Plan 01, plus at most 1-2 added during diagnosis). Total patterns: ~13-16.

If the allowlist has grown to 8+ entries, this is a signal that something is wrong — either Plans 03-06 missed many sites (escalate to user for milestone re-scoping) or the allowlist is being abused to bypass migration work (revert and migrate instead).
  </action>
  <verify>
    <automated>pytest tests/test_no_raw_storage_access.py -v</automated>
  </verify>
  <acceptance_criteria>
    - `pytest tests/test_no_raw_storage_access.py::test_allowlist_well_formed -x` exits 0
    - `pytest tests/test_no_raw_storage_access.py::test_lint_rejects_synthetic_violation -x` exits 0
    - `pytest tests/test_no_raw_storage_access.py::test_lint_handles_aliased_imports -x` exits 0
    - `pytest tests/test_no_raw_storage_access.py::test_no_raw_storage_access_outside_allowlist -x` exits 0 (THE KEY GATE)
    - Allowlist entries: between 4 and 6 (the original 4 from Plan 01; up to 2 added during diagnosis with full justification)
    - Total allowlisted patterns: between 13 and 20
    - Every allowlist entry has a non-empty `justification` (verified by `test_allowlist_well_formed`)
    - Every new allowlist entry (if any) cites a specific downstream phase that will migrate it OR explains why it is an intrinsic bootstrap site
    - All migrations from Plans 03-06 remain in place: `for f in web/components/text_editor.py web/components/translation_report.py web/pages/home.py web/pages/settings.py web/pages/search_results.py web/pages/browse.py web/pages/browse_state.py web/pages/catalog_browse.py web/pages/parallels.py web/pages/search.py web/pages/search_state.py web/api.py; do grep -c "from web.safe_storage import" "$f"; done` returns 1 for each (all 12 migrated files import the chokepoint)
  </acceptance_criteria>
  <done>All 4 lint scanner tests pass; allowlist finalized; production code has zero unallowlisted raw accesses.</done>
</task>

<task type="auto">
  <name>Task 2: Run full test suite to verify zero regressions</name>
  <read_first>
    - No new file reads; this task is a verification gate
  </read_first>
  <files>(read-only verification — no file edits)</files>
  <action>
**Step 1: Run the full pytest suite.**

```bash
pytest tests/ -x --tb=short 2>&1 | tee /tmp/full_suite_output.txt
```

Expected outcome: ALL tests pass. The v7.11.1 baseline was 1862 tests; Plan 01 added 9 tests (5 in test_session_uuid.py + 4 in test_no_raw_storage_access.py); total expected ~1871 tests.

If any test fails:

**Categorize the failure:**
- **A. test_safe_storage.py failure**: FOUND-05 invariant broken — investigate; Plan 02 added new functions without modifying existing ones, so existing tests should pass. If a test_safe_storage.py test fails, it's a regression from Plan 02 that was missed.
- **B. test_session_uuid.py failure**: FOUND-01 broken — investigate Plan 02's implementation.
- **C. test_no_raw_storage_access.py failure**: Task 1 above didn't fully address.
- **D. Other test failure**: Plans 03-06 broke a semantic that an existing test was guarding. Read the test; understand what it asserts; verify Plan 03-06 migration preserved that semantic.

For category D, the most likely culprits are tests that mock `app.storage.user` in a way that depends on raw access patterns. After migration, the test may need to mock `web.safe_storage.app.storage.user` instead. If the existing test mocks `app.storage.user` directly and Plans 03-06 didn't break the production code's behavior, the test should still pass — investigate any failure carefully.

**Step 2: If failures exist, fix or escalate.**

- If the fix is local to a single test file (e.g., update mock target from `'nicegui.app'` to `'web.safe_storage.app'`), apply the fix. Document in SUMMARY.
- If the fix requires reverting part of a Plan 03-06 migration, that's a sign that Plan 03-06 was over-aggressive — investigate, possibly add the site to the allowlist with justification.

**Step 3: Run ruff + check_docs on the full project.**

```bash
ruff check .
python scripts/check_docs.py
```

Both should exit 0.

**Step 4: Run the Phase 87 final gate.**

```bash
# All Phase 87 invariants
pytest tests/test_safe_storage.py tests/test_session_uuid.py tests/test_no_raw_storage_access.py -v

# Confirm exactly the expected test count for these 3 files
pytest tests/test_safe_storage.py tests/test_session_uuid.py tests/test_no_raw_storage_access.py --collect-only -q | tail -1
# Expected: ~15 tests (6 + 5 + 4)
```

**Step 5: Sanity-check the count of allowlisted raw accesses.**

```bash
# Production raw-access count (should equal sum of allowlist pattern counts approximately)
grep -rc "app\.storage\.user" web/ | grep -v ":0" | grep -v "safe_storage.py"
```

Expected output (approximate):
- `web/auth_state.py: 9`  (allowlisted)
- `web/main.py: 3`  (OAuth callback, allowlisted)
- `web/supabase_client.py: 1`  (line 111, allowlisted)
- `web/export_state.py: 1`  (line 48, allowlisted) — note this MAY be hidden if grep matches docstrings; verify
- (and `web/safe_storage.py: 6` — exempt per scanner's own skip rule)

Total raw count: ~14. All allowlisted. Lint scanner test PASS.
  </action>
  <verify>
    <automated>pytest tests/ -x --tb=short 2>&1 | tail -3 | grep -E "passed|failed"</automated>
  </verify>
  <acceptance_criteria>
    - `pytest tests/ -x` exits 0 (full suite green)
    - `ruff check .` exits 0 (no lint regressions)
    - `python scripts/check_docs.py` exits 0 (docs health)
    - `pytest tests/test_safe_storage.py tests/test_session_uuid.py tests/test_no_raw_storage_access.py -v` exits 0 (all 15 Phase 87 tests pass: 6 + 5 + 4)
    - `tests/test_safe_storage.py` file SHA unchanged from Plan 01 baseline: `git diff --stat tests/test_safe_storage.py` returns empty (FOUND-05 invariant)
    - `tests/test_session_uuid.py` file SHA unchanged from Plan 01 baseline: `git diff --stat tests/test_session_uuid.py` returns empty (Plan 01 contract preserved)
    - All Phase 87 success criteria from `<planning_context>` satisfied:
      - SC1: 100 sessions never share UUID (verified by test_session_uuid_unique_across_100_sessions in test_session_uuid.py)
      - SC2: Static grep of `web/` for raw `app.storage.user.get(`, `app.storage.user.pop(`, and `app.storage.user[` returns only entries in the allowlist file (verified by lint scanner)
      - SC3: Allowlist file contains per-entry justification for every remaining raw access (verified by test_allowlist_well_formed)
      - SC4: Lint rejects synthetic violation; passes production code (verified by test_lint_rejects_synthetic_violation + test_no_raw_storage_access_outside_allowlist)
      - SC5: All 6 existing safe_storage tests pass unchanged (verified by git diff + pytest)
  </acceptance_criteria>
  <done>Full test suite green; ruff + check_docs green; all 5 Phase 87 success criteria satisfied via test commands.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| CI pipeline → lint scanner | The lint scanner runs in `pytest tests/` job on Ubuntu + Windows matrix; integration with CI requires zero config changes (per PATTERNS.md ".github/workflows/ci.yml" section) |
| Allowlist → future code reviews | Every allowlist addition requires explicit justification in code review; the `test_allowlist_well_formed` enforces this at test time |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-87-04 | Tampering | Allowlist drift — future contributor adds an entry without justification | mitigate | `test_allowlist_well_formed` asserts every entry has non-empty `justification` field. Test runs in CI. Code review process is the second layer. |
| T-87-04b | Tampering | Allowlist over-expansion — future contributor adds a site to skip migration work | mitigate | Code review process — every PR touching the allowlist YAML triggers human review per repository convention. No automated mitigation; this is procedural. |
| T-87-05 | Information disclosure | Lint scanner missing an alias variant | mitigate | `test_lint_handles_aliased_imports` covers the 3 known aliases (`app`, `nicegui_app`, `_app`); any new alias introduced in future code would need an explicit `from nicegui import app as X` import, which the AST scanner detects. |
| — | Denial of Service | Lint scanner slowness on large web/ | accept | ~500ms scan time per R-03 timing; well within CI budget |

Block on: T-87-04 (MEDIUM) — every allowlist entry verifiably has a justification. Verified continuously by `test_allowlist_well_formed`.
</threat_model>

<verification>
After both tasks:

```bash
# Phase 87 final gate — all 5 success criteria
echo "=== SC1: 100-session uniqueness ==="
pytest tests/test_session_uuid.py::test_session_uuid_unique_across_100_sessions -x

echo "=== SC2 + SC4: Lint scanner against production code ==="
pytest tests/test_no_raw_storage_access.py::test_no_raw_storage_access_outside_allowlist -x

echo "=== SC3: Every allowlist entry has justification ==="
pytest tests/test_no_raw_storage_access.py::test_allowlist_well_formed -x

echo "=== SC4 (lint scanner correctness): Synthetic rejection + alias resolution ==="
pytest tests/test_no_raw_storage_access.py::test_lint_rejects_synthetic_violation tests/test_no_raw_storage_access.py::test_lint_handles_aliased_imports -x

echo "=== SC5: 6 existing safe_storage tests unchanged ==="
pytest tests/test_safe_storage.py -x
git diff --stat tests/test_safe_storage.py  # Must be empty

echo "=== Regression: full suite ==="
pytest tests/ -x --tb=short 2>&1 | tail -3

echo "=== Lint clean ==="
ruff check .

echo "=== Docs healthy ==="
python scripts/check_docs.py

echo "=== Allowlist sanity ==="
python -c "
import yaml
data = yaml.safe_load(open('.planning/phase87_storage_allowlist.yaml'))
entries = data['allowed_raw_access']
print(f'Allowlist: {len(entries)} files')
for e in entries:
    print(f\"  {e['file']}: {len(e.get('patterns', []))} patterns\")
"
```

Expected output: all checks pass; full suite ~1871 tests passed; lint clean; check_docs green; allowlist has 4-6 entries with ~13-20 patterns total.
</verification>

<success_criteria>
1. All 4 tests in `tests/test_no_raw_storage_access.py` PASS (FOUND-04)
2. All 5 tests in `tests/test_session_uuid.py` PASS (FOUND-01) — preserved from Plan 02
3. All 6 tests in `tests/test_safe_storage.py` PASS, file byte-identical to baseline (FOUND-05)
4. Allowlist YAML has 4-6 entries; every entry justified; every justification cites a downstream phase or bootstrap rationale (FOUND-03)
5. `tests/test_no_raw_storage_access.py::test_no_raw_storage_access_outside_allowlist` PASSES (zero unallowlisted raw access in `web/`) — this is THE big gate
6. Full pytest suite passes (~1871 tests including 15 new Phase 87 tests)
7. `ruff check .` and `python scripts/check_docs.py` both exit 0
8. All 5 ROADMAP Phase 87 Success Criteria verifiable via the test commands above
</success_criteria>

<output>
After completion, create `.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-07-SUMMARY.md` summarizing:
- All 4 lint scanner tests passing
- Final allowlist composition (file count, pattern count, downstream-phase citations)
- Total Phase 87 sites migrated (132 from Plans 03-06; allowlist preserves ~13-16 bootstrap sites)
- Full test suite count (passed/skipped)
- ruff + check_docs results
- Any sites that required migration in Plan 07 (Plans 03-06 missed) — should be 0 if planning was accurate
- Any allowlist entries added beyond Plan 01's 4 (should be 0-2; document each with justification)
- The 5 ROADMAP success criteria with their test commands as evidence
</output>
