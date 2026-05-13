---
phase: 87
reviewers: [gemini, codex]
reviewed_at: 2026-05-13T03:29:41Z
plans_reviewed:
  - 87-01-VALIDATION-FOUNDATION-PLAN.md
  - 87-02-SESSION-UUID-HELPERS-PLAN.md
  - 87-03-LEAF-FILE-MIGRATIONS-PLAN.md
  - 87-04-MAIN-AND-ALIAS-MIGRATIONS-PLAN.md
  - 87-05-BROWSE-CLUSTER-MIGRATIONS-PLAN.md
  - 87-06-SEARCH-CLUSTER-MIGRATIONS-PLAN.md
  - 87-07-LINT-FINALIZATION-PLAN.md
  - 87-08-ACCEPTANCE-AND-DOCS-PLAN.md
skipped: [claude]
skipped_reason:
  claude: "Running inside Claude Code (CLAUDE_CODE_ENTRYPOINT set) — skip self for independence"
unavailable: [coderabbit, opencode, qwen, cursor]
---

# Cross-AI Plan Review — Phase 87

## Gemini Review

This review evaluates the 8 implementation plans for **Phase 87: Foundations -- Session UUID and Safe Storage Chokepoint**.

### Summary
The plans are exceptionally well-structured and technically sound. They provide a robust foundation for the v7.12 Multitenant Architecture refactor by standardizing how per-user state is accessed. The strategy for generating and validating the `_session_uuid` is cryptographically sound, and the AST-based lint scanner is a superior choice over regex for enforcing the new "zero-raw-storage" invariant. The systematic migration of 132 sites is appropriately phased to manage complexity and verification.

### Strengths
- **AST-based Linting**: Using `ast.parse` and `ast.walk` to resolve aliases (`app`, `_app`, `nicegui_app`) ensures that migrations are exhaustive and future-proof against different import styles.
- **Lazy-Mint Strategy**: Generating the UUID on first call within `get_session_uuid()` is the most efficient and reliable approach given NiceGUI's middleware ordering (which guarantees storage existence before page handlers run).
- **Defensive UUID Validation**: The inclusion of `int(uid, 16)` validation in Plan 02 is an excellent security control against storage-poisoning attacks.
- **Codex Landmine Mitigation**: Plans 03 and 06 explicitly address the "deferred callback" fragility (MEDIUM-2) by using safe wrappers to prevent `AssertionError` from crashing the asyncio event loop.
- **Pattern-based Allowlist**: Using substring matches against AST source segments instead of line numbers makes the allowlist resilient to the significant code movement expected in this phase.
- **Strong Concurrency Testing**: The mock-based 100-session uniqueness test (SC1) is high-signal and accurately simulates NiceGUI's dict-per-session storage model.

### Concerns
- **Refactoring Friction (LOW)**: Migrating 80 sites in Plan 06 (`parallels.py`, `search.py`, `search_state.py`) is a massive diff. While the plans suggest descending line-number order to minimize drift, the executor will need to be extremely careful with multi-line statements.
- **Dependency Assumption (LOW)**: The assumption that `PyYAML` is transitive via NiceGUI is likely correct, but Plan 01's verification step is a necessary guard to prevent a "Wave 0" failure in CI.

### Suggestions
- **Plan 04 Deletion Sequence**: In `web/main.py`, perform the 14 migrations *before* deleting the two local helpers. Deleting functions mid-way through a long list of line-numbered edits is a common source of off-by-one errors for LLM executors.
- **Search Snippet Highlighting**: In Plan 06, ensure that the snippet highlighting logic (which often uses `*` markers) is preserved exactly when converting to `safe_user_get`. The plan appears to respect this, but a manual verification in the "Quick View" during the smoke check would be wise.

### Risk Assessment: LOW

---

## Codex Review

### Summary

The plan set is strong in intent and mostly complete in migration scope, but I would not approve it as-is. It likely delivers the broad safe-storage migration, and the raw-access inventory matches the expected clusters well. The main blockers are: `_session_uuid` is implemented but never wired into "first request to any page," the proposed AST scanner has a chain-order bug and over-broad allowlist patterns, and Plans 05/06 will break existing `browse_state` / `search_state` tests unless those tests patch `web.safe_storage.app`.

### Strengths

- Good phase boundary discipline: auth atomicity, `get_user_client()`, and `export_state._TEST_BACKEND` are deferred to Phases 88/90/91 rather than half-fixed.
- Migration coverage is broadly correct: the plan captures the main raw-access clusters, including `nicegui_app`, `_app`, `parallels.py:3520`, `text_editor.py`, and `catalog_browse.py`.
- Lazy minting is the right primitive versus `on_connect`; it avoids websocket timing issues.
- Pattern-based allowlist is better than line-number allowlist for refactor drift.
- Good TDD sequencing: failing tests first, helper implementation second, migration, then lint finalization.
- Human smoke check is valuable because NiceGUI session storage behavior is hard to fully mock.

### Concerns

- **HIGH: FOUND-01 is not actually satisfied.** Plan 02 adds `get_session_uuid()` / `ensure_session_uuid()`, but no plan calls either on every page request. Plan 08's smoke check expects `_session_uuid` to exist after visiting the app, but the current plans do not wire it into common page bootstrap. Add a call to `ensure_session_uuid()` in a shared page/layout entry point or every `ui.page` handler.

- **HIGH: The AST scanner as written is broken.** For `app.storage.user.get(...)`, the attribute chain is `['get', 'user', 'storage']`, so `chain[-2:] == ['storage', 'user']` is false. The synthetic violation test would fail.

- **HIGH: The scanner will double-report nested nodes once fixed.** Walking `ast.Attribute` as well as `ast.Call` / `ast.Subscript` means `app.storage.user[KEY]` may report both the subscript and the inner `app.storage.user` attribute. Existing allowlist patterns will not cover the inner bare segment. Add parent tracking and only check outermost nodes, or check calls/subscripts plus true bare `app.storage.user` assignments.

- **HIGH: Allowlist patterns are too broad.** `_app.storage.user` in `web/supabase_client.py` would also allow the `sign_out` raw read if Plan 04 accidentally misses it. `app.storage.user` in `web/export_state.py` has the same issue. Use exact source segments plus expected occurrence counts.

- **HIGH: Plans 05/06 likely break existing tests.** `tests/test_browse_state.py` patches `web.pages.browse_state.app`; after migration, reads/writes go through `web.safe_storage.app`. Same issue exists in `tests/test_search_state.py`. These plans must include test updates or shared fixtures that patch both modules.

- **MEDIUM: Some migration examples change behavior.** In `browse_state.restore_browse_snapshot`, the plan's example returns `(None, None)` when `browse_position` is missing, but current code can still restore `reading_desk_state`. Preserve independent restore semantics.

- **MEDIUM: Do not remove broad try/except blocks blindly.** Some existing blocks catch more than storage failures, such as snapshot compaction or malformed persisted data. Replace raw storage calls with helpers, but keep defensive wrappers around non-storage parsing/serialization where they currently provide resilience.

- **MEDIUM: Grep-based verification will count comments/docstrings.** Several files contain textual `app.storage.user` references. Use the AST lint as the authoritative check, or update comments intentionally. Simple `grep -c "app\.storage\.user"` acceptance gates will produce false failures.

- **MEDIUM: Shell commands are not Windows-safe.** The environment is PowerShell, but plans use `/tmp`, `grep | wc -l`, `tail`, `sha256sum`, `ls -la`. Use Python one-liners or PowerShell equivalents.

- **LOW: UUID validation is weaker than stated.** The implementation accepts any 32 hex chars, including uppercase and non-UUID4 values, while the threat model says `^[0-9a-f]{32}$`. Add tests for malformed, uppercase, and non-string stored values.

### Suggestions

1. Add a Plan 02 or 04 task: wire `ensure_session_uuid()` into the common request/page bootstrap path and add an integration-ish test or smoke-script assertion for real first-page minting.

2. Rewrite the AST matcher around normalized chains:
   ```python
   # app.storage.user.get -> ["storage", "user", "get"]
   # app.storage.user[...] -> ["storage", "user"]
   ```
   Then ignore child `Attribute` nodes when their parent `Call` / `Subscript` is already checked.

3. Extend the allowlist schema:
   ```yaml
   patterns:
     - source: "storage = _app.storage.user"
       expected_count: 1
       enclosing: "get_user_client"
   ```
   Also add a test that every allowlist pattern matches exactly its expected count.

4. Add `tests/test_session_uuid.py` cases for poisoned values, uppercase values, `ensure_session_uuid()` on `AssertionError`, and "first page bootstrap calls ensure."

5. Add `tests/test_browse_state.py` and `tests/test_search_state.py` to Plans 05/06 `files_modified`, or add a fixture that patches `web.safe_storage.app.storage.user` consistently.

6. Replace brittle grep gates with `pytest tests/test_no_raw_storage_access.py` plus targeted `rg` only for diagnostics.

7. Keep Plan 08 smoke check, but make it clear that automated gates must already prove FOUND-01. The smoke check should confirm, not discover, missing UUID wiring.

### Risk Assessment: HIGH

The migration scope is well understood and the architecture is sound, but three issues can block the phase: `_session_uuid` is not minted on first page request, the lint scanner would not work as written, and state-module tests will fail after moving storage access into `web.safe_storage`. Fixing those is straightforward, but until they are corrected, the plans do not reliably deliver all five ROADMAP success criteria.

---

## Consensus Summary

Gemini rates the plans **LOW risk**; Codex rates them **HIGH risk**. The divergence is informative: Gemini reviewed at the architectural level (UUID strategy, lint approach, migration sequencing) and found them sound. Codex reviewed at the implementation level (AST matcher chain order, test-fixture coupling, bootstrap wiring) and found concrete defects. **Both views are correct at their level — the architecture is right, but several implementation details would fail execution.** Codex's findings should be treated as blockers before /gsd-execute-phase.

### Agreed Strengths

- **AST-based lint** (vs regex) is the right primitive — both reviewers endorse it.
- **Lazy-mint UUID** is the right choice vs `on_connect` middleware.
- **Pattern-based allowlist** beats line-number allowlist for refactor drift.
- **Phase boundary discipline** — auth atomicity, `get_user_client`, `export_state._TEST_BACKEND` correctly deferred to Phases 88/90/91.
- **TDD sequencing** — failing tests in Wave 0 before helper implementation.

### Agreed Concerns

- **Migration diff size** is large (~132 occurrences). Gemini calls it LOW friction; Codex calls out specific behavior-preservation traps inside the diff (e.g. `browse_state.restore_browse_snapshot` semantic change). **Treat as MEDIUM** with the executor explicitly preserving snapshot-recovery semantics line-by-line.

### Divergent Views

- **FOUND-01 wiring.** Codex (HIGH): plans never call `ensure_session_uuid()` from a page bootstrap path, so SC1's "minted on first request" cannot be satisfied automatically — the smoke check in Plan 08 would catch it but should not be the discovery mechanism. Gemini didn't flag this. **Codex is correct; plans must add a bootstrap wiring task.**
- **AST scanner correctness.** Codex (HIGH): the chain-order check `chain[-2:] == ['storage', 'user']` is buggy because `ast.Attribute` walks from outer to inner (the chain order Codex shows is reversed from what the plan assumes). Gemini didn't notice. **Codex is correct; verify the AST traversal logic against actual `ast.Attribute` semantics.**
- **Plans 05/06 test fixture breakage.** Codex (HIGH): existing `tests/test_browse_state.py` and `tests/test_search_state.py` patch `web.pages.{browse,search}_state.app` directly. After migration the storage call site moves to `web.safe_storage.app`, so the monkeypatch becomes ineffective and tests will fail. Gemini didn't catch this. **Codex is correct; Plans 05/06 must update those test files OR introduce a shared fixture.**
- **Risk rating** — Gemini LOW vs Codex HIGH. Codex's three HIGH findings are concrete and testable; if they are correct (high-likelihood), the plan would fail execution. Use **HIGH** as the operative rating until those three issues are resolved.

### Recommended Actions (for /gsd-plan-phase 87 --reviews)

The replanner should address these issues, in priority order:

**BLOCKERS** (must fix before /gsd-execute-phase):
1. **Wire `ensure_session_uuid()` into page bootstrap.** Add a task to either Plan 02 or a new plan that calls it from a shared page entry point (e.g., a middleware hook, a `@ui.page` decorator wrapper, or every `def page_*()` function). Add an automated test for "first page render mints UUID" — do not rely on Plan 08 smoke check alone.
2. **Fix the AST scanner attribute-chain order in Plan 01.** Verify the normalized chain against actual Python AST: `app.storage.user.get(K)` should yield chain `['app', 'storage', 'user', 'get']` (or reversed — be explicit). Walk ONLY outermost `Call` and `Subscript` nodes; suppress inner `Attribute` to avoid double-counting.
3. **Update Plans 05 and 06** to also modify `tests/test_browse_state.py` and `tests/test_search_state.py`, OR add a shared `conftest.py` fixture that patches `web.safe_storage.app.storage.user` consistently for all state-module tests.

**HIGH** (fix in same revision):
4. **Tighten allowlist patterns** to include `expected_count` and ideally `enclosing` function/scope, so a substring match cannot accidentally legalize a new raw-access elsewhere in the same file. Add a `test_allowlist_counts_exact` test.

**MEDIUM** (fix in same revision):
5. **Replace grep-based acceptance gates** with `pytest tests/test_no_raw_storage_access.py` invocations. The AST scanner is authoritative; raw grep over comments/docstrings produces false failures.
6. **Preserve `restore_browse_snapshot` semantics** in Plan 05 — read `browse_position` and `reading_desk_state` independently; don't make one's absence short-circuit the other.
7. **Keep defensive try/except blocks** that catch non-storage failures (JSON parsing, malformed persisted data) in Plans 05/06. Only collapse them when the wrapper provably handles ONLY storage prune.
8. **Windows-safe verification commands.** Plans use `/tmp`, `grep | wc -l`, `tail`, `sha256sum`, `ls -la`. Replace with Python one-liners or PowerShell equivalents (project runs Windows + Ubuntu CI matrix).
9. **Strict UUID pattern test.** Add tests for uppercase, malformed, non-string, and `AssertionError` cases. Use `^[0-9a-f]{32}$` regex on read.

**LOW** (fix opportunistically):
10. **Plan 04 ordering** — migrate the 14 raw sites BEFORE deleting the two local helpers; reduces off-by-one risk.
11. **PyYAML availability** — Plan 01's Wave 0 check stays in; consider pinning to `requirements.txt` if it's not present.

### Verdict

**Do NOT proceed to `/gsd-execute-phase 87` as-is.** Run `/gsd-plan-phase 87 --reviews` to incorporate the 3 BLOCKER fixes (FOUND-01 wiring, AST chain order, state-test fixtures) and the 5 HIGH/MEDIUM refinements. Then re-verify and execute.

The architecture is sound — these are surgical, well-localized fixes within the existing 8-plan structure. No phase split or major redesign is warranted.

---

## Revision Response

**Generated by:** `/gsd-plan-phase 87 --reviews` (revision pass, 2026-05-13)
**Plans rewritten:** all 8 (87-01 through 87-08) — full rewrites, no diffs left for executor to apply
**Wave structure:** preserved (Wave 0 / 1 / 2 / 3 / 4) — no blocker required wave changes; the B1 bootstrap-wiring fix was small enough to fit in Plan 02 without splitting

### Finding-to-Plan Mapping

| Finding | Severity | Title | Addressed in Plan(s) | Specific Task / Location |
|---------|----------|-------|----------------------|--------------------------|
| **B1** | BLOCKER | FOUND-01 bootstrap wiring missing | **Plan 02** | Task 2 adds `ensure_session_uuid()` call to `web/main.py:create_layout()`; Task 3 adds `test_create_layout_mints_session_uuid` to `tests/test_session_uuid.py` for automated coverage. Plan 04 + Plan 08 explicitly preserve / confirm the wiring. |
| **B2** | BLOCKER | AST scanner chain-order bug + double-reporting | **Plan 01** | Task 3 rewrites the scanner with explicit `chain[-2:] == ['user', 'storage']` check (verbatim AST chain shape documented in `<ast_chain_facts>` block in the plan's `<context>`) AND uses `_StorageAccessVisitor(ast.NodeVisitor)` with `_seen_inner_ids` parent tracking. New test `test_lint_does_not_double_report_nested_nodes` is the regression guard. |
| **B3** | BLOCKER | tests/test_browse_state.py + tests/test_search_state.py break after migration | **Plan 05** (Task 3) + **Plan 06** (Task 4) | Plan 05 updates `tests/test_browse_state.py` monkeypatches from `web.pages.browse_state.app` to `web.safe_storage.app`. Plan 06 updates `tests/test_search_state.py` to the dual-patch idiom (already used by `test_stale_version_discards_snapshot`). Both test files added to respective plans' `files_modified`. Conftest fixture option NOT chosen — direct in-test patches preferred because the test files are small and self-contained. |
| **H1** | HIGH | Allowlist patterns too broad | **Plan 01** (Task 2 schema) + **Plan 07** (enforcement) | Plan 01 changes the allowlist YAML schema: each pattern is now `{source: str, expected_count: int, enclosing?: str}`. Plan 01 also adds `test_allowlist_counts_exact` to enforce exact match counts. Plan 07 verifies the test passes after migrations land; if counts drift, Plan 07's diagnose-fix loop adjusts `expected_count` with justification. |
| **M1** | MEDIUM | Grep-based gates unreliable | **Plans 03, 04, 05, 06, 07, 08** | Every `<acceptance_criteria>` block now uses `pytest tests/test_no_raw_storage_access.py` invocations OR Python one-liners that import `_scan_file` from the test module. Grep-counts only appear in human-readable diagnostics inside `<action>` text, never as gates. |
| **M2** | MEDIUM | restore_browse_snapshot semantic preservation | **Plan 05** (Task 2) | Plan 05 includes explicit BEFORE/AFTER code in `<action>` showing the CORRECT migration where `browse_position` and `reading_desk_state` are read via SEPARATE `safe_user_get` calls. Acceptance criterion specifically verifies the function body contains both calls and that the existing `test_clear_snapshot_keep_position_preserves_position` still passes. Same applies in Plan 06 for `restore_search_snapshot`. |
| **M3** | MEDIUM | Preserve defensive try/except for non-storage failures | **Plans 03, 05, 06** | Each plan includes a `<defensive_wrapper_preservation>` block in `<context>` that defines Class A (collapse: AssertionError-only) vs Class B (preserve: catches json/ValueError/TypeError/KeyError). Tasks instruct executor to classify each wrapper before editing. SUMMARY files list per-file audit results. |
| **M4** | MEDIUM | Windows-safe verification commands | **All 8 plans** | All `<acceptance_criteria>` and `<action>` shell snippets audited and replaced with Python one-liners or PowerShell-friendly commands. No `/tmp`, `grep \| wc -l`, `tail`, `sha256sum`, `ls -la`, `cat \| json.tool \| grep`. Where temp paths are needed, `$env:TEMP` is referenced (though plans now use stdout capture instead). |
| **M5** | MEDIUM | UUID validation weaker than stated | **Plan 01** + **Plan 02** | Plan 01 adds 4 new tests to `tests/test_session_uuid.py`: rejects_uppercase_hex, rejects_non_string, rejects_malformed_length, ensure_session_uuid_returns_false_on_assertion. Plan 02 implements `_SESSION_UUID_RE = re.compile(r"^[0-9a-f]{32}$")` and a private `_is_valid_uuid(value)` helper called by both public functions; uppercase/non-string/malformed all rejected with regeneration. |
| **L1** | LOW | Plan 04 migrate-before-delete ordering | **Plan 04** (Task 1) | Task 1 explicitly enumerates 6 steps: import then migrate 12 inline sites in DESCENDING line-number order then replace helper call sites then DELETE helpers (LAST). Rationale documented at the top of the task. Same descending-order principle applied within other plans for within-file edits. |
| **L2** | LOW | PyYAML availability check | **Plan 01** (Task 1, Step 0) | Pre-flight check `python -c "import yaml; print(yaml.__version__)"` is the first action of Task 1 with explicit fallback (add PyYAML to requirements.txt if import fails). Version 6.0.3 verified at planning time. |

### Wave Structure (preserved)

| Wave | Plans | Notes |
|------|-------|-------|
| 0 | 87-01 | Validation foundation (tests + allowlist with H1 schema + B2 corrected AST scanner) |
| 1 | 87-02, 87-03 | Helper implementation (Plan 02 also adds B1 bootstrap wiring), leaf-file migrations |
| 2 | 87-04, 87-05, 87-06 | Main+aliases, browse cluster (B3 test fix), search cluster (B3 test fix) |
| 3 | 87-07 | Lint finalization (H1 count enforcement gate) |
| 4 | 87-08 | Docs + smoke check (B1 clarification: confirms, not discovers) |

### Items Declined / Not Applicable

None. All 11 review findings (B1, B2, B3, H1, M1-M5, L1-L2) have concrete remediation in the revised plans.

### New Tests Added

| Test name | File | Plan | Purpose |
|-----------|------|------|---------|
| `test_session_uuid_rejects_uppercase_hex` | tests/test_session_uuid.py | 01 | M5 |
| `test_session_uuid_rejects_non_string` | tests/test_session_uuid.py | 01 | M5 |
| `test_session_uuid_rejects_malformed_length` | tests/test_session_uuid.py | 01 | M5 |
| `test_ensure_session_uuid_returns_false_on_assertion` | tests/test_session_uuid.py | 01 | M5 |
| `test_create_layout_mints_session_uuid` | tests/test_session_uuid.py | 02 | B1 automated coverage |
| `test_lint_does_not_double_report_nested_nodes` | tests/test_no_raw_storage_access.py | 01 | B2 parent-tracking regression guard |
| `test_allowlist_counts_exact` | tests/test_no_raw_storage_access.py | 01 | H1 enforcement |

Pre-revision test count: 16 (6 safe_storage + 5 session_uuid + 4 no_raw_storage_access — original plans) -> Post-revision: 22 (6 + 10 + 6).

### Existing Tests Updated

| Test file | Plan | Reason |
|-----------|------|--------|
| tests/test_browse_state.py | 05 | B3 monkeypatch target change |
| tests/test_search_state.py | 06 | B3 monkeypatch target change |

### Files Added to files_modified

| File | Plan | Reason |
|------|------|--------|
| web/main.py | 02 | B1 bootstrap wiring (in addition to Plan 04's main.py migrations) |
| tests/test_session_uuid.py | 02 | B1 added test (in addition to Plan 01's test creation) |
| tests/test_browse_state.py | 05 | B3 monkeypatch update |
| tests/test_search_state.py | 06 | B3 monkeypatch update |

### Ready for Execution

After this revision, the quality gate from `/gsd-plan-phase 87 --reviews` is met:

- [x] All 8 PLAN.md files rewritten in full
- [x] B1, B2, B3 BLOCKERS addressed with concrete tasks + automated tests
- [x] H1 reflected in Plan 01 schema + Plan 07 enforcement test
- [x] M1-M5 applied across all 8 plans
- [x] L1, L2 applied
- [x] 87-REVIEWS.md has this Revision Response section
- [x] All FOUND-01..FOUND-05 still covered
- [x] Wave structure preserved
- [x] Every task has `<read_first>` and `<acceptance_criteria>`
- [x] Every `<action>` contains concrete values (AST chain shapes, regex patterns, line numbers)

Next step: `/gsd-execute-phase 87` against the revised plans.

---

## Revision Response - Iteration 2 (Plan-Checker Surgical Fixes)

The plan-checker re-ran against the iteration-1 revisions and surfaced 1 BLOCKER (B1-followup) + 3 warnings (W1, W2, W3) + 1 confirmed-acceptable note (W4). All fixed via minimal edits - no plan rewrites, no wave reassignments.

### B1-followup - Plan 01 H1 allowlist patterns substring-mismatch + count bug

**Root cause:** `ast.get_source_segment(source, node)` records ONLY the chain expression for bare `Attribute` and `Subscript` nodes, not the enclosing statement. Patterns like `"storage = _app.storage.user"` and `"return app.storage.user"` would never substring-match the recorded segments `"_app.storage.user"` and `"app.storage.user"` respectively. Additionally, `app.storage.user[cls.PROFILE_KEY]` appears at 2 lines (97, 117) in `web/auth_state.py`, not 1.

**File: `87-01-VALIDATION-FOUNDATION-PLAN.md`** - 3 surgical YAML edits:

| Edit | Pattern Before -> After | Count Before -> After | Justification |
|------|------------------------|----------------------|---------------|
| (a) PROFILE_KEY assignment in auth_state.py | (unchanged: `"app.storage.user[cls.PROFILE_KEY]"`) | 1 -> **2** | Verified by `grep -c` in web/auth_state.py: lines 97 (set_auth) + 117 (update_profile_cache). Justification rewrote with full 9-site audit table. |
| (b) supabase_client.py captured-handle | `"storage = _app.storage.user"` -> `"_app.storage.user"` | (unchanged: 1) | AST records bare `Attribute` segment, not the assignment LHS. Enclosing `get_user_client` retained for scoping. |
| (c) export_state.py production fallthrough | `"return app.storage.user"` -> `"app.storage.user"` | (unchanged: 1) | AST records bare `Attribute` segment, not the `return` keyword. Enclosing changed to `_backend` (the actual function containing line 48). |

**Also added new `<allowlist_matcher>` block to Plan 01 `<context>` section** documenting:
1. What `ast.get_source_segment` actually records for each node type (Call, Subscript, bare Attribute).
2. The substring-containment contract `S in seg`.
3. The authoring rule for allowlist patterns ("write `source:` as a substring of what get_source_segment will record, NOT as a substring of the human source line").

**Verified counts in `web/auth_state.py`** (revision-2 audit, via grep per pattern):

| Pattern | Count | Lines |
|---------|-------|-------|
| `app.storage.user.get(cls.USER_KEY)` | 1 | 42 |
| `app.storage.user.get(cls.PROFILE_KEY)` | 1 | 50 |
| `app.storage.user[cls.USER_KEY]` | 1 | 95 |
| `app.storage.user[cls.PROFILE_KEY]` | **2** | 97, 117 |
| `app.storage.user['auth_session']` | 1 | 176 |
| `app.storage.user.pop(cls.USER_KEY, None)` | 1 | 122 |
| `app.storage.user.pop(cls.PROFILE_KEY, None)` | 1 | 123 |
| `app.storage.user.pop('auth_session', None)` | 1 | 124 |
| **TOTAL** | **9** | (8 distinct patterns covering 9 access sites) |

After these fixes, the existing `test_allowlist_counts_exact` will pass first-run for the 3 corrected entries; Plan 07's diagnose-fix loop is now a no-op for B1-followup.

### W1 - Plan 02 truths claim overstated bootstrap wiring coverage

**Root cause:** `web/main.py` has 19 `@ui.page` decorators but only 16 call `create_layout()` (not 17 as initially flagged by the checker). The 3 non-callers verified by `grep -n "create_layout()"` against the actual file: `/privacy-extension` (line 1245), `/reset-hints` (line 1279), `/auth/callback` (line 1436).

**File: `87-02-SESSION-UUID-HELPERS-PLAN.md`** - 3 surgical edits:

1. **`<truths>` line** rewritten to: "16 of 19 routes ... the 3 remaining ... rely on lazy-mint via `get_session_uuid()` on first storage read, which is the documented design per R-01 in 87-RESEARCH.md".
2. **`must_haves.artifacts[web/main.py].provides`** extended with the same lazy-vs-eager clause so the executor does not try to "fix" the gap accidentally.
3. **`<interfaces>` inline comment** (the create_layout natural-hook-point paragraph) rewritten with the actual 16 line numbers + explicit enumeration of the 3 non-calling routes + rationale (lazy-mint is the design, not a gap).

Chose Option A (preferred - minimal). Option B (eager-mint on all 19 routes) was not chosen because R-01 already documents lazy-mint as the design and Phase 87 does not have a hard eager-mint requirement.

### W2 - Plan 06 wrong test count (7 vs actual 8)

**Root cause:** `tests/test_search_state.py` actually has 8 `def test_*` functions, not 7. Verified via `grep -n "^def test_" tests/test_search_state.py`:

```
10:  test_persist_and_restore_round_trip
45:  test_clear_snapshot_wipes_all_keys
75:  test_missing_stamp_adopts_legacy_payload
101: test_clear_search_filters_preserves_live_search_state
147: test_stale_version_discards_snapshot
172: test_restore_prefers_tab_snapshot_over_legacy_user_results
201: test_restore_falls_back_to_compact_user_snapshot_when_tab_missing
222: test_search_history_compacts_embedded_results
```

**File: `87-06-SEARCH-CLUSTER-MIGRATIONS-PLAN.md`** - 13 surgical "7 -> 8" replacements across `truths`, Task 3 read_first, Task 4 read_first / Step 1 / Step 2 / Step 3 / Step 4 / acceptance_criteria / done / success_criteria / output summary.

Also adjusted the expected `patch('web.pages.search_state.app')` count from "7 occurrences" to "8 occurrences (one per test, with test_stale_version_discards_snapshot ALSO having a patch('web.safe_storage.app') so total `patch(` calls = 9)" - verified with `grep -c` against the actual test file.

**NOTE:** Line 365 in Plan 06 (`all 7 keys preserved`) refers to search.py's 7 storage keys (`search_query`, `search_preset`, `search_gap`, `search_text_position`, `search_max_changes`, `search_mode`, `show_translations`), NOT the test count. That "7" is legitimate and unchanged.

### W3 - RESEARCH.md Open Questions section lacks RESOLVED markers

**File: `87-RESEARCH.md`** - surface-level annotation only:

1. Section header `## Open Questions` -> `## Open Questions (RESOLVED)`.
2. Each of the 5 `Recommendation:` lines prefixed with `RESOLVED - ` to make resolution status machine-readable (Dimension 11 of the plan-checker).

Substantive content unchanged.

### W4 - Plan 06 scope borderline (acceptable per checker)

No action taken. Checker explicitly noted "Acceptable as-is given file-cluster coherence and DESCENDING-line-order discipline; no split needed." Confirmed reading this and chose not to split.

### Files Changed (Iteration 2)

| File | Change |
|------|--------|
| `87-01-VALIDATION-FOUNDATION-PLAN.md` | 3 allowlist YAML edits (PROFILE_KEY count, supabase_client.py pattern, export_state.py pattern) + new `<allowlist_matcher>` block + auth_state.py audit justification |
| `87-02-SESSION-UUID-HELPERS-PLAN.md` | 3 edits to truths / must_haves / interfaces clarifying 16-of-19 + lazy-mint rationale |
| `87-06-SEARCH-CLUSTER-MIGRATIONS-PLAN.md` | 13 surgical "7 -> 8" replacements for test count |
| `87-RESEARCH.md` | 1 section header + 5 Recommendation-line markers added |
| `87-REVIEWS.md` | This appendix |

### Plans NOT Touched (Iteration 2)

- 87-03 (leaf-file migrations) - unchanged
- 87-04 (main.py + supabase_client.py migrations) - unchanged
- 87-05 (browse cluster) - unchanged
- 87-07 (lint finalization) - unchanged
- 87-08 (docs + smoke) - unchanged

No wave reassignments. No new requirements. FOUND-01..FOUND-05 coverage unchanged.
